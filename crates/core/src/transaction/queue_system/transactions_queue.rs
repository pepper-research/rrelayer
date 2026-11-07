use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use super::{
    log_summary::{compact_rpc_error, summarize_rpc_error, transaction_context},
    types::{
        CompetitionResolutionResult, CompetitionType, CompetitiveTransaction, EditableTransaction,
        MoveInmempoolTransactionToMinedError, MovePendingTransactionToInmempoolError,
        SendTransactionGasPriceError, TransactionQueueSendTransactionError,
        TransactionSentWithRelayer, TransactionsQueueSetup,
    },
};
use crate::transaction::types::{TransactionNonce, TransactionValue};
use crate::{
    gas::{
        BlobGasOracleCache, BlobGasPriceResult, GasLimit, GasOracleCache, GasPrice, GasPriceResult,
        MaxFee, MaxPriorityFee, BLOB_GAS_PER_BLOB,
    },
    network::ChainId,
    postgres::PostgresClient,
    provider::EvmProvider,
    relayer::{Relayer, RelayerId},
    safe_proxy::SafeProxyManager,
    shared::common_types::EvmAddress,
    transaction::types::TransactionData,
    transaction::{
        nonce_manager::NonceManager,
        types::{Transaction, TransactionHash, TransactionId, TransactionSpeed, TransactionStatus},
    },
    yaml::GasBumpBlockConfig,
    WalletError,
};
use alloy::network::{AnyTransactionReceipt, ReceiptResponse};
use alloy::{
    consensus::{SignableTransaction, TypedTransaction},
    hex,
    transports::{RpcError, TransportErrorKind},
};
use chrono::Utc;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;

fn fixed_envelope_matches_hash(bytes: &[u8], hash: TransactionHash) -> bool {
    alloy::primitives::keccak256(bytes) == hash.into_alloy_hash()
}

fn fixed_nonce_available(
    latest: TransactionNonce,
    pending: TransactionNonce,
    signed: TransactionNonce,
) -> bool {
    latest.into_inner() <= signed.into_inner() && pending == signed
}

#[cfg(test)]
mod fixed_envelope_tests {
    use super::{fixed_envelope_matches_hash, fixed_nonce_available};
    use crate::transaction::types::{TransactionHash, TransactionNonce};

    #[test]
    fn restart_recovery_accepts_only_original_signed_bytes() {
        let original = [0x02, 0x80, 0x01, 0x00];
        let hash = TransactionHash::from_alloy_hash(&alloy::primitives::keccak256(original));
        assert!(fixed_envelope_matches_hash(&original, hash));
        assert!(!fixed_envelope_matches_hash(&[0x02, 0x80, 0x01, 0x01], hash));
    }

    #[test]
    fn consumed_nonce_blocks_rebroadcast_of_original() {
        let nonce = TransactionNonce::new(7);
        assert!(fixed_nonce_available(nonce, nonce, nonce));
        assert!(!fixed_nonce_available(TransactionNonce::new(8), nonce, nonce));
        assert!(!fixed_nonce_available(nonce, TransactionNonce::new(8), nonce));
        assert!(!fixed_nonce_available(nonce, TransactionNonce::new(6), nonce));
    }
}

pub struct TransactionsQueue {
    pending_transactions: Mutex<VecDeque<Transaction>>,
    inmempool_transactions: Mutex<VecDeque<CompetitiveTransaction>>,
    mined_transactions: Mutex<HashMap<TransactionId, Transaction>>,
    evm_provider: EvmProvider,
    relayer: Relayer,
    pub nonce_manager: NonceManager,
    gas_oracle_cache: Arc<Mutex<GasOracleCache>>,
    blob_oracle_cache: Arc<Mutex<BlobGasOracleCache>>,
    confirmations: u64,
    safe_proxy_manager: Arc<SafeProxyManager>,
    gas_bump_config: GasBumpBlockConfig,
    max_gas_price_multiplier: u64,
}

impl TransactionsQueue {
    pub async fn reload(
        &mut self,
        db: &PostgresClient,
    ) -> Result<(), crate::postgres::PostgresError> {
        use super::start::{
            repopulate_competitive_transaction_queue, repopulate_transaction_queue,
        };
        let map_error = |e: super::start::RepopulateTransactionsQueueError| {
            crate::postgres::PostgresError::Handoff(e.to_string())
        };
        *self.pending_transactions.get_mut() =
            repopulate_transaction_queue(db, &self.relayer.id, &TransactionStatus::PENDING)
                .await
                .map_err(map_error)?;
        *self.inmempool_transactions.get_mut() =
            repopulate_competitive_transaction_queue(db, &self.relayer.id)
                .await
                .map_err(map_error)?;
        *self.mined_transactions.get_mut() =
            repopulate_transaction_queue(db, &self.relayer.id, &TransactionStatus::MINED)
                .await
                .map_err(map_error)?
                .into_iter()
                .map(|tx| (tx.id, tx))
                .collect();
        self.relayer = db
            .get_relayer(&self.relayer.id)
            .await?
            .ok_or_else(|| crate::postgres::PostgresError::Handoff("Relayer removed".into()))?;
        // All durable admissions reserve their nonce, including unresolved attempts.
        let row = db.query_one("SELECT COALESCE(MAX(nonce)+1,0) AS next FROM relayer.transaction WHERE chain_id=$1 AND \"from\"=$2 AND (status <> 'FAILED' OR broadcast_attempted)", &[&self.relayer.chain_id, &self.relayer.address]).await?;
        self.nonce_manager = NonceManager::new(TransactionNonce::new(row.get::<_, i64>(0) as u64));
        Ok(())
    }

    pub fn is_isolated_limit_base(&self) -> bool {
        self.evm_provider.is_isolated_limit_base()
    }

    pub async fn prepare_fixed_signed_transaction(
        &self,
        transaction: &TypedTransaction,
    ) -> Result<(Vec<u8>, TransactionHash), WalletError> {
        let (bytes, hash) = self
            .evm_provider
            .sign_raw_transaction(&self.relayer, transaction.clone())
            .await
            .map_err(|error| WalletError::GenericSignerError(error.to_string()))?;
        Ok((bytes, hash))
    }

    async fn gas_price_for_speed(&self, speed: &TransactionSpeed) -> Option<GasPriceResult> {
        if self.is_isolated_limit_base() {
            let prices = self.evm_provider.calculate_gas_price().await.ok()?;
            return Some(match speed {
                TransactionSpeed::SUPER => prices.super_fast,
                TransactionSpeed::FAST => prices.fast,
                TransactionSpeed::MEDIUM => prices.medium,
                TransactionSpeed::SLOW => prices.slow,
            });
        }
        self.gas_oracle_cache
            .lock()
            .await
            .get_gas_price_for_speed(&self.relayer.chain_id, speed)
            .await
    }

    pub fn new(
        setup: TransactionsQueueSetup,
        gas_oracle_cache: Arc<Mutex<GasOracleCache>>,
        blob_oracle_cache: Arc<Mutex<BlobGasOracleCache>>,
    ) -> Self {
        info!(
            "Creating new TransactionsQueue for relayer: {} (name: {}) on chain: {}",
            setup.relayer.id, setup.relayer.name, setup.relayer.chain_id
        );
        let confirmations = setup.evm_provider.confirmations;
        Self {
            pending_transactions: Mutex::new(setup.pending_transactions),
            inmempool_transactions: Mutex::new(setup.inmempool_transactions),
            mined_transactions: Mutex::new(setup.mined_transactions),
            evm_provider: setup.evm_provider,
            relayer: setup.relayer,
            nonce_manager: setup.nonce_manager,
            gas_oracle_cache,
            blob_oracle_cache,
            confirmations,
            safe_proxy_manager: setup.safe_proxy_manager,
            gas_bump_config: setup.gas_bump_config,
            max_gas_price_multiplier: setup.max_gas_price_multiplier,
        }
    }

    fn blocks_to_wait_before_bump(&self, speed: &TransactionSpeed) -> u64 {
        self.gas_bump_config.blocks_to_wait_before_bump(speed)
    }

    pub fn should_bump_gas(&self, ms_between_times: u64, speed: &TransactionSpeed) -> bool {
        let time_threshold_met = ms_between_times
            > (self.evm_provider.blocks_every * self.blocks_to_wait_before_bump(speed));

        if !time_threshold_met {
            return false;
        }

        info!(
            "Gas bump time threshold met for relayer: {} - elapsed: {}ms, threshold: {}ms, speed: {:?}",
            self.relayer.name,
            ms_between_times,
            self.evm_provider.blocks_every * self.blocks_to_wait_before_bump(speed),
            speed
        );

        true
    }

    /// Checks if a transaction has reached the maximum gas price cap and shouldn't be bumped further
    pub async fn is_at_max_gas_price_cap(&self, sent_gas: &GasPriceResult) -> bool {
        // Get SUPER speed gas price for cap calculation
        let super_gas_price = self.gas_price_for_speed(&TransactionSpeed::SUPER).await;

        if let Some(super_price) = super_gas_price {
            let max_allowed_max_fee =
                super_price.max_fee.into_u128() * (self.max_gas_price_multiplier as u128);
            let max_allowed_priority_fee =
                super_price.max_priority_fee.into_u128() * (self.max_gas_price_multiplier as u128);

            let at_max_fee_cap = sent_gas.max_fee.into_u128() >= max_allowed_max_fee;
            let at_max_priority_fee_cap =
                sent_gas.max_priority_fee.into_u128() >= max_allowed_priority_fee;

            if at_max_fee_cap || at_max_priority_fee_cap {
                info!(
                    "Transaction at maximum gas price cap for relayer: {} - max_fee: {} (cap: {}), max_priority_fee: {} (cap: {})",
                    self.relayer.name,
                    sent_gas.max_fee.into_u128(),
                    max_allowed_max_fee,
                    sent_gas.max_priority_fee.into_u128(),
                    max_allowed_priority_fee
                );
                return true;
            }
        }

        false
    }

    /// Checks if a transaction has reached the maximum blob gas price cap and shouldn't be bumped further
    pub async fn is_at_max_blob_gas_price_cap(&self, sent_blob_gas: &BlobGasPriceResult) -> bool {
        let super_blob_gas_price = {
            let blob_gas_oracle = self.blob_oracle_cache.lock().await;
            blob_gas_oracle
                .get_blob_gas_price_for_speed(&self.relayer.chain_id, &TransactionSpeed::SUPER)
                .await
        };

        if let Some(super_price) = super_blob_gas_price {
            let max_allowed_blob_gas_price =
                super_price.blob_gas_price * (self.max_gas_price_multiplier as u128);

            if sent_blob_gas.blob_gas_price >= max_allowed_blob_gas_price {
                info!(
                    "Transaction at maximum blob gas price cap for relayer: {} - blob_gas_price: {} (cap: {})",
                    self.relayer.name,
                    sent_blob_gas.blob_gas_price,
                    max_allowed_blob_gas_price
                );
                return true;
            }
        }

        false
    }

    pub async fn update_pending_transaction(&mut self, transaction: Transaction) {
        if let Some(item) =
            self.pending_transactions.lock().await.iter_mut().find(|tx| tx.id == transaction.id)
        {
            *item = transaction;
        }
    }

    pub async fn add_pending_transaction(&mut self, transaction: Transaction) {
        info!(
            "Adding pending transaction {} to queue for relayer: {}",
            transaction.id, self.relayer.name
        );
        let mut transactions = self.pending_transactions.lock().await;
        transactions.push_back(transaction);
        info!(
            "Pending transactions count for relayer {}: {}",
            self.relayer.name,
            transactions.len()
        );
    }

    pub async fn get_next_pending_transaction(&self) -> Option<Transaction> {
        let transactions = self.pending_transactions.lock().await;

        transactions.front().cloned()
    }

    pub async fn get_pending_transaction_count(&self) -> usize {
        let transactions = self.pending_transactions.lock().await;
        let count = transactions.len();
        info!("Current pending transaction count for relayer {}: {}", self.relayer.name, count);
        count
    }

    pub async fn get_editable_transaction_by_id(
        &self,
        id: &TransactionId,
    ) -> Option<EditableTransaction> {
        info!("Looking for editable transaction {} for relayer: {}", id, self.relayer.name);
        let transactions = self.pending_transactions.lock().await;

        let pending = transactions.iter().find(|t| t.id == *id);

        match pending {
            Some(transaction) => {
                info!(
                    "Found transaction {} in pending queue for relayer: {}",
                    id, self.relayer.name
                );
                Some(EditableTransaction::to_pending(transaction.clone()))
            }
            None => {
                let transactions = self.inmempool_transactions.lock().await;
                let result = transactions
                    .iter()
                    .find(|t| t.original.id == *id)
                    .map(|comp_tx| EditableTransaction::to_inmempool(comp_tx.original.clone()));

                if result.is_some() {
                    info!(
                        "Found transaction {} in inmempool queue for relayer: {}",
                        id, self.relayer.name
                    );
                } else {
                    info!(
                        "Transaction {} not found in any queue for relayer: {}",
                        id, self.relayer.name
                    );
                }
                result
            }
        }
    }

    pub async fn move_pending_to_inmempool(
        &mut self,
        transaction_sent: &TransactionSentWithRelayer,
    ) -> Result<(), MovePendingTransactionToInmempoolError> {
        info!(
            "Moving transaction {} from pending to inmempool for relayer: {} with hash: {}",
            transaction_sent.id, self.relayer.name, transaction_sent.hash
        );

        let mut transactions = self.pending_transactions.lock().await;
        let item = transactions.front().cloned();

        if let Some(transaction) = item {
            if transaction.id == transaction_sent.id {
                let mut inmempool_transactions = self.inmempool_transactions.lock().await;
                let updated_transaction = Transaction {
                    known_transaction_hash: Some(transaction_sent.hash),
                    status: TransactionStatus::INMEMPOOL,
                    sent_with_max_fee_per_gas: Some(transaction_sent.sent_with_gas.max_fee),
                    sent_with_max_priority_fee_per_gas: Some(
                        transaction_sent.sent_with_gas.max_priority_fee,
                    ),
                    sent_with_gas: Some(transaction_sent.sent_with_gas.clone()),
                    sent_with_blob_gas: transaction_sent.sent_with_blob_gas.clone(),
                    sent_at: Some(Utc::now()),
                    ..transaction
                };
                inmempool_transactions.push_back(CompetitiveTransaction::new(updated_transaction));

                transactions.pop_front();
                info!("Successfully moved transaction {} to inmempool for relayer: {}. Pending: {}, Inmempool: {}",
                    transaction_sent.id, self.relayer.name, transactions.len(), inmempool_transactions.len());
                Ok(())
            } else {
                info!("Transaction ID mismatch when moving to inmempool for relayer: {}. Expected: {}, Found: {}",
                    self.relayer.name, transaction_sent.id, transaction.id);
                Err(MovePendingTransactionToInmempoolError::TransactionIdDoesNotMatch(
                    self.relayer.id,
                    self.relayer.address,
                    Box::new(transaction_sent.clone()),
                    Box::new(transaction.clone()),
                ))
            }
        } else {
            info!("No pending transaction found to move to inmempool for relayer: {} (transaction: {})",
                self.relayer.name, transaction_sent.id);
            Err(MovePendingTransactionToInmempoolError::TransactionNotFound(
                self.relayer.id,
                self.relayer.address,
                Box::new(transaction_sent.clone()),
            ))
        }
    }

    pub async fn move_next_pending_to_failed(&mut self) {
        let mut transactions = self.pending_transactions.lock().await;
        if let Some(tx) = transactions.front() {
            info!(
                "Moving pending transaction {} to failed for relayer: {}",
                tx.id, self.relayer.name
            );
        }
        transactions.pop_front();
        info!(
            "Remaining pending transactions for relayer {}: {}",
            self.relayer.name,
            transactions.len()
        );
    }

    /// For a head that was just given the next free nonce.
    pub async fn move_next_pending_to_back(&mut self) {
        move_head_to_back(&mut *self.pending_transactions.lock().await);
    }

    pub async fn remove_pending_transaction_by_id(
        &mut self,
        transaction_id: &TransactionId,
    ) -> bool {
        let mut transactions = self.pending_transactions.lock().await;
        if let Some(pos) = transactions.iter().position(|tx| tx.id == *transaction_id) {
            transactions.remove(pos);
            info!(
                "Removed pending transaction {} from relayer {}: {} remaining",
                transaction_id,
                self.relayer.name,
                transactions.len()
            );
            true
        } else {
            false
        }
    }

    pub async fn add_competitor_to_inmempool_transaction(
        &mut self,
        original_transaction_id: &TransactionId,
        competitor_transaction: Transaction,
        competition_type: CompetitionType,
    ) -> Result<(), TransactionQueueSendTransactionError> {
        let mut transactions = self.inmempool_transactions.lock().await;

        if let Some(comp_tx) = transactions.front_mut() {
            if comp_tx.original.id == *original_transaction_id {
                comp_tx.add_competitor(competitor_transaction, competition_type);
                info!(
                    "Added competitor to inmempool transaction {} for relayer {}",
                    original_transaction_id, self.relayer.name
                );
                return Ok(());
            }
        }

        Err(TransactionQueueSendTransactionError::NoTransactionInQueue)
    }

    pub async fn get_next_inmempool_transaction(&self) -> Option<Transaction> {
        let transactions = self.inmempool_transactions.lock().await;

        transactions.front().map(|comp_tx| comp_tx.get_active_transaction().clone())
    }

    pub async fn get_inmempool_transaction_count(&self) -> usize {
        let transactions = self.inmempool_transactions.lock().await;
        let count = transactions.len();
        info!("Current inmempool transaction count for relayer {}: {}", self.relayer.name, count);
        count
    }

    pub async fn update_inmempool_transaction_gas(
        &mut self,
        transaction_sent: &TransactionSentWithRelayer,
    ) {
        let mut transactions = self.inmempool_transactions.lock().await;
        if let Some(comp_tx) = transactions.front_mut() {
            if comp_tx.original.id == transaction_sent.id {
                info!(
                    "Updating inmempool transaction {} with new gas values for relayer: {}",
                    transaction_sent.id, self.relayer.name
                );
                comp_tx.original.known_transaction_hash = Some(transaction_sent.hash);
                comp_tx.original.sent_with_max_fee_per_gas =
                    Some(transaction_sent.sent_with_gas.max_fee);
                comp_tx.original.sent_with_max_priority_fee_per_gas =
                    Some(transaction_sent.sent_with_gas.max_priority_fee);
                comp_tx.original.sent_with_gas = Some(transaction_sent.sent_with_gas.clone());
                comp_tx.original.sent_with_blob_gas = transaction_sent.sent_with_blob_gas.clone();
                comp_tx.original.sent_at = Some(Utc::now());
            } else if let Some((ref mut competitor, _)) = comp_tx.competitive {
                if competitor.id == transaction_sent.id {
                    info!(
                        "Updating competitive transaction {} with new gas values for relayer: {}",
                        transaction_sent.id, self.relayer.name
                    );
                    competitor.known_transaction_hash = Some(transaction_sent.hash);
                    competitor.sent_with_max_fee_per_gas =
                        Some(transaction_sent.sent_with_gas.max_fee);
                    competitor.sent_with_max_priority_fee_per_gas =
                        Some(transaction_sent.sent_with_gas.max_priority_fee);
                    competitor.sent_with_gas = Some(transaction_sent.sent_with_gas.clone());
                    competitor.sent_with_blob_gas = transaction_sent.sent_with_blob_gas.clone();
                    competitor.sent_at = Some(Utc::now());
                }
            }
        }
    }

    pub async fn update_inmempool_transaction_noop(
        &mut self,
        transaction_id: &TransactionId,
        transaction_sent: &TransactionSentWithRelayer,
    ) {
        let mut transactions = self.inmempool_transactions.lock().await;
        if let Some(comp_tx) = transactions.front_mut() {
            if let Some(transaction) = comp_tx.get_transaction_by_id_mut(transaction_id) {
                info!(
                    "Updating inmempool transaction {} with no-op details for relayer: {}",
                    transaction_id, self.relayer.name
                );
                transaction.known_transaction_hash = Some(transaction_sent.hash);
                transaction.to = self.relay_address();
                transaction.value = TransactionValue::zero();
                transaction.data = TransactionData::empty();
                transaction.is_noop = true;
                transaction.speed = TransactionSpeed::FAST;
                transaction.sent_at = Some(Utc::now());
            }
        }
    }

    pub async fn update_inmempool_transaction_replaced(
        &mut self,
        transaction_id: &TransactionId,
        transaction_sent_with_relayer: &TransactionSentWithRelayer,
        replacement_transaction: &Transaction,
    ) {
        let mut transactions = self.inmempool_transactions.lock().await;
        if let Some(comp_tx) = transactions.front_mut() {
            if let Some(transaction) = comp_tx.get_transaction_by_id_mut(transaction_id) {
                info!(
                    "Replacing inmempool transaction {} for relayer: {}",
                    transaction_id, self.relayer.name
                );
                transaction.external_id = replacement_transaction.external_id.clone();
                transaction.to = replacement_transaction.to;
                transaction.from = replacement_transaction.from;
                transaction.value = replacement_transaction.value;
                transaction.data = replacement_transaction.data.clone();
                transaction.nonce = replacement_transaction.nonce;
                transaction.speed = replacement_transaction.speed.clone();
                transaction.gas_limit = replacement_transaction.gas_limit;
                transaction.status = replacement_transaction.status;
                transaction.blobs = replacement_transaction.blobs.clone();
                transaction.known_transaction_hash = Some(transaction_sent_with_relayer.hash);
                transaction.queued_at = replacement_transaction.queued_at;
                transaction.expires_at = replacement_transaction.expires_at;
                transaction.sent_at = replacement_transaction.sent_at;
                transaction.sent_with_gas =
                    Some(transaction_sent_with_relayer.sent_with_gas.clone());
                transaction.sent_with_blob_gas =
                    transaction_sent_with_relayer.sent_with_blob_gas.clone();
                transaction.speed = replacement_transaction.speed.clone();
                transaction.sent_with_max_fee_per_gas =
                    replacement_transaction.sent_with_max_fee_per_gas;
                transaction.sent_with_max_priority_fee_per_gas =
                    replacement_transaction.sent_with_max_priority_fee_per_gas;
                transaction.is_noop = replacement_transaction.is_noop;
                transaction.external_id = replacement_transaction.external_id.clone();
            }
        }
    }

    pub async fn move_inmempool_to_mining(
        &mut self,
        id: &TransactionId,
        receipt: &AnyTransactionReceipt,
    ) -> Result<CompetitionResolutionResult, MoveInmempoolTransactionToMinedError> {
        info!(
            "Moving transaction {} from inmempool to mined for relayer: {} with receipt status: {}",
            id,
            self.relayer.name,
            receipt.status()
        );

        let mut transactions = self.inmempool_transactions.lock().await;
        let item = transactions.front().cloned();

        if let Some(comp_tx) = item {
            if comp_tx.get_transaction_by_id(id).is_some() {
                let transaction_status: TransactionStatus;

                if receipt.status() {
                    transaction_status = TransactionStatus::MINED;
                    info!(
                        "Transaction {} successfully mined for relayer: {}",
                        id, self.relayer.name
                    );
                } else {
                    transaction_status = TransactionStatus::FAILED;
                    info!("Transaction {} failed on-chain for relayer: {}", id, self.relayer.name);
                }

                let (winner_transaction, loser_transaction, loser_status) = if comp_tx.original.id
                    == *id
                {
                    let loser_status = if let Some((_, comp_type)) = &comp_tx.competitive {
                        match comp_type {
                            CompetitionType::Cancel => TransactionStatus::DROPPED,
                            CompetitionType::Replace => TransactionStatus::DROPPED,
                        }
                    } else {
                        TransactionStatus::DROPPED
                    };

                    let winner = Transaction {
                        status: transaction_status,
                        mined_at: Some(Utc::now()),
                        cancelled_by_transaction_id: None,
                        ..comp_tx.original
                    };

                    (
                        winner,
                        comp_tx
                            .competitive
                            .map(|(tx, _)| Transaction { status: loser_status, ..tx }),
                        loser_status,
                    )
                } else if let Some((competitor, comp_type)) = comp_tx.competitive {
                    let (loser_status, loser_transaction) = match comp_type {
                        CompetitionType::Cancel => {
                            let status = if comp_tx.original.expires_at <= competitor.queued_at {
                                TransactionStatus::EXPIRED
                            } else {
                                TransactionStatus::CANCELLED
                            };
                            let cancelled_original = Transaction {
                                status,
                                cancelled_by_transaction_id: Some(competitor.id),
                                ..comp_tx.original
                            };
                            (status, cancelled_original)
                        }
                        CompetitionType::Replace => {
                            let replaced_original = Transaction {
                                status: TransactionStatus::REPLACED,
                                ..comp_tx.original
                            };
                            (TransactionStatus::REPLACED, replaced_original)
                        }
                    };

                    let winner = Transaction {
                        status: transaction_status,
                        mined_at: Some(Utc::now()),
                        ..competitor
                    };

                    (winner, Some(loser_transaction), loser_status)
                } else {
                    return Err(MoveInmempoolTransactionToMinedError::TransactionIdDoesNotMatch(
                        self.relayer.id,
                        self.relayer.address,
                        *id,
                        Box::new(comp_tx.original),
                    ));
                };

                let mut mining_transactions = self.mined_transactions.lock().await;
                mining_transactions.insert(winner_transaction.id, winner_transaction.clone());

                // Log competition resolution but don't put loser transactions in mined queue
                // since they weren't actually mined - they were cancelled/dropped
                let loser_for_result = loser_transaction.clone();
                if let Some(loser) = loser_transaction {
                    info!(
                        "Competition resolved for relayer {} - Winner: {} ({}), Loser: {} ({})",
                        self.relayer.name,
                        winner_transaction.id,
                        transaction_status,
                        loser.id,
                        loser_status
                    );
                } else {
                    info!(
                        "No competition - transaction {} mined normally for relayer: {}",
                        winner_transaction.id, self.relayer.name
                    );
                }

                transactions.pop_front();
                info!("Successfully moved transaction {} to mined status for relayer: {}. Inmempool: {}, Mined: {}",
                    id, self.relayer.name, transactions.len(), mining_transactions.len());

                Ok(CompetitionResolutionResult {
                    winner: winner_transaction,
                    winner_status: transaction_status,
                    loser: loser_for_result,
                })
            } else {
                info!(
                    "Transaction ID {} not found in competitive transaction for relayer: {}",
                    id, self.relayer.name
                );
                Err(MoveInmempoolTransactionToMinedError::TransactionIdDoesNotMatch(
                    self.relayer.id,
                    self.relayer.address,
                    *id,
                    Box::new(comp_tx.original),
                ))
            }
        } else {
            info!(
                "No inmempool transaction found to move to mined for relayer: {} (transaction: {})",
                self.relayer.name, id
            );
            Err(MoveInmempoolTransactionToMinedError::TransactionNotFound(
                self.relayer.id,
                self.relayer.address,
                *id,
            ))
        }
    }

    pub async fn get_next_mined_transaction(&self) -> Option<Transaction> {
        let transactions = self.mined_transactions.lock().await;

        if let Some((_, value)) = transactions.iter().next() {
            return Some(value.clone());
        }

        None
    }

    pub async fn is_transaction_mined(&self, id: &TransactionId) -> bool {
        let transactions = self.mined_transactions.lock().await;
        transactions.contains_key(id)
    }

    pub async fn move_mining_to_confirmed(&mut self, id: &TransactionId) {
        info!(
            "Moving transaction {} from mined to confirmed for relayer: {}",
            id, self.relayer.name
        );
        let mut transactions = self.mined_transactions.lock().await;
        transactions.remove(id);
        info!(
            "Successfully confirmed transaction {} for relayer: {}. Remaining mined: {}",
            id,
            self.relayer.name,
            transactions.len()
        );
    }

    pub fn relay_address(&self) -> EvmAddress {
        self.relayer.address
    }

    pub fn relay_id(&self) -> RelayerId {
        self.relayer.id
    }

    pub fn is_legacy_transactions(&self) -> bool {
        !self.relayer.eip_1559_enabled
    }

    pub fn set_is_legacy_transactions(&mut self, is_legacy_transactions: bool) {
        info!(
            "Setting legacy transactions to {} for relayer: {}",
            is_legacy_transactions, self.relayer.name
        );
        self.relayer.eip_1559_enabled = is_legacy_transactions;
    }

    pub fn is_paused(&self) -> bool {
        self.relayer.paused
    }

    pub fn set_is_paused(&mut self, is_paused: bool) {
        info!("Setting paused to {} for relayer: {}", is_paused, self.relayer.name);
        self.relayer.paused = is_paused;
    }

    pub fn set_name(&mut self, name: &str) {
        info!("Changing relayer name from {} to {}", self.relayer.name, name);
        self.relayer.name = name.to_string();
    }

    pub fn max_gas_price(&self) -> Option<GasPrice> {
        self.relayer.max_gas_price
    }

    pub fn set_max_gas_price(&mut self, max_gas_price: Option<GasPrice>) {
        info!("Setting max gas price to {:?} for relayer: {}", max_gas_price, self.relayer.name);
        self.relayer.max_gas_price = max_gas_price;
    }

    pub fn chain_id(&self) -> ChainId {
        self.relayer.chain_id
    }

    pub fn relayer_name(&self) -> &str {
        &self.relayer.name
    }

    /// Returns whether the wallet manager supports EIP-4844 blob transactions
    pub fn supports_blobs(&self) -> bool {
        self.evm_provider.supports_blobs()
    }

    fn within_gas_price_bounds(&self, gas: &GasPriceResult) -> bool {
        if let Some(max) = &self.max_gas_price() {
            let within_bounds = if self.relayer.eip_1559_enabled {
                max.into_u128() >= gas.max_fee.into_u128()
            } else {
                max.into_u128() >= gas.legacy_gas_price().into_u128()
            };

            if !within_bounds {
                info!(
                    "Gas price exceeds bounds for relayer: {}. Max: {}, Proposed: {}",
                    self.relayer.name,
                    max.into_u128(),
                    if self.relayer.eip_1559_enabled {
                        gas.max_fee.into_u128()
                    } else {
                        gas.legacy_gas_price().into_u128()
                    }
                );
            }

            return within_bounds;
        }

        true
    }

    pub fn blocks_every_ms(&self) -> u64 {
        self.evm_provider.blocks_every
    }

    pub fn in_confirmed_range(&self, elapsed: u64) -> bool {
        let threshold = self.blocks_every_ms() * self.confirmations;
        let in_range = elapsed > threshold;
        if in_range {
            info!(
                "Transaction in confirmed range for relayer: {} - elapsed: {}ms, threshold: {}ms",
                self.relayer.name, elapsed, threshold
            );
        }
        in_range
    }

    pub async fn compute_gas_price_for_transaction(
        &self,
        transaction_speed: &TransactionSpeed,
        sent_last_with: Option<&GasPriceResult>,
    ) -> Result<GasPriceResult, SendTransactionGasPriceError> {
        info!(
            "Computing gas price for transaction with speed {:?} for relayer: {}",
            transaction_speed, self.relayer.name
        );

        let mut gas_price = self
            .gas_price_for_speed(transaction_speed)
            .await
            .ok_or(SendTransactionGasPriceError::GasCalculationError)?;

        if let Some(sent_gas) = sent_last_with {
            info!("Adjusting gas price based on previous attempt for relayer: {}. Previous max_fee: {}, max_priority_fee: {}",
                self.relayer.name, sent_gas.max_fee.into_u128(), sent_gas.max_priority_fee.into_u128());

            // If we haven't escalated to SUPER speed yet, try to get the next speed level
            if transaction_speed != &TransactionSpeed::SUPER {
                if let Some(next_speed) = transaction_speed.next_speed() {
                    info!(
                        "Using speed escalation for relayer: {} from {:?} to {:?}",
                        self.relayer.name, transaction_speed, next_speed
                    );
                    // Get gas price for the next speed level
                    if let Some(escalated_gas_price) = self.gas_price_for_speed(&next_speed).await {
                        gas_price = escalated_gas_price;
                        info!(
                            "Escalated gas price for relayer: {} - max_fee: {}, max_priority_fee: {}",
                            self.relayer.name,
                            gas_price.max_fee.into_u128(),
                            gas_price.max_priority_fee.into_u128()
                        );
                    }
                }
            } else {
                // Already at SUPER speed, do small percentage bumps
                if gas_price.max_fee <= sent_gas.max_fee {
                    let old_max_fee = gas_price.max_fee;
                    gas_price.max_fee = sent_gas.max_fee + (sent_gas.max_fee / 20); // 5% bump
                    info!(
                        "Small bump max_fee for relayer: {} from {} to {} (5%)",
                        self.relayer.name,
                        old_max_fee.into_u128(),
                        gas_price.max_fee.into_u128()
                    );
                }

                if gas_price.max_priority_fee <= sent_gas.max_priority_fee {
                    let old_priority_fee = gas_price.max_priority_fee;
                    gas_price.max_priority_fee =
                        sent_gas.max_priority_fee + (sent_gas.max_priority_fee / 20); // 5% bump
                    info!(
                        "Small bump max_priority_fee for relayer: {} from {} to {} (5%)",
                        self.relayer.name,
                        old_priority_fee.into_u128(),
                        gas_price.max_priority_fee.into_u128()
                    );
                }
            }

            // Ensure we never send a transaction with lower or equal gas prices than previously sent
            if gas_price.max_fee <= sent_gas.max_fee {
                info!(
                    "Escalated max_fee {} is not higher than previously sent {}, using previous + small bump for relayer: {}",
                    gas_price.max_fee.into_u128(),
                    sent_gas.max_fee.into_u128(),
                    self.relayer.name
                );
                gas_price.max_fee = sent_gas.max_fee + (sent_gas.max_fee / 20); // 5% bump
            }

            if gas_price.max_priority_fee <= sent_gas.max_priority_fee {
                info!(
                    "Escalated max_priority_fee {} is not higher than previously sent {}, using previous + small bump for relayer: {}",
                    gas_price.max_priority_fee.into_u128(),
                    sent_gas.max_priority_fee.into_u128(),
                    self.relayer.name
                );
                gas_price.max_priority_fee =
                    sent_gas.max_priority_fee + (sent_gas.max_priority_fee / 20);
                // 5% bump
            }

            // Get SUPER speed gas price for cap calculation
            let super_gas_price = self.gas_price_for_speed(&TransactionSpeed::SUPER).await;

            if let Some(super_price) = super_gas_price {
                let max_allowed_max_fee =
                    super_price.max_fee.into_u128() * (self.max_gas_price_multiplier as u128);
                let max_allowed_priority_fee = super_price.max_priority_fee.into_u128()
                    * (self.max_gas_price_multiplier as u128);

                if gas_price.max_fee.into_u128() > max_allowed_max_fee {
                    info!(
                        "Gas price max_fee {} exceeds cap {} ({}x SUPER speed), capping for relayer: {}",
                        gas_price.max_fee.into_u128(),
                        max_allowed_max_fee,
                        self.max_gas_price_multiplier,
                        self.relayer.name
                    );
                    gas_price.max_fee = MaxFee::from(max_allowed_max_fee);
                }

                if gas_price.max_priority_fee.into_u128() > max_allowed_priority_fee {
                    info!(
                        "Gas price max_priority_fee {} exceeds cap {} ({}x SUPER speed), capping for relayer: {}",
                        gas_price.max_priority_fee.into_u128(),
                        max_allowed_priority_fee,
                        self.max_gas_price_multiplier,
                        self.relayer.name
                    );
                    gas_price.max_priority_fee = MaxPriorityFee::from(max_allowed_priority_fee);
                }
            }
        }

        info!(
            "Final gas price for relayer: {} - max_fee: {}, max_priority_fee: {}",
            self.relayer.name,
            gas_price.max_fee.into_u128(),
            gas_price.max_priority_fee.into_u128()
        );

        Ok(gas_price)
    }

    pub async fn compute_blob_gas_price_for_transaction(
        &self,
        transaction_speed: &TransactionSpeed,
        sent_last_with: &Option<BlobGasPriceResult>,
    ) -> Result<BlobGasPriceResult, SendTransactionGasPriceError> {
        info!(
            "Computing blob gas price for transaction with speed {:?} for relayer: {}",
            transaction_speed, self.relayer.name
        );

        let mut blob_gas_price = {
            let blob_gas_oracle = self.blob_oracle_cache.lock().await;
            blob_gas_oracle
                .get_blob_gas_price_for_speed(&self.relayer.chain_id, transaction_speed)
                .await
                .ok_or(SendTransactionGasPriceError::BlobGasCalculationError)?
        };

        if let Some(sent_blob_gas) = sent_last_with {
            info!("Adjusting blob gas price based on previous attempt for relayer: {}. Previous blob_gas_price: {}",
                self.relayer.name, sent_blob_gas.blob_gas_price);

            // If we haven't escalated to SUPER speed yet, try to get the next speed level
            if transaction_speed != &TransactionSpeed::SUPER {
                if let Some(next_speed) = transaction_speed.next_speed() {
                    info!(
                        "Using speed escalation for blob gas relayer: {} from {:?} to {:?}",
                        self.relayer.name, transaction_speed, next_speed
                    );
                    if let Some(escalated_blob_gas_price) = {
                        let blob_gas_oracle = self.blob_oracle_cache.lock().await;
                        blob_gas_oracle
                            .get_blob_gas_price_for_speed(&self.relayer.chain_id, &next_speed)
                            .await
                    } {
                        blob_gas_price = escalated_blob_gas_price;
                        info!(
                            "Escalated blob gas price for relayer: {} - blob_gas_price: {}, total_fee: {}",
                            self.relayer.name,
                            blob_gas_price.blob_gas_price,
                            blob_gas_price.total_fee_for_blob
                        );
                    }
                }
            } else {
                // Already at SUPER speed, do small percentage bumps
                if blob_gas_price.blob_gas_price < sent_blob_gas.blob_gas_price {
                    let old_blob_gas_price = blob_gas_price.blob_gas_price;
                    blob_gas_price.blob_gas_price =
                        sent_blob_gas.blob_gas_price + (sent_blob_gas.blob_gas_price / 20); // 5% bump
                    blob_gas_price.total_fee_for_blob =
                        blob_gas_price.blob_gas_price * BLOB_GAS_PER_BLOB;

                    info!(
                        "Small bump blob gas price for relayer: {} from {} to {} (5%), total_fee: {}",
                        self.relayer.name,
                        old_blob_gas_price,
                        blob_gas_price.blob_gas_price,
                        blob_gas_price.total_fee_for_blob
                    );
                }
            }

            // Ensure we never send a transaction with lower or equal blob gas price than previously sent
            if blob_gas_price.blob_gas_price <= sent_blob_gas.blob_gas_price {
                info!(
                    "Escalated blob gas price {} is not higher than previously sent {}, using previous + small bump for relayer: {}",
                    blob_gas_price.blob_gas_price,
                    sent_blob_gas.blob_gas_price,
                    self.relayer.name
                );
                blob_gas_price.blob_gas_price =
                    sent_blob_gas.blob_gas_price + (sent_blob_gas.blob_gas_price / 20); // 5% bump
                blob_gas_price.total_fee_for_blob =
                    blob_gas_price.blob_gas_price * BLOB_GAS_PER_BLOB;
            }

            // Get SUPER speed blob gas price for cap calculation
            let super_blob_gas_price = {
                let blob_gas_oracle = self.blob_oracle_cache.lock().await;
                blob_gas_oracle
                    .get_blob_gas_price_for_speed(&self.relayer.chain_id, &TransactionSpeed::SUPER)
                    .await
            };

            if let Some(super_price) = super_blob_gas_price {
                let max_allowed_blob_gas_price =
                    super_price.blob_gas_price * (self.max_gas_price_multiplier as u128);

                if blob_gas_price.blob_gas_price > max_allowed_blob_gas_price {
                    info!(
                        "Blob gas price {} exceeds cap {} ({}x SUPER speed), capping for relayer: {}",
                        blob_gas_price.blob_gas_price,
                        max_allowed_blob_gas_price,
                        self.max_gas_price_multiplier,
                        self.relayer.name
                    );
                    blob_gas_price.blob_gas_price = max_allowed_blob_gas_price;
                    blob_gas_price.total_fee_for_blob =
                        blob_gas_price.blob_gas_price * BLOB_GAS_PER_BLOB;
                }
            }
        }

        info!(
            "Final blob gas price for relayer: {} - blob_gas_price: {}, total_fee: {}",
            self.relayer.name, blob_gas_price.blob_gas_price, blob_gas_price.total_fee_for_blob
        );

        Ok(blob_gas_price)
    }

    pub async fn compute_tx_hash(
        &self,
        transaction: &TypedTransaction,
    ) -> Result<TransactionHash, WalletError> {
        info!("Computing transaction hash for relayer: {}", self.relayer.name);

        let signature = self.evm_provider.sign_transaction(&self.relayer, transaction).await?;

        let hash = match transaction {
            TypedTransaction::Legacy(tx) => {
                let signed = tx.clone().into_signed(signature);
                *signed.hash()
            }
            TypedTransaction::Eip2930(tx) => {
                let signed = tx.clone().into_signed(signature);
                *signed.hash()
            }
            TypedTransaction::Eip1559(tx) => {
                let signed = tx.clone().into_signed(signature);
                *signed.hash()
            }
            TypedTransaction::Eip4844(tx) => {
                let signed = tx.clone().into_signed(signature);
                *signed.hash()
            }
            TypedTransaction::Eip7702(tx) => {
                let signed = tx.clone().into_signed(signature);
                *signed.hash()
            }
        };

        let tx_hash = TransactionHash::from_alloy_hash(&hash);
        info!("Computed transaction hash {} for relayer: {}", tx_hash, self.relayer.name);
        Ok(tx_hash)
    }

    pub async fn estimate_gas(
        &self,
        transaction_request: &TypedTransaction,
        is_noop: bool,
        transaction: Option<&Transaction>,
    ) -> Result<GasLimit, RpcError<TransportErrorKind>> {
        let tx_context =
            transaction.map(|tx| transaction_context(tx, &self.relayer)).unwrap_or_else(|| {
                format!(
                    "relayer={} relayer_id={} noop={}",
                    self.relayer.name, self.relayer.id, is_noop
                )
            });

        let estimated_gas_result = self
            .evm_provider
            .estimate_gas(transaction_request, &self.relayer.address)
            .await
            .inspect_err(|e| {
                if let Some(decoded) = summarize_rpc_error(e) {
                    error!(
                        "rrelayer_gas_estimate_failed {} decoded_revert=\"{}\" provider_error=\"{}\"",
                        tx_context,
                        decoded,
                        compact_rpc_error(e)
                    );
                } else {
                    error!(
                        "rrelayer_gas_estimate_failed {} provider_error=\"{}\"",
                        tx_context,
                        compact_rpc_error(e)
                    );
                }
            })?;

        let block_gas_limit = self.evm_provider.block_gas_limit().await?;
        let estimated_gas = bounded_gas_estimate(estimated_gas_result, block_gas_limit, is_noop)?;
        if !is_noop {
            info!(
                "rrelayer_gas_estimate_ok {} base_gas={} final_gas={} max_buffer_pct=20 block_gas_limit={}",
                tx_context,
                estimated_gas_result.into_inner(),
                estimated_gas.into_inner(),
                block_gas_limit.into_inner()
            );
            return Ok(estimated_gas);
        }

        info!(
            "rrelayer_gas_estimate_ok {} base_gas={} final_gas={} buffer_pct=0",
            tx_context,
            estimated_gas_result.into_inner(),
            estimated_gas_result.into_inner()
        );
        Ok(estimated_gas_result)
    }

    pub async fn send_transaction(
        &mut self,
        db: &mut PostgresClient,
        transaction: &mut Transaction,
    ) -> Result<TransactionSentWithRelayer, TransactionQueueSendTransactionError> {
        if transaction.status == TransactionStatus::PENDING {
            if let Some(attempt) = db.last_attempt(&transaction.id).await? {
                return self.broadcast_attempt(db, transaction, attempt).await;
            }
        }
        if self.is_isolated_limit_base() {
            let hash = transaction.known_transaction_hash.ok_or_else(|| {
                TransactionQueueSendTransactionError::TransactionConversionError(
                    "Fixed Base transaction has no signed hash".to_string(),
                )
            })?;
            let gas = transaction.sent_with_gas.clone().ok_or_else(|| {
                TransactionQueueSendTransactionError::TransactionConversionError(
                    "Fixed Base transaction has no signed gas quote".to_string(),
                )
            })?;
            let bytes = db.get_fixed_signed_envelope(&transaction.id).await?.ok_or_else(|| {
                TransactionQueueSendTransactionError::TransactionConversionError(
                    "Fixed Base transaction has no durable signed envelope".to_string(),
                )
            })?;
            if !fixed_envelope_matches_hash(&bytes, hash) {
                return Err(TransactionQueueSendTransactionError::TransactionConversionError(
                    "Fixed Base signed envelope hash mismatch".to_string(),
                ));
            }
            let receipt = self
                .evm_provider
                .get_receipt(&hash)
                .await
                .map_err(TransactionQueueSendTransactionError::TransactionEstimateGasError)?;
            let known = if receipt.is_some() {
                true
            } else {
                self.evm_provider
                    .has_transaction_hash(&hash)
                    .await
                    .map_err(TransactionQueueSendTransactionError::TransactionEstimateGasError)?
            };
            if !known {
                let latest = self
                    .evm_provider
                    .get_latest_nonce_from_address(&self.relayer.address)
                    .await
                    .map_err(TransactionQueueSendTransactionError::TransactionEstimateGasError)?;
                let pending =
                    self.evm_provider.get_nonce_from_address(&self.relayer.address).await.map_err(
                        TransactionQueueSendTransactionError::TransactionEstimateGasError,
                    )?;
                if !fixed_nonce_available(latest, pending, transaction.nonce) {
                    return Err(TransactionQueueSendTransactionError::TransactionConversionError(
                        "Fixed Base nonce consumed while original hash is absent; holding request"
                            .to_string(),
                    ));
                }
                db.execute(
                    "UPDATE relayer.transaction SET broadcast_attempted=TRUE WHERE id=$1",
                    &[&transaction.id],
                )
                .await?;
                let returned = self
                    .evm_provider
                    .send_raw_signed(&bytes, hash)
                    .await
                    .map_err(TransactionQueueSendTransactionError::TransactionSendError)?;
                if returned != hash {
                    return Err(TransactionQueueSendTransactionError::TransactionConversionError(
                        "Fixed Base gateway returned a different transaction hash".to_string(),
                    ));
                }
            }
            let sent = TransactionSentWithRelayer {
                id: transaction.id,
                hash,
                sent_with_gas: gas,
                sent_with_blob_gas: None,
            };
            db.transaction_sent(&sent.id, &sent.hash, &sent.sent_with_gas, None, false).await?;
            return Ok(sent);
        }
        info!(
            "rrelayer_send_prepare {} speed={:?}",
            transaction_context(transaction, &self.relayer),
            transaction.speed
        );

        let gas_price = self
            .compute_gas_price_for_transaction(
                &transaction.speed,
                transaction.sent_with_gas.as_ref(),
            )
            .await?;

        if !self.within_gas_price_bounds(&gas_price) {
            info!(
                "rrelayer_send_rejected_gas_price_high {}",
                transaction_context(transaction, &self.relayer)
            );
            return Err(TransactionQueueSendTransactionError::GasPriceTooHigh);
        }

        let (final_to, final_data) = if let Some(safe_address) = self
            .safe_proxy_manager
            .get_safe_proxy_for_relayer(&self.relayer.address, transaction.chain_id)
        {
            info!(
                "Routing transaction {} through safe proxy {} for relayer: {}",
                transaction.id, safe_address, self.relayer.name
            );

            let safe_nonce =
                self.safe_proxy_manager.get_safe_nonce(&self.evm_provider, &safe_address).await?;

            let (safe_addr, safe_tx) = self
                .safe_proxy_manager
                .wrap_transaction_for_safe(
                    &self.relayer.address,
                    transaction.chain_id,
                    transaction.to,
                    transaction.value,
                    transaction.data.clone(),
                    safe_nonce,
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;

            let safe_tx_hash = self
                .safe_proxy_manager
                .get_safe_transaction_hash(&safe_addr, &safe_tx, self.evm_provider.chain_id.u64())
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;

            let hash_hex = format!("0x{}", hex::encode(safe_tx_hash));

            let signature =
                self.evm_provider.sign_text(&self.relayer, &hash_hex).await.map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(format!(
                        "Failed to sign safe transaction hash: {}",
                        e
                    ))
                })?;

            // Encode the signature into bytes according to Safe's requirements
            // Safe signature format: r + s + v where v = recovery_id + 4
            let mut sig_bytes = Vec::with_capacity(65);
            sig_bytes.extend_from_slice(&signature.r().to_be_bytes::<32>());
            sig_bytes.extend_from_slice(&signature.s().to_be_bytes::<32>());
            // Safe requires v = recovery_id + 4 for ECDSA signatures
            let recovery_id = if signature.v() { 1u8 } else { 0u8 };
            sig_bytes.push(recovery_id + 4);
            let signatures = alloy::primitives::Bytes::from(sig_bytes);

            let safe_call_data = self
                .safe_proxy_manager
                .encode_safe_transaction(&safe_tx, signatures)
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;

            (safe_addr, TransactionData::new(safe_call_data))
        } else {
            (transaction.to, transaction.data.clone())
        };

        let mut working_transaction = transaction.clone();
        working_transaction.to = final_to;
        working_transaction.data = final_data;

        // If using safe proxy, the transaction value should be 0 because the ETH transfer
        // amount is encoded in the execTransaction call data, not in the transaction value
        if self
            .safe_proxy_manager
            .get_safe_proxy_for_relayer(&self.relayer.address, transaction.chain_id)
            .is_some()
        {
            working_transaction.value = TransactionValue::zero();
        }

        // Validate again at send time: persisted estimates can predate this limit or this code.
        let block_gas_limit = self.evm_provider.block_gas_limit().await.map_err(|error| {
            TransactionQueueSendTransactionError::TransactionSendError(error.into())
        })?;
        let temp_gas_limit = block_gas_limit;

        let temp_transaction_request = if working_transaction.is_7702_transaction() {
            working_transaction
                .to_eip7702_typed_transaction_with_gas_limit(Some(&gas_price), Some(temp_gas_limit))
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?
        } else if working_transaction.is_blob_transaction() {
            let blob_gas_price = self
                .compute_blob_gas_price_for_transaction(
                    &working_transaction.speed,
                    &working_transaction.sent_with_blob_gas,
                )
                .await?;
            working_transaction
                .to_blob_typed_transaction_with_gas_limit(
                    Some(&gas_price),
                    Some(&blob_gas_price),
                    Some(temp_gas_limit),
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?
        } else if self.is_legacy_transactions() {
            working_transaction
                .to_legacy_typed_transaction_with_gas_limit(Some(&gas_price), Some(temp_gas_limit))
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?
        } else {
            working_transaction
                .to_eip1559_typed_transaction_with_gas_limit(Some(&gas_price), Some(temp_gas_limit))
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?
        };

        let mut estimated_gas_limit = gas_limit_for_send(
            transaction.gas_limit,
            block_gas_limit,
            self.estimate_gas(
                &temp_transaction_request,
                working_transaction.is_noop,
                Some(&working_transaction),
            ),
        )
        .await
        .map_err(TransactionQueueSendTransactionError::TransactionEstimateGasError)?;

        if self
            .safe_proxy_manager
            .get_safe_proxy_for_relayer(&self.relayer.address, transaction.chain_id)
            .is_some()
        {
            let original_estimate = estimated_gas_limit;

            // Safe proxy gas overhead calculation:
            // Test data shows: Failed at 25k and 37k gas, succeeded at 65k gas
            // Safe execTransaction overhead includes:
            // - Signature verification (~5-15k gas per signature)
            // - Safe contract state checks (~5-10k gas)
            // - Payment/refund logic (~5-10k gas)
            // - Event emission (~5k gas)
            // Total overhead: ~20-40k gas minimum

            // Add 45k gas overhead to base estimate to be safe and cater for the overhead
            let safe_overhead = GasLimit::new(45_000);
            estimated_gas_limit = estimated_gas_limit + safe_overhead;

            info!(
                "rrelayer_safe_proxy_gas_overhead {} original_gas={} overhead_gas={} final_gas={}",
                transaction_context(&working_transaction, &self.relayer),
                original_estimate.into_inner(),
                safe_overhead.into_inner(),
                estimated_gas_limit.into_inner()
            );
        }

        // Safe overhead is optional headroom too; it must not invalidate a valid estimate.
        estimated_gas_limit = std::cmp::min(estimated_gas_limit, block_gas_limit);
        working_transaction.gas_limit = Some(estimated_gas_limit);
        transaction.gas_limit = Some(estimated_gas_limit);

        let (transaction_request, sent_with_blob_gas): (
            TypedTransaction,
            Option<BlobGasPriceResult>,
        ) = if working_transaction.is_7702_transaction() {
            let tx_request = working_transaction
                .to_eip7702_typed_transaction_with_gas_limit(
                    Some(&gas_price),
                    Some(estimated_gas_limit),
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;
            (tx_request, None)
        } else if working_transaction.is_blob_transaction() {
            let blob_gas_price = self
                .compute_blob_gas_price_for_transaction(
                    &working_transaction.speed,
                    &working_transaction.sent_with_blob_gas,
                )
                .await?;
            let tx_request = working_transaction
                .to_blob_typed_transaction_with_gas_limit(
                    Some(&gas_price),
                    Some(&blob_gas_price),
                    Some(estimated_gas_limit),
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;
            (tx_request, Some(blob_gas_price))
        } else if self.is_legacy_transactions() {
            let tx_request = working_transaction
                .to_legacy_typed_transaction_with_gas_limit(
                    Some(&gas_price),
                    Some(estimated_gas_limit),
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;
            (tx_request, None)
        } else {
            let tx_request = working_transaction
                .to_eip1559_typed_transaction_with_gas_limit(
                    Some(&gas_price),
                    Some(estimated_gas_limit),
                )
                .map_err(|e| {
                    TransactionQueueSendTransactionError::TransactionConversionError(e.to_string())
                })?;
            (tx_request, None)
        };
        info!("rrelayer_send_network {}", transaction_context(&working_transaction, &self.relayer));

        let (bytes, hash) =
            self.evm_provider.sign_raw_transaction(&self.relayer, transaction_request).await?;
        let attempt = crate::transaction::db::attempt::SignedAttempt {
            bytes,
            hash,
            gas: gas_price,
            blob_gas: sent_with_blob_gas,
        };
        db.save_attempt(transaction, &attempt).await?;
        self.broadcast_attempt(db, transaction, attempt).await
    }

    async fn broadcast_attempt(
        &self,
        db: &mut PostgresClient,
        transaction: &Transaction,
        attempt: crate::transaction::db::attempt::SignedAttempt,
    ) -> Result<TransactionSentWithRelayer, TransactionQueueSendTransactionError> {
        use alloy::eips::eip2718::Decodable2718;
        let mut encoded = attempt.bytes.as_slice();
        let valid = alloy::consensus::TxEnvelope::decode_2718(&mut encoded)
            .map(|envelope| {
                encoded.is_empty() && envelope.tx_hash() == &attempt.hash.into_alloy_hash()
            })
            .unwrap_or(false);
        if !valid {
            return Err(TransactionQueueSendTransactionError::TransactionConversionError(
                "Durable envelope hash mismatch".into(),
            ));
        }
        // Recheck the fence immediately before the network boundary. It cannot
        // retract an RPC already in flight; identical nonce/payload recovery does.
        db.execute(
            "UPDATE relayer.transaction SET broadcast_attempted=TRUE WHERE id=$1",
            &[&transaction.id],
        )
        .await?;
        #[cfg(debug_assertions)]
        crate::sender_handoff::test_barrier("before_broadcast").await;
        let send = async {
            if self
                .evm_provider
                .get_receipt(&attempt.hash)
                .await
                .map_err(TransactionQueueSendTransactionError::TransactionEstimateGasError)?
                .is_none()
            {
                let hash = self.evm_provider.send_raw_signed(&attempt.bytes, attempt.hash).await?;
                if hash != attempt.hash {
                    return Err(TransactionQueueSendTransactionError::TransactionConversionError(
                        "RPC returned a different signed hash".into(),
                    ));
                }
            }
            Ok::<_, TransactionQueueSendTransactionError>(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(3), send).await.map_err(|_| {
            TransactionQueueSendTransactionError::TransactionConversionError(
                "Broadcast outcome unknown; recovering persisted attempt".into(),
            )
        })??;
        #[cfg(debug_assertions)]
        crate::sender_handoff::test_barrier("after_broadcast").await;
        db.transaction_sent(
            &transaction.id,
            &attempt.hash,
            &attempt.gas,
            attempt.blob_gas.as_ref(),
            self.is_legacy_transactions(),
        )
        .await?;
        Ok(TransactionSentWithRelayer {
            id: transaction.id,
            hash: attempt.hash,
            sent_with_gas: attempt.gas,
            sent_with_blob_gas: attempt.blob_gas,
        })
    }

    pub async fn receipt_for_competition(
        &mut self,
        db: &PostgresClient,
        active: &Transaction,
    ) -> Result<Option<(Transaction, AnyTransactionReceipt)>, RpcError<TransportErrorKind>> {
        let competition = self.inmempool_transactions.lock().await.front().cloned();
        if let Some(competition) = competition {
            if let Some(receipt) = self.receipt_for_transaction(db, &competition.original).await? {
                return Ok(Some((competition.original, receipt)));
            }
        }
        Ok(self.receipt_for_transaction(db, active).await?.map(|r| (active.clone(), r)))
    }

    pub async fn receipt_for_transaction(
        &mut self,
        db: &PostgresClient,
        transaction: &Transaction,
    ) -> Result<Option<AnyTransactionReceipt>, RpcError<TransportErrorKind>> {
        let mut hashes = db
            .attempt_hashes(&transaction.id)
            .await
            .map_err(|e| RpcError::Transport(TransportErrorKind::Custom(e.to_string().into())))?;
        if let Some(hash) = transaction.known_transaction_hash {
            if !hashes.contains(&hash) {
                hashes.push(hash);
            }
        }
        for hash in hashes {
            if let Some(receipt) = self.get_receipt(&hash).await? {
                return Ok(Some(receipt));
            }
        }
        Ok(None)
    }

    pub async fn get_receipt(
        &mut self,
        transaction_hash: &TransactionHash,
    ) -> Result<Option<AnyTransactionReceipt>, RpcError<TransportErrorKind>> {
        info!(
            "Getting receipt for transaction hash {} on relayer: {}",
            transaction_hash, self.relayer.name
        );
        let receipt = self.evm_provider.get_receipt(transaction_hash).await?;

        if receipt.is_some() {
            info!(
                "Receipt found for transaction hash {} on relayer: {}",
                transaction_hash, self.relayer.name
            );
        } else {
            info!(
                "No receipt found for transaction hash {} on relayer: {}",
                transaction_hash, self.relayer.name
            );
        }

        Ok(receipt)
    }

    pub async fn get_nonce(&self) -> Result<TransactionNonce, RpcError<TransportErrorKind>> {
        let nonce = self.evm_provider.get_nonce_from_address(&self.relay_address()).await?;

        Ok(nonce)
    }

    pub async fn get_balance(
        &self,
    ) -> Result<alloy::primitives::U256, RpcError<TransportErrorKind>> {
        let address = self.relay_address();
        self.evm_provider.get_balance(&address).await
    }

    pub async fn update_pending_transaction_nonce(
        &self,
        transaction_id: &TransactionId,
        new_nonce: TransactionNonce,
    ) {
        let mut pending = self.pending_transactions.lock().await;
        if let Some(transaction) = pending.iter_mut().find(|tx| tx.id == *transaction_id) {
            transaction.nonce = new_nonce;
        }
    }

    pub async fn update_inmempool_transaction_nonce(
        &self,
        transaction_id: &TransactionId,
        new_nonce: TransactionNonce,
    ) {
        let mut inmempool = self.inmempool_transactions.lock().await;
        if let Some(competitive_tx) =
            inmempool.iter_mut().find(|ctx| ctx.get_transaction_by_id(transaction_id).is_some())
        {
            if let Some(transaction) = competitive_tx.get_transaction_by_id_mut(transaction_id) {
                transaction.nonce = new_nonce;
            }
        }
    }
}

// The queue is sent front to back, so nonces have to follow queue order. A head that takes the
// next free nonce is above every nonce queued behind it: left in front, the node rejects it as
// "nonce too high" until they are sent, which is never (robinhood-pk-0 froze this way, 2026-09-29).
fn move_head_to_back(pending: &mut VecDeque<Transaction>) {
    if let Some(head) = pending.pop_front() {
        pending.push_back(head);
    }
}

// Re-estimate oversized stored limits instead of resending them or blindly lowering them.
// Keep valid caller-supplied limits and avoid an unnecessary RPC on ordinary retries.
async fn gas_limit_for_send(
    stored: Option<GasLimit>,
    block_limit: GasLimit,
    estimate: impl std::future::Future<Output = Result<GasLimit, RpcError<TransportErrorKind>>>,
) -> Result<GasLimit, RpcError<TransportErrorKind>> {
    if let Some(limit) = stored.filter(|limit| *limit <= block_limit) {
        return Ok(limit);
    }
    estimate.await
}

// Only trim optional headroom. An estimate that itself exceeds the block limit must fail.
fn bounded_gas_estimate(
    estimate: GasLimit,
    block_limit: GasLimit,
    is_noop: bool,
) -> Result<GasLimit, RpcError<TransportErrorKind>> {
    if estimate > block_limit {
        return Err(RpcError::Transport(TransportErrorKind::Custom(
            "Estimated gas exceeds the current block gas limit".to_string().into(),
        )));
    }
    Ok(if is_noop { estimate } else { std::cmp::min(estimate * 12 / 10, block_limit) })
}

#[cfg(test)]
mod gas_limit_tests {
    use super::*;

    #[test]
    fn citrea_buffer_cannot_exceed_block_limit() {
        assert_eq!(
            bounded_gas_estimate(GasLimit::new(8_572_950), GasLimit::new(10_000_000), false)
                .unwrap(),
            GasLimit::new(10_000_000)
        );
    }

    #[test]
    fn ordinary_estimate_keeps_twenty_percent_headroom() {
        assert_eq!(
            bounded_gas_estimate(GasLimit::new(4_897_200), GasLimit::new(10_000_000), false)
                .unwrap(),
            GasLimit::new(5_876_640)
        );
    }

    #[test]
    fn cannot_hide_an_estimate_above_the_limit() {
        assert!(bounded_gas_estimate(GasLimit::new(10_000_001), GasLimit::new(10_000_000), false)
            .is_err());
    }

    #[test]
    fn noop_does_not_add_headroom() {
        assert_eq!(
            bounded_gas_estimate(GasLimit::new(21_000), GasLimit::new(10_000_000), true).unwrap(),
            GasLimit::new(21_000)
        );
    }

    #[tokio::test]
    async fn persisted_oversized_limit_is_reestimated_before_send() {
        let limit =
            gas_limit_for_send(Some(GasLimit::new(10_287_540)), GasLimit::new(10_000_000), async {
                bounded_gas_estimate(GasLimit::new(4_897_200), GasLimit::new(10_000_000), false)
            })
            .await
            .unwrap();
        assert_eq!(limit, GasLimit::new(5_876_640));
    }

    #[tokio::test]
    async fn valid_persisted_limit_is_preserved_without_reestimation() {
        let limit =
            gas_limit_for_send(Some(GasLimit::new(10_000_000)), GasLimit::new(10_000_000), async {
                panic!("valid stored limit must not trigger estimation")
            })
            .await
            .unwrap();
        assert_eq!(limit, GasLimit::new(10_000_000));
    }

    #[tokio::test]
    async fn failed_reestimation_does_not_reuse_invalid_stored_limit() {
        assert!(gas_limit_for_send(
            Some(GasLimit::new(10_287_540)),
            GasLimit::new(10_000_000),
            async {
                Err(RpcError::Transport(TransportErrorKind::Custom(
                    "execution reverted".to_string().into(),
                )))
            },
        )
        .await
        .is_err());
    }
}

#[cfg(test)]
mod pending_order_tests {
    use super::*;
    use alloy::primitives::Address;

    fn pending(nonce: u64) -> Transaction {
        let relayer = EvmAddress::new(Address::ZERO);
        Transaction {
            id: TransactionId::new(),
            relayer_id: RelayerId::new(),
            authorization_list: None,
            to: relayer,
            from: relayer,
            value: TransactionValue::zero(),
            data: TransactionData::empty(),
            nonce: TransactionNonce::new(nonce),
            gas_limit: None,
            status: TransactionStatus::PENDING,
            blobs: None,
            chain_id: ChainId::new(4663),
            known_transaction_hash: None,
            queued_at: Utc::now(),
            expires_at: Utc::now(),
            sent_at: None,
            mined_at: None,
            mined_at_block_number: None,
            confirmed_at: None,
            speed: TransactionSpeed::FAST,
            sent_with_max_priority_fee_per_gas: None,
            sent_with_max_fee_per_gas: None,
            is_noop: false,
            sent_with_gas: None,
            sent_with_blob_gas: None,
            external_id: None,
            cancelled_by_transaction_id: None,
        }
    }

    fn nonces(queue: &VecDeque<Transaction>) -> Vec<u64> {
        queue.iter().map(|transaction| transaction.nonce.into_inner()).collect()
    }

    #[test]
    fn a_head_given_the_next_free_nonce_is_sent_after_the_queue() {
        // robinhood-pk-0, 2026-09-29: the head went from 4393 to 4400 with 4394-4399 behind it.
        let mut queue: VecDeque<Transaction> =
            [4400, 4394, 4395, 4396, 4397, 4398, 4399].into_iter().map(pending).collect();
        let head = queue[0].id;

        move_head_to_back(&mut queue);

        assert_eq!(nonces(&queue), vec![4394, 4395, 4396, 4397, 4398, 4399, 4400]);
        assert_eq!(queue[6].id, head);
    }

    #[test]
    fn a_lone_or_missing_head_is_left_alone() {
        let mut queue: VecDeque<Transaction> = [4400].into_iter().map(pending).collect();
        move_head_to_back(&mut queue);
        assert_eq!(nonces(&queue), vec![4400]);

        let mut empty: VecDeque<Transaction> = VecDeque::new();
        move_head_to_back(&mut empty);
        assert!(empty.is_empty());
    }
}
