use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use alloy::{
    consensus::TypedTransaction,
    transports::{RpcError, TransportErrorKind},
};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Error types for transaction queues operations.
#[derive(Error, Debug)]
pub enum TransactionsQueuesError {
    #[error("Wallet or provider error: {0}")]
    WalletOrProvider(#[from] WalletOrProviderError),
    #[error("Database connection error: {0}")]
    DatabaseConnection(#[from] PostgresConnectionError),
}

use super::{
    log_summary::summarize_rpc_error,
    start::spawn_processing_tasks_for_relayer,
    transactions_queue::TransactionsQueue,
    types::{
        AddTransactionError, CancelTransactionError, CancelTransactionResult,
        ProcessInmempoolStatus, ProcessInmempoolTransactionError, ProcessMinedStatus,
        ProcessMinedTransactionError, ProcessPendingStatus, ProcessPendingTransactionError,
        ProcessResult, ReplaceTransactionError, ReplaceTransactionResult, TransactionRelayerSetup,
        TransactionToSend, TransactionsQueueSetup,
    },
};
use crate::transaction::api::RelayTransactionRequest;
use crate::transaction::queue_system::types::SendTransactionGasPriceError;
use crate::transaction::types::{TransactionBlob, TransactionConversionError, TransactionSpeed};
use crate::{
    gas::{BlobGasOracleCache, BlobGasPriceResult, GasLimit, GasOracleCache, GasPriceResult},
    postgres::{PostgresClient, PostgresConnectionError},
    relayer::RelayerId,
    safe_proxy::SafeProxyManager,
    shared::{cache::Cache, common_types::WalletOrProviderError},
    shutdown::enter_critical_operation,
    transaction::{
        cache::invalidate_transaction_no_state_cache,
        nonce_manager::NonceManager,
        types::{
            Transaction, TransactionData, TransactionId, TransactionNonce, TransactionStatus,
            TransactionValue,
        },
    },
    webhooks::WebhookManager,
};

/// Container for managing multiple transaction queues across different relayers.
///
/// This struct coordinates transaction processing for all active relayers,
/// providing centralized access to individual queue operations and shared resources.
pub struct TransactionsQueues {
    pub queues: HashMap<RelayerId, Arc<Mutex<TransactionsQueue>>>,
    pub relayer_block_times_ms: HashMap<RelayerId, u64>,
    gas_oracle_cache: Arc<Mutex<GasOracleCache>>,
    blob_gas_oracle_cache: Arc<Mutex<BlobGasOracleCache>>,
    db: PostgresClient,
    cache: Arc<Cache>,
    webhook_manager: Option<Arc<Mutex<WebhookManager>>>,
    safe_proxy_manager: Arc<SafeProxyManager>,
}

impl TransactionsQueues {
    pub async fn new(
        setups: Vec<TransactionRelayerSetup>,
        gas_oracle_cache: Arc<Mutex<GasOracleCache>>,
        blob_gas_oracle_cache: Arc<Mutex<BlobGasOracleCache>>,
        cache: Arc<Cache>,
        webhook_manager: Option<Arc<Mutex<WebhookManager>>>,
        safe_proxy_manager: Arc<SafeProxyManager>,
    ) -> Result<Self, TransactionsQueuesError> {
        let mut queues = HashMap::new();
        let mut relayer_block_times_ms = HashMap::new();

        for setup in setups {
            let current_nonce = match setup.evm_provider.get_nonce(&setup.relayer).await {
                Ok(nonce) => nonce,
                Err(error) if setup.evm_provider.is_isolated_limit_base() => {
                    warn!("Fixed Base gateway nonce unavailable for relayer {}; leaving its queue offline: {}", setup.relayer.id, error);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let reserved_nonce = if setup.evm_provider.is_isolated_limit_base() {
                let highest = setup
                    .pending_transactions
                    .iter()
                    .map(|tx| tx.nonce.into_inner())
                    .chain(
                        setup
                            .inmempool_transactions
                            .iter()
                            .map(|tx| tx.original.nonce.into_inner()),
                    )
                    .max();
                highest
                    .map(|nonce| {
                        TransactionNonce::new(
                            current_nonce.into_inner().max(nonce.saturating_add(1)),
                        )
                    })
                    .unwrap_or(current_nonce)
            } else {
                current_nonce
            };

            info!(
                "Startup nonce synchronization for relayer {} ({}): synchronizing nonce manager with on-chain nonce {}",
                setup.relayer.name, setup.relayer.id, current_nonce.into_inner()
            );

            relayer_block_times_ms.insert(setup.relayer.id, setup.evm_provider.blocks_every);

            queues.insert(
                setup.relayer.id,
                Arc::new(Mutex::new(TransactionsQueue::new(
                    TransactionsQueueSetup::new(
                        setup.relayer,
                        setup.evm_provider,
                        NonceManager::new(reserved_nonce),
                        setup.pending_transactions,
                        setup.inmempool_transactions,
                        setup.mined_transactions,
                        safe_proxy_manager.clone(),
                        setup.gas_bump_config,
                        setup.max_gas_price_multiplier,
                    ),
                    gas_oracle_cache.clone(),
                    blob_gas_oracle_cache.clone(),
                ))),
            );
        }

        Ok(Self {
            queues,
            relayer_block_times_ms,
            gas_oracle_cache,
            blob_gas_oracle_cache,
            db: PostgresClient::new().await?,
            cache,
            webhook_manager,
            safe_proxy_manager,
        })
    }

    /// Retrieves a transaction queue for the specified relayer.
    pub fn get_transactions_queue(
        &self,
        relayer_id: &RelayerId,
    ) -> Option<Arc<Mutex<TransactionsQueue>>> {
        self.queues.get(relayer_id).cloned()
    }

    /// Retrieves a transaction queue for the specified relayer.
    pub fn get_transactions_queue_unsafe(
        &self,
        relayer_id: &RelayerId,
    ) -> Result<Arc<Mutex<TransactionsQueue>>, String> {
        self.queues
            .get(relayer_id)
            .cloned()
            .ok_or_else(|| format!("transactions queue does not exist for relayer: {}", relayer_id))
    }

    /// Removes a transaction queue for the specified relayer.
    pub async fn delete_queue(&mut self, relayer_id: &RelayerId) {
        self.queues.remove(relayer_id);
    }

    /// Invalidates the cache entry for a specific transaction.
    async fn invalidate_transaction_cache(&self, id: &TransactionId) {
        invalidate_transaction_no_state_cache(&self.cache, id).await;
    }

    /// Returns the count of pending transactions for a specific relayer.
    pub async fn pending_transactions_count(
        &self,
        relayer_id: &RelayerId,
    ) -> Result<usize, crate::postgres::PostgresError> {
        self.db
            .query_one(
                "SELECT COUNT(*) FROM relayer.transaction WHERE relayer_id=$1 AND status='PENDING'",
                &[relayer_id],
            )
            .await
            .map(|r| r.get::<_, i64>(0) as usize)
    }

    pub async fn inmempool_transactions_count(
        &self,
        relayer_id: &RelayerId,
    ) -> Result<usize, crate::postgres::PostgresError> {
        self.db.query_one("SELECT COUNT(*) FROM relayer.transaction WHERE relayer_id=$1 AND status='INMEMPOOL'", &[relayer_id]).await.map(|r|r.get::<_,i64>(0) as usize)
    }

    /// Adds a new relayer and its transaction queue to the system.
    pub async fn add_new_relayer(
        &mut self,
        setup: TransactionsQueueSetup,
        queues_arc: Arc<Mutex<TransactionsQueues>>,
    ) -> Result<(), WalletOrProviderError> {
        let current_nonce = setup.evm_provider.get_nonce(&setup.relayer).await?;
        let relayer_id = setup.relayer.id;

        self.queues.insert(
            relayer_id,
            Arc::new(Mutex::new(TransactionsQueue::new(
                TransactionsQueueSetup::new(
                    setup.relayer,
                    setup.evm_provider,
                    NonceManager::new(current_nonce),
                    VecDeque::new(),
                    VecDeque::new(),
                    HashMap::new(),
                    self.safe_proxy_manager.clone(),
                    setup.gas_bump_config,
                    setup.max_gas_price_multiplier,
                ),
                self.gas_oracle_cache.clone(),
                self.blob_gas_oracle_cache.clone(),
            ))),
        );

        spawn_processing_tasks_for_relayer(queues_arc, &relayer_id).await;

        Ok(())
    }

    fn expires_at(&self) -> DateTime<Utc> {
        Utc::now() + chrono::Duration::hours(12)
    }

    /// Replaces the content of an existing transaction with new parameters.
    fn transaction_replace(
        &self,
        current_transaction: &mut Transaction,
        replace_with: &RelayTransactionRequest,
    ) {
        current_transaction.authorization_list = replace_with.authorization_list.clone();
        current_transaction.to = replace_with.to;
        current_transaction.data = replace_with.data.clone();
        current_transaction.value = replace_with.value;
        current_transaction.is_noop = current_transaction.from == current_transaction.to;

        if let Some(ref blob_strings) = replace_with.blobs {
            current_transaction.blobs = Some(
                blob_strings
                    .iter()
                    .map(|blob_hex| TransactionBlob::from_hex(blob_hex))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("Failed to convert blob hex strings to TransactionBlob"),
            );
        } else {
            current_transaction.blobs = None;
        }
        current_transaction.gas_limit = None;
        current_transaction.external_id = replace_with.external_id.clone();
    }

    /// Computes gas prices for a transaction based on its type.
    async fn compute_transaction_gas_prices(
        transactions_queue: &TransactionsQueue,
        transaction: &Transaction,
        speed: &TransactionSpeed,
    ) -> Result<(GasPriceResult, Option<BlobGasPriceResult>), SendTransactionGasPriceError> {
        let blob_gas_price = if transaction.is_blob_transaction() {
            Some(transactions_queue.compute_blob_gas_price_for_transaction(speed, &None).await?)
        } else {
            None
        };

        let gas_price = transactions_queue.compute_gas_price_for_transaction(speed, None).await?;

        Ok((gas_price, blob_gas_price))
    }

    /// Creates a typed transaction request for gas estimation or sending.
    fn create_typed_transaction(
        transactions_queue: &TransactionsQueue,
        transaction: &Transaction,
        gas_price: &GasPriceResult,
        blob_gas_price: Option<&BlobGasPriceResult>,
        gas_limit: GasLimit,
    ) -> Result<TypedTransaction, TransactionConversionError> {
        if transaction.is_7702_transaction() {
            Ok(transaction
                .to_eip7702_typed_transaction_with_gas_limit(Some(gas_price), Some(gas_limit))?)
        } else if transaction.is_blob_transaction() {
            Ok(transaction.to_blob_typed_transaction_with_gas_limit(
                Some(gas_price),
                blob_gas_price,
                Some(gas_limit),
            )?)
        } else if transactions_queue.is_legacy_transactions() {
            Ok(transaction
                .to_legacy_typed_transaction_with_gas_limit(Some(gas_price), Some(gas_limit))?)
        } else {
            Ok(transaction
                .to_eip1559_typed_transaction_with_gas_limit(Some(gas_price), Some(gas_limit))?)
        }
    }

    /// Estimates gas limit for a transaction and validates it via simulation.
    async fn estimate_and_validate_gas(
        transactions_queue: &mut TransactionsQueue,
        transaction: &Transaction,
        gas_price: &GasPriceResult,
        blob_gas_price: Option<&BlobGasPriceResult>,
    ) -> Result<GasLimit, AddTransactionError> {
        // Use a reasonable temporary limit for gas estimation
        const TEMP_GAS_LIMIT: u128 = 10_000_000;
        let temp_gas_limit = GasLimit::new(TEMP_GAS_LIMIT);

        let current_onchain_nonce = transactions_queue.get_nonce().await.map_err(|e| {
            AddTransactionError::CouldNotGetCurrentOnChainNonce(transaction.relayer_id, e)
        })?;

        let mut estimation_transaction = transaction.clone();
        estimation_transaction.nonce = current_onchain_nonce;

        let temp_transaction_request = Self::create_typed_transaction(
            transactions_queue,
            &estimation_transaction,
            gas_price,
            blob_gas_price,
            temp_gas_limit,
        )?;

        let estimated_gas_limit = transactions_queue
            .estimate_gas(&temp_transaction_request, transaction.is_noop, Some(transaction))
            .await
            .map_err(|e| {
                AddTransactionError::TransactionEstimateGasError(transaction.relayer_id, e)
            })?;

        let relayer_balance = transactions_queue.get_balance().await.map_err(|e| {
            AddTransactionError::TransactionEstimateGasError(transaction.relayer_id, e)
        })?;

        let gas_cost = estimated_gas_limit.into_inner() * gas_price.legacy_gas_price().into_u128();
        let total_required =
            transaction.value.into_inner() + alloy::primitives::U256::from(gas_cost);

        if relayer_balance < total_required {
            error!(
                "Insufficient balance for relayer {}: has {}, needs {}",
                transaction.relayer_id, relayer_balance, total_required
            );
            return Err(AddTransactionError::TransactionEstimateGasError(
                transaction.relayer_id,
                RpcError::Transport(TransportErrorKind::Custom(
                    "Insufficient funds for gas * price + value".to_string().into(),
                )),
            ));
        }

        Ok(estimated_gas_limit)
    }

    /// Adds a new transaction to the specified relayer's queue.
    pub async fn add_transaction(
        &mut self,
        relayer_id: &RelayerId,
        transaction_to_send: &TransactionToSend,
    ) -> Result<Transaction, AddTransactionError> {
        let _sender = self
            .db
            .acquire_sender(relayer_id, false)
            .await
            .map_err(AddTransactionError::CouldNotSaveTransactionDb)?;
        let expires_at = self.expires_at();

        let queue_arc = self
            .get_transactions_queue(relayer_id)
            .ok_or(AddTransactionError::RelayerNotFound(*relayer_id))?;

        let mut transactions_queue = queue_arc.lock().await;

        transactions_queue
            .reload(&self.db)
            .await
            .map_err(AddTransactionError::CouldNotSaveTransactionDb)?;
        if transactions_queue.is_paused() {
            return Err(AddTransactionError::RelayerIsPaused(*relayer_id));
        }

        if transactions_queue.is_isolated_limit_base()
            && (transactions_queue.get_pending_transaction_count().await > 0
                || transactions_queue.get_inmempool_transaction_count().await > 0)
        {
            return Err(AddTransactionError::FixedBaseLaneBusy);
        }

        // Check if this is a blob transaction and if the wallet manager supports blobs
        if transaction_to_send.blobs.is_some() && !transactions_queue.supports_blobs() {
            return Err(AddTransactionError::UnsupportedTransactionType {
                message: "EIP-4844 blob transactions are not supported by this wallet manager"
                    .to_string(),
            });
        }

        // Sync nonce manager with on-chain nonce to ensure consistency
        let current_onchain_nonce = transactions_queue
            .get_nonce()
            .await
            .map_err(|e| AddTransactionError::CouldNotGetCurrentOnChainNonce(*relayer_id, e))?;

        transactions_queue.nonce_manager.sync_with_onchain_nonce(current_onchain_nonce).await;

        let mut transaction = Transaction {
            id: transaction_to_send.id,
            relayer_id: *relayer_id,
            authorization_list: transaction_to_send.authorization_list.clone(),
            to: transaction_to_send.to,
            from: transactions_queue.relay_address(),
            value: transaction_to_send.value,
            data: transaction_to_send.data.clone(),
            nonce: current_onchain_nonce,
            gas_limit: None,
            status: TransactionStatus::PENDING,
            blobs: transaction_to_send.blobs.clone(),
            chain_id: transactions_queue.chain_id(),
            known_transaction_hash: None,
            queued_at: Utc::now(),
            expires_at,
            sent_at: None,
            mined_at: None,
            mined_at_block_number: None,
            confirmed_at: None,
            speed: transaction_to_send.speed.clone(),
            sent_with_max_priority_fee_per_gas: None,
            sent_with_max_fee_per_gas: None,
            is_noop: false,
            sent_with_gas: None,
            sent_with_blob_gas: None,
            external_id: transaction_to_send.external_id.clone(),
            cancelled_by_transaction_id: None,
        };

        let (gas_price, blob_gas_price) = Self::compute_transaction_gas_prices(
            &transactions_queue,
            &transaction,
            &transaction_to_send.speed,
        )
        .await?;

        let estimated_gas_limit = Self::estimate_and_validate_gas(
            &mut transactions_queue,
            &transaction,
            &gas_price,
            blob_gas_price.as_ref(),
        )
        .await;

        let estimated_gas_limit = match estimated_gas_limit {
            Ok(limit) => limit,
            Err(err) => {
                let failed_transaction =
                    Transaction { status: TransactionStatus::FAILED, ..transaction };
                let decoded_reason = match &err {
                    AddTransactionError::TransactionEstimateGasError(_, rpc_error) => {
                        summarize_rpc_error(rpc_error)
                            .map(|summary| format!("; decoded revert: {summary}"))
                            .unwrap_or_default()
                    }
                    _ => String::new(),
                };
                self.db
                    .transaction_failed_on_send(
                        relayer_id,
                        &failed_transaction,
                        format!(
                            "Failed to send transaction as always failing on gas estimation: {err}{decoded_reason}"
                        ),
                    )
                    .await
                    .map_err(AddTransactionError::CouldNotSaveTransactionDb)?;

                self.invalidate_transaction_cache(&transaction.id).await;
                return Err(err);
            }
        };

        let fixed_lane = transactions_queue.is_isolated_limit_base();
        let assigned_nonce = if fixed_lane {
            transactions_queue.nonce_manager.get_current_nonce().await
        } else {
            transactions_queue.nonce_manager.get_and_increment().await
        };
        transaction.nonce = assigned_nonce;
        transaction.gas_limit = Some(estimated_gas_limit);

        let transaction_request = Self::create_typed_transaction(
            &transactions_queue,
            &transaction,
            &gas_price,
            blob_gas_price.as_ref(),
            estimated_gas_limit,
        )?;

        let fixed_signed_envelope = if fixed_lane {
            let (bytes, hash) =
                transactions_queue.prepare_fixed_signed_transaction(&transaction_request).await?;
            transaction.known_transaction_hash = Some(hash);
            transaction.sent_with_gas = Some(gas_price);
            Some(bytes)
        } else {
            transaction.known_transaction_hash =
                Some(transactions_queue.compute_tx_hash(&transaction_request).await?);
            None
        };

        let save_result = self
            .db
            .save_transaction_with_signed_envelope(
                relayer_id,
                &transaction,
                fixed_signed_envelope.as_deref(),
            )
            .await;
        match save_result {
            Ok(()) => {
                if fixed_lane {
                    let _ = transactions_queue.nonce_manager.get_and_increment().await;
                }
            }
            Err(crate::postgres::PostgresError::FixedBaseLaneBusy) => {
                return Err(AddTransactionError::FixedBaseLaneBusy);
            }
            Err(error) => return Err(AddTransactionError::CouldNotSaveTransactionDb(error)),
        }

        transactions_queue.add_pending_transaction(transaction.clone()).await;
        self.invalidate_transaction_cache(&transaction.id).await;

        if let Some(webhook_manager) = &self.webhook_manager {
            let webhook_manager = webhook_manager.clone();
            let transaction_clone = transaction.clone();
            tokio::spawn(async move {
                let webhook_manager = webhook_manager.lock().await;
                webhook_manager.on_transaction_queued(&transaction_clone).await;
            });
        }

        Ok(transaction)
    }

    /// Persist the whole competition before either process can broadcast it.
    async fn queue_competitor(
        &mut self,
        original: &Transaction,
        replace: Option<&RelayTransactionRequest>,
    ) -> Result<Transaction, crate::postgres::PostgresError> {
        if let Some(id) = original.cancelled_by_transaction_id {
            return Err(crate::postgres::PostgresError::Handoff(format!(
                "Competition already exists: {id}; inspect its status"
            )));
        }
        let mut competitor = original.clone();
        competitor.id = TransactionId::new();
        competitor.status = TransactionStatus::PENDING;
        competitor.cancelled_by_transaction_id = None;
        competitor.known_transaction_hash = None;
        competitor.sent_at = None;
        competitor.mined_at = None;
        competitor.confirmed_at = None;
        competitor.queued_at = Utc::now();
        competitor.expires_at = self.expires_at();
        competitor.speed = TransactionSpeed::SUPER;
        if let Some(request) = replace {
            competitor.is_noop = false;
            self.transaction_replace(&mut competitor, request);
        } else {
            competitor.to = original.from;
            competitor.value = TransactionValue::zero();
            competitor.data = TransactionData::empty();
            competitor.authorization_list = None;
            competitor.blobs = None;
            competitor.is_noop = true;
            competitor.gas_limit = Some(GasLimit::new(21_000));
            competitor.external_id = Some(format!("cancel_{}", original.id));
        }
        self.db.save_competitor(&original.id, &competitor).await?;
        self.invalidate_transaction_cache(&original.id).await;
        Ok(competitor)
    }

    pub async fn cancel_transaction(
        &mut self,
        transaction: &Transaction,
    ) -> Result<CancelTransactionResult, CancelTransactionError> {
        let _sender = self
            .db
            .acquire_sender(&transaction.relayer_id, false)
            .await
            .map_err(CancelTransactionError::Handoff)?;
        if self
            .db
            .get_relayer(&transaction.relayer_id)
            .await
            .map_err(CancelTransactionError::Handoff)?
            .map(|r| r.paused)
            .unwrap_or(true)
        {
            return Err(CancelTransactionError::RelayerIsPaused(transaction.relayer_id));
        }
        let original = self
            .db
            .get_transaction(&transaction.id)
            .await
            .map_err(CancelTransactionError::Handoff)?
            .ok_or(CancelTransactionError::RelayerNotFound(transaction.relayer_id))?;
        if !matches!(original.status, TransactionStatus::PENDING | TransactionStatus::INMEMPOOL) {
            return Ok(CancelTransactionResult::failed());
        }
        if let Some(id) = original.cancelled_by_transaction_id {
            let competitor = self
                .db
                .get_transaction(&id)
                .await
                .map_err(CancelTransactionError::Handoff)?
                .ok_or(CancelTransactionError::RelayerNotFound(transaction.relayer_id))?;
            if competitor.is_noop {
                return Ok(CancelTransactionResult::success(id));
            }
        }
        let competitor = self
            .queue_competitor(&original, None)
            .await
            .map_err(CancelTransactionError::Handoff)?;
        Ok(CancelTransactionResult::success(competitor.id))
    }

    pub async fn replace_transaction(
        &mut self,
        transaction: &Transaction,
        replace_with: &RelayTransactionRequest,
    ) -> Result<ReplaceTransactionResult, ReplaceTransactionError> {
        let _sender = self
            .db
            .acquire_sender(&transaction.relayer_id, false)
            .await
            .map_err(ReplaceTransactionError::Handoff)?;
        if self
            .db
            .get_relayer(&transaction.relayer_id)
            .await
            .map_err(ReplaceTransactionError::Handoff)?
            .map(|r| r.paused)
            .unwrap_or(true)
        {
            return Err(ReplaceTransactionError::RelayerIsPaused(transaction.relayer_id));
        }
        let mut original = self
            .db
            .get_transaction(&transaction.id)
            .await?
            .ok_or(ReplaceTransactionError::TransactionNotFound(transaction.id))?;
        if !matches!(original.status, TransactionStatus::PENDING | TransactionStatus::INMEMPOOL) {
            return Ok(ReplaceTransactionResult::failed());
        }
        if original.status == TransactionStatus::PENDING {
            self.transaction_replace(&mut original, replace_with);
            original.known_transaction_hash = None;
            if !self.db.transaction_replace_unbroadcast(&original).await? {
                return Err(ReplaceTransactionError::BroadcastAlreadyAttempted(original.id));
            }
            self.invalidate_transaction_cache(&original.id).await;
            return Ok(ReplaceTransactionResult {
                success: true,
                replace_transaction_id: Some(original.id),
                replace_transaction_hash: None,
            });
        }
        let competitor = self.queue_competitor(&original, Some(replace_with)).await?;
        Ok(ReplaceTransactionResult {
            success: true,
            replace_transaction_id: Some(competitor.id),
            replace_transaction_hash: None,
        })
    }

    pub async fn process_single_pending(
        &mut self,
        relayer_id: &RelayerId,
    ) -> Result<ProcessResult<ProcessPendingStatus>, ProcessPendingTransactionError> {
        let _sender = self
            .db
            .acquire_sender(relayer_id, true)
            .await
            .map_err(ProcessPendingTransactionError::Handoff)?;
        let queue = self
            .get_transactions_queue(relayer_id)
            .ok_or(ProcessPendingTransactionError::RelayerTransactionsQueueNotFound(*relayer_id))?;
        let mut queue = queue.lock().await;
        queue.reload(&self.db).await.map_err(ProcessPendingTransactionError::Handoff)?;
        if queue.is_paused() {
            return Ok(ProcessResult::other(ProcessPendingStatus::RelayerPaused, Some(&250)));
        }
        if let Some(mut transaction) = queue.get_next_pending_transaction().await {
            let _critical = enter_critical_operation().ok_or(
                ProcessPendingTransactionError::RelayerTransactionsQueueNotFound(*relayer_id),
            )?;
            // An unattempted expired request becomes a durable nonce-consuming
            // no-op competitor. Attempted sends remain ambiguous until receipt.
            if transaction.expires_at <= Utc::now() && !transaction.is_noop {
                let attempted: bool = self
                    .db
                    .query_one(
                        "SELECT broadcast_attempted FROM relayer.transaction WHERE id=$1",
                        &[&transaction.id],
                    )
                    .await
                    .map_err(ProcessPendingTransactionError::Handoff)?
                    .get(0);
                if !attempted && !queue.is_isolated_limit_base() {
                    self.queue_competitor(&transaction, None)
                        .await
                        .map_err(ProcessPendingTransactionError::Handoff)?;
                    return Ok(ProcessResult::success());
                }
            }
            // Expiry is not evidence that a prior broadcast failed. Never turn an
            // uncertain send into a new payload, a fresh nonce, or a refund signal.
            let sent =
                queue.send_transaction(&mut self.db, &mut transaction).await.map_err(|e| {
                    ProcessPendingTransactionError::SendTransactionError(
                        *relayer_id,
                        queue.relay_address(),
                        e,
                    )
                })?;
            queue.move_pending_to_inmempool(&sent).await.map_err(|e| {
                ProcessPendingTransactionError::MovePendingTransactionToInmempoolError(
                    *relayer_id,
                    queue.relay_address(),
                    Box::new(e),
                )
            })?;
            self.invalidate_transaction_cache(&transaction.id).await;
            if let Some(manager) = &self.webhook_manager {
                let sent_transaction = Transaction {
                    status: TransactionStatus::INMEMPOOL,
                    known_transaction_hash: Some(sent.hash),
                    sent_at: Some(Utc::now()),
                    ..transaction
                };
                manager.lock().await.on_transaction_sent(&sent_transaction).await;
            }
            Ok(ProcessResult::success())
        } else {
            Ok(ProcessResult::other(ProcessPendingStatus::NoPendingTransactions, Some(&250)))
        }
    }

    /// Processes a single in-mempool transaction for the specified relayer.
    pub async fn process_single_inmempool(
        &mut self,
        relayer_id: &RelayerId,
    ) -> Result<ProcessResult<ProcessInmempoolStatus>, ProcessInmempoolTransactionError> {
        let _sender = self
            .db
            .acquire_sender(relayer_id, true)
            .await
            .map_err(ProcessInmempoolTransactionError::Handoff)?;
        if let Some(queue_arc) = self.get_transactions_queue(relayer_id) {
            let mut transactions_queue = queue_arc.lock().await;
            transactions_queue
                .reload(&self.db)
                .await
                .map_err(ProcessInmempoolTransactionError::Handoff)?;

            let relayer_address = transactions_queue.relay_address();

            if let Some(mut transaction) = transactions_queue.get_next_inmempool_transaction().await
            {
                let _guard = enter_critical_operation().ok_or_else(|| {
                    info!(
                        "process_single_inmempool: refusing to start during shutdown for relayer {}",
                        relayer_id
                    );
                    ProcessInmempoolTransactionError::RelayerTransactionsQueueNotFound(*relayer_id)
                })?;
                {
                    match transactions_queue.receipt_for_competition(&self.db, &transaction).await {
                        Ok(Some((winner, receipt))) => {
                            let competition_result = transactions_queue
                                .move_inmempool_to_mining(&winner.id, &receipt)
                                .await.map_err(|e| ProcessInmempoolTransactionError::MoveInmempoolTransactionToMinedError(*relayer_id, relayer_address, Box::new(e)))?;

                            self.db.transaction_mined(&competition_result.winner, &receipt, competition_result.loser.as_ref()).await
                                .map_err(|e| ProcessInmempoolTransactionError::CouldNotUpdateTransactionStatusInTheDatabase(*relayer_id,relayer_address,Box::new(competition_result.winner.clone()),competition_result.winner_status,e))?;
                            self.invalidate_transaction_cache(&competition_result.winner.id).await;
                            if let Some(loser) = &competition_result.loser {
                                self.invalidate_transaction_cache(&loser.id).await;
                            }
                            if let Some(manager) = &self.webhook_manager {
                                let manager = manager.lock().await;
                                if competition_result.winner_status == TransactionStatus::MINED {
                                    manager
                                        .on_transaction_mined(&competition_result.winner, &receipt)
                                        .await;
                                } else {
                                    manager.on_transaction_failed(&competition_result.winner).await;
                                }
                            }

                            Ok(ProcessResult::<ProcessInmempoolStatus>::success())
                        }
                        Ok(None) => {
                            if transactions_queue.is_isolated_limit_base() {
                                return Ok(ProcessResult::<ProcessInmempoolStatus>::other(
                                    ProcessInmempoolStatus::StillInmempool,
                                    Some(&1_000),
                                ));
                            }
                            if let Some(sent_at) = transaction.sent_at {
                                let elapsed = Utc::now() - sent_at;

                                let at_max_gas_cap =
                                    if let Some(ref sent_gas) = transaction.sent_with_gas {
                                        transactions_queue.is_at_max_gas_price_cap(sent_gas).await
                                    } else {
                                        false
                                    };

                                let at_max_blob_gas_cap = if let Some(ref sent_blob_gas) =
                                    transaction.sent_with_blob_gas
                                {
                                    transactions_queue
                                        .is_at_max_blob_gas_price_cap(sent_blob_gas)
                                        .await
                                } else {
                                    false
                                };

                                if at_max_gas_cap || at_max_blob_gas_cap {
                                    info!(
                                        "Transaction {} has reached maximum gas price cap (gas: {}, blob: {}), skipping gas bump for relayer: {}",
                                        transaction.id, at_max_gas_cap, at_max_blob_gas_cap, transactions_queue.relayer_name()
                                    );
                                    return Ok(ProcessResult::<ProcessInmempoolStatus>::other(
                                        ProcessInmempoolStatus::StillInmempool,
                                        self.relayer_block_times_ms
                                            .get(relayer_id)
                                            .map(|&block_time| block_time / 10)
                                            .as_ref(),
                                    ));
                                }

                                if transaction.cancelled_by_transaction_id.is_none()
                                    && transactions_queue.should_bump_gas(
                                        elapsed.num_milliseconds() as u64,
                                        &transaction.speed,
                                    )
                                {
                                    let transaction_sent = transactions_queue
                                        .send_transaction(&mut self.db, &mut transaction)
                                        .await
                                        .map_err(|e| {
                                            ProcessInmempoolTransactionError::SendTransactionError(
                                                *relayer_id,
                                                relayer_address,
                                                e,
                                            )
                                        })?;

                                    // Update the actual transaction in the inmempool queue
                                    transactions_queue
                                        .update_inmempool_transaction_gas(&transaction_sent)
                                        .await;

                                    // Update the local transaction with the new gas values so subsequent bumps work correctly
                                    transaction.known_transaction_hash =
                                        Some(transaction_sent.hash);
                                    transaction.sent_with_max_fee_per_gas =
                                        Some(transaction_sent.sent_with_gas.max_fee);
                                    transaction.sent_with_max_priority_fee_per_gas =
                                        Some(transaction_sent.sent_with_gas.max_priority_fee);
                                    transaction.sent_with_gas =
                                        Some(transaction_sent.sent_with_gas.clone());
                                    transaction.sent_at = Some(Utc::now());

                                    self.invalidate_transaction_cache(&transaction.id).await;

                                    return Ok(ProcessResult::<ProcessInmempoolStatus>::other(
                                        ProcessInmempoolStatus::GasIncreased,
                                        Default::default(),
                                    ));
                                }
                            }

                            Ok(ProcessResult::<ProcessInmempoolStatus>::other(
                                ProcessInmempoolStatus::StillInmempool,
                                self.relayer_block_times_ms
                                    .get(relayer_id)
                                    .map(|&block_time| block_time / 10)
                                    .as_ref(),
                            ))
                        }
                        Err(e) => {
                            Err(ProcessInmempoolTransactionError::CouldNotGetTransactionReceipt(
                                *relayer_id,
                                relayer_address,
                                Box::new(transaction.clone()),
                                e,
                            ))
                        }
                    }
                }
            } else {
                Ok(ProcessResult::<ProcessInmempoolStatus>::other(
                    ProcessInmempoolStatus::NoInmempoolTransactions,
                    Default::default(),
                ))
            }
        } else {
            Err(ProcessInmempoolTransactionError::RelayerTransactionsQueueNotFound(*relayer_id))
        }
    }

    /// Processes a single mined transaction for the specified relayer.
    pub async fn process_single_mined(
        &mut self,
        relayer_id: &RelayerId,
    ) -> Result<ProcessResult<ProcessMinedStatus>, ProcessMinedTransactionError> {
        let _sender = self
            .db
            .acquire_sender(relayer_id, true)
            .await
            .map_err(ProcessMinedTransactionError::Handoff)?;
        if let Some(queue_arc) = self.get_transactions_queue(relayer_id) {
            let mut transactions_queue = queue_arc.lock().await;
            transactions_queue
                .reload(&self.db)
                .await
                .map_err(ProcessMinedTransactionError::Handoff)?;

            let relayer_address = transactions_queue.relay_address();

            if let Some(transaction) = transactions_queue.get_next_mined_transaction().await {
                let _guard = enter_critical_operation().ok_or_else(|| {
                    info!(
                        "process_single_mined: refusing to start during shutdown for relayer {}",
                        relayer_id
                    );
                    ProcessMinedTransactionError::RelayerTransactionsQueueNotFound(*relayer_id)
                })?;

                if let Some(mined_at) = transaction.mined_at {
                    let elapsed = Utc::now() - mined_at;
                    if transactions_queue.in_confirmed_range(elapsed.num_milliseconds() as u64) {
                        let receipt = if let Some(tx_hash) = transaction.known_transaction_hash {
                            transactions_queue
                                .get_receipt(&tx_hash)
                                .await
                                .map_err(|e| {
                                    ProcessMinedTransactionError::CouldNotGetTransactionReceipt(
                                        *relayer_id,
                                        relayer_address,
                                        Box::new(transaction.clone()),
                                        e,
                                    )
                                })?
                                .ok_or(
                                    ProcessMinedTransactionError::CouldNotGetTransactionReceipt(
                                        *relayer_id,
                                        relayer_address,
                                        Box::new(transaction.clone()),
                                        RpcError::Transport(TransportErrorKind::Custom(
                                            "No receipt".to_string().into(),
                                        )),
                                    ),
                                )?
                        } else {
                            return Err(
                                ProcessMinedTransactionError::CouldNotGetTransactionReceipt(
                                    *relayer_id,
                                    relayer_address,
                                    Box::new(transaction.clone()),
                                    RpcError::Transport(TransportErrorKind::Custom(
                                        "Transaction hash not found".to_string().into(),
                                    )),
                                ),
                            );
                        };

                        self.db.transaction_confirmed(&transaction.id).await.map_err(|e| {
                            ProcessMinedTransactionError::TransactionConfirmedNotSaveToDatabase(
                                *relayer_id,
                                relayer_address,
                                Box::new(transaction.clone()),
                                e,
                            )
                        })?;

                        transactions_queue.move_mining_to_confirmed(&transaction.id).await;

                        self.invalidate_transaction_cache(&transaction.id).await;

                        if let Some(webhook_manager) = &self.webhook_manager {
                            let webhook_manager = webhook_manager.clone();
                            let confirmed_transaction = Transaction {
                                status: TransactionStatus::CONFIRMED,
                                confirmed_at: Some(Utc::now()),
                                ..transaction
                            };
                            let receipt_clone = receipt.clone();
                            tokio::spawn(async move {
                                let webhook_manager = webhook_manager.lock().await;
                                webhook_manager
                                    .on_transaction_confirmed(
                                        &confirmed_transaction,
                                        &receipt_clone,
                                    )
                                    .await;
                            });
                        }

                        return Ok(ProcessResult::<ProcessMinedStatus>::success());
                    }

                    Ok(ProcessResult::<ProcessMinedStatus>::other(
                        ProcessMinedStatus::NotConfirmedYet,
                        Default::default(),
                    ))
                } else {
                    Err(ProcessMinedTransactionError::NoMinedAt(
                        *relayer_id,
                        relayer_address,
                        Box::new(transaction.clone()),
                    ))
                }
            } else {
                Ok(ProcessResult::<ProcessMinedStatus>::other(
                    ProcessMinedStatus::NoMinedTransactions,
                    Default::default(),
                ))
            }
        } else {
            Err(ProcessMinedTransactionError::RelayerTransactionsQueueNotFound(*relayer_id))
        }
    }
}
