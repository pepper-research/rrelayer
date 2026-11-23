use std::{collections::HashMap, sync::Arc};

use alloy::{
    consensus::TypedTransaction,
    transports::{RpcError, TransportErrorKind},
};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, Semaphore};
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

/// Local serialization and durable-enqueue wakeups for one relayer.
#[derive(Default)]
pub(super) struct QueueCoordination {
    pub processing: Arc<Mutex<()>>,
    pub pending: Arc<Notify>,
}

/// Registry and shared resources for the active sender queues.
pub struct TransactionsQueues {
    pub queues: HashMap<RelayerId, Arc<Mutex<TransactionsQueue>>>,
    draining: HashMap<RelayerId, tokio::sync::watch::Receiver<()>>,
    pub(super) processing_slots: Arc<Semaphore>,
    pub(super) coordination: HashMap<RelayerId, Arc<QueueCoordination>>,
    pub relayer_block_times_ms: HashMap<RelayerId, u64>,
    gas_oracle_cache: Arc<Mutex<GasOracleCache>>,
    blob_gas_oracle_cache: Arc<Mutex<BlobGasOracleCache>>,
    db: PostgresClient,
    cache: Arc<Cache>,
    webhook_manager: Option<Arc<Mutex<WebhookManager>>>,
    safe_proxy_manager: Arc<SafeProxyManager>,
}

impl TransactionsQueues {
    // Processing keeps the existing per-queue mutex and database sender fence,
    // but must not hold the registry lock across another chain's RPC work.
    // Each operation gets its own fence token; sharing that mutable token would
    // let concurrent signers overwrite one another's database authorization.
    pub(super) fn for_processing(&self, relayer_id: &RelayerId) -> Self {
        Self {
            draining: HashMap::new(),
            processing_slots: self.processing_slots.clone(),
            coordination: self
                .coordination
                .get(relayer_id)
                .map(|coordination| (*relayer_id, coordination.clone()))
                .into_iter()
                .collect(),
            queues: self
                .queues
                .get(relayer_id)
                .map(|queue| (*relayer_id, queue.clone()))
                .into_iter()
                .collect(),
            relayer_block_times_ms: self
                .relayer_block_times_ms
                .get(relayer_id)
                .map(|ms| (*relayer_id, *ms))
                .into_iter()
                .collect(),
            gas_oracle_cache: self.gas_oracle_cache.clone(),
            blob_gas_oracle_cache: self.blob_gas_oracle_cache.clone(),
            db: PostgresClient { pool: self.db.pool.inner.clone().into() },
            cache: self.cache.clone(),
            webhook_manager: self.webhook_manager.clone(),
            safe_proxy_manager: self.safe_proxy_manager.clone(),
        }
    }

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
            coordination: queues
                .keys()
                .map(|id| (*id, Arc::new(QueueCoordination::default())))
                .collect(),
            queues,
            draining: HashMap::new(),
            processing_slots: Arc::new(Semaphore::new(4)),
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
    pub async fn delete_queue(registry: &Arc<Mutex<Self>>, relayer_id: &RelayerId) {
        let mut drained = {
            let mut queues = registry.lock().await;
            if let Some(drained) = queues.draining.get(relayer_id) {
                drained.clone()
            } else if let Some(queue) = queues.queues.remove(relayer_id) {
                let coordination = queues.coordination.remove(relayer_id);
                queues.relayer_block_times_ms.remove(relayer_id);
                let (finished, drained) = tokio::sync::watch::channel(());
                queues.draining.insert(*relayer_id, drained.clone());
                let registry = registry.clone();
                let id = *relayer_id;
                // Finish draining even if the DELETE client disconnects.
                tokio::spawn(async move {
                    {
                        let _worker = match &coordination {
                            Some(coordination) => Some(coordination.processing.lock().await),
                            None => None,
                        };
                        let _inflight = queue.lock().await;
                    }
                    registry.lock().await.draining.remove(&id);
                    drop(finished);
                });
                drained
            } else {
                return;
            }
        };
        let _ = drained.changed().await;
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
        self.db.count_transactions_by_status(relayer_id, TransactionStatus::PENDING).await
    }

    pub async fn inmempool_transactions_count(
        &self,
        relayer_id: &RelayerId,
    ) -> Result<usize, crate::postgres::PostgresError> {
        self.db.count_transactions_by_status(relayer_id, TransactionStatus::INMEMPOOL).await
    }

    /// Adds a new relayer and its transaction queue to the system.
    pub async fn add_new_relayer(queues: Arc<Mutex<Self>>, setup: TransactionsQueueSetup) {
        let relayer_id = setup.relayer.id;
        {
            let mut queues = queues.lock().await;
            if queues.queues.contains_key(&relayer_id) || queues.draining.contains_key(&relayer_id)
            {
                return;
            }
            queues.relayer_block_times_ms.insert(relayer_id, setup.evm_provider.blocks_every);
            let queue = TransactionsQueue::new(
                setup,
                queues.gas_oracle_cache.clone(),
                queues.blob_gas_oracle_cache.clone(),
            );
            queues.coordination.insert(relayer_id, Arc::new(QueueCoordination::default()));
            queues.queues.insert(relayer_id, Arc::new(Mutex::new(queue)));
        }
        spawn_processing_tasks_for_relayer(queues, &relayer_id).await;
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

        let temp_transaction_request = Self::create_typed_transaction(
            transactions_queue,
            transaction,
            gas_price,
            blob_gas_price,
            temp_gas_limit,
        )?;

        let (estimated_gas_limit, relayer_balance) = tokio::join!(
            transactions_queue.estimate_gas(
                &temp_transaction_request,
                transaction.is_noop,
                Some(transaction)
            ),
            transactions_queue.get_balance(),
        );
        // Preserve gas-validation error precedence when both reads fail.
        let estimated_gas_limit = estimated_gas_limit.map_err(|e| {
            AddTransactionError::TransactionEstimateGasError(transaction.relayer_id, e)
        })?;
        let relayer_balance = relayer_balance.map_err(|e| {
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

    async fn estimate_admission_gas(
        queue: &mut TransactionsQueue,
        transaction: &Transaction,
        gas_price: GasPriceResult,
        blob_gas_price: Option<&BlobGasPriceResult>,
    ) -> Result<(GasLimit, GasPriceResult), AddTransactionError> {
        let estimated =
            Self::estimate_and_validate_gas(queue, transaction, &gas_price, blob_gas_price).await;
        // Only the exact pre-signing Ethereum base-fee race is refreshable.
        // Preserve all normal simulation, gas-limit and affordability checks.
        if queue.is_ordinary_ethereum(transaction)
            && matches!(
                &estimated,
                Err(AddTransactionError::TransactionEstimateGasError(
                    _,
                    RpcError::ErrorResp(error)
                )) if error.code == -32000
                    && error.message == "max fee per gas less than block base fee"
            )
        {
            if let Some(refreshed) =
                queue.refresh_ordinary_ethereum_fee(transaction, &gas_price).await
            {
                let limit =
                    Self::estimate_and_validate_gas(queue, transaction, &refreshed, None).await?;
                return Ok((limit, refreshed));
            }
        }
        estimated.map(|limit| (limit, gas_price))
    }

    pub(crate) async fn sender_operation(
        registry: &Arc<Mutex<Self>>,
        relayer_id: &RelayerId,
    ) -> Option<(tokio::sync::OwnedMutexGuard<()>, tokio::sync::OwnedSemaphorePermit, Self)> {
        let coordination = registry.lock().await.coordination.get(relayer_id).cloned()?;
        let processing = coordination.processing.clone().lock_owned().await;
        let (slot, queues) = super::start::processing_context(registry, relayer_id).await;
        queues.queues.contains_key(relayer_id).then_some((processing, slot, queues))
    }

    pub async fn admit_transaction(
        registry: &Arc<Mutex<Self>>,
        relayer_id: &RelayerId,
        transaction: &TransactionToSend,
    ) -> Result<Transaction, AddTransactionError> {
        let (_processing, _slot, mut queues) =
            Self::sender_operation(registry, relayer_id)
                .await
                .ok_or(AddTransactionError::RelayerNotFound(*relayer_id))?;
        let admitted = queues.add_transaction(relayer_id, transaction).await?;
        if let Some(coordination) = queues.coordination.get(relayer_id) {
            coordination.pending.notify_one();
        }
        Ok(admitted)
    }

    /// Adds a new transaction to the specified relayer's queue.
    pub async fn add_transaction(
        &mut self,
        relayer_id: &RelayerId,
        transaction_to_send: &TransactionToSend,
    ) -> Result<Transaction, AddTransactionError> {
        let sender = self
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

        let estimated_gas_limit = Self::estimate_admission_gas(
            &mut transactions_queue,
            &transaction,
            gas_price,
            blob_gas_price.as_ref(),
        )
        .await;

        let (estimated_gas_limit, gas_price) = match estimated_gas_limit {
            Ok(estimate) => estimate,
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
            // The sender lock covers this read through admission. Fill unused gaps,
            // but keep every durable admission/attempt at its original nonce,
            // including rows belonging to another relayer with the same signer.
            let reserved = self
                .db
                .query(
                    "SELECT nonce, bool_or(status IN ('PENDING','INMEMPOOL','MINED')) AS recoverable
                 FROM relayer.transaction
                 WHERE chain_id=$1 AND \"from\"=$2 AND nonce >= $3
                   AND (status <> 'FAILED' OR broadcast_attempted)
                 GROUP BY nonce ORDER BY nonce",
                    &[&transaction.chain_id, &transaction.from, &current_onchain_nonce],
                )
                .await
                .map_err(AddTransactionError::CouldNotSaveTransactionDb)?;
            let mut next = current_onchain_nonce;
            for row in reserved {
                let occupied: TransactionNonce = row.get("nonce");
                if occupied != next {
                    break;
                }
                if !row.get::<_, bool>("recoverable") {
                    return Err(AddTransactionError::CouldNotSaveTransactionDb(
                        crate::postgres::PostgresError::Handoff(format!(
                            "Nonce {} is reserved by terminal history; reconcile the original nonce before admitting higher sends", next.into_inner()
                        )),
                    ));
                }
                next = next + 1;
            }
            next
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
            Err(crate::postgres::PostgresError::RelayerDeleted) => {
                return Err(AddTransactionError::RelayerNotFound(*relayer_id))
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

        sender.release().await;
        Ok(transaction)
    }

    /// Persist the whole competition before either process can broadcast it.
    async fn queue_competitor(
        &mut self,
        original: &Transaction,
        replace: Option<&RelayTransactionRequest>,
        legacy_nonce: bool,
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
            // A migrated sender may have EIP-7702 delegated code. Estimate the
            // explicit recovery cancellation instead of assuming EOA gas.
            competitor.gas_limit = if legacy_nonce { None } else { Some(GasLimit::new(21_000)) };
            competitor.external_id = Some(format!("cancel_{}", original.id));
        }
        self.db.save_competitor(&original.id, &competitor, legacy_nonce).await?;
        self.invalidate_transaction_cache(&original.id).await;
        Ok(competitor)
    }

    pub async fn cancel_transaction(
        &mut self,
        transaction: &Transaction,
    ) -> Result<CancelTransactionResult, CancelTransactionError> {
        self.cancel_transaction_inner(transaction, None).await
    }

    pub async fn cancel_legacy_nonce(
        &mut self,
        transaction: &Transaction,
        expected_nonce: TransactionNonce,
    ) -> Result<CancelTransactionResult, CancelTransactionError> {
        self.cancel_transaction_inner(transaction, Some(expected_nonce)).await
    }

    async fn cancel_transaction_inner(
        &mut self,
        transaction: &Transaction,
        legacy_nonce: Option<TransactionNonce>,
    ) -> Result<CancelTransactionResult, CancelTransactionError> {
        let _sender = self
            .db
            .acquire_sender(&transaction.relayer_id, false)
            .await
            .map_err(CancelTransactionError::Handoff)?;
        let paused = self
            .db
            .get_relayer(&transaction.relayer_id)
            .await
            .map_err(CancelTransactionError::Handoff)?
            .map(|r| r.paused)
            .unwrap_or(true);
        let original = self
            .db
            .get_transaction(&transaction.id)
            .await
            .map_err(CancelTransactionError::Handoff)?
            .ok_or(CancelTransactionError::RelayerNotFound(transaction.relayer_id))?;
        if legacy_nonce.is_some_and(|expected| expected != original.nonce) {
            return Err(CancelTransactionError::Handoff(crate::postgres::PostgresError::Handoff(
                "Legacy cancellation nonce does not match the original".into(),
            )));
        }
        if legacy_nonce.is_none() {
            if paused {
                return Err(CancelTransactionError::RelayerIsPaused(original.relayer_id));
            }
            if !matches!(original.status, TransactionStatus::PENDING | TransactionStatus::INMEMPOOL)
            {
                return Ok(CancelTransactionResult::failed());
            }
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
        if let Some(expected) = legacy_nonce {
            // This explicit administrative cancellation does not certify a legacy
            // failure as unbroadcast. Only its same-nonce receipt can retire it.
            if expected != original.nonce || !paused {
                return Err(CancelTransactionError::Handoff(crate::postgres::PostgresError::Handoff(
                    "Legacy nonce cancellation requires a paused sender and the exact original nonce".into())));
            }
            let eligible: bool = self.db.query_one(
                "SELECT status='FAILED' AND broadcast_attempted AND hash IS NULL
                   AND signed_envelope IS NULL
                   AND NOT EXISTS (SELECT 1 FROM relayer.transaction_attempt WHERE transaction_id=$1)
                   AND NOT EXISTS (SELECT 1 FROM relayer.transaction other
                     WHERE other.chain_id=t.chain_id AND other.\"from\"=t.\"from\"
                       AND other.nonce=t.nonce AND other.status IN ('PENDING','INMEMPOOL','MINED'))
                 FROM relayer.transaction t WHERE id=$1", &[&original.id]).await
                .map_err(CancelTransactionError::Handoff)?.get(0);
            if !eligible {
                return Err(CancelTransactionError::Handoff(
                    crate::postgres::PostgresError::Handoff(
                        "Original legacy nonce is not eligible for explicit cancellation".into(),
                    ),
                ));
            }
            let queue = self
                .get_transactions_queue(&original.relayer_id)
                .ok_or(CancelTransactionError::RelayerNotFound(original.relayer_id))?;
            let queue = queue.lock().await;
            let pending = queue.get_nonce().await.map_err(|e| {
                CancelTransactionError::Handoff(crate::postgres::PostgresError::Handoff(
                    e.to_string(),
                ))
            })?;
            let latest = queue.get_latest_nonce().await.map_err(|e| {
                CancelTransactionError::Handoff(crate::postgres::PostgresError::Handoff(
                    e.to_string(),
                ))
            })?;
            if latest != expected || pending != expected {
                return Err(CancelTransactionError::Handoff(crate::postgres::PostgresError::Handoff(
                    "Original nonce is consumed or pending on chain; reconcile it without a new cancellation".into())));
            }
        } else {
            if paused {
                return Err(CancelTransactionError::RelayerIsPaused(original.relayer_id));
            }
            if !matches!(original.status, TransactionStatus::PENDING | TransactionStatus::INMEMPOOL)
            {
                return Ok(CancelTransactionResult::failed());
            }
        }
        let competitor = self
            .queue_competitor(&original, None, legacy_nonce.is_some())
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
        let competitor = self.queue_competitor(&original, Some(replace_with), false).await?;
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
                    self.queue_competitor(&transaction, None, false)
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
                                            .map(|&block_time| (block_time / 10).max(250))
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
                                    .map(|&block_time| (block_time / 10).max(250))
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

#[cfg(test)]
mod admission_fee_tests {
    use super::super::transactions_queue::elysium_tests::Fixture;
    use super::*;
    use crate::gas::{GasPrice, MaxFee, MaxPriorityFee};
    use serde_json::Value;

    #[tokio::test]
    async fn ordinary_ethereum_admission_refreshes_only_a_stale_fee_within_its_cap() {
        let cases = [
            // chain, refreshed fee, simulated base fee, configured cap, 7702, error, attempts, clears
            (1, 200, 150, None, false, None, 2, true),
            (1, 200, 150, Some(180), false, None, 1, false),
            (1, 100, 150, None, false, None, 1, false),
            (1, 200, 250, None, false, None, 2, false),
            (1, 200, 150, None, true, None, 1, false),
            (1, 200, 150, None, false, Some("execution reverted"), 1, false),
            (8453, 200, 150, None, false, None, 1, false),
        ];
        for (chain, fresh, base_fee, cap, authorized, error, attempts, clears) in cases {
            let mut fixture = Fixture::new(chain, Value::Null, 0).await;
            fixture.set_estimation_fees(fresh, base_fee, error).await;
            fixture.queue.set_max_gas_price(cap.map(GasPrice::new));
            let mut transaction = fixture.queue.get_next_inmempool_transaction().await.unwrap();
            transaction.authorization_list = authorized.then(|| vec![fixture.authorization()]);
            let original = GasPriceResult {
                max_fee: MaxFee::new(100),
                max_priority_fee: MaxPriorityFee::new(1),
                min_wait_time_estimate: None,
                max_wait_time_estimate: None,
            };
            let result = TransactionsQueues::estimate_admission_gas(
                &mut fixture.queue,
                &transaction,
                original,
                None,
            )
            .await;
            let requests = fixture.requests.lock().await;
            let estimate_fees: Vec<_> = requests
                .iter()
                .filter(|request| request["method"] == "eth_estimateGas")
                .map(|request| request["params"][0]["maxFeePerGas"].as_str().unwrap())
                .collect();
            assert_eq!(estimate_fees.len(), attempts);
            assert_eq!(estimate_fees[0], "0x64");
            if attempts == 2 {
                assert_eq!(estimate_fees[1], "0xc8");
            }
            if clears {
                let (limit, price) = result.unwrap();
                assert_eq!(limit, GasLimit::new(48_000));
                assert_eq!(price.max_fee, MaxFee::new(200));
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[tokio::test]
    async fn ordinary_ethereum_first_send_revalidates_admitted_gas_before_signing() {
        let mut fixture = Fixture::new(1, Value::Null, 0).await;
        fixture.set_estimation_fees(200, 150, None).await;
        let mut transaction = fixture.queue.get_next_inmempool_transaction().await.unwrap();
        transaction.status = TransactionStatus::PENDING;
        let cached = GasPriceResult {
            max_fee: MaxFee::new(100),
            max_priority_fee: MaxPriorityFee::new(1),
            min_wait_time_estimate: None,
            max_wait_time_estimate: None,
        };
        let (admitted_limit, admitted_fee) = TransactionsQueues::estimate_admission_gas(
            &mut fixture.queue,
            &transaction,
            cached.clone(),
            None,
        )
        .await
        .unwrap();
        transaction.gas_limit = Some(admitted_limit);
        assert_eq!(admitted_fee.max_fee, MaxFee::new(200));

        // A worker may see the old cache after admission or restart, and the
        // block may have advanced. Use the exact gas and price being signed.
        fixture.set_estimation_fees(240, 210, None).await;
        let (block_limit, minimum_fee) = fixture.block_gas_parameters().await;
        assert_eq!(minimum_fee, Some(210));
        let (limit, price) = fixture
            .queue
            .prepare_first_ethereum_send_fee(&transaction, &cached, block_limit, minimum_fee)
            .await
            .unwrap()
            .unwrap();
        let envelope = transaction
            .to_eip1559_typed_transaction_with_gas_limit(Some(&price), Some(limit))
            .unwrap();
        let TypedTransaction::Eip1559(envelope) = envelope else {
            panic!("ordinary Ethereum must keep its EIP-1559 envelope");
        };
        assert_eq!(envelope.max_fee_per_gas, 240);
        assert_eq!(envelope.gas_limit, admitted_limit.into_inner() as u64);

        fixture.budget.lock().await.balance = alloy::primitives::U256::from(10_000_000u64);
        assert!(
            fixture
                .queue
                .prepare_first_ethereum_send_fee(&transaction, &cached, block_limit, minimum_fee)
                .await
                .is_err(),
            "a refreshed maximum fee must still be affordable before signing"
        );
        transaction.sent_with_gas = Some(admitted_fee);
        assert!(
            fixture
                .queue
                .prepare_first_ethereum_send_fee(&transaction, &cached, block_limit, minimum_fee)
                .await
                .unwrap()
                .is_none(),
            "a previous fee-bearing send must retain the existing replacement path"
        );
        transaction.sent_with_gas = None;
        transaction.is_noop = true;
        assert!(
            fixture
                .queue
                .prepare_first_ethereum_send_fee(&transaction, &cached, block_limit, minimum_fee)
                .await
                .unwrap()
                .is_none(),
            "a no-op cancellation must retain its existing gas path"
        );
    }
}

#[cfg(test)]
mod receipt_poll_tests {
    use super::super::transactions_queue::elysium_tests::Fixture;
    use super::*;
    use crate::transaction::types::TransactionHash;
    use alloy::primitives::keccak256;
    use serde_json::{json, Value};

    async fn receipt_requests(requests: &Arc<Mutex<Vec<Value>>>) -> Vec<Value> {
        requests
            .lock()
            .await
            .iter()
            .filter(|request| request["method"] == "eth_getTransactionReceipt")
            .map(|request| request["params"][0].clone())
            .collect()
    }

    fn receipt(hash: TransactionHash, address: crate::common_types::EvmAddress) -> Value {
        json!({
            "transactionHash": hash, "transactionIndex": "0x0",
            "blockHash": format!("0x{}", "11".repeat(32)), "blockNumber": "0x1",
            "from": address, "to": address, "contractAddress": null,
            "cumulativeGasUsed": "0x5208", "gasUsed": "0x5208",
            "effectiveGasPrice": "0x64", "logs": [],
            "logsBloom": format!("0x{}", "00".repeat(256)), "status": "0x1", "type": "0x4"
        })
    }

    #[tokio::test]
    async fn receipt_polling_deduplicates_original_preserves_competition_and_bounds_fast_chain_waits(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(url) =
            std::env::var("RRELAYER_TEST_DATABASE_URL").ok().filter(|url| !url.is_empty())
        else {
            return Ok(());
        };
        // Isolate from the existing database concurrency test and its schema setup.
        let mut isolated_url = reqwest::Url::parse(&url)?;
        assert!(matches!(isolated_url.host_str(), Some("127.0.0.1" | "localhost" | "::1")));
        let admin = PostgresClient::for_test_url(&url).await?;
        let database = format!("receipt_poll_{}", uuid::Uuid::new_v4().simple());
        admin.batch_execute(&format!("CREATE DATABASE \"{database}\"")).await?;
        isolated_url.set_path(&format!("/{database}"));
        let mut db = PostgresClient::for_test_url(isolated_url.as_str()).await?;
        crate::schema::apply_schema(&db).await?;
        let mut failures = Vec::new();

        for (chain_id, block_time, expected_wait, capped) in [
            (99901, 250, 250, false),
            (1, 12_000, 1_200, false),
            (99901, 250, 250, true),
            (1, 12_000, 1_200, true),
        ] {
            db.batch_execute("UPDATE relayer.sender_deployment SET enabled=FALSE").await?;
            let fixture = Fixture::new(chain_id, json!(null), 0).await;
            if capped {
                fixture.cache_gas_prices().await;
            }
            let requests = fixture.requests.clone();
            let budget = fixture.budget.clone();
            let queue = Arc::new(Mutex::new(fixture.queue));
            let mut original = queue.lock().await.get_next_inmempool_transaction().await.unwrap();
            if capped {
                let sent_gas = GasPriceResult {
                    max_fee: crate::gas::MaxFee::new(10),
                    max_priority_fee: crate::gas::MaxPriorityFee::new(10),
                    min_wait_time_estimate: None,
                    max_wait_time_estimate: None,
                };
                assert!(queue.lock().await.is_at_max_gas_price_cap(&sent_gas).await);
                original.sent_with_gas = Some(sent_gas);
            }
            let relayer_id = original.relayer_id;
            db.execute("INSERT INTO relayer.record(id,name,chain_id,address,wallet_index) VALUES($1,'receipt-fixture',$2,$3,0)",
                &[&relayer_id, &original.chain_id, &original.from]).await?;
            db.save_transaction(&relayer_id, &original).await?;
            if capped {
                db.execute(
                    "UPDATE relayer.transaction SET sent_at=NOW() WHERE id=$1",
                    &[&original.id],
                )
                .await?;
            }
            let original_hash = original.known_transaction_hash.unwrap();
            db.execute("INSERT INTO relayer.transaction_attempt(transaction_id,hash,signed_envelope,gas) VALUES($1,$2,''::bytea,'{}'::jsonb)",
                &[&original.id, &original_hash]).await?;

            assert!(queue.lock().await.receipt_for_competition(&db, &original).await?.is_none());
            if receipt_requests(&requests).await != [json!(original_hash)] {
                failures.push(format!(
                    "single-hash lookup duplicated: chain={chain_id} capped={capped}"
                ));
            }

            if chain_id == 99901 && !capped {
                // An active snapshot's newer known hash must not be lost by deduplication.
                requests.lock().await.clear();
                let mut active = original.clone();
                let newer_hash =
                    TransactionHash::from_alloy_hash(&keccak256(b"newer receipt fixture"));
                active.known_transaction_hash = Some(newer_hash);
                assert!(queue.lock().await.receipt_for_competition(&db, &active).await?.is_none());
                if receipt_requests(&requests).await != [json!(original_hash), json!(newer_hash)] {
                    failures.push("active snapshot did not check each known hash once".to_string());
                }

                let mut competitor = original.clone();
                competitor.id = TransactionId::new();
                let competitor_hash =
                    TransactionHash::from_alloy_hash(&keccak256(b"competitor receipt fixture"));
                competitor.known_transaction_hash = Some(competitor_hash);
                db.save_competitor(&original.id, &competitor, false).await?;
                queue
                    .lock()
                    .await
                    .add_competitor_to_inmempool_transaction(
                        &original.id,
                        competitor.clone(),
                        super::super::types::CompetitionType::Replace,
                    )
                    .await?;

                requests.lock().await.clear();
                budget
                    .lock()
                    .await
                    .receipts
                    .insert(original_hash.hex(), receipt(original_hash, original.from));
                let (winner, _) =
                    queue.lock().await.receipt_for_competition(&db, &competitor).await?.unwrap();
                assert_eq!(winner.id, original.id);
                assert_eq!(receipt_requests(&requests).await, [json!(original_hash)]);

                requests.lock().await.clear();
                budget.lock().await.receipts.clear();
                budget
                    .lock()
                    .await
                    .receipts
                    .insert(competitor_hash.hex(), receipt(competitor_hash, competitor.from));
                let (winner, _) =
                    queue.lock().await.receipt_for_competition(&db, &competitor).await?.unwrap();
                assert_eq!(winner.id, competitor.id);
                assert_eq!(
                    receipt_requests(&requests).await,
                    [json!(original_hash), json!(competitor_hash)]
                );
                budget.lock().await.receipts.clear();
            }

            requests.lock().await.clear();
            let release = crate::sender_handoff::release_id();
            db.execute(
                "UPDATE relayer.sender_deployment SET enabled=TRUE,active_release=$1",
                &[&release],
            )
            .await?;
            let queues = TransactionsQueues {
                coordination: HashMap::from([(relayer_id, Arc::new(QueueCoordination::default()))]),
                draining: HashMap::new(),
                processing_slots: Arc::new(Semaphore::new(4)),
                queues: HashMap::from([(relayer_id, queue)]),
                relayer_block_times_ms: HashMap::from([(relayer_id, block_time)]),
                gas_oracle_cache: Arc::new(Mutex::new(GasOracleCache::new())),
                blob_gas_oracle_cache: Arc::new(Mutex::new(BlobGasOracleCache::new())),
                db,
                cache: Arc::new(Cache::new().await),
                webhook_manager: None,
                safe_proxy_manager: Arc::new(SafeProxyManager::new(vec![])),
            };
            let registry = Arc::new(Mutex::new(queues));
            let (_slot, mut processing) =
                super::super::start::processing_context(&registry, &relayer_id).await;
            // An admitted worker must not hold the shared registry while it
            // waits on its chain. API intake and other chains can still enter.
            let registry_guard = registry.try_lock().expect("worker held global registry lock");
            let mut competing = registry_guard.for_processing(&relayer_id);
            let original_token = registry_guard.db.pool.token.clone();
            drop(registry_guard);
            let sender = processing.db.acquire_sender(&relayer_id, true).await?;
            let token = processing.db.pool.token.clone();
            assert!(matches!(
                competing.db.acquire_sender(&relayer_id, true).await,
                Err(crate::postgres::PostgresError::SenderUnavailable)
            ));
            // Another signer can acquire its fence without overwriting this
            // worker's token or the API intake client's token.
            let other_id = RelayerId::new();
            processing.db.execute(
                "INSERT INTO relayer.record(id,name,chain_id,address,wallet_index) VALUES($1,'other-signer',$2,$3,1)",
                &[&other_id, &original.chain_id, &vec![0x99u8; 20]],
            ).await?;
            let other_sender = competing.db.acquire_sender(&other_id, true).await?;
            assert_ne!(token, competing.db.pool.token);
            assert_eq!(token, processing.db.pool.token);
            assert_eq!(original_token, registry.lock().await.db.pool.token);
            other_sender.release().await;
            sender.release().await;
            // Wait for asynchronous advisory unlock before processing the same signer.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            let result = loop {
                match processing.process_single_inmempool(&relayer_id).await {
                    Ok(result) => break result,
                    Err(ProcessInmempoolTransactionError::Handoff(
                        crate::postgres::PostgresError::SenderUnavailable,
                    )) => {
                        assert!(std::time::Instant::now() < deadline);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            assert_eq!(result.status, ProcessInmempoolStatus::StillInmempool);
            if result.process_again_after != expected_wait {
                failures.push(format!("receipt wait: chain={chain_id} capped={capped} actual={} expected={expected_wait}", result.process_again_after));
            }
            assert!(!requests
                .lock()
                .await
                .iter()
                .any(|request| request["method"] == "eth_sendRawTransaction"));
            drop(competing);
            let active_queue = processing.get_transactions_queue(&relayer_id).unwrap();
            let other_queue = Fixture::new(chain_id, json!(null), 0).await.queue;
            registry.lock().await.queues.insert(other_id, Arc::new(Mutex::new(other_queue)));
            let active = active_queue.lock().await;
            let deleting_registry = registry.clone();
            let mut deletion = tokio::spawn(async move {
                TransactionsQueues::delete_queue(&deleting_registry, &relayer_id).await;
            });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(25), &mut deletion)
                    .await
                    .is_err(),
                "deletion returned while an operation was active"
            );
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let registry_guard = registry.lock().await;
                    if registry_guard.get_transactions_queue(&relayer_id).is_none() {
                        drop(registry_guard);
                        let (_slot, mut other) =
                            super::super::start::processing_context(&registry, &other_id).await;
                        let queue = other.get_transactions_queue(&other_id).unwrap();
                        assert!(queue
                            .lock()
                            .await
                            .get_next_inmempool_transaction()
                            .await
                            .is_some());
                        let sender = other.db.acquire_sender(&other_id, true).await.unwrap();
                        sender.release().await;
                        break;
                    }
                    drop(registry_guard);
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("unrelated sender could not progress during deletion");
            assert!(!deletion.is_finished());
            deletion.abort();
            assert!(deletion.await.unwrap_err().is_cancelled());
            {
                let retry = TransactionsQueues::delete_queue(&registry, &relayer_id);
                tokio::pin!(retry);
                assert!(tokio::time::timeout(std::time::Duration::from_millis(25), &mut retry)
                    .await
                    .is_err());
                drop(active);
                retry.await;
            }
            assert!(registry.lock().await.for_processing(&relayer_id).queues.is_empty());
            drop(processing);
            db = Arc::try_unwrap(registry).ok().unwrap().into_inner().db;
        }
        drop(db);
        // Owned pools and sender guards close normally; never terminate database clients.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let clients: i64 = admin
                .query_one("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=$1", &[&database])
                .await?
                .get(0);
            if clients == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "receipt fixture database still has owned clients"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        admin.batch_execute(&format!("DROP DATABASE \"{database}\"")).await?;
        assert!(failures.is_empty(), "receipt regressions: {failures:?}");
        Ok(())
    }
}
