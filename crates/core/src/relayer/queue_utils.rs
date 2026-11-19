use std::sync::Arc;
use tracing::warn;

use crate::{
    app_state::AppState,
    network::ChainId,
    provider::EvmProvider,
    relayer::{cache::invalidate_relayer_cache, Relayer},
    shared::HttpError,
    transaction::{
        queue_system::{TransactionsQueueSetup, TransactionsQueues},
        NonceManager,
    },
};

/// Starts the transaction queue for a relayer and initializes it with the current nonce.
///
/// This is shared logic used by both `create_relayer` and `import_relayer` endpoints.
pub async fn start_relayer_queue(
    state: &Arc<AppState>,
    relayer: Relayer,
    provider: &EvmProvider,
    chain_id: &ChainId,
) -> Result<(), HttpError> {
    let provider = provider.for_relayer(&relayer).map_err(|_| {
        crate::shared::internal_server_error(Some(
            "Fixed Base gateway configuration invalid".to_string(),
        ))
    })?;
    let Some(provider) = provider else {
        return Ok(());
    };
    let current_nonce = match provider.get_nonce(&relayer).await {
        Ok(nonce) => nonce,
        Err(error) if provider.is_isolated_limit_base() => {
            warn!(
                "Fixed Base gateway nonce unavailable for relayer {}; leaving queue offline: {}",
                relayer.id, error
            );
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    let network_config = state.network_configs.iter().find(|config| &config.chain_id == chain_id);

    let gas_bump_config =
        network_config.map(|config| config.gas_bump_blocks_every.clone()).unwrap_or_default();

    let max_gas_price_multiplier =
        network_config.map(|config| config.max_gas_price_multiplier).unwrap_or(2);

    let relayer_id = relayer.id;

    TransactionsQueues::add_new_relayer(
        state.transactions_queues.clone(),
        TransactionsQueueSetup::new(
            relayer,
            provider.clone(),
            NonceManager::new(current_nonce),
            Default::default(),
            Default::default(),
            Default::default(),
            state.safe_proxy_manager.clone(),
            gas_bump_config,
            max_gas_price_multiplier,
        ),
    )
    .await;

    invalidate_relayer_cache(&state.cache, &relayer_id).await;

    Ok(())
}

pub(crate) async fn discover_relayer_queues(state: Arc<AppState>) {
    use futures_util::stream::{self, StreamExt};
    let chains: Vec<_> = state.network_configs.iter().map(|config| config.chain_id).collect();
    let mut shutdown = crate::shutdown::subscribe_to_shutdown();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = interval.tick() => {
                let known: Vec<_> = state.transactions_queues.lock().await.queues.keys().copied().collect();
                match state.db.missing_queue_relayers(&chains, &known).await {
                    Ok(missing) => {
                        stream::iter(missing).for_each_concurrent(4, |relayer| async {
                            let chain_id = relayer.chain_id;
                            if let Some(provider) = crate::provider::find_provider_for_chain_id(&state.evm_providers, &chain_id).await {
                                let id = relayer.id;
                                if let Err(error) = start_relayer_queue(&state, relayer, provider, &chain_id).await {
                                    warn!("Could not discover relayer queue {}: {:?}", id, error);
                                }
                            }
                        }).await;
                    }
                    Err(error) => warn!("Could not discover relayer queues: {}", error),
                }
            }
        }
    }
}
