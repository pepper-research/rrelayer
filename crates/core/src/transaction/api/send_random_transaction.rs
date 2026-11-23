use crate::app_state::AppState;
use crate::network::ChainId;
use crate::relayer::Relayer;
use crate::shared::{bad_request, common_types::EvmAddress, not_found, HttpError};
use crate::transaction::api::send_transaction::send_transaction;
use crate::transaction::api::{RelayTransactionRequest, SendTransactionResult};
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use rand::seq::SliceRandom;
use std::sync::Arc;

/// Handles random relayer selection for transaction requests
/// across multiple relayers on the same chain.
///
/// This endpoint selects a random available (non-paused, non-internal) relayer
/// and forwards the transaction request to it.
pub async fn send_transaction_random(
    State(state): State<Arc<AppState>>,
    Path(chain_id): Path<ChainId>,
    headers: HeaderMap,
    Json(transaction): Json<RelayTransactionRequest>,
) -> Result<Json<SendTransactionResult>, HttpError> {
    state.validate_allowed_passed_basic_auth(&headers)?;
    let relayer = select_available_relayer(&state, &chain_id, None).await?;
    let result = send_transaction(relayer, transaction, &state, &headers).await?;
    Ok(Json(result))
}

/// Selects an available relayer for the chain, pinned when requested.
///
/// Filters out paused, internal-only, and relayers only allowed for random selection.
/// Note: The random relayer feature must be explicitly enabled via `allowed_random_relayers`
/// config for the network, otherwise all relayers will be filtered out.
pub(super) async fn select_available_relayer(
    state: &Arc<AppState>,
    chain_id: &ChainId,
    expected_sender: Option<EvmAddress>,
) -> Result<Relayer, HttpError> {
    let relayers = state.db.get_all_relayers_for_chain(chain_id).await?;

    if relayers.is_empty() {
        return Err(not_found(format!("No relayers found for chain {}", chain_id)));
    }

    // TODO: it should be smart enough to also only pick the one with enough native funds to send the tx
    let available_relayers: Vec<_> = relayers
        .into_iter()
        .filter(|r| {
            !r.paused
                && expected_sender.is_none_or(|sender| sender == r.address)
                && !crate::provider::is_limit_base_address(&r.chain_id, &r.address)
                && !state.relayer_internal_only.restricted(&r.address, &r.chain_id)
                && state.relayers_allowed_for_random.is_allowed(&r.address, &r.chain_id)
        })
        .collect();
    if expected_sender.is_some() {
        return available_relayers.into_iter().next().ok_or_else(|| {
            bad_request("Expected sender is not an available relayer for this chain".to_string())
        });
    }
    available_relayers.choose(&mut rand::thread_rng()).cloned().ok_or_else(|| {
        bad_request(format!(
            "No available relayers for chain {} (all relayers are paused, internal-only, or not allowed for random selection)",
            chain_id
        ))
    })
}
