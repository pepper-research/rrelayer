use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde::Serialize;

use crate::{
    app_state::AppState,
    shared::HttpError,
    transaction::types::{Transaction, TransactionId},
};

#[derive(Serialize)]
pub struct PrebroadcastFailure {
    kind: &'static str,
    transaction: Transaction,
}

/// Only the keyed submission owner can establish terminal rejection. A missing
/// hash or generic FAILED status is not proof that no signed attempt escaped.
pub async fn get_prebroadcast_failure(
    State(state): State<Arc<AppState>>,
    Path(id): Path<TransactionId>,
    headers: HeaderMap,
) -> Result<Json<Option<PrebroadcastFailure>>, HttpError> {
    state.validate_allowed_passed_basic_auth(&headers)?;
    // Read the durable ledger, never the status cache. Keyed retries return the
    // original rejection; failed rows cannot enter replacement/cancel queues.
    let transaction = state.db.get_prebroadcast_failure(&id).await?;
    if let Some(transaction) = &transaction {
        state.validate_auth_basic_or_api_key(&headers, &transaction.from, &transaction.chain_id)?;
    }
    Ok(Json(
        transaction.map(|transaction| PrebroadcastFailure {
            kind: "terminal_prebroadcast_v1",
            transaction,
        }),
    ))
}
