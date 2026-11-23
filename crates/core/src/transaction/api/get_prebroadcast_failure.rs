use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    app_state::AppState,
    shared::{bad_request, HttpError},
    transaction::types::{Transaction, TransactionId},
};

#[derive(Serialize)]
pub struct PrebroadcastFailure {
    kind: &'static str,
    transaction: Transaction,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrebroadcastFailureQuery {
    legacy_external_id: Option<String>,
}

/// A missing hash or generic FAILED status cannot establish terminal rejection.
/// Legacy journal proof requires the relayer operator and both exact IDs.
pub async fn get_prebroadcast_failure(
    State(state): State<Arc<AppState>>,
    Path(id): Path<TransactionId>,
    Query(query): Query<PrebroadcastFailureQuery>,
    headers: HeaderMap,
) -> Result<Json<Option<PrebroadcastFailure>>, HttpError> {
    let legacy_external_id = query.legacy_external_id.as_deref();
    let kind = if let Some(external_id) = legacy_external_id {
        state.validate_basic_auth_valid(&headers)?;
        if !external_id.strip_prefix("send_").is_some_and(|suffix| {
            suffix.len() == 32
                && suffix.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            return Err(bad_request("Invalid legacy send external ID".to_string()));
        }
        "legacy_terminal_prebroadcast_v1"
    } else {
        state.validate_allowed_passed_basic_auth(&headers)?;
        "terminal_prebroadcast_v1"
    };

    // Read the durable ledger. Failed rows cannot enter replacement/cancel queues.
    let transaction = state.db.get_prebroadcast_failure(&id, legacy_external_id).await?;
    if let Some(transaction) = &transaction {
        state.validate_auth_basic_or_api_key(&headers, &transaction.from, &transaction.chain_id)?;
    }
    Ok(Json(transaction.map(|transaction| PrebroadcastFailure { kind, transaction })))
}
