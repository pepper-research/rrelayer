use std::sync::Arc;

use crate::shared::HttpError;
use crate::{
    app_state::AppState,
    relayer::{cache::invalidate_relayer_cache, types::RelayerId},
    transaction::queue_system::TransactionsQueues,
};
use axum::http::HeaderMap;
use axum::{
    extract::{Path, State},
    http::StatusCode,
};

/// Soft deletes a relayer from the system.
pub async fn delete_relayer(
    State(state): State<Arc<AppState>>,
    Path(relayer_id): Path<RelayerId>,
    headers: HeaderMap,
) -> Result<StatusCode, HttpError> {
    state.validate_basic_auth_valid(&headers)?;

    if !state.db.delete_relayer(&relayer_id).await? {
        return Err(crate::shared::conflict("Relayer has outstanding transactions".to_string()));
    }

    invalidate_relayer_cache(&state.cache, &relayer_id).await;
    TransactionsQueues::delete_queue(&state.transactions_queues, &relayer_id).await;
    Ok(StatusCode::NO_CONTENT)
}
