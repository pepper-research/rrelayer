use super::send_random_transaction::select_random_relayer;
use super::send_transaction::{
    send_transaction_with_id, RelayTransactionRequest, SendTransactionResult,
};
use crate::app_state::AppState;
use crate::network::ChainId;
use crate::postgres::PostgresError;
use crate::shared::{bad_request, internal_server_error, HttpError};
use crate::transaction::types::TransactionId;
use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use once_cell::sync::Lazy;
use std::{str::FromStr, sync::Arc};
use tokio::sync::Mutex;

// Do not let waiting requests fill the pool needed by the queue's durable write.
static SUBMISSIONS: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// The caller's UUID is also the durable transaction primary key. The database
/// lock serializes retries across processes; the queue commits before broadcast.
pub async fn send_idempotent_transaction(
    State(state): State<Arc<AppState>>,
    Path(chain_id): Path<ChainId>,
    headers: HeaderMap,
    Json(request): Json<RelayTransactionRequest>,
) -> Result<Json<SendTransactionResult>, HttpError> {
    state.validate_allowed_passed_basic_auth(&headers)?;
    let external_id = request
        .external_id
        .as_deref()
        .ok_or_else(|| bad_request("externalId must be a UUID".to_string()))?;
    let id = TransactionId::from_str(external_id)
        .map_err(|_| bad_request("externalId must be a UUID".to_string()))?;
    let _submission = SUBMISSIONS.lock().await;
    let started = std::time::Instant::now();
    loop {
        let mut connection = state.db.pool.get().await.map_err(PostgresError::from)?;
        let lock = connection.transaction().await.map_err(PostgresError::from)?;
        let acquired = lock
            .query_one(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                &[&format!("submission:{id}")],
            )
            .await
            .map_err(PostgresError::from)?
            .get::<_, bool>(0);
        if !acquired {
            drop(lock);
            drop(connection);
            if started.elapsed() > std::time::Duration::from_secs(20) {
                return Err(internal_server_error(Some(
                    "Submission is busy; retry the same idempotency key".to_string(),
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            continue;
        }
        if let Some(existing) = state.db.get_transaction(&id).await? {
            // Auth must target the original relayer, even if random selection changed.
            state.validate_auth_basic_or_api_key(&headers, &existing.from, &existing.chain_id)?;
            // Queue cancellation/replacement may change the current row's calldata;
            // bind retries to the immutable first audit record instead.
            let original = lock.query_one(
            "SELECT \"to\"=$2 AND value=$3 AND data=$4 AND chain_id=$5 AND external_id=$6 AND blobs IS NULL AS matches FROM relayer.transaction_audit_log WHERE id=$1 ORDER BY history_id LIMIT 1",
            &[&id, &request.to, &request.value, &request.data, &chain_id, &external_id],
        ).await.map_err(PostgresError::from)?;
            if !original.get::<_, bool>("matches") || request.blobs.is_some() {
                return Err(bad_request(
                    "Idempotency key is already bound to a different transaction".to_string(),
                ));
            }
            let hash = existing.known_transaction_hash.ok_or_else(|| {
                bad_request(
                    "Original transaction failed before signing; inspect its status".to_string(),
                )
            })?;
            return Ok(Json(SendTransactionResult { id, hash }));
        }
        // Blob requests need their own immutable payload comparison.
        if request.blobs.is_some() {
            return Err(bad_request("Idempotent blob submission is not supported".to_string()));
        }
        let relayer = select_random_relayer(&state, &chain_id).await?;
        let result = send_transaction_with_id(relayer, request, &state, &headers, Some(id)).await?;
        lock.commit().await.map_err(|_| {
            internal_server_error(Some("Could not finish idempotent submission".to_string()))
        })?;
        return Ok(Json(result));
    }
}
