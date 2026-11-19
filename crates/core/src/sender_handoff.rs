//! Exclusive queue steps plus a durable fence. A lost connection can leave an RPC
//! in flight; safety also requires persisted signed bytes and immutable nonces.
use crate::{
    app_state::AppState,
    postgres::{PostgresClient, PostgresError},
    relayer::RelayerId,
    shared::{internal_server_error, HttpError},
};
use axum::{extract::State, http::HeaderMap, Json};
use bb8::PooledConnection;
use serde::Deserialize;
use std::sync::Arc;

type Connection = PooledConnection<'static, crate::postgres::ConnectionManager>;

pub struct SenderGuard {
    connection: Option<Connection>,
    key: i64,
}
impl SenderGuard {
    async fn unlock(mut conn: Connection, key: i64) {
        // Cancellation or an unlock error must destroy the connection rather
        // than return a live advisory lock to the pool.
        conn.discard = true;
        if let Ok(row) = conn.query_one("SELECT pg_advisory_unlock($1)", &[&key]).await {
            if row.get::<_, bool>(0) {
                conn.discard = false;
            }
        }
    }

    pub async fn release(mut self) {
        if let Some(conn) = self.connection.take() {
            Self::unlock(conn, self.key).await;
        }
    }
}
impl Drop for SenderGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            tokio::spawn(Self::unlock(conn, self.key));
        }
    }
}

pub fn release_id() -> String {
    std::env::var("RRELAYER_RELEASE_ID").unwrap_or_default()
}

impl PostgresClient {
    pub async fn acquire_sender(
        &mut self,
        relayer: &RelayerId,
        processing: bool,
    ) -> Result<SenderGuard, PostgresError> {
        let conn = self.pool.inner.get_owned().await?;
        conn.batch_execute("SET statement_timeout = '10s'; SET idle_in_transaction_session_timeout = '10s'; SET idle_session_timeout = '15s'").await?;
        let row = conn.query_opt(
            "SELECT r.chain_id, r.address, hashtextextended(r.chain_id::text || ':' || encode(r.address, 'hex'), 761832) AS key,
                d.enabled AND (NOT $1 OR d.active_release=$2) AS eligible
             FROM relayer.record r CROSS JOIN relayer.sender_deployment d
             WHERE d.singleton AND r.id=$3 AND NOT r.deleted",
            &[&processing, &release_id(), relayer],
        ).await?.ok_or(PostgresError::RelayerDeleted)?;
        if !row.get::<_, bool>("eligible") {
            return Err(PostgresError::SenderUnavailable);
        }
        let chain: i64 = row.get("chain_id");
        let signer: Vec<u8> = row.get("address");
        let key: i64 = row.get("key");
        let started = std::time::Instant::now();
        loop {
            let locked: bool =
                conn.query_one("SELECT pg_try_advisory_lock($1)", &[&key]).await?.get(0);
            if locked {
                break;
            }
            if processing || started.elapsed() > std::time::Duration::from_secs(17) {
                return Err(PostgresError::SenderUnavailable);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let guard = SenderGuard { connection: Some(conn), key };
        let conn = guard.connection.as_ref().unwrap();
        // Abandoned database sessions have a bounded lifetime, even if the process
        // is SIGSTOP'ed. Normal operations must finish inside this interval.
        conn.batch_execute("SET idle_session_timeout = '15s'").await?;
        let allowed: bool = conn.query_one("SELECT enabled AND (NOT $1 OR active_release = $2) FROM relayer.sender_deployment WHERE singleton", &[&processing, &release_id()]).await?.get(0);
        if !allowed {
            return Err(PostgresError::SenderUnavailable);
        }
        let token = uuid::Uuid::new_v4().to_string();
        conn.execute("INSERT INTO relayer.sender_owner(chain_id,signer,token) VALUES($1,$2,$3) ON CONFLICT(chain_id,signer) DO UPDATE SET token=EXCLUDED.token", &[&chain,&signer,&token]).await?;
        self.pool.token = token;
        Ok(guard)
    }
}

async fn queues_ready(state: &AppState) -> Result<bool, PostgresError> {
    let chains: Vec<_> = state.network_configs.iter().map(|config| config.chain_id).collect();
    let known: Vec<_> = state.transactions_queues.lock().await.queues.keys().copied().collect();
    for relayer in state.db.missing_queue_relayers(&chains, &known).await? {
        let Some(provider) =
            crate::provider::find_provider_for_chain_id(&state.evm_providers, &relayer.chain_id)
                .await
        else {
            return Ok(false);
        };
        if !matches!(provider.for_relayer(&relayer), Ok(None))
            || state.db.has_unresolved_sender_work(&relayer.id).await?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub async fn readiness(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, HttpError> {
    let row = state.db.query_one("SELECT protocol, enabled, active_release FROM relayer.sender_deployment WHERE singleton", &[]).await?;
    let active: Option<String> = row.get("active_release");
    let enabled: bool = row.get("enabled");
    let queues_ready = queues_ready(&state).await?;
    let unresolved: i64 = state.db.query_one("SELECT COUNT(*) FROM relayer.transaction WHERE status IN ('PENDING','INMEMPOOL','MINED')", &[]).await?.get(0);
    Ok(Json(
        serde_json::json!({"activeRelease":active,"unresolvedTransactions":unresolved,"protocol": row.get::<_,i32>("protocol"), "release":release_id(),
        "queuesReady": queues_ready, "durableIntakeReady": enabled && queues_ready, "processing":enabled && queues_ready && active.as_deref()==Some(&release_id())}),
    ))
}

#[derive(Deserialize)]
pub struct Activate {
    expected_release: Option<String>,
    bootstrap: bool,
}
pub async fn activate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<Activate>,
) -> Result<Json<serde_json::Value>, HttpError> {
    state.validate_basic_auth_valid(&headers)?;
    if !queues_ready(&state).await? {
        return Err(internal_server_error(Some("Configured relayer queues are not ready".into())));
    }
    let release = release_id();
    if release.is_empty() {
        return Err(internal_server_error(Some("RRELAYER_RELEASE_ID is required".into())));
    }
    let mut conn = state.db.pool.get().await.map_err(PostgresError::from)?;
    let tx = conn.transaction().await.map_err(PostgresError::from)?;
    let row = tx.query_one("SELECT enabled, active_release FROM relayer.sender_deployment WHERE singleton FOR UPDATE", &[]).await.map_err(PostgresError::from)?;
    let active: Option<String> = row.get("active_release");
    if active != request.expected_release {
        return Err(internal_server_error(Some(
            "Active release changed; reconcile deployment".into(),
        )));
    }
    if !row.get::<_, bool>("enabled") {
        // This endpoint is called only after the deployment controller proves all
        // legacy ECS tasks stopped. Unknown legacy sends cannot be adopted safely.
        let unresolved: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM relayer.transaction WHERE status IN ('PENDING','INMEMPOOL','MINED'))", &[]).await.map_err(PostgresError::from)?.get(0);
        if !request.bootstrap || unresolved {
            return Err(internal_server_error(Some(
                "Bootstrap requires stopped legacy tasks and no unresolved legacy transactions"
                    .into(),
            )));
        }
    }
    tx.execute(
        "UPDATE relayer.sender_deployment SET enabled=TRUE, active_release=$1 WHERE singleton",
        &[&release],
    )
    .await
    .map_err(PostgresError::from)?;
    tx.commit().await.map_err(PostgresError::from)?;
    Ok(Json(serde_json::json!({"activeRelease":release,"protocol":1})))
}

#[cfg(debug_assertions)]
pub async fn test_barrier(point: &str) {
    if std::env::var("RRELAYER_TEST_POINT").ok().as_deref() != Some(point) {
        return;
    }
    let Ok(dir) = std::env::var("RRELAYER_TEST_BARRIER_DIR") else {
        return;
    };
    let path = std::path::Path::new(&dir).join(point);
    std::fs::write(&path, "reached").expect("test barrier");
    while !path.with_extension("release").exists() {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
