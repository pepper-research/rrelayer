use std::sync::Arc;

use super::types::{Transaction, TransactionId};
use crate::{
    postgres::{PostgresClient, PostgresError},
    shared::cache::Cache,
};

pub async fn get_transaction_by_id(
    cache: &Arc<Cache>,
    db: &PostgresClient,
    id: TransactionId,
) -> Result<Option<Transaction>, PostgresError> {
    // Another process can advance this transaction after a handoff. Process-local
    // cache invalidation cannot establish current durable status.
    let _ = cache;
    db.get_transaction(&id).await
}
