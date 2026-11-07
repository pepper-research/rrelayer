use std::sync::Arc;

use super::types::TransactionId;
use crate::shared::cache::Cache;

const TRANSACTION_CACHE_KEY: &str = "transaction";

fn build_transaction_cache_key(id: &TransactionId) -> String {
    format!("{}__{}", TRANSACTION_CACHE_KEY, id)
}

pub async fn invalidate_transaction_no_state_cache(cache: &Arc<Cache>, id: &TransactionId) {
    cache.delete(&build_transaction_cache_key(id).to_string()).await;
}
