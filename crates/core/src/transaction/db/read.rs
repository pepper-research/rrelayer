use super::builders::build_transaction_from_transaction_view;
use crate::{
    postgres::{PostgresClient, PostgresError},
    relayer::RelayerId,
    shared::common_types::{PagingContext, PagingResult},
    transaction::types::{Transaction, TransactionHash, TransactionId, TransactionStatus},
};

impl PostgresClient {
    pub async fn count_transactions_by_status(
        &self,
        relayer_id: &crate::relayer::RelayerId,
        status: crate::transaction::types::TransactionStatus,
    ) -> Result<usize, PostgresError> {
        self.query_one(
            "SELECT COUNT(*) FROM relayer.transaction WHERE relayer_id=$1 AND status=$2",
            &[relayer_id, &status],
        )
        .await
        .map(|row| row.get::<_, i64>(0) as usize)
    }

    pub async fn get_prebroadcast_failure(
        &self,
        id: &TransactionId,
        legacy_external_id: Option<&str>,
    ) -> Result<Option<Transaction>, PostgresError> {
        let row = self.query_one_or_none(
            "SELECT t.* FROM relayer.transaction t
             WHERE t.id=$1 AND t.external_id=COALESCE($2::text, t.id::text) AND t.status='FAILED'
               AND NOT t.broadcast_attempted AND t.hash IS NULL
               AND t.gas_limit IS NULL AND t.signed_envelope IS NULL
               AND t.sent_at IS NULL AND t.mined_at IS NULL AND t.confirmed_at IS NULL
               AND t.cancelled_by_transaction_id IS NULL AND t.competition_kind IS NULL
               AND ($2::text IS NULL OR (
                   t.authorization_list IS NULL AND t.blobs IS NULL
                   AND t.failed_at IS NOT NULL
                   AND t.failed_reason LIKE 'Failed to send transaction as always failing on gas estimation:%'
                   AND NOT EXISTS (SELECT 1 FROM relayer.transaction other
                       WHERE other.chain_id=t.chain_id AND other.\"from\"=t.\"from\"
                         AND other.nonce=t.nonce AND other.id<>t.id)))
               AND EXISTS (SELECT 1 FROM relayer.sender_deployment WHERE singleton AND enabled)
               AND NOT EXISTS (SELECT 1 FROM relayer.transaction_attempt a WHERE a.transaction_id=t.id)
               AND NOT EXISTS (SELECT 1 FROM relayer.transaction other WHERE other.external_id=t.external_id AND other.id<>t.id)
               AND COALESCE((SELECT bool_and((
                   a.id<>t.id OR a.status<>'FAILED' OR a.broadcast_attempted OR a.hash IS NOT NULL
                   OR a.gas_limit IS NOT NULL OR a.signed_envelope IS NOT NULL
                   OR a.sent_at IS NOT NULL OR a.mined_at IS NOT NULL OR a.confirmed_at IS NOT NULL
                   OR a.external_id IS DISTINCT FROM t.external_id OR a.chain_id<>t.chain_id
                   OR a.\"from\"<>t.\"from\" OR a.\"to\"<>t.\"to\" OR a.value<>t.value
                   OR a.data IS DISTINCT FROM t.data OR a.authorization_list IS DISTINCT FROM t.authorization_list
                   OR ($2::text IS NOT NULL AND (
                       a.relayer_id<>t.relayer_id OR a.nonce<>t.nonce
                       OR a.cancelled_by_transaction_id IS NOT NULL OR a.blobs IS NOT NULL
                       OR a.failed_at IS DISTINCT FROM t.failed_at
                       OR a.failed_reason IS DISTINCT FROM t.failed_reason
                       OR a.value IS DISTINCT FROM t.value))) IS NOT TRUE)
                   FROM relayer.transaction_audit_log a
                   WHERE a.id=t.id OR ($2::text IS NOT NULL AND a.external_id=t.external_id)), FALSE)",
            &[id, &legacy_external_id],
        ).await?;
        Ok(row.map(|row| build_transaction_from_transaction_view(&row)))
    }

    pub async fn get_fixed_signed_envelope(
        &self,
        id: &TransactionId,
    ) -> Result<Option<Vec<u8>>, PostgresError> {
        let row = self
            .query_one_or_none(
                "SELECT signed_envelope FROM relayer.transaction WHERE id = $1",
                &[id],
            )
            .await?;
        Ok(row.and_then(|row| row.get::<_, Option<Vec<u8>>>("signed_envelope")))
    }

    pub async fn get_transaction(
        &self,
        id: &TransactionId,
    ) -> Result<Option<Transaction>, PostgresError> {
        let row = self
            .query_one_or_none(
                "
                    SELECT *
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[id],
            )
            .await?;

        match row {
            None => Ok(None),
            Some(row) => Ok(Some(build_transaction_from_transaction_view(&row))),
        }
    }

    pub async fn get_transactions_for_relayer(
        &self,
        id: &RelayerId,
        paging_context: &PagingContext,
    ) -> Result<PagingResult<Transaction>, PostgresError> {
        let rows = self
            .query(
                "
                    SELECT *
                    FROM relayer.transaction
                    WHERE relayer_id = $1
                    LIMIT $2
                    OFFSET $3;
                ",
                &[&id, &(paging_context.limit as i64), &(paging_context.offset as i64)],
            )
            .await?;

        let results: Vec<Transaction> =
            rows.iter().map(build_transaction_from_transaction_view).collect();

        let result_count = results.len();

        Ok(PagingResult::new(results, paging_context.next(result_count), paging_context.previous()))
    }

    pub async fn get_transactions_by_status_for_relayer(
        &self,
        id: &RelayerId,
        status: &TransactionStatus,
        paging_context: &PagingContext,
    ) -> Result<PagingResult<Transaction>, PostgresError> {
        let rows = self
            .query(
                "
                    SELECT *
                    FROM relayer.transaction
                    WHERE relayer_id = $1
                    AND status = $2
                    ORDER BY nonce ASC
                    LIMIT $3
                    OFFSET $4;
                ",
                &[id, status, &(paging_context.limit as i64), &(paging_context.offset as i64)],
            )
            .await?;

        let results: Vec<Transaction> =
            rows.iter().map(build_transaction_from_transaction_view).collect();

        let result_count = results.len();

        Ok(PagingResult::new(results, paging_context.next(result_count), paging_context.previous()))
    }

    pub async fn get_transaction_by_hash(
        &self,
        hash: &TransactionHash,
    ) -> Result<Option<Transaction>, PostgresError> {
        let row = self
            .query_one_or_none(
                "
                    SELECT *
                    FROM relayer.transaction
                    WHERE hash = $1 OR id IN (SELECT transaction_id FROM relayer.transaction_attempt WHERE hash=$1);
                ",
                &[hash],
            )
            .await?;

        match row {
            None => Ok(None),
            Some(row) => Ok(Some(build_transaction_from_transaction_view(&row))),
        }
    }

    pub async fn get_transaction_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Transaction>, PostgresError> {
        let row = self
            .query_one_or_none(
                "
                    SELECT *
                    FROM relayer.transaction
                    WHERE external_id = $1;
                ",
                &[&external_id],
            )
            .await?;

        match row {
            None => Ok(None),
            Some(row) => Ok(Some(build_transaction_from_transaction_view(&row))),
        }
    }
}
