use super::builders::build_transaction_from_transaction_view;
use crate::{
    postgres::{PostgresClient, PostgresError},
    relayer::RelayerId,
    shared::common_types::{PagingContext, PagingResult},
    transaction::types::{Transaction, TransactionHash, TransactionId, TransactionStatus},
};

impl PostgresClient {
    pub async fn get_prebroadcast_failure(
        &self,
        id: &TransactionId,
    ) -> Result<Option<Transaction>, PostgresError> {
        let row = self.query_one_or_none(
            "SELECT t.* FROM relayer.transaction t
             WHERE t.id=$1 AND t.external_id=t.id::text AND t.status='FAILED'
               AND NOT t.broadcast_attempted AND t.hash IS NULL
               AND t.gas_limit IS NULL AND t.signed_envelope IS NULL
               AND t.sent_at IS NULL AND t.mined_at IS NULL AND t.confirmed_at IS NULL
               AND t.cancelled_by_transaction_id IS NULL AND t.competition_kind IS NULL
               AND EXISTS (SELECT 1 FROM relayer.sender_deployment WHERE singleton AND enabled)
               AND NOT EXISTS (SELECT 1 FROM relayer.transaction_attempt a WHERE a.transaction_id=t.id)
               AND NOT EXISTS (SELECT 1 FROM relayer.transaction other WHERE other.external_id=t.external_id AND other.id<>t.id)
               AND EXISTS (SELECT 1 FROM relayer.transaction_audit_log a WHERE a.id=t.id)
               AND NOT EXISTS (SELECT 1 FROM relayer.transaction_audit_log a WHERE a.id=t.id AND (
                   a.status<>'FAILED' OR a.broadcast_attempted OR a.hash IS NOT NULL
                   OR a.gas_limit IS NOT NULL OR a.signed_envelope IS NOT NULL
                   OR a.sent_at IS NOT NULL OR a.mined_at IS NOT NULL OR a.confirmed_at IS NOT NULL
                   OR a.external_id IS DISTINCT FROM t.external_id OR a.chain_id<>t.chain_id
                   OR a.\"from\"<>t.\"from\" OR a.\"to\"<>t.\"to\" OR a.value<>t.value
                   OR a.data IS DISTINCT FROM t.data OR a.authorization_list IS DISTINCT FROM t.authorization_list))",
            &[id],
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
