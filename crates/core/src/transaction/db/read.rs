use super::builders::build_transaction_from_transaction_view;
use crate::{
    postgres::{PostgresClient, PostgresError},
    relayer::RelayerId,
    shared::common_types::{PagingContext, PagingResult},
    transaction::types::{
        Transaction, TransactionHash, TransactionId, TransactionNonce, TransactionStatus,
    },
};

impl PostgresClient {
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

    /// The highest nonce this relayer has broadcast at, from transactions that are in the mempool,
    /// mined or confirmed. Unlike the in-memory queues this still covers confirmed transactions.
    pub async fn get_highest_used_nonce(
        &self,
        relayer_id: &RelayerId,
    ) -> Result<Option<TransactionNonce>, PostgresError> {
        let row = self
            .query_one_or_none(
                "
                    SELECT MAX(nonce) AS nonce
                    FROM relayer.transaction
                    WHERE relayer_id = $1
                    AND status IN ($2, $3, $4);
                ",
                &[
                    relayer_id,
                    &TransactionStatus::INMEMPOOL,
                    &TransactionStatus::MINED,
                    &TransactionStatus::CONFIRMED,
                ],
            )
            .await?;

        Ok(row
            .and_then(|row| row.get::<_, Option<i64>>("nonce"))
            .map(|nonce| TransactionNonce::new(nonce as u64)))
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
                    WHERE hash = $1;
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
