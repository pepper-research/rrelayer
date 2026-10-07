use crate::{
    common_types::EvmAddress,
    gas::{BlobGasPriceResult, GasLimit, GasPriceResult},
    postgres::{PostgresClient, PostgresError},
    relayer::RelayerId,
    shared::{
        common_types::{BlockHash, BlockNumber},
        utils::option_if,
    },
    transaction::types::{
        Transaction, TransactionData, TransactionHash, TransactionId, TransactionNonce,
        TransactionStatus, TransactionValue,
    },
};
use alloy::network::AnyTransactionReceipt;
use serde_json;

const TRANSACTION_TABLES: [&str; 2] = ["relayer.transaction", "relayer.transaction_audit_log"];

impl PostgresClient {
    pub async fn save_transaction(
        &mut self,
        relayer_id: &RelayerId,
        transaction: &Transaction,
    ) -> Result<(), PostgresError> {
        self.save_transaction_with_signed_envelope(relayer_id, transaction, None).await
    }

    pub async fn save_transaction_with_signed_envelope(
        &mut self,
        relayer_id: &RelayerId,
        transaction: &Transaction,
        signed_envelope: Option<&[u8]>,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        if signed_envelope.is_some() {
            // Serialize admissions across overlapping ECS tasks using the durable
            // relayer identity. The lock is released with this DB transaction.
            trans
                .query_one(
                    "SELECT pg_advisory_xact_lock(8453, hashtext($1::uuid::text))",
                    &[relayer_id],
                )
                .await?;
            let active = trans
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM relayer.transaction
                 WHERE relayer_id = $1 AND (
                   status IN ('PENDING', 'INMEMPOOL')
                   OR (signed_envelope IS NOT NULL AND nonce = $2)))",
                    &[relayer_id, &transaction.nonce],
                )
                .await?
                .get::<_, bool>(0);
            if active {
                return Err(PostgresError::FixedBaseLaneBusy);
            }
        }

        let authorization_list_json = transaction
            .authorization_list
            .as_ref()
            .map(|list| serde_json::to_value(list).unwrap_or(serde_json::Value::Null));
        let signed_gas_json = transaction
            .sent_with_gas
            .as_ref()
            .map(|gas| serde_json::to_value(gas).unwrap_or(serde_json::Value::Null));

        for table_name in TRANSACTION_TABLES.iter() {
            trans.execute(
                format!("
                INSERT INTO {}(id, relayer_id, authorization_list, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit, speed, status, expires_at, queued_at, hash, external_id, cancelled_by_transaction_id, signed_envelope, sent_with_gas)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20);
            ", table_name).as_str(),
                &[&transaction.id,
                    &relayer_id,
                    &authorization_list_json,
                    &transaction.to,
                    &transaction.from,
                    &transaction.nonce,
                    &transaction.chain_id,
                    &transaction.data,
                    &transaction.value,
                    &transaction.blobs,
                    &transaction.gas_limit,
                    &transaction.speed,
                    &transaction.status,
                    &transaction.expires_at,
                    &transaction.queued_at,
                    &transaction.known_transaction_hash,
                    &transaction.external_id,
                    &transaction.cancelled_by_transaction_id,
                    &signed_envelope,
                    &signed_gas_json
                ],
            )
                .await?;
        }

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_sent(
        &mut self,
        transaction_id: &TransactionId,
        transaction_hash: &TransactionHash,
        sent_with_gas: &GasPriceResult,
        sent_with_blob_gas: Option<&BlobGasPriceResult>,
        legacy_transaction: bool,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        let max_priority_fee_option =
            option_if(!legacy_transaction, &sent_with_gas.max_priority_fee);
        let max_fee_fee_option = option_if(!legacy_transaction, &sent_with_gas.max_fee);
        let legacy_gas_price = option_if(legacy_transaction, sent_with_gas.legacy_gas_price());

        let sent_with_gas_json =
            serde_json::to_value(sent_with_gas).unwrap_or(serde_json::Value::Null);

        let sent_with_blob_gas_json = sent_with_blob_gas
            .map(|blob_gas| serde_json::to_value(blob_gas).unwrap_or(serde_json::Value::Null));

        trans
            .execute(
                "
                    UPDATE relayer.transaction
                    SET status = $2,
                        hash = $3,
                        sent_max_priority_fee_per_gas = $4,
                        sent_max_fee_per_gas = $5,
                        gas_price = $6,
                        sent_with_gas = $7,
                        sent_with_blob_gas = $8,
                        sent_at = NOW()
                    WHERE id = $1;
                ",
                &[
                    &transaction_id,
                    &TransactionStatus::INMEMPOOL,
                    &transaction_hash,
                    &max_priority_fee_option,
                    &max_fee_fee_option,
                    &legacy_gas_price,
                    &sent_with_gas_json,
                    &sent_with_blob_gas_json,
                ],
            )
            .await?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, sent_with_gas, sent_with_blob_gas, external_id
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, $2, expires_at, queued_at, NOW(), mined_at, confirmed_at,
                        failed_at, failed_reason, $3, $4, $5, $6, $7, $8, external_id
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[
                    &transaction_id,
                    &TransactionStatus::INMEMPOOL,
                    &transaction_hash,
                    &max_priority_fee_option,
                    &max_fee_fee_option,
                    &legacy_gas_price,
                    &sent_with_gas_json,
                    &sent_with_blob_gas_json,
                ],
            )
            .await?;

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_failed_on_send(
        &self,
        relayer_id: &RelayerId,
        transaction: &Transaction,
        failed_reason: String,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        let authorization_list_json = transaction
            .authorization_list
            .as_ref()
            .map(|list| serde_json::to_value(list).unwrap_or(serde_json::Value::Null));

        for table_name in TRANSACTION_TABLES.iter() {
            trans.execute(
                format!("
                INSERT INTO {}(id, relayer_id, authorization_list, \"to\", \"from\", nonce, chain_id, data, value, blobs, speed, status, expires_at, queued_at, failed_at, failed_reason, external_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), $15, $16);
                ", table_name).as_str(),
                &[
                    &transaction.id,
                    &relayer_id,
                    &authorization_list_json,
                    &transaction.to,
                    &transaction.from,
                    &transaction.nonce,
                    &transaction.chain_id,
                    &transaction.data,
                    &transaction.value,
                    &transaction.blobs,
                    &transaction.speed,
                    &transaction.status,
                    &transaction.expires_at,
                    &transaction.queued_at,
                    &failed_reason.chars().take(2000).collect::<String>(),
                    &transaction.external_id,
                ],
            )
                .await
                .map_err(PostgresError::PgError)?;
        }

        trans.commit().await.map_err(PostgresError::PgError)?;

        Ok(())
    }

    pub async fn update_transaction_noop(
        &mut self,
        transaction_id: &TransactionId,
        to: &EvmAddress,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        trans
            .execute(
                "
                    UPDATE relayer.transaction
                    SET \"to\" = $2,
                        value = $3,
                        data = $4
                    WHERE id = $1;
                ",
                &[&transaction_id, &to, &TransactionValue::zero(), &TransactionData::empty()],
            )
            .await
            .map_err(PostgresError::PgError)?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, external_id
                    )
                    SELECT
                        id, relayer_id, $2, \"from\", nonce, chain_id, $4, $3, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, external_id
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[&transaction_id, &to, &TransactionValue::zero(), &TransactionData::empty()],
            )
            .await
            .map_err(PostgresError::PgError)?;

        trans.commit().await.map_err(PostgresError::PgError)?;

        Ok(())
    }

    pub async fn update_transaction_failed(
        &mut self,
        transaction_id: &TransactionId,
        reason: &str,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        let truncated_reason = reason.chars().take(2000).collect::<String>();

        trans
            .execute(
                "
                    UPDATE relayer.transaction
                    SET status = $2,
                        failed_at = NOW(),
                        failed_reason = $3
                    WHERE id = $1;
                ",
                &[&transaction_id, &TransactionStatus::FAILED, &truncated_reason],
            )
            .await?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, external_id
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, $2, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        NOW(), $3, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, external_id
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[
                    &transaction_id,
                    &TransactionStatus::FAILED,
                    &truncated_reason,
                ],
            )
            .await?;

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_mined(
        &mut self,
        transaction: &Transaction,
        transaction_receipt: &AnyTransactionReceipt,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        let gas_used = GasLimit::from(transaction_receipt.gas_used);
        let block_hash = transaction_receipt.block_hash.map(BlockHash::new);
        let block_number = transaction_receipt.block_number.map(BlockNumber::new);
        let hash = TransactionHash::new(transaction_receipt.transaction_hash);

        let authorization_list_json = transaction
            .authorization_list
            .as_ref()
            .map(|list| serde_json::to_value(list).unwrap_or(serde_json::Value::Null));

        trans
            .execute(
                "
                UPDATE relayer.transaction
                SET status = $2,
                    authorization_list = $3,
                    \"to\" = $4,
                    \"from\" = $5,
                    value = $6,
                    data = $7,
                    nonce = $8,
                    chain_id = $9,
                    gas_limit = $10,
                    block_hash = $11,
                    block_number = $12,
                    speed = $13,
                    hash = $14,
                    sent_max_fee_per_gas = $15,
                    sent_max_priority_fee_per_gas = $16,
                    external_id = $17,
                    mined_at = NOW()
                WHERE id = $1;
            ",
                &[
                    &transaction.id,
                    &TransactionStatus::MINED,
                    &authorization_list_json,
                    &transaction.to,
                    &transaction.from,
                    &transaction.value,
                    &transaction.data,
                    &transaction.nonce,
                    &transaction.chain_id,
                    &gas_used,
                    &block_hash,
                    &block_number,
                    &transaction.speed,
                    &hash,
                    &transaction.sent_with_max_fee_per_gas,
                    &transaction.sent_with_max_priority_fee_per_gas,
                    &transaction.external_id,
                ],
            )
            .await?;

        trans
            .execute(
                "
                INSERT INTO relayer.transaction_audit_log (
                    id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                    speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                    failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                    sent_max_fee_per_gas, gas_price, block_hash, block_number, external_id,
                    authorization_list
                )
                SELECT
                    $1, relayer_id, $4, $5, $8, $9, $7, $6, blobs, $10,
                    $13, $2, expires_at, queued_at, sent_at, NOW(), confirmed_at,
                    failed_at, failed_reason, $14, $16, $15, gas_price, $11, $12, $17,
                    $3
                FROM relayer.transaction
                WHERE id = $1;
            ",
                &[
                    &transaction.id,
                    &TransactionStatus::MINED,
                    &authorization_list_json,
                    &transaction.to,
                    &transaction.from,
                    &transaction.value,
                    &transaction.data,
                    &transaction.nonce,
                    &transaction.chain_id,
                    &gas_used,
                    &block_hash,
                    &block_number,
                    &transaction.speed,
                    &transaction.known_transaction_hash,
                    &transaction.sent_with_max_fee_per_gas,
                    &transaction.sent_with_max_priority_fee_per_gas,
                    &transaction.external_id,
                ],
            )
            .await?;

        trans.commit().await?;
        Ok(())
    }

    pub async fn transaction_confirmed(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        trans
            .execute(
                "
                    UPDATE relayer.transaction
                    SET status = $2,
                        confirmed_at = NOW()
                    WHERE id = $1;
                ",
                &[&transaction_id, &TransactionStatus::CONFIRMED],
            )
            .await?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, external_id,
                        authorization_list
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, $2, expires_at, queued_at, sent_at, mined_at, NOW(),
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, external_id,
                        authorization_list
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[&transaction_id, &TransactionStatus::CONFIRMED],
            )
            .await?;

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_update_nonce(
        &mut self,
        transaction_id: &TransactionId,
        nonce: &TransactionNonce,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        trans
            .execute(
                "UPDATE relayer.transaction SET nonce = $2 WHERE id = $1",
                &[&transaction_id, &(nonce.into_inner() as i64)],
            )
            .await?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, external_id,
                        authorization_list
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", $2, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, external_id,
                        authorization_list
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[&transaction_id, &(nonce.into_inner() as i64)],
            )
            .await?;

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_expired(
        &mut self,
        transaction_id: &TransactionId,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        trans
            .execute(
                "
                UPDATE relayer.transaction
                SET status = $2,
                    expired_at = NOW()
                WHERE id = $1;
                ",
                &[&transaction_id, &TransactionStatus::EXPIRED],
            )
            .await?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, expired_at, external_id,
                        authorization_list
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, $2, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, block_hash, block_number, NOW(), external_id,
                        authorization_list
                    FROM relayer.transaction
                    WHERE id = $1;
                ",
                &[&transaction_id, &TransactionStatus::EXPIRED],
            )
            .await?;

        trans.commit().await?;

        Ok(())
    }

    pub async fn transaction_update(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let trans = conn.transaction().await.map_err(PostgresError::PgError)?;

        let sent_with_gas_json = transaction
            .sent_with_gas
            .as_ref()
            .map(|gas| serde_json::to_value(gas).unwrap_or(serde_json::Value::Null));

        let sent_with_blob_gas_json = transaction
            .sent_with_blob_gas
            .as_ref()
            .map(|blob_gas| serde_json::to_value(blob_gas).unwrap_or(serde_json::Value::Null));

        let authorization_list_json = transaction
            .authorization_list
            .as_ref()
            .map(|list| serde_json::to_value(list).unwrap_or(serde_json::Value::Null));

        trans
            .execute(
                "
                    UPDATE relayer.transaction
                    SET relayer_id = $2,
                        authorization_list = $3,
                        \"to\" = $4,
                        \"from\" = $5,
                        nonce = $6,
                        chain_id = $7,
                        data = $8,
                        value = $9,
                        speed = $10,
                        status = $11,
                        expires_at = $12,
                        queued_at = $13,
                        sent_at = $14,
                        mined_at = $15,
                        confirmed_at = $16,
                        gas_limit = $17,
                        hash = $18,
                        sent_max_fee_per_gas = $19,
                        sent_max_priority_fee_per_gas = $20,
                        sent_with_gas = $21,
                        sent_with_blob_gas = $22,
                        external_id = $23,
                        cancelled_by_transaction_id = $24
                    WHERE id = $1
                ",
                &[
                    &transaction.id,
                    &transaction.relayer_id,
                    &authorization_list_json,
                    &transaction.to,
                    &transaction.from,
                    &transaction.nonce,
                    &transaction.chain_id,
                    &transaction.data,
                    &transaction.value,
                    &transaction.speed,
                    &transaction.status,
                    &transaction.expires_at,
                    &transaction.queued_at,
                    &transaction.sent_at,
                    &transaction.mined_at,
                    &transaction.confirmed_at,
                    &transaction.gas_limit,
                    &transaction.known_transaction_hash,
                    &transaction.sent_with_max_fee_per_gas,
                    &transaction.sent_with_max_priority_fee_per_gas,
                    &sent_with_gas_json,
                    &sent_with_blob_gas_json,
                    &transaction.external_id,
                    &transaction.cancelled_by_transaction_id,
                ],
            )
            .await
            .map_err(PostgresError::PgError)?;

        trans
            .execute(
                "
                    INSERT INTO relayer.transaction_audit_log (
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, sent_with_gas, sent_with_blob_gas,
                        block_hash, block_number, expired_at, external_id, authorization_list
                    )
                    SELECT
                        id, relayer_id, \"to\", \"from\", nonce, chain_id, data, value, blobs, gas_limit,
                        speed, status, expires_at, queued_at, sent_at, mined_at, confirmed_at,
                        failed_at, failed_reason, hash, sent_max_priority_fee_per_gas,
                        sent_max_fee_per_gas, gas_price, sent_with_gas, sent_with_blob_gas,
                        block_hash, block_number, expired_at, external_id, authorization_list
                    FROM relayer.transaction
                    WHERE id = $1
                ",
                &[&transaction.id],
            )
            .await
            .map_err(PostgresError::PgError)?;

        trans.commit().await.map_err(PostgresError::PgError)?;
        Ok(())
    }
}

#[cfg(test)]
mod fixed_base_admission_postgres_tests {
    use super::*;
    use crate::{network::ChainId, schema::apply_schema, transaction::types::TransactionSpeed};
    use chrono::Utc;
    use tokio_postgres::error::SqlState;

    fn pending(relayer_id: RelayerId, nonce: u64) -> Transaction {
        Transaction {
            id: TransactionId::new(),
            relayer_id,
            authorization_list: None,
            to: EvmAddress::zero(),
            from: EvmAddress::zero(),
            value: TransactionValue::zero(),
            data: TransactionData::empty(),
            nonce: TransactionNonce::new(nonce),
            chain_id: ChainId::new(8453),
            gas_limit: None,
            status: TransactionStatus::PENDING,
            blobs: None,
            known_transaction_hash: None,
            queued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            sent_at: None,
            confirmed_at: None,
            sent_with_gas: None,
            sent_with_blob_gas: None,
            mined_at: None,
            mined_at_block_number: None,
            speed: TransactionSpeed::FAST,
            sent_with_max_priority_fee_per_gas: None,
            sent_with_max_fee_per_gas: None,
            is_noop: false,
            external_id: None,
            cancelled_by_transaction_id: None,
        }
    }

    async fn record_relayer(db: &PostgresClient, id: RelayerId) -> Result<(), PostgresError> {
        let name = format!("test-{}", id);
        db.execute(
            "INSERT INTO relayer.record (id, name, chain_id, wallet_index)
             VALUES ($1, $2, 8453, 0)",
            &[&id, &name],
        )
        .await?;
        Ok(())
    }

    async fn count(db: &PostgresClient, table: &str, id: RelayerId) -> Result<i64, PostgresError> {
        let rows = db
            .query(&format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE relayer_id = $1"), &[&id])
            .await?;
        Ok(rows[0].get(0))
    }

    #[tokio::test]
    async fn fixed_base_admission_is_atomic_across_postgres_connections(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(url) =
            std::env::var("RRELAYER_TEST_DATABASE_URL").ok().filter(|url| !url.is_empty())
        else {
            return Ok(());
        };
        let mut first = PostgresClient::for_test_url(&url).await?;
        let mut second = PostgresClient::for_test_url(&url).await?;
        apply_schema(&first).await?;

        let fixed_id = RelayerId::new();
        record_relayer(&first, fixed_id).await?;
        let one = pending(fixed_id, 0);
        let two = pending(fixed_id, 0);
        let (a, b) = tokio::join!(
            first.save_transaction_with_signed_envelope(&fixed_id, &one, Some(&[0x02, 0x01])),
            second.save_transaction_with_signed_envelope(&fixed_id, &two, Some(&[0x02, 0x02])),
        );
        assert!(a.is_ok() ^ b.is_ok());
        assert!(matches!(
            a.as_ref().err().or(b.as_ref().err()),
            Some(PostgresError::FixedBaseLaneBusy)
        ));
        assert_eq!(count(&first, "relayer.transaction", fixed_id).await?, 1);
        assert_eq!(count(&first, "relayer.transaction_audit_log", fixed_id).await?, 1);

        // A pending row without an envelope must also hold the fixed identity.
        let legacy_id = RelayerId::new();
        record_relayer(&first, legacy_id).await?;
        first.save_transaction(&legacy_id, &pending(legacy_id, 0)).await?;
        assert!(matches!(
            first
                .save_transaction_with_signed_envelope(
                    &legacy_id,
                    &pending(legacy_id, 1),
                    Some(&[0x02, 0x03])
                )
                .await,
            Err(PostgresError::FixedBaseLaneBusy)
        ));
        // Ordinary NULL-envelope records keep their existing replacement behavior.
        first.save_transaction(&legacy_id, &pending(legacy_id, 0)).await?;
        assert_eq!(count(&first, "relayer.transaction", legacy_id).await?, 2);

        first.execute(
            "UPDATE relayer.transaction SET status = 'CONFIRMED'::relayer.tx_status WHERE relayer_id = $1",
            &[&fixed_id],
        ).await?;
        assert!(matches!(
            first
                .save_transaction_with_signed_envelope(
                    &fixed_id,
                    &pending(fixed_id, 0),
                    Some(&[0x02, 0x04])
                )
                .await,
            Err(PostgresError::FixedBaseLaneBusy)
        ));
        // Even a writer bypassing the admission helper cannot reuse a signed nonce.
        let original_id = if a.is_ok() { one.id } else { two.id };
        let direct_id = TransactionId::new();
        let direct = first
            .execute(
                "INSERT INTO relayer.transaction
             (id, relayer_id, \"to\", \"from\", nonce, value, chain_id, speed, status,
              expires_at, queued_at, signed_envelope)
             SELECT $2::uuid, relayer_id, \"to\", \"from\", nonce, value, chain_id, speed,
                    'CONFIRMED'::relayer.tx_status, expires_at, queued_at, signed_envelope
             FROM relayer.transaction WHERE id = $1",
                &[&original_id, &direct_id],
            )
            .await;
        assert!(matches!(direct, Err(PostgresError::PgError(ref error))
            if error.code() == Some(&SqlState::UNIQUE_VIOLATION)));
        assert_eq!(count(&first, "relayer.transaction", fixed_id).await?, 1);
        assert_eq!(count(&first, "relayer.transaction_audit_log", fixed_id).await?, 1);
        Ok(())
    }
}
