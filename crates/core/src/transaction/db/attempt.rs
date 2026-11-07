use crate::{
    gas::{BlobGasPriceResult, GasPriceResult},
    postgres::{PostgresClient, PostgresError},
    transaction::types::{Transaction, TransactionHash, TransactionId},
};

pub struct SignedAttempt {
    pub bytes: Vec<u8>,
    pub hash: TransactionHash,
    pub gas: GasPriceResult,
    pub blob_gas: Option<BlobGasPriceResult>,
}
impl PostgresClient {
    pub async fn last_attempt(
        &self,
        id: &TransactionId,
    ) -> Result<Option<SignedAttempt>, PostgresError> {
        let row = self.query_one_or_none("SELECT * FROM relayer.transaction_attempt WHERE transaction_id=$1 ORDER BY created_at DESC LIMIT 1", &[id]).await?;
        row.map(|r| {
            Ok(SignedAttempt {
                bytes: r.get("signed_envelope"),
                hash: r.get("hash"),
                gas: serde_json::from_value(r.get("gas"))
                    .map_err(|e| PostgresError::Handoff(e.to_string()))?,
                blob_gas: r
                    .get::<_, Option<serde_json::Value>>("blob_gas")
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| PostgresError::Handoff(e.to_string()))?,
            })
        })
        .transpose()
    }
    pub async fn save_attempt(
        &self,
        transaction: &Transaction,
        attempt: &SignedAttempt,
    ) -> Result<(), PostgresError> {
        let mut conn = self.pool.get().await?;
        let tx = conn.transaction().await?;
        // The trigger validates the current epoch under a row lock through commit.
        // Never send unless both the envelope and its owning transaction committed.
        let gas = serde_json::to_value(&attempt.gas).unwrap();
        let blob = attempt.blob_gas.as_ref().map(|g| serde_json::to_value(g).unwrap());
        tx.query_one("UPDATE relayer.transaction SET broadcast_attempted=TRUE, hash=$2, sent_with_gas=$3, sent_with_blob_gas=$4, gas_limit=$5 WHERE id=$1 AND nonce=$6 AND status IN ('PENDING','INMEMPOOL') RETURNING id", &[&transaction.id,&attempt.hash,&gas,&blob,&transaction.gas_limit,&transaction.nonce]).await?;
        tx.execute("INSERT INTO relayer.transaction_attempt(transaction_id,hash,signed_envelope,gas,blob_gas) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", &[&transaction.id,&attempt.hash,&attempt.bytes,&gas,&blob]).await?;
        #[cfg(debug_assertions)]
        crate::sender_handoff::test_barrier("before_attempt_commit").await;
        tx.commit().await?;
        #[cfg(debug_assertions)]
        crate::sender_handoff::test_barrier("after_attempt_commit").await;
        Ok(())
    }
    pub async fn attempt_hashes(
        &self,
        id: &TransactionId,
    ) -> Result<Vec<TransactionHash>, PostgresError> {
        Ok(self.query("SELECT hash FROM relayer.transaction_attempt WHERE transaction_id=$1 ORDER BY created_at", &[id]).await?.iter().map(|r|r.get(0)).collect())
    }
}
