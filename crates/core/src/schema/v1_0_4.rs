use crate::postgres::{PostgresClient, PostgresError};

/// Preserve the fixed Base lane's original signed transaction across restarts.
pub async fn apply_v1_0_4_schema(client: &PostgresClient) -> Result<(), PostgresError> {
    client
        .batch_execute(
            "ALTER TABLE relayer.transaction ADD COLUMN IF NOT EXISTS signed_envelope BYTEA;
         ALTER TABLE relayer.transaction_audit_log ADD COLUMN IF NOT EXISTS signed_envelope BYTEA;",
        )
        .await?;
    client
        .batch_execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS fixed_base_signed_nonce_unique
         ON relayer.transaction (relayer_id, nonce)
         WHERE signed_envelope IS NOT NULL;",
        )
        .await?;
    Ok(())
}
