use crate::postgres::{PostgresClient, PostgresError};

/// Older rows (and inserts from older binaries) have unknown broadcast history.
/// Deploy only after stopping older writers; they cannot maintain this marker.
pub async fn apply_v1_0_4_schema(client: &PostgresClient) -> Result<(), PostgresError> {
    client.batch_execute("ALTER TABLE relayer.transaction ADD COLUMN IF NOT EXISTS broadcast_attempted BOOLEAN NOT NULL DEFAULT TRUE;").await?;
    Ok(())
}
