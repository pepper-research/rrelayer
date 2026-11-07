use crate::tests::test_runner::TestRunner;
use alloy::network::ReceiptResponse;
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::Context;
use rrelayer::AdminRelayerClient;
use rrelayer_core::transaction::api::{RelayTransactionRequest, TransactionSpeed};
use rrelayer_core::transaction::types::{TransactionData, TransactionStatus, TransactionValue};
use std::time::Duration;

impl TestRunner {
    pub(super) async fn replacement_permissions(
        &self,
        relayer: &AdminRelayerClient,
        forbid_value: bool,
    ) -> anyhow::Result<()> {
        let provider = ProviderBuilder::new()
            .connect_http(format!("http://127.0.0.1:{}", self.config.anvil_port).parse()?);
        let address = relayer.address().await?.into_address();
        let (db, connection) =
            tokio_postgres::connect(&std::env::var("DATABASE_URL")?, tokio_postgres::NoTls).await?;
        let connection_task = tokio::spawn(connection);
        for pending in [true, false] {
            let request = RelayTransactionRequest {
                authorization_list: None,
                to: self.config.anvil_accounts[1],
                value: TransactionValue::zero(),
                data: TransactionData::empty(),
                speed: Some(TransactionSpeed::SLOW),
                external_id: Some("permission-original".into()),
                blobs: None,
            };
            let denied = RelayTransactionRequest {
                to: if forbid_value { request.to } else { self.config.anvil_accounts[2] },
                value: if forbid_value {
                    alloy::primitives::utils::parse_ether("0.1")?.into()
                } else {
                    request.value
                },
                external_id: Some("permission-denied".into()),
                ..request.clone()
            };
            // Confirm the configured policy rejects this exact payload on ordinary admission.
            anyhow::ensure!(
                relayer.transaction().send(&denied, None).await.is_err(),
                "Normal send did not enforce the permission fixture"
            );
            if pending {
                relayer.update_max_gas_price(1).await?;
            }
            let sent = relayer.transaction().send(&request, None).await?;
            let expected =
                if pending { TransactionStatus::PENDING } else { TransactionStatus::INMEMPOOL };
            let original = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let tx = self.relayer_client.get_transaction(&sent.id).await?;
                    if tx.status == expected {
                        return Ok::<_, anyhow::Error>(tx);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("Permission fixture did not reach required queue state")??;
            let hash = original.known_transaction_hash.context("Missing precomputed hash")?;
            anyhow::ensure!(
                provider.get_transaction_by_hash(hash.into_alloy_hash()).await?.is_none()
                    == pending,
                "Broadcast precondition does not match queue state"
            );
            let snapshot_sql = "SELECT (SELECT row_to_json(t)::text FROM relayer.transaction t WHERE id=$1), (SELECT count(*) FROM relayer.transaction WHERE relayer_id=$2), (SELECT count(*) FROM relayer.transaction_audit_log WHERE relayer_id=$2)";
            let before = db.query_one(snapshot_sql, &[&sent.id, relayer.id()]).await?;
            let nonce_before = provider.get_transaction_count(address).pending().await?;
            let response = reqwest::Client::new()
                .put(format!("http://localhost:3000/transactions/replace/{}", sent.id))
                .basic_auth(
                    std::env::var("RRELAYER_AUTH_USERNAME")?,
                    Some(std::env::var("RRELAYER_AUTH_PASSWORD")?),
                )
                .json(&denied)
                .send()
                .await?;
            anyhow::ensure!(response.status() == reqwest::StatusCode::UNAUTHORIZED,
                "Denied replacement accepted: pending={pending}, forbid_value={forbid_value}, status={}", response.status());
            let after = db.query_one(snapshot_sql, &[&sent.id, relayer.id()]).await?;
            anyhow::ensure!(
                before.get::<_, String>(0) == after.get::<_, String>(0)
                    && before.get::<_, i64>(1) == after.get::<_, i64>(1)
                    && before.get::<_, i64>(2) == after.get::<_, i64>(2),
                "Denied replacement mutated transaction, competitor count or audit history"
            );
            anyhow::ensure!(
                provider.get_transaction_count(address).pending().await? == nonce_before,
                "Denied replacement consumed a nonce"
            );
            self.relayer_client.sent_transaction_compare(
                request.clone(),
                self.relayer_client.get_transaction(&sent.id).await?,
            )?;
            if pending {
                relayer.remove_max_gas_price().await?;
            }
            let (mined, receipt) = self.wait_for_transaction_completion(&sent.id).await?;
            anyhow::ensure!(
                receipt.status() && mined.nonce == original.nonce,
                "Original failed or changed nonce"
            );
            self.relayer_client.sent_transaction_compare(request.clone(), mined.clone())?;
            let chain_tx = provider
                .get_transaction_by_hash(
                    mined.known_transaction_hash.context("Missing mined hash")?.into_alloy_hash(),
                )
                .await?
                .context("Missing chain transaction")?;
            use alloy::consensus::Transaction;
            anyhow::ensure!(
                chain_tx.inner.to() == Some(request.to.into_address())
                    && chain_tx.inner.value().is_zero()
                    && chain_tx.inner.input().is_empty(),
                "Denied payload reached chain instead of original"
            );
            anyhow::ensure!(
                provider.get_transaction_count(address).await? == original.nonce.into_inner() + 1,
                "Denied replacement caused duplicate execution"
            );

            // An authorized edit still succeeds under the same destination/value policy.
            if pending {
                relayer.update_max_gas_price(1).await?;
            }
            let allowed_sent = relayer.transaction().send(&request, None).await?;
            tokio::time::timeout(Duration::from_secs(30), async {
                while self.relayer_client.get_transaction(&allowed_sent.id).await?.status
                    != expected
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            let allowed = RelayTransactionRequest {
                data: TransactionData::raw_hex("0x1234").unwrap(),
                speed: Some(TransactionSpeed::FAST),
                external_id: Some("permission-allowed".into()),
                ..request
            };
            let result = relayer.transaction().replace(&allowed_sent.id, &allowed, None).await?;
            anyhow::ensure!(result.success, "Permitted replacement failed");
            let replacement_id = result.replace_transaction_id.context("Missing replacement ID")?;
            anyhow::ensure!(
                (replacement_id == allowed_sent.id) == pending,
                "Replacement identity semantics changed"
            );
            if pending {
                relayer.remove_max_gas_price().await?;
            }
            let (replacement, receipt) =
                self.wait_for_transaction_completion(&replacement_id).await?;
            anyhow::ensure!(
                receipt.status() && replacement.nonce == original.nonce + 1,
                "Allowed replacement nonce changed"
            );
            self.relayer_client.sent_transaction_compare(allowed, replacement.clone())?;
            let chain_tx = provider
                .get_transaction_by_hash(
                    replacement
                        .known_transaction_hash
                        .context("Missing replacement hash")?
                        .into_alloy_hash(),
                )
                .await?
                .context("Missing replacement on chain")?;
            anyhow::ensure!(
                chain_tx.inner.input().as_ref() == [0x12, 0x34],
                "Queue sent stale allowed payload"
            );
            if !pending {
                anyhow::ensure!(
                    self.relayer_client.get_transaction(&allowed_sent.id).await?.status
                        == TransactionStatus::REPLACED,
                    "Broadcast original was not marked replaced"
                );
            }
        }
        drop(db);
        connection_task.await??;
        Ok(())
    }
}
