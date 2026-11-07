use crate::tests::test_runner::TestRunner;
use alloy::network::ReceiptResponse;
use alloy::providers::{Provider, ProviderBuilder};
use anyhow::Context;
use rrelayer_core::transaction::api::{RelayTransactionRequest, TransactionSpeed};
use rrelayer_core::transaction::types::{TransactionData, TransactionStatus};
use std::time::Duration;
use tracing::info;

impl TestRunner {
    /// run single with:
    /// RRELAYER_PROVIDERS="raw" make run-test-debug TEST=transaction_replace
    /// RRELAYER_PROVIDERS="privy" make run-test-debug TEST=transaction_replace
    /// RRELAYER_PROVIDERS="aws_secret_manager" make run-test-debug TEST=transaction_replace
    /// RRELAYER_PROVIDERS="aws_kms" make run-test-debug TEST=transaction_replace
    /// RRELAYER_PROVIDERS="gcp_secret_manager" make run-test-debug TEST=transaction_replace
    /// RRELAYER_PROVIDERS="turnkey" make run-test-debug TEST=transaction_replace
    pub async fn transaction_replace(&self) -> anyhow::Result<()> {
        info!("Testing transaction replace operation...");

        let relayer = self.create_and_fund_relayer("tx-replace-relayer").await?;
        info!("Created relayer: {:?}", relayer);

        let tx_request = RelayTransactionRequest {
            authorization_list: None,
            to: self.config.anvil_accounts[1],
            value: alloy::primitives::utils::parse_ether("0.1")?.into(),
            data: TransactionData::empty(),
            speed: Some(TransactionSpeed::SLOW),
            external_id: Some("test-original".to_string()),
            blobs: None,
        };

        let send_result = relayer
            .transaction()
            .send(&tx_request, None)
            .await
            .context("Failed to send transaction")?;

        let transaction_id = &send_result.id;

        // Replacement of a broadcast transaction has different semantics from Pending.
        // Wait without mining so this test deterministically exercises competition.
        let original = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let tx = self.relayer_client.get_transaction(transaction_id).await?;
                if tx.status == TransactionStatus::INMEMPOOL {
                    anyhow::ensure!(tx.known_transaction_hash.is_some(), "Missing broadcast hash");
                    return Ok::<_, anyhow::Error>(tx);
                }
                anyhow::ensure!(
                    tx.status == TransactionStatus::PENDING,
                    "Unexpected precondition {}",
                    tx.status
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("Original never reached INMEMPOOL")??;

        let replacement_request = RelayTransactionRequest {
            authorization_list: None,
            to: self.config.anvil_accounts[1],
            value: alloy::primitives::utils::parse_ether("0.2")?.into(),
            data: TransactionData::empty(),
            speed: Some(TransactionSpeed::FAST),
            external_id: Some("test-replacement".to_string()),
            blobs: None,
        };

        let replace_result = relayer
            .transaction()
            .replace(transaction_id, &replacement_request, None)
            .await
            .context("Failed to replace transaction")?;
        info!("[SUCCESS] Transaction replacement result: {:?}", replace_result);

        if !replace_result.success {
            return Err(anyhow::anyhow!("Replace transaction failed"));
        }

        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;
        self.anvil_manager.mine_and_wait().await?;

        let first_transaction = self.relayer_client.get_transaction(&send_result.id).await?;
        let replace_transaction = self
            .relayer_client
            .get_transaction(&replace_result.replace_transaction_id.unwrap())
            .await?;

        if first_transaction.status != TransactionStatus::REPLACED {
            return Err(anyhow::anyhow!(format!(
                "First transaction {} is not replaced it is {}",
                first_transaction.id, first_transaction.status
            )));
        }

        if replace_transaction.status != TransactionStatus::MINED {
            return Err(anyhow::anyhow!(format!(
                "Replace transaction {} is not mined it is {}",
                replace_transaction.id, replace_transaction.status
            )));
        }

        anyhow::ensure!(
            replace_transaction.id != original.id,
            "Broadcast replacement must compete under a new ID"
        );
        anyhow::ensure!(replace_transaction.nonce == original.nonce, "Replacement changed nonce");
        self.relayer_client.sent_transaction_compare(replacement_request, replace_transaction)?;

        info!("[SUCCESS] Transaction replace operation works correctly");
        Ok(())
    }
    /// Held Pending replacements must update both persistent state and the queued send.
    pub async fn transaction_replace_pending(&self) -> anyhow::Result<()> {
        let relayer = self.create_and_fund_relayer("replace-pending").await?;
        relayer.update_max_gas_price(1).await?;
        let request = RelayTransactionRequest {
            authorization_list: None,
            to: self.config.anvil_accounts[1],
            value: alloy::primitives::utils::parse_ether("0.1")?.into(),
            data: TransactionData::empty(),
            speed: Some(TransactionSpeed::SLOW),
            external_id: Some("pending-original".into()),
            blobs: None,
        };
        let sent = relayer.transaction().send(&request, None).await?;
        let original = self.relayer_client.get_transaction(&sent.id).await?;
        anyhow::ensure!(
            original.status == TransactionStatus::PENDING && original.sent_at.is_none(),
            "Expected held unsent Pending transaction"
        );
        let provider = ProviderBuilder::new()
            .connect_http(format!("http://127.0.0.1:{}", self.config.anvil_port).parse()?);
        let original_hash =
            original.known_transaction_hash.context("Admission should precompute hash")?;
        anyhow::ensure!(
            provider.get_transaction_by_hash(original_hash.into_alloy_hash()).await?.is_none(),
            "Original was broadcast before replacement"
        );
        let replacement = RelayTransactionRequest {
            to: self.config.anvil_accounts[1],
            value: alloy::primitives::utils::parse_ether("0.2")?.into(),
            external_id: Some("pending-replacement".into()),
            ..request.clone()
        };
        let result = relayer.transaction().replace(&sent.id, &replacement, None).await?;
        anyhow::ensure!(
            result.success
                && result.replace_transaction_id == Some(sent.id)
                && result.replace_transaction_hash.is_none(),
            "Pending replacement must retain unsent identity"
        );

        let (db, connection) =
            tokio_postgres::connect(&std::env::var("DATABASE_URL")?, tokio_postgres::NoTls).await?;
        let connection_task = tokio::spawn(connection);
        let row = db
            .query_one(
                "SELECT external_id, nonce, status::text FROM relayer.transaction WHERE id = $1",
                &[&sent.id],
            )
            .await?;
        anyhow::ensure!(
            row.get::<_, String>(0) == "pending-replacement",
            "Successful replacement was not persisted"
        );
        anyhow::ensure!(
            row.get::<_, i64>(1) as u64 == original.nonce.into_inner()
                && row.get::<_, String>(2) == "PENDING",
            "Pending identity changed"
        );
        let persisted = self.relayer_client.get_transaction(&sent.id).await?;
        self.relayer_client.sent_transaction_compare(replacement.clone(), persisted)?;

        // A failed DB write must leave the previous authoritative payload in the queue.
        db.batch_execute("CREATE FUNCTION relayer.reject_test_replacement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.external_id = 'pending-rejected' THEN RAISE EXCEPTION 'deliberate replacement write failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_test_replacement BEFORE UPDATE ON relayer.transaction FOR EACH ROW EXECUTE FUNCTION relayer.reject_test_replacement();").await?;
        let rejected = RelayTransactionRequest {
            external_id: Some("pending-rejected".into()),
            ..request.clone()
        };
        let failed_result = relayer.transaction().replace(&sent.id, &rejected, None).await;
        db.batch_execute("DROP TRIGGER reject_test_replacement ON relayer.transaction; DROP FUNCTION relayer.reject_test_replacement();").await?;
        anyhow::ensure!(
            failed_result.is_err(),
            "Database failure was reported as replacement success"
        );
        let unchanged = self.relayer_client.get_transaction(&sent.id).await?;
        self.relayer_client.sent_transaction_compare(replacement.clone(), unchanged)?;
        // The main UPDATE must roll back if appending its audit row fails.
        db.batch_execute("CREATE FUNCTION relayer.reject_replacement_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.external_id = 'pending-audit-rejected' THEN RAISE EXCEPTION 'deliberate audit write failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_replacement_audit BEFORE INSERT ON relayer.transaction_audit_log FOR EACH ROW EXECUTE FUNCTION relayer.reject_replacement_audit();").await?;
        let audit_rejected = RelayTransactionRequest {
            external_id: Some("pending-audit-rejected".into()),
            ..request.clone()
        };
        let audit_result = relayer.transaction().replace(&sent.id, &audit_rejected, None).await;
        db.batch_execute("DROP TRIGGER reject_replacement_audit ON relayer.transaction_audit_log; DROP FUNCTION relayer.reject_replacement_audit();").await?;
        anyhow::ensure!(audit_result.is_err(), "Audit failure was reported as replacement success");
        self.relayer_client.sent_transaction_compare(
            replacement.clone(),
            self.relayer_client.get_transaction(&sent.id).await?,
        )?;
        let persisted_external: String = db
            .query_one("SELECT external_id FROM relayer.transaction WHERE id=$1", &[&sent.id])
            .await?
            .get(0);
        anyhow::ensure!(
            persisted_external == "pending-replacement",
            "Audit failure did not roll back the main update"
        );

        // A failure to persist the pre-send marker must prevent even the RPC send.
        db.batch_execute("CREATE SEQUENCE relayer.marker_failure_observed; CREATE FUNCTION relayer.reject_broadcast_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.external_id = 'pending-replacement' AND NEW.broadcast_attempted AND NOT OLD.broadcast_attempted THEN PERFORM nextval('relayer.marker_failure_observed'); RAISE EXCEPTION 'deliberate broadcast marker failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_broadcast_marker BEFORE UPDATE ON relayer.transaction FOR EACH ROW EXECUTE FUNCTION relayer.reject_broadcast_marker();").await?;
        relayer.remove_max_gas_price().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        relayer.update_max_gas_price(1).await?;
        let unattempted: bool = db
            .query_one(
                "SELECT NOT broadcast_attempted FROM relayer.transaction WHERE id=$1",
                &[&sent.id],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            db.query_one("SELECT is_called FROM relayer.marker_failure_observed", &[])
                .await?
                .get::<_, bool>(0),
            "Did not reach failing pre-send marker write"
        );
        anyhow::ensure!(unattempted, "Failed marker write did not roll back");
        anyhow::ensure!(
            provider.get_transaction_count(original.from.into()).pending().await?
                == original.nonce.into_inner(),
            "Broadcast escaped failed marker write"
        );
        db.batch_execute("DROP TRIGGER reject_broadcast_marker ON relayer.transaction; DROP FUNCTION relayer.reject_broadcast_marker(); DROP SEQUENCE relayer.marker_failure_observed;").await?;
        drop(db);
        connection_task.await??;

        let provider = ProviderBuilder::new()
            .connect_http(format!("http://127.0.0.1:{}", self.config.anvil_port).parse()?);
        let recipient = replacement.to.into();
        let balance_before = provider.get_balance(recipient).await?;
        relayer.remove_max_gas_price().await?;
        let (mined, receipt) = self.wait_for_transaction_completion(&sent.id).await?;
        anyhow::ensure!(
            mined.id == original.id && mined.nonce == original.nonce,
            "Replacement changed identity or nonce"
        );
        anyhow::ensure!(receipt.status(), "Replacement reverted");
        self.relayer_client.sent_transaction_compare(replacement.clone(), mined)?;
        let balance_after = provider.get_balance(recipient).await?;
        anyhow::ensure!(
            balance_after - balance_before == alloy::primitives::utils::parse_ether("0.2")?,
            "Queue sent a stale or duplicate replacement payload"
        );
        let next = relayer.transaction().send(&request, None).await?;
        let (next_mined, _) = self.wait_for_transaction_completion(&next.id).await?;
        anyhow::ensure!(
            next_mined.nonce == original.nonce + 1,
            "Replacement caused a nonce gap or duplicate send"
        );
        Ok(())
    }
    /// A node accepted the send, but recording INMEMPOOL failed. PENDING is ambiguous.
    pub async fn transaction_replace_ambiguous(&self) -> anyhow::Result<()> {
        let relayer = self.create_and_fund_relayer("replace-ambiguous").await?;
        relayer.update_max_gas_price(1).await?;
        let request = RelayTransactionRequest {
            authorization_list: None,
            to: self.config.anvil_accounts[1],
            value: alloy::primitives::utils::parse_ether("0.1")?.into(),
            data: TransactionData::empty(),
            speed: Some(TransactionSpeed::SLOW),
            external_id: Some("ambiguous-original".into()),
            blobs: None,
        };
        let sent = relayer.transaction().send(&request, None).await?;
        let original = self.relayer_client.get_transaction(&sent.id).await?;
        let (db, connection) =
            tokio_postgres::connect(&std::env::var("DATABASE_URL")?, tokio_postgres::NoTls).await?;
        let connection_task = tokio::spawn(connection);
        db.batch_execute("CREATE FUNCTION relayer.reject_sent_bookkeeping() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.external_id = 'ambiguous-original' AND NEW.status = 'INMEMPOOL' THEN RAISE EXCEPTION 'deliberate sent bookkeeping failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_sent_bookkeeping BEFORE UPDATE ON relayer.transaction FOR EACH ROW EXECUTE FUNCTION relayer.reject_sent_bookkeeping();").await?;
        let provider = ProviderBuilder::new()
            .connect_http(format!("http://127.0.0.1:{}", self.config.anvil_port).parse()?);
        let address = original.from.into();
        let balance_before = provider.get_balance(request.to.into()).await?;
        relayer.remove_max_gas_price().await?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if provider.get_transaction_count(address).pending().await?
                    > original.nonce.into_inner()
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("Original was never broadcast")??;
        relayer.update_max_gas_price(1).await?;
        let ambiguous = self.relayer_client.get_transaction(&sent.id).await?;
        anyhow::ensure!(
            ambiguous.status == TransactionStatus::PENDING && ambiguous.sent_at.is_none(),
            "Must reach ambiguous Pending state"
        );
        let replacement = RelayTransactionRequest {
            value: alloy::primitives::utils::parse_ether("0.2")?.into(),
            external_id: Some("ambiguous-replacement".into()),
            ..request.clone()
        };
        let result = relayer.transaction().replace(&sent.id, &replacement, None).await;
        db.batch_execute("DROP TRIGGER reject_sent_bookkeeping ON relayer.transaction; DROP FUNCTION relayer.reject_sent_bookkeeping();").await?;
        anyhow::ensure!(
            result.is_err(),
            "Ambiguous broadcast was incorrectly accepted as an unsent replacement"
        );
        self.relayer_client.sent_transaction_compare(
            request.clone(),
            self.relayer_client.get_transaction(&sent.id).await?,
        )?;
        relayer.remove_max_gas_price().await?;
        let (mined, receipt) = self.wait_for_transaction_completion(&sent.id).await?;
        anyhow::ensure!(
            receipt.status() && mined.nonce == original.nonce,
            "Original did not recover at its nonce"
        );
        self.relayer_client.sent_transaction_compare(request, mined)?;
        let balance_after = provider.get_balance(self.config.anvil_accounts[1].into()).await?;
        anyhow::ensure!(
            balance_after - balance_before == alloy::primitives::utils::parse_ether("0.1")?,
            "Original duplicated or replacement was falsely attributed"
        );
        drop(db);
        connection_task.await??;
        Ok(())
    }
}
