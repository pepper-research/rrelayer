use std::sync::Arc;

use alloy::dyn_abi::TypedData;
use alloy::{consensus::TypedTransaction, signers::Signature};
use async_trait::async_trait;

use crate::wallet::WalletManagerChainId;
use crate::{
    shared::common_types::EvmAddress,
    wallet::{WalletError, WalletManagerTrait},
};

/// A composite wallet manager that routes to different wallet managers based on wallet index
pub struct CompositeWalletManager {
    primary_manager: Arc<dyn WalletManagerTrait>,
    private_key_manager: Option<Arc<dyn WalletManagerTrait>>,
}

impl CompositeWalletManager {
    pub fn new(
        primary_manager: Arc<dyn WalletManagerTrait>,
        private_key_manager: Option<Arc<dyn WalletManagerTrait>>,
    ) -> Self {
        CompositeWalletManager { primary_manager, private_key_manager }
    }

    // TODO: not ideal route but only way i could find for now to work without a big refactor
    /// Determine if a wallet index is for a private key (high range)
    fn is_private_key_index(&self, wallet_index: u32) -> bool {
        wallet_index >= u32::MAX - 1000
    }

    /// Get the appropriate wallet manager for the given index
    fn get_manager_for_index(
        &self,
        wallet_index: u32,
    ) -> Result<&Arc<dyn WalletManagerTrait>, WalletError> {
        if self.is_private_key_index(wallet_index) {
            if let Some(ref manager) = self.private_key_manager {
                Ok(manager)
            } else {
                Err(WalletError::UnsupportedOperation(format!(
                    "Private key wallet index {} requested but no private key manager configured",
                    wallet_index
                )))
            }
        } else {
            Ok(&self.primary_manager)
        }
    }
}

#[async_trait]
impl WalletManagerTrait for CompositeWalletManager {
    async fn create_wallet(
        &self,
        wallet_index: u32,
        chain_id: WalletManagerChainId,
    ) -> Result<EvmAddress, WalletError> {
        let manager = self.get_manager_for_index(wallet_index)?;
        manager.create_wallet(wallet_index, chain_id).await
    }

    async fn get_address(
        &self,
        wallet_index: u32,
        chain_id: WalletManagerChainId,
    ) -> Result<EvmAddress, WalletError> {
        let manager = self.get_manager_for_index(wallet_index)?;
        manager.get_address(wallet_index, chain_id).await
    }

    async fn sign_transaction(
        &self,
        wallet_index: u32,
        transaction: &TypedTransaction,
        chain_id: WalletManagerChainId,
    ) -> Result<Signature, WalletError> {
        let manager = self.get_manager_for_index(wallet_index)?;
        manager.sign_transaction(wallet_index, transaction, chain_id).await
    }

    async fn sign_text(
        &self,
        wallet_index: u32,
        text: &str,
        chain_id: WalletManagerChainId,
    ) -> Result<Signature, WalletError> {
        let manager = self.get_manager_for_index(wallet_index)?;
        manager.sign_text(wallet_index, text, chain_id).await
    }

    async fn sign_typed_data(
        &self,
        wallet_index: u32,
        typed_data: &TypedData,
        chain_id: WalletManagerChainId,
    ) -> Result<Signature, WalletError> {
        let manager = self.get_manager_for_index(wallet_index)?;
        manager.sign_typed_data(wallet_index, typed_data, chain_id).await
    }

    fn supports_blobs(&self) -> bool {
        // Return true if either manager supports blobs
        let primary_supports = self.primary_manager.supports_blobs();
        let private_key_supports =
            self.private_key_manager.as_ref().is_some_and(|m| m.supports_blobs());
        primary_supports || private_key_supports
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::ChainId;
    use crate::relayer::WalletIndex;
    use crate::wallet::PrivateKeyWalletManager;
    use alloy::primitives::Address;
    use std::sync::Mutex;

    #[derive(Default)]
    struct PrimaryMock {
        seen_indexes: Mutex<Vec<u32>>,
    }

    #[async_trait]
    impl WalletManagerTrait for PrimaryMock {
        async fn create_wallet(
            &self,
            index: u32,
            _chain_id: WalletManagerChainId,
        ) -> Result<EvmAddress, WalletError> {
            self.seen_indexes.lock().unwrap().push(index);
            Ok(EvmAddress::new(Address::ZERO))
        }

        async fn get_address(
            &self,
            index: u32,
            _chain_id: WalletManagerChainId,
        ) -> Result<EvmAddress, WalletError> {
            self.seen_indexes.lock().unwrap().push(index);
            Ok(EvmAddress::new(Address::ZERO))
        }

        async fn sign_transaction(
            &self,
            _index: u32,
            _transaction: &TypedTransaction,
            _chain_id: WalletManagerChainId,
        ) -> Result<Signature, WalletError> {
            panic!("primary signature is outside this routing test")
        }

        async fn sign_text(
            &self,
            _index: u32,
            _text: &str,
            _chain_id: WalletManagerChainId,
        ) -> Result<Signature, WalletError> {
            panic!("primary signature is outside this routing test")
        }

        async fn sign_typed_data(
            &self,
            _index: u32,
            _typed_data: &TypedData,
            _chain_id: WalletManagerChainId,
        ) -> Result<Signature, WalletError> {
            panic!("primary signature is outside this routing test")
        }

        fn supports_blobs(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn existing_private_key_addresses_survive_composite_restart_and_new_primary_index() {
        let keys = [1u8, 2u8].map(|value| format!("0x{value:064x}")).to_vec();
        let chain = ChainId::new(8453);
        let standalone = PrivateKeyWalletManager::new(keys.clone());
        let original = [
            standalone.get_address(0, chain.into()).await.unwrap(),
            standalone.get_address(1, chain.into()).await.unwrap(),
        ];
        assert_ne!(original[0], original[1]);

        for _restart in 0..2 {
            let primary = Arc::new(PrimaryMock::default());
            let private_keys = Arc::new(PrivateKeyWalletManager::new(keys.clone()));
            let composite = CompositeWalletManager::new(primary.clone(), Some(private_keys));

            for (index, expected) in original.iter().enumerate() {
                let persisted_index = -((index + 1) as i32);
                let manager_index = WalletIndex::PrivateKey(persisted_index).index();
                assert_eq!(
                    composite.get_address(manager_index, chain.into()).await.unwrap(),
                    *expected
                );
                assert_eq!(
                    composite.create_wallet(manager_index, chain.into()).await.unwrap(),
                    *expected
                );
            }
            assert_eq!(
                composite.create_wallet(0, chain.into()).await.unwrap(),
                EvmAddress::new(Address::ZERO)
            );
            assert_eq!(*primary.seen_indexes.lock().unwrap(), vec![0]);
        }
    }
}
