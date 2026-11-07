use std::sync::OnceLock;

use reqwest::Url;
use thiserror::Error;

use crate::{network::ChainId, shared::common_types::EvmAddress};

pub(super) const PRODUCTION_GATEWAY: &str =
    "https://tx-submission-api.spicenet.io/internal/orders/base-rpc";
pub(super) const PRODUCTION_WALLET: &str = "0x43558ebdCfEb6EeD08f636fa515e221a4755e0d4";

#[derive(Clone, Copy, Debug, Error)]
#[error("Restricted Base configuration requires a paired non-production signer and gateway URL (HTTPS, or loopback HTTP, at /internal/orders/base-rpc)")]
pub struct LimitBaseConfigError;

pub(super) struct LimitBaseConfig {
    pub address: EvmAddress,
    pub gateway: Url,
    pub enabled: bool,
}

impl LimitBaseConfig {
    fn parse(
        address: Option<&str>,
        gateway: Option<&str>,
        enabled: bool,
    ) -> Result<Self, LimitBaseConfigError> {
        let (address, gateway, overridden) = match (address, gateway) {
            (None, None) => (PRODUCTION_WALLET, PRODUCTION_GATEWAY, false),
            (Some(address), Some(gateway)) => (address, gateway, true),
            _ => return Err(LimitBaseConfigError),
        };
        let address: EvmAddress = address.parse().map_err(|_| LimitBaseConfigError)?;
        let gateway = Url::parse(gateway).map_err(|_| LimitBaseConfigError)?;
        let loopback = gateway.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        });
        if !(gateway.scheme() == "https" || gateway.scheme() == "http" && loopback)
            || gateway.host_str().is_none()
            || !gateway.username().is_empty()
            || gateway.password().is_some()
            || gateway.query().is_some()
            || gateway.fragment().is_some()
            || gateway.path() != "/internal/orders/base-rpc"
        {
            return Err(LimitBaseConfigError);
        }
        // Overrides describe a separate custody/transport pair, never half of
        // the production lane. Credentials still come from the existing auth settings.
        if overridden
            && (address.to_string().eq_ignore_ascii_case(PRODUCTION_WALLET)
                || address
                    .to_string()
                    .eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                || gateway.host_str().is_some_and(|host| {
                    host.trim_end_matches('.').eq_ignore_ascii_case("tx-submission-api.spicenet.io")
                }))
        {
            return Err(LimitBaseConfigError);
        }
        Ok(Self { address, gateway, enabled })
    }

    pub fn matches(&self, chain_id: &ChainId, address: &EvmAddress) -> bool {
        chain_id.u64() == 8453 && self.address == *address
    }
}

// Provider loading validates this before serving requests. The immutable snapshot
// also owns cancel/replace/random-send identity checks throughout the process.
pub(super) fn limit_base_config() -> Result<&'static LimitBaseConfig, LimitBaseConfigError> {
    static CONFIG: OnceLock<Result<LimitBaseConfig, LimitBaseConfigError>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let read = |name| match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(_) => Err(LimitBaseConfigError),
            };
            let address = read("RRELAYER_LIMIT_RPC_SIGNER_ADDRESS")?;
            let gateway = read("RRELAYER_LIMIT_RPC_GATEWAY_URL")?;
            let enabled = read("RRELAYER_LIMIT_RPC_GATEWAY_ENABLED")?.as_deref() == Some("true");
            LimitBaseConfig::parse(address.as_deref(), gateway.as_deref(), enabled)
        })
        .as_ref()
        .map_err(|error| *error)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    const LOCAL: &str = "http://127.0.0.1:18547/internal/orders/base-rpc";

    #[test]
    fn production_defaults_and_enable_flag_are_preserved() {
        for enabled in [false, true] {
            let config = LimitBaseConfig::parse(None, None, enabled).unwrap();
            assert_eq!(config.gateway.as_str(), PRODUCTION_GATEWAY);
            assert!(config.matches(&ChainId::new(8453), &PRODUCTION_WALLET.parse().unwrap()));
            assert_eq!(config.enabled, enabled);
        }
    }

    #[test]
    fn controlled_pair_preserves_chain_and_signer_identity() {
        let config = LimitBaseConfig::parse(Some(SIGNER), Some(LOCAL), true).unwrap();
        assert!(config.matches(&ChainId::new(8453), &SIGNER.parse().unwrap()));
        assert!(!config.matches(&ChainId::new(1), &SIGNER.parse().unwrap()));
        assert!(!config.matches(&ChainId::new(8453), &PRODUCTION_WALLET.parse().unwrap()));
        assert!(LimitBaseConfig::parse(
            Some(SIGNER),
            Some("https://dev.example/internal/orders/base-rpc"),
            true
        )
        .is_ok());
    }

    #[test]
    fn rejects_partial_and_mixed_production_settings_even_when_disabled() {
        for enabled in [false, true] {
            for (address, gateway) in [
                (Some(SIGNER), None),
                (None, Some(LOCAL)),
                (Some(""), Some(LOCAL)),
                (Some(SIGNER), Some("")),
                (Some(PRODUCTION_WALLET), Some(LOCAL)),
                (Some(SIGNER), Some(PRODUCTION_GATEWAY)),
                (Some(PRODUCTION_WALLET), Some(PRODUCTION_GATEWAY)),
                (Some("0x0000000000000000000000000000000000000000"), Some(LOCAL)),
            ] {
                assert!(LimitBaseConfig::parse(address, gateway, enabled).is_err());
            }
        }
    }

    #[test]
    fn rejects_unsafe_transport_and_embedded_credentials() {
        for gateway in [
            "http://dev.example/internal/orders/base-rpc",
            "https://user:password@dev.example/internal/orders/base-rpc",
            "https://dev.example/internal/orders/base-rpc?key=value",
            "https://dev.example/internal/orders/base-rpc#fragment",
            "https://dev.example/ordinary-rpc",
            "https://TX-SUBMISSION-API.SPICENET.IO.:443/internal/orders/base-rpc",
        ] {
            assert!(LimitBaseConfig::parse(Some(SIGNER), Some(gateway), true).is_err());
        }
    }
}
