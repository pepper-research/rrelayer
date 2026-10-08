use std::sync::Arc;

use alloy::{eips::BlockNumberOrTag, primitives::utils::parse_units};
use async_trait::async_trait;

use super::base::{BaseGasFeeEstimator, GasEstimatorError, GasEstimatorResult, GasPriceResult};
use crate::{
    gas::types::{MaxFee, MaxPriorityFee},
    network::ChainId,
    provider::RelayerProvider,
};

#[derive(Clone)]
pub struct FallbackGasFeeEstimator {
    provider: Arc<RelayerProvider>,
}
impl FallbackGasFeeEstimator {
    pub fn new(provider: Arc<RelayerProvider>) -> Self {
        FallbackGasFeeEstimator { provider }
    }

    async fn estimate_with_fee_history(
        &self,
        chain_id: &ChainId,
    ) -> Result<(u128, u128, u128), GasEstimatorError> {
        let ethereum_or_ethereum_testnet = chain_id.u64() == 1 || chain_id.u64() == 11155111;
        let past_blocks = if ethereum_or_ethereum_testnet { 20 } else { 60 };
        let reward_percentile = if ethereum_or_ethereum_testnet { 60.0 } else { 25.0 };

        let fee_history = self
            .provider
            .get_fee_history(past_blocks, BlockNumberOrTag::Latest, &[reward_percentile])
            .await
            .map_err(|e| GasEstimatorError::CustomError(e.to_string()))?;

        let base_fee_per_gas = match fee_history.latest_block_base_fee() {
            Some(base_fee) if base_fee != 0 => base_fee,
            _ => self
                .provider
                .get_block_by_number(BlockNumberOrTag::Latest)
                .await
                .map_err(|e| GasEstimatorError::CustomError(e.to_string()))?
                .ok_or_else(|| {
                    GasEstimatorError::CustomError("Latest block not found".to_string())
                })?
                .header
                .base_fee_per_gas
                .ok_or_else(|| {
                    GasEstimatorError::CustomError("EIP-1559 not supported".to_string())
                })?
                .into(),
        };

        let priority_fee = if let Some(rewards) = &fee_history.reward {
            if !rewards.is_empty() {
                let mut all_rewards: Vec<u128> = rewards
                    .iter()
                    .filter_map(|block_rewards| block_rewards.first().copied())
                    .collect();

                if !all_rewards.is_empty() {
                    all_rewards.sort();
                    let median_idx = all_rewards.len() / 2;
                    all_rewards[median_idx]
                } else if ethereum_or_ethereum_testnet {
                    parse_units("2", "gwei").unwrap().try_into().unwrap() // 2 gwei default for Ethereum
                } else {
                    parse_units("0.01", "gwei").unwrap().try_into().unwrap()
                }
            } else if ethereum_or_ethereum_testnet {
                parse_units("2", "gwei").unwrap().try_into().unwrap() // 2 gwei default for Ethereum
            } else {
                parse_units("0.01", "gwei").unwrap().try_into().unwrap() // 0.01 gwei default for other chains
            }
        } else if ethereum_or_ethereum_testnet {
            parse_units("2", "gwei").unwrap().try_into().unwrap() // 2 gwei default for Ethereum
        } else {
            parse_units("0.01", "gwei").unwrap().try_into().unwrap() // 0.01 gwei default for other chains
        };

        let max_fee = if chain_id.u64() == 1 {
            (base_fee_per_gas + priority_fee).max(priority_fee * 2)
        } else {
            base_fee_per_gas + (priority_fee * 2)
        };

        Ok((priority_fee, max_fee, base_fee_per_gas))
    }
}

#[async_trait]
impl BaseGasFeeEstimator for FallbackGasFeeEstimator {
    async fn get_gas_prices(
        &self,
        _chain_id: &ChainId,
    ) -> Result<GasEstimatorResult, GasEstimatorError> {
        let (base_priority_fee, base_max_fee, current_base_fee) =
            match self.estimate_with_fee_history(_chain_id).await {
                Ok(fees) => fees,
                Err(_) => {
                    let suggested = self
                        .provider
                        .estimate_eip1559_fees()
                        .await
                        .map_err(|e| GasEstimatorError::CustomError(e.to_string()))?;

                    let priority_fee = suggested.max_priority_fee_per_gas;
                    let max_fee = if _chain_id.u64() == 1 {
                        suggested.max_fee_per_gas.max(priority_fee * 2) // Original logic for Ethereum
                    } else {
                        suggested.max_fee_per_gas // Simplified for other chains
                    };
                    let current_base_fee = self
                        .provider
                        .get_block_by_number(BlockNumberOrTag::Latest)
                        .await
                        .map_err(|e| GasEstimatorError::CustomError(e.to_string()))?
                        .and_then(|block| block.header.base_fee_per_gas)
                        .map(|fee| fee as u128)
                        .unwrap_or(0);
                    (priority_fee, max_fee, current_base_fee)
                }
            };

        // Reuse the base fee from the same snapshot used to price the tiers.
        Ok(GasEstimatorResult {
            slow: GasPriceResult {
                max_priority_fee: MaxPriorityFee::new((base_priority_fee * 80) / 100), // -20%
                max_fee: MaxFee::new(
                    ((base_max_fee * 90) / 100).max(current_base_fee + base_priority_fee),
                ),
                min_wait_time_estimate: Some(120), // 2 minutes
                max_wait_time_estimate: Some(300), // 5 minutes
            },
            medium: GasPriceResult {
                max_priority_fee: MaxPriorityFee::new(base_priority_fee),
                max_fee: MaxFee::new(base_max_fee.max(current_base_fee + base_priority_fee)),
                min_wait_time_estimate: Some(30),  // 30 seconds
                max_wait_time_estimate: Some(120), // 2 minutes
            },
            fast: GasPriceResult {
                max_priority_fee: MaxPriorityFee::new((base_priority_fee * 130) / 100), // +30%
                max_fee: MaxFee::new(
                    ((base_max_fee * 120) / 100).max(current_base_fee + base_priority_fee),
                ),
                min_wait_time_estimate: Some(15), // 15 seconds
                max_wait_time_estimate: Some(60), // 1 minute
            },
            super_fast: GasPriceResult {
                max_priority_fee: MaxPriorityFee::new((base_priority_fee * 180) / 100), // +80%
                max_fee: MaxFee::new(
                    ((base_max_fee * 150) / 100).max(current_base_fee + base_priority_fee),
                ),
                min_wait_time_estimate: Some(5),  // 5 seconds
                max_wait_time_estimate: Some(30), // 30 seconds
            },
        })
    }

    fn is_chain_supported(&self, _: &ChainId) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{network::AnyNetwork, providers::ProviderBuilder};
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::sync::Mutex;

    struct RpcState {
        calls: Mutex<Vec<String>>,
        base_fees: Vec<&'static str>,
        fail_history: usize,
        fail_block: bool,
    }

    async fn rpc(State(state): State<Arc<RpcState>>, Json(request): Json<Value>) -> Json<Value> {
        let method = request["method"].as_str().unwrap();
        let history_calls = {
            let mut calls = state.calls.lock().unwrap();
            calls.push(method.to_string());
            calls.iter().filter(|method| *method == "eth_feeHistory").count()
        };
        let result = match method {
            "eth_feeHistory" if history_calls > state.fail_history => json!({
                "oldestBlock":"0x1", "baseFeePerGas":state.base_fees,
                "gasUsedRatio":[0.5], "reward":[["0xa"],["0x14"],["0x1e"]]
            }),
            "eth_getBlockByNumber" if !state.fail_block => {
                let block = alloy::rpc::types::Block::<alloy::rpc::types::Transaction>::default();
                let mut value = serde_json::to_value(block).unwrap();
                value["baseFeePerGas"] = json!("0x64");
                value
            }
            _ => {
                return Json(json!({"jsonrpc":"2.0","id":request["id"],
                "error":{"code":-32603,"message":"fixture RPC failure"}}))
            }
        };
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    }

    async fn run_estimate(
        chain_id: u64,
        base_fees: Vec<&'static str>,
        fail_history: usize,
        fail_block: bool,
    ) -> (Result<GasEstimatorResult, GasEstimatorError>, Vec<String>) {
        let state = Arc::new(RpcState {
            calls: Mutex::new(Vec::new()),
            base_fees,
            fail_history,
            fail_block,
        });
        let app = Router::new().route("/", post(rpc)).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap()).parse().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = ProviderBuilder::new().network::<AnyNetwork>().connect_http(url);
        let estimator = FallbackGasFeeEstimator::new(Arc::new(Box::new(provider)));
        let result = estimator.get_gas_prices(&ChainId::new(chain_id)).await;
        server.abort();
        let calls = state.calls.lock().unwrap().clone();
        (result, calls)
    }

    #[tokio::test]
    async fn valid_history_reuses_base_fee_and_preserves_tiers() {
        for chain in [1, 8453] {
            let (result, calls) = run_estimate(chain, vec!["0x64", "0x70"], 0, false).await;
            let prices = result.unwrap();
            assert_eq!(calls, ["eth_feeHistory"]);
            let expected = if chain == 1 { [120, 120, 144, 180] } else { [126, 140, 168, 210] };
            for ((tier, max), priority) in
                [prices.slow, prices.medium, prices.fast, prices.super_fast]
                    .iter()
                    .zip(expected)
                    .zip([16, 20, 26, 36])
            {
                assert_eq!(tier.max_fee.into_u128(), max);
                assert_eq!(tier.max_priority_fee.into_u128(), priority);
            }
        }
    }

    #[tokio::test]
    async fn missing_or_zero_history_base_fee_reads_block_once() {
        for fees in [vec![], vec!["0x0", "0x70"]] {
            let (result, calls) = run_estimate(8453, fees, 0, false).await;
            assert_eq!(result.unwrap().medium.max_fee.into_u128(), 140);
            assert_eq!(calls, ["eth_feeHistory", "eth_getBlockByNumber"]);
        }
    }

    #[tokio::test]
    async fn history_failure_retains_provider_fallback() {
        let (result, calls) = run_estimate(8453, vec![], usize::MAX, false).await;
        // Alloy's fallback also needs fee history, so its RPC error must propagate.
        assert!(result.is_err());
        assert!(calls.iter().filter(|method| *method == "eth_feeHistory").count() >= 2);
    }

    #[tokio::test]
    async fn transient_history_failure_uses_provider_estimate_and_block_floor() {
        let (result, calls) = run_estimate(8453, vec!["0x64", "0x70"], 1, false).await;
        let prices = result.unwrap();
        assert_eq!(calls.iter().filter(|method| *method == "eth_feeHistory").count(), 2);
        assert!(calls.contains(&"eth_getBlockByNumber".to_string()));
        let floor = 100 + prices.medium.max_priority_fee.into_u128();
        for tier in [prices.slow, prices.medium, prices.fast, prices.super_fast] {
            assert!(tier.max_fee.into_u128() >= floor);
        }
    }

    #[tokio::test]
    async fn unavailable_base_fee_remains_an_error() {
        let (result, calls) = run_estimate(8453, vec![], 0, true).await;
        assert!(result.is_err());
        assert!(calls.contains(&"eth_getBlockByNumber".to_string()));
    }
}
