use super::*;
use crate::gas::CustomGasFeeEstimator;
use crate::NetworkSetupConfig;
use alloy::consensus::{Transaction as _, TxEip7702, TxEnvelope, TxLegacy};
use alloy::primitives::{address, Bytes, U256};
use alloy::rpc::types::{serde_helpers::WithOtherFields, TransactionRequest};
use alloy::signers::{local::PrivateKeySigner, Signer};
use alloy_eips::{eip2718::Decodable2718, eip7702::Authorization};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};

pub(in crate::transaction::queue_system) struct SimulationBudget {
    balance: U256,
    inner_native_value: U256,
    fee_sensitive: bool,
    final_reply: Option<Value>,
    pub(in crate::transaction::queue_system) receipts: HashMap<String, Value>,
}

pub(in crate::transaction::queue_system) struct Fixture {
    pub(in crate::transaction::queue_system) queue: TransactionsQueue,
    transaction: TypedTransaction,
    pub(in crate::transaction::queue_system) requests: Arc<Mutex<Vec<Value>>>,
    pub(in crate::transaction::queue_system) budget: Arc<Mutex<SimulationBudget>>,
    _server: FixtureServer,
}

struct FixtureServer(tokio::task::JoinHandle<()>);

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Fixture {
    pub(in crate::transaction::queue_system) async fn new(
        chain_id: u64,
        simulation_reply: Value,
        parent_fee: u64,
    ) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let budget = Arc::new(Mutex::new(SimulationBudget {
            balance: U256::from(100_000_000),
            inner_native_value: U256::ZERO,
            fee_sensitive: false,
            final_reply: None,
            receipts: HashMap::new(),
        }));
        let state = (chain_id, simulation_reply, parent_fee, requests.clone(), budget.clone());
        let app = Router::new()
            .route("/", post(rpc))
            .route("/gas/:chain_id", get(gas_prices))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config: NetworkSetupConfig = serde_yaml::from_str(&format!(
            "name: fixture\nchain_id: {chain_id}\nprovider_urls: [\"{endpoint}\"]"
        ))
        .unwrap();
        let provider = EvmProvider::new_with_mnemonic(
            &config,
            "test test test test test test test test test test test junk",
            Arc::new(CustomGasFeeEstimator {
                endpoint: format!("{endpoint}/gas"),
                supported_chains: vec![ChainId::new(chain_id)],
                auth_header: None,
            }),
        )
        .await
        .unwrap();
        let relayer = Relayer {
            id: RelayerId::new(),
            name: "fixture".to_string(),
            chain_id: ChainId::new(chain_id),
            cloned_from_chain_id: None,
            address: provider.get_address(0).await.unwrap(),
            wallet_index: 0,
            max_gas_price: None,
            paused: false,
            eip_1559_enabled: true,
            created_at: Utc::now(),
            is_private_key: false,
        };
        // A real fixture authorization must survive both RPC and signed-envelope serialization.
        let authority: PrivateKeySigner =
            "2121212121212121212121212121212121212121212121212121212121212121".parse().unwrap();
        let authorization = Authorization {
            chain_id: U256::from(chain_id),
            address: address!("2222222222222222222222222222222222222222"),
            nonce: 0,
        };
        let signature = authority.sign_hash(&authorization.signature_hash()).await.unwrap();
        let transaction = TypedTransaction::Eip7702(TxEip7702 {
            chain_id,
            nonce: 1, // Latest sender state is nonce 0; nonce 0 can already be in flight.
            gas_limit: 10_000_000,
            max_fee_per_gas: 200,
            max_priority_fee_per_gas: 1,
            to: relayer.address.into_address(),
            value: U256::from(7),
            input: Bytes::from_static(&[0x12, 0x34]),
            authorization_list: vec![authorization.into_signed(signature)],
            ..Default::default()
        });
        let prior = Transaction {
            id: TransactionId::new(),
            relayer_id: relayer.id,
            authorization_list: None,
            to: relayer.address,
            from: relayer.address,
            value: TransactionValue::zero(),
            data: TransactionData::empty(),
            nonce: TransactionNonce::new(0),
            gas_limit: Some(GasLimit::new(21_000)),
            status: TransactionStatus::INMEMPOOL,
            blobs: None,
            chain_id: ChainId::new(chain_id),
            known_transaction_hash: Some(TransactionHash::from_alloy_hash(
                &alloy::primitives::keccak256(b"prior fixture envelope"),
            )),
            queued_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(1),
            sent_at: Some(Utc::now()),
            mined_at: None,
            mined_at_block_number: None,
            confirmed_at: None,
            speed: TransactionSpeed::FAST,
            sent_with_max_priority_fee_per_gas: None,
            sent_with_max_fee_per_gas: None,
            is_noop: false,
            sent_with_gas: None,
            sent_with_blob_gas: None,
            external_id: None,
            cancelled_by_transaction_id: None,
        };
        let setup = TransactionsQueueSetup::new(
            relayer,
            provider,
            NonceManager::new(TransactionNonce::new(1)),
            VecDeque::new(),
            VecDeque::from([CompetitiveTransaction::new(prior)]),
            HashMap::new(),
            Arc::new(SafeProxyManager::new(vec![])),
            GasBumpBlockConfig::default(),
            10,
        );
        let queue = TransactionsQueue::new(
            setup,
            Arc::new(Mutex::new(GasOracleCache::new())),
            Arc::new(Mutex::new(BlobGasOracleCache::new())),
        );
        requests.lock().await.clear();
        Self { queue, transaction, requests, budget, _server: FixtureServer(server) }
    }

    pub(in crate::transaction::queue_system) async fn cache_gas_prices(&self) {
        crate::gas::gas_oracle(
            Arc::new(vec![self.queue.evm_provider.clone()]),
            self.queue.gas_oracle_cache.clone(),
        )
        .await;
    }
}

async fn gas_prices() -> Json<Value> {
    let speed = json!({ "suggestedMaxPriorityFeePerGas": "0.000000001", "suggestedMaxFeePerGas": "0.000000001" });
    Json(json!({"slow": speed, "medium": speed, "fast": speed, "superFast": speed}))
}

async fn rpc(
    State((chain_id, reply, parent_fee, requests, budget)): State<(
        u64,
        Value,
        u64,
        Arc<Mutex<Vec<Value>>>,
        Arc<Mutex<SimulationBudget>>,
    )>,
    Json(request): Json<Value>,
) -> Json<Value> {
    requests.lock().await.push(request.clone());
    let response = match request["method"].as_str().unwrap_or("") {
        "eth_chainId" => json!({ "result": format!("0x{chain_id:x}") }),
        "eth_blockNumber" => json!({ "result": "0x0" }),
        "eth_getTransactionCount" => {
            json!({ "result": if request["params"][1] == "pending" { "0x1" } else { "0x0" } })
        }
        "eth_estimateGas" => json!({ "result": "0x9c40" }), // Nitro ignoring the fresh authorization.
        "eth_getTransactionReceipt" => json!({ "result": budget.lock().await.receipts
            .get(request["params"][0].as_str().unwrap()).cloned().unwrap_or(Value::Null) }),
        "eth_getBlockByNumber" => json!({ "result": {
            "number": "0x1", "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "00".repeat(32)),
            "sha3Uncles": format!("0x{}", "00".repeat(32)),
            "stateRoot": format!("0x{}", "00".repeat(32)),
            "transactionsRoot": format!("0x{}", "00".repeat(32)),
            "receiptsRoot": format!("0x{}", "00".repeat(32)),
            "logsBloom": format!("0x{}", "00".repeat(256)),
            "miner": format!("0x{}", "00".repeat(20)), "nonce": "0x0000000000000000",
            "mixHash": format!("0x{}", "00".repeat(32)),
            "difficulty": "0x0", "gasLimit": "0x989680", "gasUsed": "0x0",
            "timestamp": "0x1", "extraData": "0x", "baseFeePerGas": "0x64",
            "transactions": [], "uncles": []
        }}),
        "eth_call" => json!({ "result": format!("0x{parent_fee:064x}{}", "0".repeat(320)) }),
        "eth_simulateV1" if request["params"][0]["validation"] == true => {
            json!({ "error": { "code": -32000, "message": "nonce too high: latest sender nonce is zero" } })
        }
        "eth_simulateV1" => {
            let block = &request["params"][0]["blockStateCalls"][0];
            assert!(block.get("stateOverrides").is_none());
            let call = &block["calls"][0];
            let gas = quantity(&call["gas"]);
            let value = quantity(&call["value"]);
            let legacy = call.get("gasPrice").is_some();
            let fee = quantity(&call[if legacy { "gasPrice" } else { "maxFeePerGas" }]);
            let tip = call.get("maxPriorityFeePerGas").map(quantity).unwrap_or_default();
            let base =
                block["blockOverrides"].get("baseFeePerGas").map(quantity).unwrap_or_default();
            let effective = if legacy { fee } else { fee.min(base + tip) };
            let budget = budget.lock().await;
            if budget.balance < gas * fee + value {
                json!({ "error": { "code": -38014, "message": "insufficient funds for gas * price + value" } })
            } else if budget.balance - gas * effective - value < budget.inner_native_value {
                json!({ "result": [{ "calls": [{ "status": "0x0", "error": {
                    "code": 3, "message": "CallReverted(nativecall)", "data": "0xdeadbeef"
                } }] }] })
            } else if budget.fee_sensitive && effective > U256::ZERO {
                json!({ "result": [{ "calls": [{ "status": "0x0", "error": {
                    "code": -32000, "message": "out of gas on GASPRICE-sensitive path", "data": "0x1234"
                } }] }] })
            } else if fee > U256::ZERO {
                budget.final_reply.clone().unwrap_or(reply)
            } else {
                reply
            }
        }
        _ => json!({ "error": { "code": -32601, "message": "Unexpected fixture RPC method" } }),
    };
    let mut response = response.as_object().unwrap().clone();
    response.insert("jsonrpc".to_string(), json!("2.0"));
    response.insert("id".to_string(), request["id"].clone());
    Json(Value::Object(response))
}

fn quantity(value: &Value) -> U256 {
    U256::from_str_radix(value.as_str().unwrap().strip_prefix("0x").unwrap(), 16).unwrap()
}

fn success() -> Value {
    json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x13880", "maxUsedGas": "0x186a0" }] }] })
}

#[tokio::test]
async fn elysium_queue_signs_authorization_aware_gas_without_rejecting_a_future_sender_nonce() {
    let fixture = Fixture::new(99901, success(), 0).await;
    let mut request: TransactionRequest = fixture.transaction.clone().into();
    request.from = Some(fixture.queue.relayer.address.into_address());
    let ordinary = fixture
        .queue
        .evm_provider
        .rpc_client()
        .estimate_gas(WithOtherFields::new(request.clone()))
        .await
        .unwrap();
    assert_eq!(ordinary, 40_000);
    let nonce_control: Result<Value, _> = fixture
        .queue
        .evm_provider
        .rpc_client()
        .client()
        .request(
            "eth_simulateV1",
            (json!({ "blockStateCalls": [{ "calls": [request] }], "validation": true }), "0x1"),
        )
        .await;
    assert!(nonce_control.is_err());
    let prior_id = fixture.queue.inmempool_transactions.lock().await[0].original.id;

    let gas = fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap();
    assert_eq!(gas, GasLimit::new(120_000)); // The existing queue applies its 20% buffer once.
    assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
    let nonce = fixture.queue.nonce_manager.get_and_increment().await;
    let TypedTransaction::Eip7702(mut transaction) = fixture.transaction.clone() else {
        panic!("Expected type-4 fixture transaction");
    };
    transaction.gas_limit = gas.into();
    transaction.nonce = nonce.into_inner();
    let (bytes, _) = fixture
        .queue
        .evm_provider
        .sign_raw_transaction(
            &fixture.queue.relayer,
            TypedTransaction::Eip7702(transaction.clone()),
        )
        .await
        .unwrap();
    let signed = TxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap();
    assert_eq!(signed.gas_limit(), 120_000);
    assert_eq!(signed.nonce(), 1);
    assert_eq!(signed.input(), &transaction.input);
    assert_eq!(signed.authorization_list().unwrap(), transaction.authorization_list.as_slice());
    let prior = fixture.queue.inmempool_transactions.lock().await;
    assert_eq!(prior.len(), 1);
    assert_eq!(prior[0].original.id, prior_id);
    assert_eq!(prior[0].original.status, TransactionStatus::INMEMPOOL);
    assert_eq!(prior[0].original.nonce.into_inner(), 0);
    assert!(prior[0].competitive.is_none());

    let requests = fixture.requests.lock().await;
    assert_eq!(requests.iter().filter(|request| request["method"] == "eth_estimateGas").count(), 1);
    let simulations: Vec<_> =
        requests.iter().filter(|request| request["method"] == "eth_simulateV1").collect();
    assert_eq!(simulations.len(), 3);
    assert_eq!(simulations[1]["params"][1], "0x1");
    let block = &simulations[1]["params"][0]["blockStateCalls"][0];
    assert_eq!(simulations[1]["params"][0]["validation"], false);
    assert!(block.get("stateOverrides").is_none());
    let call = &block["calls"][0];
    assert_eq!(call["from"], json!(fixture.queue.relayer.address));
    assert_eq!(call["to"], json!(fixture.queue.relayer.address));
    assert_eq!(call["input"], "0x1234");
    assert_eq!(call["value"], "0x7");
    assert_eq!(call["nonce"], "0x1");
    assert_eq!(call["chainId"], "0x1863d");
    assert_eq!(call["authorizationList"], json!(transaction.authorization_list));
    assert_eq!(call["maxFeePerGas"], "0x0");
    assert_eq!(call["maxPriorityFeePerGas"], "0x0");
    assert_eq!(call["gas"], "0x989680");
    let final_block = &simulations[2]["params"][0]["blockStateCalls"][0];
    assert_eq!(simulations[2]["params"][0]["validation"], false);
    assert_eq!(final_block["blockOverrides"]["baseFeePerGas"], "0x64");
    assert!(final_block.get("stateOverrides").is_none());
    let final_call = &final_block["calls"][0];
    assert_eq!(final_call["maxFeePerGas"], "0xc8");
    assert_eq!(final_call["maxPriorityFeePerGas"], "0x1");
    assert_eq!(final_call["gas"], "0x1d4c0");
    for field in ["from", "to", "input", "value", "nonce", "chainId", "authorizationList"] {
        assert_eq!(final_call[field], call[field]);
    }
    assert!(!requests.iter().any(|request| request["method"] == "eth_sendRawTransaction"));
}

#[tokio::test]
async fn elysium_estimation_fails_closed_without_consuming_a_queue_nonce() {
    let invalid = [
        json!({ "error": { "code": -32601, "message": "eth_simulateV1 unsupported" } }),
        json!({ "result": [] }),
        json!({ "result": [{ "calls": [] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x0", "error": { "code": 3, "message": "insufficient native balance", "data": "0x1234" } }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x1" }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x186a0", "maxUsedGas": "0x1" }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x0", "maxUsedGas": "0x0" }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x1", "maxUsedGas": "0x989681" }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x1", "maxUsedGas": "0x+1" }] }] }),
    ];
    for response in invalid {
        let fixture = Fixture::new(99901, response, 0).await;
        assert!(fixture.queue.estimate_gas(&fixture.transaction, false, None).await.is_err());
        assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
        assert!(!fixture
            .requests
            .lock()
            .await
            .iter()
            .any(|request| request["method"] == "eth_estimateGas"));
    }
    let fixture = Fixture::new(99901, success(), 1).await;
    assert!(fixture.queue.estimate_gas(&fixture.transaction, false, None).await.is_err());
    assert!(!fixture
        .requests
        .lock()
        .await
        .iter()
        .any(|request| request["method"] == "eth_simulateV1"));
}

#[tokio::test]
async fn elysium_change_preserves_ordinary_network_estimation() {
    let fixture = Fixture::new(1, success(), 1).await;
    assert_eq!(
        fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap(),
        GasLimit::new(48_000)
    );
    let requests = fixture.requests.lock().await;
    assert!(requests.iter().any(|request| request["method"] == "eth_estimateGas"));
    assert!(!requests
        .iter()
        .any(|request| request["method"] == "eth_simulateV1" || request["method"] == "eth_call"));
}

#[tokio::test]
async fn elysium_measurement_does_not_require_full_cap_funding() {
    let fixture = Fixture::new(99901, success(), 0).await;
    fixture.budget.lock().await.balance = U256::from(120_000 * 200 + 8);
    assert_eq!(
        fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap(),
        GasLimit::new(120_000)
    );
    let requests = fixture.requests.lock().await;
    let simulations: Vec<_> = requests.iter().filter(|r| r["method"] == "eth_simulateV1").collect();
    assert_eq!(simulations.len(), 2);
    assert_eq!(
        simulations[0]["params"][0]["blockStateCalls"][0]["calls"][0]["maxFeePerGas"],
        "0x0"
    );
    assert_eq!(simulations[1]["params"][0]["blockStateCalls"][0]["calls"][0]["gas"], "0x1d4c0");
    assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
}

#[tokio::test]
async fn elysium_inner_native_principal_survives_measurement_and_exact_budget_validation() {
    let mut fixture = Fixture::new(99901, success(), 0).await;
    let TypedTransaction::Eip7702(transaction) = &mut fixture.transaction else {
        panic!("type 4");
    };
    transaction.value = U256::ZERO;
    let principal = U256::from(1_000_000_000_000_000_000u128);
    {
        let mut budget = fixture.budget.lock().await;
        budget.balance = principal + U256::from(120_000 * 200 + 1);
        budget.inner_native_value = principal;
    }
    let mut call: TransactionRequest = fixture.transaction.clone().into();
    call.from = Some(fixture.queue.relayer.address.into_address());
    let cap_control: Value = fixture.queue.evm_provider.rpc_client().client().request(
        "eth_simulateV1", (json!({ "blockStateCalls": [{ "blockOverrides": { "baseFeePerGas": "0x64" }, "calls": [call] }], "validation": false }), "0x1")
    ).await.unwrap();
    assert_eq!(cap_control[0]["calls"][0]["status"], "0x0");
    assert_eq!(cap_control[0]["calls"][0]["error"]["data"], "0xdeadbeef");
    assert_eq!(
        fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap(),
        GasLimit::new(120_000)
    );
    assert_eq!(fixture.budget.lock().await.balance, principal + U256::from(120_000 * 200 + 1));
}

#[tokio::test]
async fn elysium_final_fee_sensitive_failure_cannot_be_accepted_as_zero_fee_success() {
    let fixture = Fixture::new(99901, success(), 0).await;
    fixture.budget.lock().await.fee_sensitive = true;
    let error = fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap_err();
    assert!(error.to_string().contains("GASPRICE-sensitive"));
    assert!(error.to_string().contains("0x1234"));
    assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
    let requests = fixture.requests.lock().await;
    let simulations: Vec<_> = requests.iter().filter(|r| r["method"] == "eth_simulateV1").collect();
    assert_eq!(simulations.len(), 2);
    assert_eq!(
        simulations[1]["params"][0]["blockStateCalls"][0]["blockOverrides"]["baseFeePerGas"],
        "0x64"
    );
}

#[tokio::test]
async fn elysium_final_response_must_fit_the_exact_final_budget() {
    let invalid = [
        json!({ "error": { "code": -32601, "message": "validation unavailable" } }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x1", "maxUsedGas": "0x1d4c1" }] }] }),
        json!({ "result": [{ "calls": [{ "status": "0x1", "gasUsed": "0x1" }] }] }),
    ];
    for reply in invalid {
        let fixture = Fixture::new(99901, success(), 0).await;
        fixture.budget.lock().await.final_reply = Some(reply);
        assert!(fixture.queue.estimate_gas(&fixture.transaction, false, None).await.is_err());
        assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
    }
}

#[tokio::test]
async fn elysium_cached_gas_is_checked_against_changed_signing_fees() {
    let fixture = Fixture::new(99901, success(), 0).await;
    let gas = fixture.queue.estimate_gas(&fixture.transaction, false, None).await.unwrap();
    let TypedTransaction::Eip7702(mut transaction) = fixture.transaction.clone() else {
        panic!("type 4");
    };
    transaction.gas_limit = gas.into();
    transaction.max_fee_per_gas = 1000;
    let error = fixture
        .queue
        .evm_provider
        .validate_estimated_gas(
            &TypedTransaction::Eip7702(transaction),
            &fixture.queue.relayer.address,
            gas,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("insufficient funds"));
    assert_eq!(fixture.queue.nonce_manager.get_current_nonce().await.into_inner(), 1);
}

#[tokio::test]
async fn elysium_legacy_fees_are_zero_only_during_measurement() {
    let fixture = Fixture::new(99901, success(), 0).await;
    let transaction = TypedTransaction::Legacy(TxLegacy {
        chain_id: Some(99901),
        nonce: 1,
        gas_price: 200,
        gas_limit: 10_000_000,
        to: fixture.queue.relayer.address.into_address().into(),
        value: U256::from(7),
        input: Bytes::from_static(&[0x12, 0x34]),
    });
    assert_eq!(
        fixture.queue.estimate_gas(&transaction, false, None).await.unwrap(),
        GasLimit::new(120_000)
    );
    let requests = fixture.requests.lock().await;
    let simulations: Vec<_> = requests.iter().filter(|r| r["method"] == "eth_simulateV1").collect();
    assert_eq!(simulations.len(), 2);
    let first = &simulations[0]["params"][0]["blockStateCalls"][0]["calls"][0];
    let final_call = &simulations[1]["params"][0]["blockStateCalls"][0]["calls"][0];
    assert_eq!(first["gasPrice"], "0x0");
    assert_eq!(final_call["gasPrice"], "0xc8");
    assert!(first.get("maxFeePerGas").is_none());
}

#[tokio::test]
async fn elysium_noop_keeps_the_unbuffered_budget() {
    let fixture = Fixture::new(99901, success(), 0).await;
    assert_eq!(
        fixture.queue.estimate_gas(&fixture.transaction, true, None).await.unwrap(),
        GasLimit::new(100_000)
    );
    let requests = fixture.requests.lock().await;
    let last = requests.iter().filter(|r| r["method"] == "eth_simulateV1").last().unwrap();
    assert_eq!(last["params"][0]["blockStateCalls"][0]["calls"][0]["gas"], "0x186a0");
}
