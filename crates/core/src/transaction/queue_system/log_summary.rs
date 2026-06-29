use std::fmt::Debug;

use alloy::sol_types::{decode_revert_reason, SolCall, SolError};
use alloy::{hex, sol, transports::RpcError};
use chrono::Utc;

use crate::{relayer::Relayer, transaction::types::Transaction};

const CALL_REVERTED_SELECTOR: &str = "0x863d2b4f";

sol! {
    error CallReverted(address account, address target, uint256 value, bytes callData, bytes innerError);

    function transfer(address to, uint256 amount);
    function approve(address spender, uint256 amount);
    function stake(uint256 amount, uint256 duration);
}

pub(super) fn transaction_context(transaction: &Transaction, relayer: &Relayer) -> String {
    format!(
        "tx_id={} external_id={} relayer={} relayer_id={} relayer_addr={} chain={} nonce={} tx_type={} to={} selector={} data_bytes={} auths={} noop={} age_ms={}",
        transaction.id,
        transaction.external_id.as_deref().unwrap_or("-"),
        relayer.name,
        relayer.id,
        relayer.address,
        transaction.chain_id,
        transaction.nonce.into_inner(),
        transaction_type(transaction),
        transaction.to,
        transaction_selector(transaction),
        transaction_data_len(transaction),
        transaction.authorization_list.as_ref().map(Vec::len).unwrap_or_default(),
        transaction.is_noop,
        Utc::now().signed_duration_since(transaction.queued_at).num_milliseconds().max(0),
    )
}

pub(super) fn summarize_rpc_error<E>(error: &RpcError<E>) -> Option<String>
where
    E: Debug,
{
    if let Some(data) = error.as_error_resp().and_then(|payload| payload.as_revert_data()) {
        return Some(summarize_revert_bytes(&data));
    }

    let raw = extract_longest_hex(&format!("{error:?}"))?;
    let bytes = hex::decode(raw.strip_prefix("0x").unwrap_or(&raw)).ok()?;
    Some(summarize_revert_bytes(&bytes))
}

pub(super) fn compact_rpc_error<E>(error: &RpcError<E>) -> String
where
    E: Debug,
{
    let debug = format!("{error:?}");
    let mut output = String::with_capacity(debug.len().min(512));
    let bytes = debug.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if i + 2 <= bytes.len() && bytes[i] == b'0' && matches!(bytes.get(i + 1), Some(b'x' | b'X'))
        {
            let start = i;
            i += 2;
            while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
                i += 1;
            }

            let candidate = &debug[start..i];
            if candidate.len() > 74 {
                output.push_str(&candidate[..74]);
                output.push_str("...");
            } else {
                output.push_str(candidate);
            }
        } else {
            output.push(bytes[i] as char);
            i += 1;
        }
    }

    const MAX_LEN: usize = 900;
    if output.len() > MAX_LEN {
        output.truncate(MAX_LEN);
        output.push_str("...");
    }

    output
}

fn transaction_type(transaction: &Transaction) -> &'static str {
    if transaction.is_7702_transaction() {
        "eip7702"
    } else if transaction.is_blob_transaction() {
        "eip4844"
    } else {
        "eip1559_or_legacy"
    }
}

fn transaction_selector(transaction: &Transaction) -> String {
    let data = transaction.data.hex();
    if data.len() >= 8 {
        format!("0x{}", &data[..8])
    } else {
        "0x".to_string()
    }
}

fn transaction_data_len(transaction: &Transaction) -> usize {
    transaction.data.hex().len() / 2
}

fn summarize_revert_bytes(bytes: &[u8]) -> String {
    if bytes.len() < 4 {
        return format!("raw_bytes={}", bytes.len());
    }

    let selector = selector(&bytes);
    if selector == CALL_REVERTED_SELECTOR {
        if let Some(decoded) = decode_call_reverted(bytes, 0) {
            return decoded.summary();
        }
    }

    if let Some(reason) = decode_revert_reason(bytes) {
        return reason;
    }

    format!(
        "revert_selector={} revert_name={} raw_bytes={}",
        selector,
        selector_name(&selector),
        bytes.len()
    )
}

fn extract_longest_hex(input: &str) -> Option<String> {
    let mut best = String::new();
    let bytes = input.as_bytes();
    let mut i = 0;

    while i + 2 <= bytes.len() {
        if bytes[i] == b'0' && matches!(bytes.get(i + 1), Some(b'x' | b'X')) {
            let start = i;
            i += 2;
            while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
                i += 1;
            }

            let candidate = &input[start..i];
            if candidate.len() > best.len() && candidate.len() >= 10 && candidate.len() % 2 == 0 {
                best = candidate.to_string();
            }
        } else {
            i += 1;
        }
    }

    if best.is_empty() {
        None
    } else {
        Some(best)
    }
}

struct DecodedCallReverted {
    account: String,
    to: String,
    value: String,
    call_name: String,
    call_args: Option<String>,
    error_selector: String,
    error_name: String,
    nested: Option<Box<DecodedCallReverted>>,
}

impl DecodedCallReverted {
    fn summary(&self) -> String {
        let mut frames = Vec::new();
        self.collect_frames(&mut frames);
        let leaf = self.leaf();
        format!(
            "revert_path={} leaf_target={} leaf_call={} leaf_error={} leaf_error_selector={} account={} value={}",
            frames.join(" -> "),
            leaf.to,
            leaf.call_label(),
            leaf.error_name,
            leaf.error_selector,
            leaf.account,
            leaf.value,
        )
    }

    fn collect_frames(&self, frames: &mut Vec<String>) {
        frames.push(format!("Delegate.CallReverted(to={}, call={})", self.to, self.call_label()));
        if let Some(nested) = &self.nested {
            nested.collect_frames(frames);
        } else {
            frames.push(self.error_name.clone());
        }
    }

    fn leaf(&self) -> &Self {
        self.nested.as_deref().map(Self::leaf).unwrap_or(self)
    }

    fn call_label(&self) -> String {
        match &self.call_args {
            Some(args) => format!("{} {}", self.call_name, args),
            None => self.call_name.clone(),
        }
    }
}

fn decode_call_reverted(bytes: &[u8], depth: usize) -> Option<DecodedCallReverted> {
    if depth > 8 || selector(bytes) != CALL_REVERTED_SELECTOR {
        return None;
    }

    let decoded = CallReverted::abi_decode(bytes).ok()?;
    let call_data = decoded.callData.as_ref();
    let error = decoded.innerError.as_ref();

    let call_selector = selector(&call_data);
    let error_selector = selector(&error);
    let nested = if error_selector == CALL_REVERTED_SELECTOR {
        decode_call_reverted(error, depth + 1).map(Box::new)
    } else {
        None
    };

    Some(DecodedCallReverted {
        account: decoded.account.to_string(),
        to: decoded.target.to_string(),
        value: decoded.value.to_string(),
        call_name: selector_name(&call_selector).to_string(),
        call_args: decode_known_call_args(&call_selector, call_data),
        error_name: selector_name(&error_selector).to_string(),
        error_selector,
        nested,
    })
}

fn selector(bytes: &[u8]) -> String {
    if bytes.len() < 4 {
        "0x".to_string()
    } else {
        format!("0x{}", hex::encode(&bytes[..4]))
    }
}

fn selector_name(selector: &str) -> &'static str {
    match selector {
        "0x08c379a0" => "Error(string)",
        "0x4e487b71" => "Panic(uint256)",
        "0x863d2b4f" => "Delegate.CallReverted(address,address,uint256,bytes,bytes)",
        "0xa9059cbb" => "ERC20.transfer(address,uint256)",
        "0x095ea7b3" => "ERC20.approve(address,uint256)",
        "0x09b81346" => "swap/exactOutput",
        "0x7b0472f0" => "veREPPO.stake(uint256,uint256)",
        "0x2520601d" => "CheckpointUnorderedInsertion()",
        "0x817275ab" => "TransferHelper.STF()",
        "0xf9a6d8ca" => "TransferHelper.SA()",
        "0x8b986265" => "TransferHelper.TF()",
        "0xe450d38c" => "ERC20InsufficientBalance(address,uint256,uint256)",
        "0xfb8f41b2" => "ERC20InsufficientAllowance(address,uint256,uint256)",
        "0xcbca5aa2" => "AmountZero()",
        "0x9c0a4763" => "StakingTooShort()",
        "0x5f16f762" => "StakingTooLong()",
        _ => "UNKNOWN",
    }
}

fn decode_known_call_args(selector: &str, data: &[u8]) -> Option<String> {
    match selector {
        "0x7b0472f0" => {
            let call = stakeCall::abi_decode(data).ok()?;
            Some(format!("amount={} duration={}", call.amount, call.duration))
        }
        "0xa9059cbb" => {
            let call = transferCall::abi_decode(data).ok()?;
            Some(format!("to={} amount={}", call.to, call.amount))
        }
        "0x095ea7b3" => {
            let call = approveCall::abi_decode(data).ok()?;
            Some(format!("spender={} amount={}", call.spender, call.amount))
        }
        _ => None,
    }
}
