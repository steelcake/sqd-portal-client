use anyhow::{anyhow, Context, Result};
use arrow::{datatypes::i256, record_batch::RecordBatch};
use cherry_evm_schema::{BlocksBuilder, LogsBuilder, TracesBuilder, TransactionsBuilder};
use serde::{Deserialize, Serialize};
use simd_json::base::ValueAsScalar;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    #[serde(rename = "type")]
    pub type_: QueryType,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    pub fields: Fields,
    pub logs: Vec<LogRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub traces: Vec<TraceRequest>,
    pub state_diffs: Vec<StateDiffRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryType {
    Evm,
}

impl Default for QueryType {
    fn default() -> Self {
        Self::Evm
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topic0: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topic1: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topic2: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub topic3: Vec<String>,
    pub transaction: bool,
    pub transaction_traces: bool,
    pub transaction_logs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sighash: Vec<String>,
    pub logs: bool,
    pub traces: bool,
    pub state_diffs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRequest {
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub type_: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub create_from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_from: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub call_sighash: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suicide_refund_address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reward_author: Vec<String>,
    pub transaction: bool,
    pub transaction_logs: bool,
    pub subtraces: bool,
    pub parents: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiffRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub kind: Vec<String>,
    pub transaction: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fields {
    pub log: LogFields,
    pub transaction: TransactionFields,
    pub state_diff: StateDiffFields,
    pub trace: TraceFields,
    pub block: BlockFields,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFields {
    pub log_index: bool,
    pub transaction_hash: bool,
    pub address: bool,
    pub data: bool,
    pub topics: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFields {
    pub transaction_index: bool,
    pub hash: bool,
    pub nonce: bool,
    pub from: bool,
    pub to: bool,
    pub input: bool,
    pub value: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub max_fee_per_gas: bool,
    pub max_priority_fee_per_gas: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub y_parity: bool,
    pub chain_id: bool,
    pub sighash: bool,
    pub contract_address: bool,
    pub gas_used: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    #[serde(rename = "type")]
    pub type_: bool,
    pub status: bool,
    pub max_fee_per_blob_gas: bool,
    pub blob_versioned_hashes: bool,
    pub l1_fee: bool,
    pub l1_fee_scalar: bool,
    pub l1_gas_price: bool,
    pub l1_gas_used: bool,
    pub l1_blob_base_fee: bool,
    pub l1_blob_base_fee_scalar: bool,
    pub l1_base_fee_scalar: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiffFields {
    pub transaction_index: bool,
    pub address: bool,
    pub key: bool,
    pub kind: bool,
    pub prev: bool,
    pub next: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceFields {
    pub transaction_index: bool,
    pub trace_address: bool,
    pub subtraces: bool,
    #[serde(rename = "type")]
    pub type_: bool,
    pub error: bool,
    pub revert_reason: bool,
    pub create_from: bool,
    pub create_value: bool,
    pub create_gas: bool,
    pub create_init: bool,
    pub create_result_gas_used: bool,
    pub create_result_code: bool,
    pub create_result_address: bool,
    pub call_from: bool,
    pub call_to: bool,
    pub call_value: bool,
    pub call_gas: bool,
    pub call_input: bool,
    pub call_sighash: bool,
    pub call_type: bool,
    pub call_call_type: bool,
    pub call_result_gas_used: bool,
    pub call_result_output: bool,
    pub suicide_address: bool,
    pub suicide_refund_address: bool,
    pub suicide_balance: bool,
    pub reward_author: bool,
    pub reward_value: bool,
    pub reward_type: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFields {
    pub number: bool,
    pub hash: bool,
    pub parent_hash: bool,
    pub timestamp: bool,
    pub transactions_root: bool,
    pub receipts_root: bool,
    pub state_root: bool,
    pub logs_bloom: bool,
    pub sha3_uncles: bool,
    pub extra_data: bool,
    pub miner: bool,
    pub nonce: bool,
    pub mix_hash: bool,
    pub size: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub difficulty: bool,
    pub total_difficulty: bool,
    pub base_fee_per_gas: bool,
    pub blob_gas_used: bool,
    pub excess_blob_gas: bool,
    pub l1_block_number: bool,
}

#[derive(Debug)]
pub struct ArrowResponse {
    pub blocks: RecordBatch,
    pub transactions: RecordBatch,
    pub logs: RecordBatch,
    pub traces: RecordBatch,
    // state_diffs: recordbatch,
}

#[derive(Default)]
pub(crate) struct ArrowResponseParser {
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    logs: LogsBuilder,
    traces: TracesBuilder,
}

impl ArrowResponseParser {
    pub(crate) fn parse_tape(&mut self, tape: &simd_json::tape::Tape<'_>) -> Result<()> {
        let obj = tape.as_value().as_object().context("tape as object")?;
        let header = obj.get("header").context("get header")?;

        let header = header.as_object().context("header as object")?;
        self.parse_header(&header).context("parse header")?;

        Ok(())
    }

    fn parse_header(&mut self, header: &simd_json::tape::Object<'_, '_>) -> Result<()> {
        let number = get_tape_u64(header, "number")?;
        let hash = get_tape_hex(header, "hash")?;
        let parent_hash = get_tape_hex(header, "parentHash")?;
        let timestamp = get_tape_u64(header, "timestamp")?;
        let transactions_root = get_tape_hex(header, "transactionsRoot")?;
        let receipts_root = get_tape_hex(header, "receiptsRoot")?;
        let state_root = get_tape_hex(header, "stateRoot")?;
        let logs_bloom = get_tape_hex(header, "logsBloom")?;
        let sha3_uncles = get_tape_hex(header, "sha3Uncles")?;
        let extra_data = get_tape_hex(header, "extraData")?;
        let miner = get_tape_hex(header, "miner")?;
        let nonce = get_tape_hex(header, "nonce")?;
        let mix_hash = get_tape_hex(header, "mixHash")?;
        let size = get_tape_u64(header, "size")?;
        let gas_limit = get_tape_i256(header, "gasLimit")?;
        let gas_used = get_tape_i256(header, "gasUsed")?;
        let difficulty = get_tape_i256(header, "difficulty")?;
        let total_difficulty = get_tape_i256(header, "totalDifficulty")?;
        let base_fee_per_gas = get_tape_i256(header, "baseFeePerGas")?;
        let blob_gas_used = get_tape_i256(header, "blobGasUsed")?;
        let excess_blob_gas = get_tape_i256(header, "excessBlobGas")?;
        let l1_block_number = get_tape_u64(header, "l1BlockNumber")?;

        self.blocks.number.append_option(number);
        self.blocks.hash.append_option(hash);
        self.blocks.parent_hash.append_option(parent_hash);
        self.blocks.nonce.append_option(nonce);
        self.blocks.sha3_uncles.append_option(sha3_uncles);
        self.blocks.logs_bloom.append_option(logs_bloom);
        self.blocks.transactions_root.append_option(transactions_root);
        self.blocks.state_root.append_option(state_root);
        self.blocks.receipts_root.append_option(receipts_root);
        self.blocks.miner.append_option(miner);
        self.blocks.difficulty.append_option(difficulty);
        self.blocks.total_difficulty.append_option(total_difficulty);
        self.blocks.extra_data.append_option(extra_data);
        self.blocks.size.append_option(size.map(|s| i256::from_i128(i128::from(s))));
        self.blocks.gas_limit.append_option(gas_limit);
        self.blocks.gas_used.append_option(gas_used);
        self.blocks.timestamp.append_option(timestamp.map(|t| i256::from_i128(i128::from(t))));
        self.blocks.uncles.append_null();
        self.blocks.base_fee_per_gas.append_option(base_fee_per_gas);
        self.blocks.blob_gas_used.append_option(blob_gas_used);
        self.blocks.excess_blob_gas.append_option(excess_blob_gas);
        self.blocks.parent_beacon_block_root.append_null();
        self.blocks.withdrawals_root.append_null();
        self.blocks.withdrawals.0.append_null();
        self.blocks.l1_block_number.append_option(l1_block_number);
        self.blocks.send_count.append_null();
        self.blocks.send_root.append_null();
        self.blocks.mix_hash.append_option(mix_hash);
        
        Ok(())
    }

    pub(crate) fn finish(self) -> ArrowResponse {
        ArrowResponse {
            blocks: self.blocks.finish(),
            transactions: self.transactions.finish(),
            logs: self.logs.finish(),
            traces: self.traces.finish(),
        }
    }
}

fn get_tape_i256(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i256>> {
    let hex = get_tape_hex(obj, name).context("get_tape_hex")?;
    
    hex.map(|v| i256_from_be_slice(&v).with_context(|| format!("parse i256 from {}", name))).transpose()
}

fn i256_from_be_slice(data: &[u8]) -> Result<i256> {
    if data.len() > 32 {
        return Err(anyhow!("data is bigger than 32 bytes"));
    }

    let mut bytes = [0; 32];
    bytes[0..data.len()].copy_from_slice(data);

    Ok(i256::from_be_bytes(bytes))
}

fn get_tape_u64(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u64>> {
    let val = match obj.get(name) {
        Some(val) => val,
        None => return Ok(None),
    };
    val.as_u64()
        .with_context(|| format!("get {} as u64", name))
        .map(Some)
}

fn get_tape_hex<'a>(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<Vec<u8>>> {
    let hex = match obj.get(name) {
        Some(hex) => hex,
        None => return Ok(None),
    };
    let hex = hex.as_str().with_context(|| format!("{} as str", name))?;

    decode_prefixed_hex(hex).with_context(|| format!("prefix_hex_decode {}", name)).map(Some)
}

fn decode_prefixed_hex(val: &str) -> Result<Vec<u8>> {
    let val = val.strip_prefix("0x").context("invalid hex prefix")?;

    if val.len() % 2 == 0 {
        decode_hex(val)
    } else {
        let val = format!("0{val}");
        decode_hex(val.as_str())
    }
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    let len = hex.as_bytes().len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst)?;

    Ok(dst)
}
