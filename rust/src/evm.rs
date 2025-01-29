use anyhow::{anyhow, Context, Result};
use arrow::{array::builder, record_batch::RecordBatch};
use cherry_evm_schema::{BlocksBuilder, LogsBuilder, TracesBuilder, TransactionsBuilder};
use serde::{Deserialize, Serialize};

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

pub struct ArrowResponse {
    pub blocks: RecordBatch,
    pub transactions: RecordBatch,
    pub logs: RecordBatch,
    pub traces: RecordBatch,
    // state_diffs: recordbatch,
}

pub(crate) struct ArrowResponseParser {
    blocks: BlocksBuilder,
    transactions: TransactionsBuilder,
    logs: LogsBuilder,
    traces: TracesBuilder,
}

impl ArrowResponseParser {
    fn parse_tape(&mut self, tape: &simd_json::tape::Tape<'_>) -> Result<()> {
        todo!()
    }
}

impl crate::ResponseParser for ArrowResponseParser {
    type Output = ArrowResponse;

    fn parse(&self, bytes: &mut [u8]) -> Result<ArrowResponse> {
        let mut blocks_builder = BlocksBuilder::default(); 
        let mut transactions_builder = TransactionsBuilder::default();
        let mut logs_builder = LogsBuilder::default();
        let mut traces_builder = TracesBuilder::default();
    
        let tape = simd_json::to_tape(bytes).context("json to tape")?; 

        panic!("{:?}", tape);

        todo!()
    }
}
