use std::collections::BTreeMap;

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
    pub address: Vec<String>,
    pub topic0: Vec<String>,
    pub topic1: Vec<String>,
    pub topic2: Vec<String>,
    pub topic3: Vec<String>,
    pub transaction: bool,
    pub transaction_traces: bool,
    pub transaction_logs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub sighash: Vec<String>,
    pub logs: bool,
    pub traces: bool,
    pub state_diffs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRequest {
    #[serde(rename = "type")]
    pub type_: Vec<String>,
    pub create_from: Vec<String>,
    pub call_from: Vec<String>,
    pub call_to: Vec<String>,
    pub call_sighash: Vec<String>,
    pub suicide_refund_address: Vec<String>,
    pub reward_author: Vec<String>,
    pub transaction: bool,
    pub transaction_logs: bool,
    pub subtraces: bool,
    pub parents: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiffRequest {
    pub address: Vec<String>,
    pub key: Vec<String>,
    pub kind: Vec<String>,
    pub transaction: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fields {
    pub block: BTreeMap<String, bool>,
    pub transaction: BTreeMap<String, bool>,
    pub log: BTreeMap<String, bool>,
    pub trace: BTreeMap<String, bool>,
    pub state_diff: BTreeMap<String, bool>,
}

pub struct Response {}

impl crate::Response for Response {}
