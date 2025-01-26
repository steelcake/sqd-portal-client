use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    from_block: u64,
    to_block: Option<u64>,
    include_all_blocks: bool,
    fields: Fields,
    logs: Vec<LogRequest>,
    transactions: Vec<TransactionRequest>,
    traces: Vec<TraceRequest>,
    state_diffs: Vec<StateDiffRequest>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    address: Vec<String>,
    topic0: Vec<String>,
    topic1: Vec<String>,
    topic2: Vec<String>,
    topic3: Vec<String>,
    transaction: bool,
    transaction_traces: bool,
    transaction_logs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    from: Vec<String>,
    to: Vec<String>,
    sighash: Vec<String>,
    logs: bool,
    traces: bool,
    state_diffs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRequest {
    #[serde(rename = "type")]
    type_: Vec<String>,
    create_from: Vec<String>,
    call_from: Vec<String>,
    call_to: Vec<String>,
    call_sighash: Vec<String>,
    suicide_refund_address: Vec<String>,
    reward_author: Vec<String>,
    transaction: bool,
    transaction_logs: bool,
    subtraces: bool,
    parents: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDiffRequest {
    address: Vec<String>,
    key: Vec<String>,
    kind: Vec<String>,
    transaction: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fields {
    block: Vec<String>,
    transaction: Vec<String>,
    log: Vec<String>,
    trace: Vec<String>,
    state_diff: Vec<String>,
}

pub struct Response {}

impl crate::Response for Response {}
