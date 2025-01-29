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
    pub transaction_index: bool,
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
        let block_info = self.parse_header(&header).context("parse block header")?;

        self.parse_transactions(&block_info, &obj)
            .context("parse transactions")?;

        self.parse_logs(&block_info, &obj)
            .context("parse logs")?;

        self.parse_traces(&block_info, &obj)
            .context("parse traces")?;

        Ok(())
    }

    fn parse_traces(&mut self, block_info: &BlockInfo, obj: &simd_json::tape::Object<'_, '_>) -> Result<()> {
        let traces = match obj.get("traces") {
            Some(traces) => traces,
            None => return Ok(()),
        };

        let traces = traces.as_array().context("traces as array")?;

        for trace in traces.iter() {
            let trace = trace.as_object().context("trace as object")?;

            let transaction_index = get_tape_u64(&trace, "transactionIndex")?;
            // let trace_address = get_
        }

        Ok(())
    }

    fn parse_logs(&mut self, block_info: &BlockInfo, obj: &simd_json::tape::Object<'_, '_>) -> Result<()> {
        let logs = match obj.get("logs") {
            Some(logs) => logs,
            None => return Ok(()),
        };

        let logs = logs.as_array().context("logs as array")?;

        for log in logs.iter() {
            let log = log.as_object().context("log as object")?;

            let log_index = get_tape_u64(&log, "logIndex")?;
            let transaction_index = get_tape_u64(&log, "transactionIndex")?;
            let transaction_hash = get_tape_hex(&log, "transactionHash")?;
            let address = get_tape_hex(&log, "address")?;
            let data = get_tape_hex(&log, "data")?;
            let topics = get_tape_array_of_hex(&log, "topics")?;

            self.logs.removed.append_null();
            self.logs.log_index.append_option(log_index);
            self.logs.transaction_index.append_option(transaction_index);
            self.logs.transaction_hash.append_option(transaction_hash);
            self.logs.block_hash.append_option(block_info.hash.clone());
            self.logs.block_number.append_option(block_info.number);
            self.logs.address.append_option(address);
            self.logs.data.append_option(data);
            if let Some(topics) = topics {
                self.logs.topic0.append_option(topics.get(0));
                self.logs.topic0.append_option(topics.get(1));
                self.logs.topic0.append_option(topics.get(2));
                self.logs.topic0.append_option(topics.get(3));
            } else {
                self.logs.topic0.append_null();
                self.logs.topic1.append_null();
                self.logs.topic2.append_null();
                self.logs.topic3.append_null();
            }
        }

        Ok(())
    }

    fn parse_transactions(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
        let transactions = match obj.get("transactions") {
            Some(txs) => txs,
            None => return Ok(()),
        };

        let transactions = transactions.as_array().context("transactions as array")?;

        for tx in transactions.iter() {
            let tx = tx.as_object().context("transaction as object")?;

            let transaction_index = get_tape_u64(&tx, "transactionIndex")?;
            let hash = get_tape_hex(&tx, "hash")?;
            let nonce = get_tape_i256(&tx, "nonce")?;
            let from = get_tape_hex(&tx, "from")?;
            let to = get_tape_hex(&tx, "to")?;
            let input = get_tape_hex(&tx, "input")?;
            let value = get_tape_i256(&tx, "value")?;
            let gas = get_tape_i256(&tx, "gas")?;
            let gas_price = get_tape_i256(&tx, "gasPrice")?;
            let max_fee_per_gas = get_tape_i256(&tx, "maxFeePerGas")?;
            let max_priority_fee_per_gas = get_tape_i256(&tx, "maxPriorityFeePerGas")?;
            let v = get_tape_i256(&tx, "v")?;
            let r = get_tape_i256(&tx, "r")?;
            let s = get_tape_i256(&tx, "s")?;
            let y_parity = get_tape_u8(&tx, "yParity")?;
            let chain_id = get_tape_i256(&tx, "chainId")?;
            let sighash = get_tape_hex(&tx, "sighash")?;
            let contract_address = get_tape_hex(&tx, "contractAddress")?;
            let gas_used = get_tape_i256(&tx, "gasUsed")?;
            let cumulative_gas_used = get_tape_i256(&tx, "cumulativeGasUsed")?;
            let effective_gas_price = get_tape_i256(&tx, "effectiveGasPrice")?;
            let type_ = get_tape_u8(&tx, "type")?;
            let status = get_tape_u8(&tx, "status")?;
            let max_fee_per_blob_gas = get_tape_i256(&tx, "maxFeePerBlobGas")?;
            let blob_versioned_hashes = get_tape_array_of_hex(&tx, "blobVersionedHashes")?;
            let l1_fee = get_tape_i256(&tx, "l1Fee")?;
            let l1_fee_scalar = get_tape_i256(&tx, "l1FeeScalar")?;
            let l1_gas_price = get_tape_i256(&tx, "l1GasPrice")?;
            let l1_gas_used = get_tape_i256(&tx, "l1GasUsed")?;
            let l1_blob_base_fee = get_tape_i256(&tx, "l1BlobBaseFee")?;
            let l1_blob_base_fee_scalar = get_tape_i256(&tx, "l1BlobBaseFeeScalar")?;
            let l1_base_fee_scalar = get_tape_i256(&tx, "l1BaseFeeScalar")?;

            self.transactions
                .block_hash
                .append_option(block_info.hash.clone());
            self.transactions
                .block_number
                .append_option(block_info.number.clone());
            self.transactions.from.append_option(from);
            self.transactions.gas.append_option(gas);
            self.transactions.gas_price.append_option(gas_price);
            self.transactions.hash.append_option(hash);
            self.transactions.input.append_option(input);
            self.transactions.nonce.append_option(nonce);
            self.transactions.to.append_option(to);
            self.transactions
                .transaction_index
                .append_option(transaction_index);
            self.transactions.value.append_option(value);
            self.transactions.v.append_option(v);
            self.transactions.r.append_option(r);
            self.transactions.s.append_option(s);
            self.transactions
                .max_priority_fee_per_gas
                .append_option(max_priority_fee_per_gas);
            self.transactions
                .max_fee_per_gas
                .append_option(max_fee_per_gas);
            self.transactions.chain_id.append_option(chain_id);
            self.transactions
                .cumulative_gas_used
                .append_option(cumulative_gas_used);
            self.transactions
                .effective_gas_price
                .append_option(effective_gas_price);
            self.transactions.gas_used.append_option(gas_used);
            self.transactions
                .contract_address
                .append_option(contract_address);
            self.transactions.logs_bloom.append_null();
            self.transactions.type_.append_option(type_);
            self.transactions.root.append_null();
            self.transactions.status.append_option(status);
            self.transactions.sighash.append_option(sighash);
            self.transactions.y_parity.append_option(y_parity.map(|x| {
                if x == 1 {
                    true
                } else if x == 0 {
                    false
                } else {
                    unreachable!()
                }
            }));
            self.transactions.access_list.0.append_null();
            self.transactions.l1_fee.append_option(l1_fee);
            self.transactions.l1_gas_price.append_option(l1_gas_price);
            self.transactions.l1_gas_used.append_option(l1_gas_used);
            self.transactions.l1_fee_scalar.append_option(l1_fee_scalar);
            self.transactions.gas_used_for_l1.append_null();
            self.transactions
                .max_fee_per_blob_gas
                .append_option(max_fee_per_blob_gas);
            self.transactions
                .blob_versioned_hashes
                .append_option(blob_versioned_hashes.map(|v| v.into_iter().map(Some)));
            self.transactions.deposit_nonce.append_null();
            self.transactions.blob_gas_price.append_null();
            self.transactions.deposit_receipt_version.append_null();
            self.transactions.blob_gas_used.append_null();
            self.transactions
                .l1_base_fee_scalar
                .append_option(l1_base_fee_scalar);
            self.transactions
                .l1_blob_base_fee
                .append_option(l1_blob_base_fee);
            self.transactions
                .l1_blob_base_fee_scalar
                .append_option(l1_blob_base_fee_scalar);
            self.transactions.l1_block_number.append_null();
            self.transactions.mint.append_null();
            self.transactions.source_hash.append_null();
        }

        Ok(())
    }

    fn parse_header(&mut self, header: &simd_json::tape::Object<'_, '_>) -> Result<BlockInfo> {
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
        self.blocks.hash.append_option(hash.clone());
        self.blocks.parent_hash.append_option(parent_hash);
        self.blocks.nonce.append_option(nonce);
        self.blocks.sha3_uncles.append_option(sha3_uncles);
        self.blocks.logs_bloom.append_option(logs_bloom);
        self.blocks
            .transactions_root
            .append_option(transactions_root);
        self.blocks.state_root.append_option(state_root);
        self.blocks.receipts_root.append_option(receipts_root);
        self.blocks.miner.append_option(miner);
        self.blocks.difficulty.append_option(difficulty);
        self.blocks.total_difficulty.append_option(total_difficulty);
        self.blocks.extra_data.append_option(extra_data);
        self.blocks
            .size
            .append_option(size.map(|s| i256::from_i128(i128::from(s))));
        self.blocks.gas_limit.append_option(gas_limit);
        self.blocks.gas_used.append_option(gas_used);
        self.blocks
            .timestamp
            .append_option(timestamp.map(|t| i256::from_i128(i128::from(t))));
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

        Ok(BlockInfo { number, hash })
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

struct BlockInfo {
    number: Option<u64>,
    hash: Option<Vec<u8>>,
}

fn get_tape_array_of_hex(
    obj: &simd_json::tape::Object<'_, '_>,
    name: &str,
) -> Result<Option<Vec<Vec<u8>>>> {
    let arr = match obj.get(name) {
        Some(v) => v,
        None => return Ok(None),
    };
    let arr = arr
        .as_array()
        .with_context(|| format!("{} as array", name))?;

    let mut out = Vec::with_capacity(arr.len());

    for v in arr.iter() {
        let v = v
            .as_str()
            .with_context(|| format!("element of {} as str", name))?;
        let v =
            decode_prefixed_hex(v).with_context(|| format!("decode element of {} as hex", name))?;
        out.push(v);
    }

    Ok(Some(out))
}

fn get_tape_u8(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u8>> {
    let val = match obj.get(name) {
        Some(v) => v,
        None => return Ok(None),
    };
    val.as_u8()
        .with_context(|| format!("{} as u8", name))
        .map(Some)
}

fn get_tape_i256(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i256>> {
    let hex = get_tape_hex(obj, name).context("get_tape_hex")?;

    hex.map(|v| i256_from_be_slice(&v).with_context(|| format!("parse i256 from {}", name)))
        .transpose()
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

    decode_prefixed_hex(hex)
        .with_context(|| format!("prefix_hex_decode {}", name))
        .map(Some)
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
