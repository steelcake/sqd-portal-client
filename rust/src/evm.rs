use anyhow::{Context, Result};
use arrow::array::UInt64Array;
use arrow::{datatypes::i256, record_batch::RecordBatch};
use cherry_evm_schema::{BlocksBuilder, LogsBuilder, TracesBuilder, TransactionsBuilder};
use serde::{Deserialize, Serialize};
use simd_json::base::ValueAsScalar;
use simd_json::derived::TypedScalarValue;

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
    // pub state_diff: StateDiffFields,
    pub trace: TraceFields,
    pub block: BlockFields,
}

impl Fields {
    pub fn all() -> Self {
        Self {
            log: LogFields::all(),
            transaction: TransactionFields::all(),
            // state_diff: StateDiffFields::all(),
            trace: TraceFields::all(),
            block: BlockFields::all(),
        }
    }
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

impl LogFields {
    pub fn all() -> Self {
        Self {
            log_index: true,
            transaction_index: true,
            transaction_hash: true,
            address: true,
            data: true,
            topics: true,
        }
    }
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

impl TransactionFields {
    pub fn all() -> Self {
        Self {
            transaction_index: true,
            hash: true,
            nonce: true,
            from: true,
            to: true,
            input: true,
            value: true,
            gas: true,
            gas_price: true,
            max_fee_per_gas: true,
            max_priority_fee_per_gas: true,
            v: true,
            r: true,
            s: true,
            y_parity: true,
            chain_id: true,
            sighash: true,
            contract_address: true,
            gas_used: true,
            cumulative_gas_used: true,
            effective_gas_price: true,
            type_: true,
            status: true,
            max_fee_per_blob_gas: true,
            blob_versioned_hashes: true,
            l1_fee: true,
            l1_fee_scalar: true,
            l1_gas_price: true,
            l1_gas_used: true,
            l1_blob_base_fee: true,
            l1_blob_base_fee_scalar: true,
            l1_base_fee_scalar: true,
        }
    }
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

impl TraceFields {
    pub fn all() -> Self {
        Self {
            transaction_index: true,
            trace_address: true,
            subtraces: true,
            type_: true,
            error: true,
            revert_reason: true,
            create_from: true,
            create_value: true,
            create_gas: true,
            create_init: true,
            create_result_gas_used: true,
            create_result_code: true,
            create_result_address: true,
            call_from: true,
            call_to: true,
            call_value: true,
            call_gas: true,
            call_input: true,
            call_sighash: true,
            call_type: true,
            call_call_type: true,
            call_result_gas_used: true,
            call_result_output: true,
            suicide_address: true,
            suicide_refund_address: true,
            suicide_balance: true,
            reward_author: true,
            reward_value: true,
            reward_type: true,
        }
    }
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

impl BlockFields {
    pub fn all() -> Self {
        Self {
            number: true,
            hash: true,
            parent_hash: true,
            timestamp: true,
            transactions_root: true,
            receipts_root: true,
            state_root: true,
            logs_bloom: true,
            sha3_uncles: true,
            extra_data: true,
            miner: true,
            nonce: true,
            mix_hash: true,
            size: true,
            gas_limit: true,
            gas_used: true,
            difficulty: true,
            total_difficulty: true,
            base_fee_per_gas: true,
            blob_gas_used: true,
            excess_blob_gas: true,
            l1_block_number: true,
        }
    }
}

#[derive(Debug)]
pub struct ArrowResponse {
    pub blocks: RecordBatch,
    pub transactions: RecordBatch,
    pub logs: RecordBatch,
    pub traces: RecordBatch,
    // state_diffs: recordbatch,
}

impl ArrowResponse {
    pub fn next_block(&self) -> Result<u64> {
        let numbers = self
            .blocks
            .column_by_name("number")
            .context("get number col")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("get number col as u64")?;
        numbers
            .values()
            .last()
            .context("get last value from block numbers")
            .map(|v| *v + 1)
    }
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

        self.parse_logs(&block_info, &obj).context("parse logs")?;

        self.parse_traces(&block_info, &obj)
            .context("parse traces")?;

        Ok(())
    }

    fn parse_traces(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
        let traces = match obj.get("traces") {
            Some(traces) => traces,
            None => return Ok(()),
        };

        let traces = traces.as_array().context("traces as array")?;

        for trace in traces.iter() {
            let trace = trace.as_object().context("trace as object")?;

            let transaction_index = get_tape_u64(&trace, "transactionIndex")?;
            let trace_address = get_tape_array_of_u64(&trace, "traceAddress")?;
            let type_ = get_tape_string(&trace, "type")?;
            let subtraces = get_tape_u64(&trace, "subtraces")?;
            let error = get_tape_string(&trace, "error")?;
            let revert_reason = get_tape_string(&trace, "revertReason")?;

            self.traces
                .block_hash
                .append_option(block_info.hash.clone());
            self.traces.block_number.append_option(block_info.number);
            self.traces.subtraces.append_option(subtraces);
            self.traces
                .trace_address
                .append_option(trace_address.map(|v| v.into_iter().map(Some)));
            self.traces.transaction_hash.append_null();
            self.traces
                .transaction_position
                .append_option(transaction_index);
            self.traces.type_.append_option(type_);
            self.traces.error.append_option(error.or(revert_reason));

            if let Some(action) = trace.get("action") {
                let action = action.as_object().context("tx.action as object")?;

                let from = get_tape_hex(&action, "from")?;
                let to = get_tape_hex(&action, "to")?;
                let value = get_tape_u256(&action, "value")?;
                let gas = get_tape_u256(&action, "gas")?;
                let input = get_tape_hex(&action, "input")?;
                let sighash = get_tape_hex(&action, "sighash")?;
                let call_type = get_tape_string(&action, "type")?;
                let init = get_tape_hex(&trace, "init")?;
                let refund_address = get_tape_hex(&trace, "refundAddress")?;
                let balance = get_tape_u256(&trace, "balance")?;
                let reward_author = get_tape_hex(&trace, "rewardAuthor")?;
                let reward_type = get_tape_string(&trace, "type")?;

                self.traces.from.append_option(from);
                self.traces.to.append_option(to);
                self.traces.call_type.append_option(call_type);
                self.traces.gas.append_option(gas);
                self.traces.input.append_option(input);
                self.traces.init.append_option(init);
                self.traces.value.append_option(value);
                self.traces.author.append_option(reward_author);
                self.traces.reward_type.append_option(reward_type);
                self.traces.sighash.append_option(sighash);
                self.traces.action_address.append_null();
                self.traces.balance.append_option(balance);
                self.traces.refund_address.append_option(refund_address);
            } else {
                self.traces.from.append_null();
                self.traces.to.append_null();
                self.traces.call_type.append_null();
                self.traces.gas.append_null();
                self.traces.input.append_null();
                self.traces.init.append_null();
                self.traces.value.append_null();
                self.traces.author.append_null();
                self.traces.reward_type.append_null();
                self.traces.sighash.append_null();
                self.traces.action_address.append_null();
                self.traces.balance.append_null();
                self.traces.refund_address.append_null();
            }

            if let Some(result) = trace.get("result") {
                let result = result.as_object().context("trace.result as object")?;

                let gas_used = get_tape_u256(&result, "gasUsed")?;
                let code = get_tape_hex(&result, "code")?;
                let address = get_tape_hex(&result, "address")?;
                let output = get_tape_hex(&result, "output")?;

                self.traces.address.append_option(address);
                self.traces.code.append_option(code);
                self.traces.gas_used.append_option(gas_used);
                self.traces.output.append_option(output);
            } else {
                self.traces.address.append_null();
                self.traces.code.append_null();
                self.traces.gas_used.append_null();
                self.traces.output.append_null();
            }
        }

        Ok(())
    }

    fn parse_logs(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
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
                self.logs.topic0.append_option(topics.first());
                self.logs.topic1.append_option(topics.get(1));
                self.logs.topic2.append_option(topics.get(2));
                self.logs.topic3.append_option(topics.get(3));
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
            let nonce = get_tape_u64(&tx, "nonce")?;
            let from = get_tape_hex(&tx, "from")?;
            let to = get_tape_hex(&tx, "to")?;
            let input = get_tape_hex(&tx, "input")?;
            let value = get_tape_u256(&tx, "value")?;
            let gas = get_tape_u256(&tx, "gas")?;
            let gas_price = get_tape_u256(&tx, "gasPrice")?;
            let max_fee_per_gas = get_tape_u256(&tx, "maxFeePerGas")?;
            let max_priority_fee_per_gas = get_tape_u256(&tx, "maxPriorityFeePerGas")?;
            let v = get_tape_u8_hex(&tx, "v")?;
            let r = get_tape_hex(&tx, "r")?;
            let s = get_tape_hex(&tx, "s")?;
            let y_parity = get_tape_u8(&tx, "yParity")?;
            let chain_id = get_tape_u64(&tx, "chainId")?;
            let sighash = get_tape_hex(&tx, "sighash")?;
            let contract_address = get_tape_hex(&tx, "contractAddress")?;
            let gas_used = get_tape_u256(&tx, "gasUsed")?;
            let cumulative_gas_used = get_tape_u256(&tx, "cumulativeGasUsed")?;
            let effective_gas_price = get_tape_u256(&tx, "effectiveGasPrice")?;
            let type_ = get_tape_u8(&tx, "type")?;
            let status = get_tape_u8(&tx, "status")?;
            let max_fee_per_blob_gas = get_tape_u256(&tx, "maxFeePerBlobGas")?;
            let blob_versioned_hashes = get_tape_array_of_hex(&tx, "blobVersionedHashes")?;
            let l1_fee = get_tape_u256(&tx, "l1Fee")?;
            let l1_fee_scalar = get_tape_u256(&tx, "l1FeeScalar")?;
            let l1_gas_price = get_tape_u256(&tx, "l1GasPrice")?;
            let l1_gas_used = get_tape_u256(&tx, "l1GasUsed")?;
            let l1_blob_base_fee = get_tape_u256(&tx, "l1BlobBaseFee")?;
            let l1_blob_base_fee_scalar = get_tape_u256(&tx, "l1BlobBaseFeeScalar")?;
            let l1_base_fee_scalar = get_tape_u256(&tx, "l1BaseFeeScalar")?;

            self.transactions
                .block_hash
                .append_option(block_info.hash.clone());
            self.transactions
                .block_number
                .append_option(block_info.number);
            self.transactions.from.append_option(from);
            self.transactions.gas.append_option(gas);
            self.transactions.gas_price.append_option(gas_price);
            self.transactions.hash.append_option(hash);
            self.transactions.input.append_option(input);
            self.transactions
                .nonce
                .append_option(nonce.map(|n| i256::from_i128(i128::from(n))));
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
            self.transactions
                .chain_id
                .append_option(chain_id.map(|c| i256::from_i128(i128::from(c))));
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
        let gas_limit = get_tape_u256(header, "gasLimit")?;
        let gas_used = get_tape_u256(header, "gasUsed")?;
        let difficulty = get_tape_u256(header, "difficulty")?;
        let total_difficulty = get_tape_u256(header, "totalDifficulty")?;
        let base_fee_per_gas = get_tape_u256(header, "baseFeePerGas")?;
        let blob_gas_used = get_tape_u256(header, "blobGasUsed")?;
        let excess_blob_gas = get_tape_u256(header, "excessBlobGas")?;
        let l1_block_number = get_tape_u64(header, "l1BlockNumber")?;

        self.blocks.number.append_option(number);
        self.blocks.hash.append_option(hash.as_ref());
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

fn get_tape_array_of_u64(
    obj: &simd_json::tape::Object<'_, '_>,
    name: &str,
) -> Result<Option<Vec<u64>>> {
    let arr = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    let arr = arr
        .as_array()
        .with_context(|| format!("{} as array", name))?;

    let mut out = Vec::with_capacity(arr.len());

    for v in arr.iter() {
        let v = v
            .as_u64()
            .with_context(|| format!("element of {} as u64", name))?;
        out.push(v);
    }

    Ok(Some(out))
}

fn get_tape_array_of_hex(
    obj: &simd_json::tape::Object<'_, '_>,
    name: &str,
) -> Result<Option<Vec<Vec<u8>>>> {
    let arr = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
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

fn get_tape_u8_hex(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u8>> {
    let hex = get_tape_hex(obj, name).context("get_tape_hex")?;

    hex.map(|v| u8_from_be_slice(&v).with_context(|| format!("parse u8 from {}", name)))
        .transpose()
}

fn u8_from_be_slice(data: &[u8]) -> Result<u8> {
    let num = ruint::aliases::U256::try_from_be_slice(data).context("parse ruint u256")?;
    let num = alloy_primitives::I256::try_from(num)
        .with_context(|| format!("u256 to i256. val was {}", num))?;
    let num = u8::try_from(num).context("try to u8")?;
    Ok(num)
}

fn get_tape_u8(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u8>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u8()
        .with_context(|| format!("{} as u8", name))
        .map(Some)
}

fn get_tape_string(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<String>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_str()
        .with_context(|| format!("{} as str", name))
        .map(|x| Some(x.to_owned()))
}

fn get_tape_u256(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i256>> {
    let hex = get_tape_hex(obj, name).context("get_tape_hex")?;

    hex.map(|v| u256_from_be_slice(&v).with_context(|| format!("parse i256 from {}", name)))
        .transpose()
}

fn u256_from_be_slice(data: &[u8]) -> Result<i256> {
    let num = ruint::aliases::U256::try_from_be_slice(data).context("parse ruint u256")?;
    let num = alloy_primitives::I256::try_from(num)
        .with_context(|| format!("u256 to i256. val was {}", num))?;

    let val = i256::from_be_bytes(num.to_be_bytes::<32>());

    Ok(val)
}

fn get_tape_u64(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u64>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u64()
        .with_context(|| format!("get {} as u64", name))
        .map(Some)
}

fn get_tape_hex(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<Vec<u8>>> {
    let hex = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
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
    let len = hex.len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst)?;

    Ok(dst)
}
