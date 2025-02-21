use anyhow::{Context, Result};
use arrow::array::UInt64Array;
use arrow::{datatypes::i256, record_batch::RecordBatch};
use cherry_svm_schema::*;
use serde::{Deserialize, Serialize};

use crate::util::*;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    #[serde(rename = "type")]
    pub type_: QueryType,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    pub fields: Fields,
    pub instructions: Vec<InstructionRequest>,
    pub transactions: Vec<TransactionRequest>,
    pub logs: Vec<LogRequest>,
    pub balances: Vec<BalanceRequest>,
    pub token_balances: Vec<TokenBalanceRequest>,
    pub rewards: Vec<RewardRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum QueryType {
    Solana,
}

impl Default for QueryType {
    fn default() -> Self {
        Self::Solana
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub program_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub d1: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub d2: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub d3: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub d4: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub d8: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a0: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a1: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a2: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a3: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a4: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a5: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a6: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a7: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a8: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub a9: Vec<String>,
    pub is_committed: bool,
    pub transaction: bool,
    pub transaction_token_balances: bool,
    pub logs: bool,
    pub inner_instructions: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fee_payer: Vec<String>,
    pub instructions: bool,
    pub logs: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub program_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub kind: Vec<String>,
    pub transaction: bool,
    pub instruction: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub account: Vec<String>,
    pub transaction: bool,
    pub transaction_instructions: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalanceRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub account: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pre_program_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub post_program_id: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pre_mint: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub post_mint: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pre_owner: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub post_owner: Vec<String>,
    pub transaction: bool,
    pub transaction_instructions: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pubkey: Vec<String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fields {
    pub instruction: InstructionFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub balance: BalanceFields,
    pub token_balance: TokenBalanceFields,
    pub reward: RewardFields,
    pub block: BlockFields,
}

impl Fields {
    pub fn all() -> Self {
        Self {
            instruction: InstructionFields::all(),
            transaction: TransactionFields::all(),
            log: LogFields::all(),
            balance: BalanceFields::all(),
            token_balance: TokenBalanceFields::all(),
            reward: RewardFields::all(),
            block: BlockFields::all(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionFields {
    pub transaction_index: bool,
    pub instruction_address: bool,
    pub program_id: bool,
    pub accounts: bool,
    pub data: bool,
    pub d1: bool,
    pub d2: bool,
    pub d4: bool,
    pub d8: bool,
    pub error: bool,
    pub compute_units_consumed: bool,
    pub is_committed: bool,
    pub has_dropped_log_messages: bool,
}

impl InstructionFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFields {
    pub transaction_index: bool,
    pub version: bool,
    pub account_keys: bool,
    pub address_table_lookups: bool,
    pub num_readonly_signed_accounts: bool,
    pub num_readonly_unsigned_accounts: bool,
    pub num_required_signatures: bool,
    pub recent_blockhash: bool,
    pub signatures: bool,
    pub err: bool,
    pub fee: bool,
    pub compute_units_consumed: bool,
    pub loaded_addresses: bool,
    pub fee_payer: bool,
    pub has_dropped_log_messages: bool,
}

impl TransactionFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFields {
    pub transaction_index: bool,
    pub log_index: bool,
    pub instruction_address: bool,
    pub program_id: bool,
    pub kind: bool,
    pub message: bool,
}

impl LogFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceFields {
    pub transaction_index: bool,
    pub account: bool,
    pub pre: bool,
    pub post: bool,
}

impl BalanceFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalanceFields {
    pub transaction_index: bool,
    pub account: bool,
    pub pre_mint: bool,
    pub post_mint: bool,
    pub pre_decimals: bool,
    pub post_decimals: bool,
    pub pre_program_id: bool,
    pub post_program_id: bool,
    pub pre_owner: bool,
    pub post_owner: bool,
    pub pre_amount: bool,
    pub post_amount: bool,
}

impl TokenBalanceFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardFields {
    pub pubkey: bool,
    pub lamports: bool,
    pub post_balance: bool,
    pub reward_type: bool,
    pub commission: bool,
}

impl RewardFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFields {
    pub number: bool,
    pub hash: bool,
    pub parent_number: bool,
    pub parent_hash: bool,
    pub height: bool,
    pub timestamp: bool,
}

impl BlockFields {
    pub fn all() -> Self {
        todo!()
    }
}

#[derive(Debug)]
pub struct ArrowResponse {
    pub instructions: RecordBatch,
    pub transactions: RecordBatch,
    pub logs: RecordBatch,
    pub balances: RecordBatch,
    pub token_balances: RecordBatch,
    pub rewards: RecordBatch,
    pub blocks: RecordBatch,
}

impl ArrowResponse {
    pub fn next_block(&self) -> Result<u64> {
        todo!()
    }
}

#[derive(Default)]
pub(crate) struct ArrowResponseParser {
    instructions: InstructionsBuilder,
    transactions: TransactionsBuilder,
    logs: LogsBuilder,
    balances: BalancesBuilder,
    token_balances: TokenBalancesBuilder,
    rewards: RewardsBuilder,
    blocks: BlocksBuilder,
}

impl ArrowResponseParser {
    pub(crate) fn parse_tape(&mut self, tape: &simd_json::tape::Tape<'_>) -> Result<()> {
        let obj = tape.as_value().as_object().context("tape as object")?;
        let header = obj.get("header").context("get header")?;

        let header = header.as_object().context("header as object")?;
        let block_info = self.parse_header(&header).context("parse block header")?;

        // self.parse_transactions(&block_info, &obj)
        //     .context("parse transactions")?;

        // self.parse_logs(&block_info, &obj).context("parse logs")?;

        // self.parse_traces(&block_info, &obj)
        //     .context("parse traces")?;

        Ok(())
    }

    fn parse_header(&mut self, header: &simd_json::tape::Object<'_, '_>) -> Result<BlockInfo> {
        let slot = get_tape_u64(header, "number")?;
        let hash = get_tape_hex(header, "hash")?;
        let parent_slot = get_tape_hex(header, "parentNumber")?;
        let parent_hash = get_tape_hex(header, "parentHash")?;

        todo!()
    }

    pub(crate) fn finish(self) -> ArrowResponse {
        ArrowResponse {
            instructions: self.instructions.finish(),
            transactions: self.transactions.finish(),
            logs: self.logs.finish(),
            balances: self.balances.finish(),
            token_balances: self.token_balances.finish(),
            rewards: self.rewards.finish(),
            blocks: self.blocks.finish(),
        }
    }
}

struct BlockInfo {
    slot: Option<u64>,
    hash: Option<Vec<u8>>,
}
