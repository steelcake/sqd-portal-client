use anyhow::{Context, Result};
use arrow::array::UInt64Array;
use arrow::{datatypes::i256, record_batch::RecordBatch};
use cherry_evm_schema::{BlocksBuilder, LogsBuilder, TracesBuilder, TransactionsBuilder};
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
    salam: bool,
}

impl InstructionFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFields {
    salam: bool,
}

impl TransactionFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFields {
    salam: bool,
}

impl LogFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceFields {
    salam: bool,
}

impl BalanceFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalanceFields {
    salam: bool,
}

impl TokenBalanceFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardFields {
    salam: bool,
}

impl RewardFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFields {
    salam: bool,
}

impl BlockFields {
    pub fn all() -> Self {
        Self {
            salam: true,
        }
    }
}

#[derive(Default)]
pub(crate) struct ArrowResponseParser {
    pub instruction: InstructionFields,
    pub transaction: TransactionFields,
    pub log: LogFields,
    pub balance: BalanceFields,
    pub token_balance: TokenBalanceFields,
    pub reward: RewardFields,
    pub block: BlockFields,
}
