use anyhow::{anyhow, Context, Result};
use arrow::array::{builder, UInt64Array};
use arrow::{datatypes::i256, record_batch::RecordBatch};
use cherry_svm_schema::{
    AddressTableLookupsBuilder, BalancesBuilder, BlocksBuilder, InstructionsBuilder, LogsBuilder,
    RewardsBuilder, TokenBalancesBuilder, TransactionsBuilder,
};
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

        self.parse_rewards(&block_info, &obj)
            .context("parse rewards")?;

        self.parse_token_balances(&block_info, &obj)
            .context("parse token balances")?;

        self.parse_balances(&block_info, &obj)
            .context("parse balances")?;

        self.parse_logs(&block_info, &obj).context("parse logs")?;

        self.parse_transactions(&block_info, &obj)
            .context("parse transactions")?;

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
            let obj = tx.as_object().context("transaction as object")?;

            let transaction_index = get_tape_u32(&obj, "transactionIndex")?;
            let version = get_tape_version(&obj, "version")?;
            let account_keys = get_tape_array_of_base58(&obj, "accountKeys")?;
            // address table lookups will be read later
            let num_readonly_signed_accounts = get_tape_u32(&obj, "numReadonlySignedAccounts")?;
            let num_readonly_unsigned_accounts = get_tape_u32(&obj, "numReadonlyUnsignedAccounts")?;
            let num_required_signatures = get_tape_u32(&obj, "numRequiredSignatures")?;
            let recent_blockhash = get_tape_base58(&obj, "recentBlockhash")?;
            let signatures = get_tape_array_of_base58(&obj, "signatures")?;
            let err = get_tape_string(&obj, "err")?;
            let fee = get_tape_u64(&obj, "fee")?;
            let compute_units_consumed = get_tape_u64(&obj, "computeUnitsConsumed")?;
            // loaded addresses will be read later
            let fee_payer = get_tape_base58(&obj, "feePayer")?;
            let has_dropped_log_messages = get_tape_bool(&obj, "hasDroppedLogMessages")?;

            self.transactions.block_slot.append_option(block_info.slot);
            self.transactions
                .block_hash
                .append_option(block_info.hash.as_ref());
            self.transactions
                .transaction_index
                .append_option(transaction_index);
            self.transactions.version.append_option(version);
            self.transactions
                .account_keys
                .append_option(account_keys.map(|v| v.into_iter().map(Some)));

            self.parse_address_table_lookups(&obj)
                .context("parse address table lookups")?;

            self.transactions
                .num_readonly_signed_accounts
                .append_option(num_readonly_signed_accounts);
            self.transactions
                .num_readonly_unsigned_accounts
                .append_option(num_readonly_unsigned_accounts);
            self.transactions
                .num_required_signatures
                .append_option(num_required_signatures);
            self.transactions
                .recent_blockhash
                .append_option(recent_blockhash);
            self.transactions
                .signatures
                .append_option(signatures.map(|v| v.into_iter().map(Some)));
            self.transactions.err.append_option(err);
            self.transactions.fee.append_option(fee);
            self.transactions
                .compute_units_consumed
                .append_option(compute_units_consumed);

            self.parse_loaded_addresses(&obj)
                .context("parse loaded addresses")?;

            self.transactions.fee_payer.append_option(fee_payer);
            self.transactions
                .has_dropped_log_messages
                .append_option(has_dropped_log_messages);
        }

        Ok(())
    }

    fn parse_loaded_addresses(&mut self, obj: &simd_json::tape::Object<'_, '_>) -> Result<()> {
        let v = match obj.get("loadedAddresses") {
            Some(v) if v.is_null() => {
                self.transactions.loaded_readonly_addresses.append_null();
                self.transactions.loaded_writeable_addresses.append_null();
                return Ok(());
            }
            None => {
                self.transactions.loaded_readonly_addresses.append_null();
                self.transactions.loaded_writeable_addresses.append_null();
                return Ok(());
            }
            Some(v) => v,
        };

        let v = v.as_object().context("loaded addresses as object")?;

        let readonly = get_tape_array_of_base58(&v, "readonly")?.context("readonly is null")?;
        self.transactions
            .loaded_readonly_addresses
            .append_value(readonly.into_iter().map(Some));
        let writeable = get_tape_array_of_base58(&v, "writeable")?.context("writeable is null")?;
        self.transactions
            .loaded_writeable_addresses
            .append_value(writeable.into_iter().map(Some));

        Ok(())
    }

    fn parse_address_table_lookups(&mut self, obj: &simd_json::tape::Object<'_, '_>) -> Result<()> {
        if let Some(v) = obj.get("addressTableLookups") {
            let v = v.as_array().context("address table lookups as array")?;

            let atl_builder = self.transactions.address_table_lookups.0.values();

            for v in v.iter() {
                let v = v.as_object().context("address table lookup as object")?;

                let account_key = get_tape_base58(&v, "accountKey")?;
                atl_builder
                    .field_builder::<builder::BinaryBuilder>(0)
                    .unwrap()
                    .append_option(account_key);

                {
                    let b = atl_builder
                        .field_builder::<builder::ListBuilder<builder::UInt64Builder>>(1)
                        .unwrap();

                    let v = get_tape_array_of_u64(&v, "writeableIndexes")?;
                    b.append_option(v.map(|v| v.into_iter().map(Some)));
                }
                {
                    let b = atl_builder
                        .field_builder::<builder::ListBuilder<builder::UInt64Builder>>(0)
                        .unwrap();

                    let v = get_tape_array_of_u64(&v, "readonlyIndexes")?;
                    b.append_option(v.map(|v| v.into_iter().map(Some)));
                }

                atl_builder.append(true);
            }

            self.transactions.address_table_lookups.0.append(true);
        } else {
            self.transactions.address_table_lookups.0.append_null();
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
            let obj = log.as_object().context("log as object")?;

            let transaction_index = get_tape_u32(&obj, "transactionIndex")?;
            let log_index = get_tape_u32(&obj, "logIndex")?;
            let instruction_address = get_tape_base58(&obj, "instructionAddress")?;
            let program_id = get_tape_base58(&obj, "programId")?;
            let kind = get_tape_string(&obj, "kind")?;
            let message = get_tape_string(&obj, "message")?;

            self.logs.block_slot.append_option(block_info.slot);
            self.logs.block_hash.append_option(block_info.hash.as_ref());
            self.logs.transaction_index.append_option(transaction_index);
            self.logs.log_index.append_option(log_index);
            self.logs
                .instruction_address
                .append_option(instruction_address);
            self.logs.program_id.append_option(program_id);
            self.logs.kind.append_option(kind);
            self.logs.message.append_option(message);
        }

        Ok(())
    }

    fn parse_balances(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
        let balances = match obj.get("balances") {
            Some(r) => r,
            None => return Ok(()),
        };

        let balances = balances.as_array().context("balances as array")?;

        for obj in balances.iter() {
            let obj = obj.as_object().context("balance as object")?;

            let transaction_index = get_tape_u32(&obj, "transactionIndex")?;
            let account = get_tape_base58(&obj, "account")?;
            let pre = get_tape_u64(&obj, "pre")?;
            let post = get_tape_u64(&obj, "post")?;

            self.balances.block_slot.append_option(block_info.slot);
            self.balances
                .block_hash
                .append_option(block_info.hash.as_ref());
            self.balances
                .transaction_index
                .append_option(transaction_index);
            self.balances.account.append_option(account);
            self.balances.pre.append_option(pre);
            self.balances.post.append_option(post);
        }

        Ok(())
    }

    fn parse_rewards(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
        let rewards = match obj.get("rewards") {
            Some(r) => r,
            None => return Ok(()),
        };

        let rewards = rewards.as_array().context("rewards as array")?;

        for obj in rewards.iter() {
            let obj = obj.as_object().context("reward as object")?;

            let pubkey = get_tape_base58(&obj, "pubkey")?;
            let lamports = get_tape_u64(&obj, "lamports")?;
            let post_balance = get_tape_u64(&obj, "postBalance")?;
            let reward_type = get_tape_string(&obj, "rewardType")?;
            let commission = get_tape_u8(&obj, "commission")?;

            self.rewards.block_slot.append_option(block_info.slot);
            self.rewards
                .block_hash
                .append_option(block_info.hash.as_ref());
            self.rewards.pubkey.append_option(pubkey);
            self.rewards.lamports.append_option(lamports);
            self.rewards.post_balance.append_option(post_balance);
            self.rewards.reward_type.append_option(reward_type);
            self.rewards.commission.append_option(commission);
        }

        Ok(())
    }

    fn parse_token_balances(
        &mut self,
        block_info: &BlockInfo,
        obj: &simd_json::tape::Object<'_, '_>,
    ) -> Result<()> {
        let token_balances = match obj.get("tokenBalances") {
            Some(r) => r,
            None => return Ok(()),
        };

        let tb = token_balances
            .as_array()
            .context("token balances as array")?;

        for obj in tb.iter() {
            let obj = obj.as_object().context("token balance as object")?;

            let transaction_index = get_tape_u32(&obj, "transactionIndex")?;
            let account = get_tape_base58(&obj, "account")?;
            let pre_mint = get_tape_base58(&obj, "preMint")?;
            let post_mint = get_tape_base58(&obj, "postMint")?;
            let pre_decimals = get_tape_u16(&obj, "preDecimals")?;
            let post_decimals = get_tape_u16(&obj, "postDecimals")?;
            let pre_program_id = get_tape_base58(&obj, "preProgramId")?;
            let post_program_id = get_tape_base58(&obj, "postProgramId")?;
            let pre_owner = get_tape_base58(&obj, "preOwner")?;
            let post_owner = get_tape_base58(&obj, "postOwner")?;
            let pre_amount = get_tape_u64(&obj, "preAmount")?;
            let post_amount = get_tape_u64(&obj, "postAmount")?;

            self.token_balances
                .block_slot
                .append_option(block_info.slot);
            self.token_balances
                .block_hash
                .append_option(block_info.hash.as_ref());
            self.token_balances
                .transaction_index
                .append_option(transaction_index);
            self.token_balances.account.append_option(account);
            self.token_balances.pre_mint.append_option(pre_mint);
            self.token_balances.post_mint.append_option(post_mint);
            self.token_balances.pre_decimals.append_option(pre_decimals);
            self.token_balances
                .post_decimals
                .append_option(post_decimals);
            self.token_balances
                .pre_program_id
                .append_option(pre_program_id);
            self.token_balances
                .post_program_id
                .append_option(post_program_id);
            self.token_balances.pre_owner.append_option(pre_owner);
            self.token_balances.post_owner.append_option(post_owner);
            self.token_balances.pre_amount.append_option(pre_amount);
            self.token_balances.post_amount.append_option(post_amount);
        }

        Ok(())
    }

    fn parse_header(&mut self, header: &simd_json::tape::Object<'_, '_>) -> Result<BlockInfo> {
        let slot = get_tape_u64(header, "number")?;
        let hash = get_tape_base58(header, "hash")?;
        let parent_slot = get_tape_u64(header, "parentNumber")?;
        let parent_hash = get_tape_base58(header, "parentHash")?;
        let height = get_tape_u64(header, "height")?;
        let timestamp = get_tape_i64(header, "timestamp")?;

        self.blocks.slot.append_option(slot);
        self.blocks.hash.append_option(hash.as_ref());
        self.blocks.parent_slot.append_option(parent_slot);
        self.blocks.parent_hash.append_option(parent_hash);
        self.blocks.height.append_option(height);
        self.blocks.timestamp.append_option(timestamp);

        Ok(BlockInfo { slot, hash })
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

fn decode_base58(data: &str) -> Result<Vec<u8>> {
    bs58::decode(data)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
        .context("base58 decode")
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

fn get_tape_array_of_base58(
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
        let v = decode_base58(v).with_context(|| format!("decode element of {} as hex", name))?;
        out.push(v);
    }

    Ok(Some(out))
}

fn get_tape_version(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i8>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };

    if val.as_str() == Some("legacy") {
        return Ok(Some(-1));
    }

    let val = val
        .as_i8()
        .with_context(|| format!("{} as i8 version", name))?;

    if val < 0 {
        return Err(anyhow!("invalid version column {} value: {}", name, val));
    }

    Ok(Some(val))
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

fn get_tape_bool(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<bool>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_bool()
        .with_context(|| format!("{} as bool", name))
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

fn get_tape_u32(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u32>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u32()
        .with_context(|| format!("get {} as u32", name))
        .map(Some)
}

fn get_tape_u16(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<u16>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_u16()
        .with_context(|| format!("get {} as u16", name))
        .map(Some)
}

fn get_tape_i64(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<i64>> {
    let val = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    val.as_i64()
        .with_context(|| format!("get {} as i64", name))
        .map(Some)
}

fn get_tape_base58(obj: &simd_json::tape::Object<'_, '_>, name: &str) -> Result<Option<Vec<u8>>> {
    let hex = match obj.get(name) {
        None => return Ok(None),
        Some(v) if v.is_null() => return Ok(None),
        Some(v) => v,
    };
    let hex = hex.as_str().with_context(|| format!("{} as str", name))?;

    decode_base58(hex)
        .with_context(|| format!("decode_base58({})", name))
        .map(Some)
}
