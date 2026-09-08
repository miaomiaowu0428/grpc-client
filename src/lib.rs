use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Error, Result};
use solana_sdk::{
    hash::Hash,
    message::{
        compiled_instruction::CompiledInstruction, legacy::Message as LegacyMessage, v0, v1,
        MessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use tokio::sync::Mutex;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::{
    geyser::SubscribeUpdateTransaction,
    solana::storage::confirmed_block::{
        Message as ProtoMessage, Transaction as ProtoTransaction, TransactionStatusMeta,
    },
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct AppError(Error);

impl<E> From<E> for AppError
where
    E: Into<Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

/// 一笔交易：标识 + 执行结果(proto meta) + solana 原生交易。
///
/// - `meta`: proto 的 `TransactionStatusMeta`（自带 `err`，`err.is_none()` 即成功），
///   不依赖 `solana_transaction_status`，避免其 wincode 依赖冲突。
/// - `transaction`: solana 原生 `VersionedTransaction`。proto 把交易的
///   `message` 已解析成字段，这里按 `versioned`/`config` 分支组装成
///   `Legacy`/`V0`/`V1` 三种原生消息。
#[derive(Debug, Clone)]
pub struct TransactionFormat {
    pub slot: u64,
    #[allow(dead_code)]
    pub index: u64,
    pub meta: Option<TransactionStatusMeta>,
    pub transaction: VersionedTransaction,
}

impl TransactionFormat {
    /// 交易是否成功执行。失败交易（revert 但仍消耗 CU + tip）返回 false。
    pub fn is_successful(&self) -> bool {
        self.meta.as_ref().map(|m| m.err.is_none()).unwrap_or(false)
    }
}

fn hash_from_bytes(b: &[u8]) -> Result<Hash> {
    let arr: [u8; 32] = b
        .try_into()
        .map_err(|_| anyhow!("recent_blockhash not 32 bytes"))?;
    Ok(Hash::new_from_array(arr))
}

fn keys_from_bytes(keys: &[Vec<u8>]) -> Result<Vec<Pubkey>> {
    keys.iter()
        .map(|k| Pubkey::try_from(k.as_slice()).map_err(|e| anyhow!("bad pubkey: {e:?}")))
        .collect()
}

fn map_instructions(ins: &ProtoMessage) -> Vec<CompiledInstruction> {
    ins.instructions
        .iter()
        .map(|i| CompiledInstruction {
            program_id_index: i.program_id_index as u8,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .collect()
}

/// 把 geyser proto 交易组装成 solana 原生 `VersionedTransaction`。
///
/// proto `Message` 已把消息体解析成字段，因此这里是"字段搬运 + 类型提升"，
/// 并按格式分支成三种原生消息：
/// - `versioned == false`                 -> `VersionedMessage::Legacy`
/// - `versioned == true` + `config.is_some()` -> `VersionedMessage::V1`（SIMD-0385）
/// - `versioned == true` + `config.is_none()` -> `VersionedMessage::V0`（含 ALT）
fn to_versioned_transaction(tx: &ProtoTransaction) -> Result<VersionedTransaction> {
    let msg = tx
        .message
        .as_ref()
        .ok_or_else(|| anyhow!("transaction missing message"))?;
    let header = msg
        .header
        .as_ref()
        .ok_or_else(|| anyhow!("message missing header"))?;
    let header = MessageHeader {
        num_required_signatures: header.num_required_signatures as u8,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
    };

    let signatures = tx
        .signatures
        .iter()
        .map(|s| {
            Signature::try_from(s.as_slice()).map_err(|e| anyhow!("bad signature: {e:?}"))
        })
        .collect::<Result<Vec<Signature>>>()?;

    let account_keys = keys_from_bytes(&msg.account_keys)?;
    let recent_blockhash = hash_from_bytes(&msg.recent_blockhash)?;
    let instructions = map_instructions(msg);

    let vmessage = if !msg.versioned {
        // Legacy 消息（无 versioned 前缀、无 ALT）
        VersionedMessage::Legacy(LegacyMessage {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        })
    } else if msg.config.is_some() {
        // V1 消息（SIMD-0385）：含 config，无 ALT；recent_blockhash 即 lifetime_specifier
        let cfg = msg.config.as_ref().expect("checked some");
        let config = v1::TransactionConfig {
            priority_fee: cfg.priority_fee,
            compute_unit_limit: cfg.compute_unit_limit,
            loaded_accounts_data_size_limit: cfg.loaded_accounts_data_size_limit,
            heap_size: cfg.heap_size,
        };
        VersionedMessage::V1(v1::Message {
            header,
            config,
            lifetime_specifier: recent_blockhash,
            account_keys,
            instructions,
        })
    } else {
        // V0 消息：含 ALT
        let lookups = msg
            .address_table_lookups
            .iter()
            .map(|l| {
                Ok(v0::MessageAddressTableLookup {
                    account_key: Pubkey::try_from(l.account_key.as_slice())
                        .map_err(|e| anyhow!("bad alt account key: {e:?}"))?,
                    writable_indexes: l.writable_indexes.clone(),
                    readonly_indexes: l.readonly_indexes.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        VersionedMessage::V0(v0::Message {
            header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups: lookups,
        })
    };

    Ok(VersionedTransaction {
        signatures,
        message: vmessage,
    })
}

impl TransactionFormat {
    pub fn try_from_update(u: SubscribeUpdateTransaction) -> Result<Self> {
        let SubscribeUpdateTransaction { transaction, slot } = u;
        let info = transaction.ok_or_else(|| anyhow!("update missing transaction"))?;
        let proto = info
            .transaction
            .ok_or_else(|| anyhow!("update missing tx body"))?;
        Ok(Self {
            slot,
            index: info.index,
            meta: info.meta,
            transaction: to_versioned_transaction(&proto)?,
        })
    }
}

impl From<SubscribeUpdateTransaction> for TransactionFormat {
    fn from(u: SubscribeUpdateTransaction) -> Self {
        TransactionFormat::try_from_update(u).expect("valid geyser transaction")
    }
}

pub struct YellowstoneGrpc {
    endpoint: String,
    x_token: Option<String>,
}

impl YellowstoneGrpc {
    pub fn new(endpoint: String, x_token: Option<String>) -> Self {
        Self { endpoint, x_token }
    }

    pub async fn build_client(self) -> Result<Arc<Mutex<GeyserGrpcClient>>, AppError> {
        let client = GeyserGrpcClient::build_from_shared(self.endpoint)?
            .x_token(self.x_token)?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .timeout(Duration::from_secs(60))
            .connect()
            .await?;
        Ok(Arc::new(Mutex::new(client)))
    }
}
