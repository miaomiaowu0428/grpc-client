use std::{sync::Arc, time::Duration};

use tonic::codec::CompressionEncoding;

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
use solana_transaction_error::TransactionError;
use solana_transaction_status::TransactionStatusMeta;
use tokio::sync::Mutex;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::{
    geyser::SubscribeUpdateTransaction,
    solana::storage::confirmed_block::{
        Message as ProtoMessage, Transaction as ProtoTransaction,
        TransactionStatusMeta as ProtoTransactionStatusMeta,
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

/// 一笔交易：标识 + 执行结果(official meta) + solana 原生交易。
///
/// - `meta`: 官方工具链的 `solana_transaction_status::TransactionStatusMeta`。
///   由 geyser proto 的 meta 映射而来，成功与否看 `meta.status.is_ok()`。
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
        self.meta
            .as_ref()
            .map(|m| m.status.is_ok())
            .unwrap_or(false)
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

/// 把 geyser proto 的 `TransactionStatusMeta` 映射成官方
/// `solana_transaction_status::TransactionStatusMeta`。
///
/// 字段差异处理：
/// - `err: Option<proto TransactionError>` -> `status: TransactionResult<()>`，
///   proto 用 bincode 序列化官方 `TransactionError`，这里反序列化还原；
///   解码失败时退化为 `TransactionError::InvalidAccountData`（仅在网络返回
///   非法字节时发生，正常路径不会走到）。
/// - proto 的 `*_none: bool`（显式"无值"标记）-> 官方 `Option<...>::None`。
/// - `loaded_writable/readonly_addresses` -> `LoadedAddresses`。
fn to_official_meta(p: &ProtoTransactionStatusMeta) -> TransactionStatusMeta {
    use solana_account_decoder_client_types::token::UiTokenAmount;
    use solana_transaction_context::transaction::TransactionReturnData;
    use solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, RewardType, TransactionTokenBalance,
    };

    let status = match p.err.as_ref() {
        None => Ok(()),
        Some(e) => match bincode::deserialize::<TransactionError>(e.err.as_slice()) {
            Ok(err) => Err(err),
            Err(_) => Err(TransactionError::InvalidAccountIndex),
        },
    };

    let inner_instructions = if p.inner_instructions_none {
        None
    } else {
        Some(
            p.inner_instructions
                .iter()
                .map(|ii| InnerInstructions {
                    index: ii.index as u8,
                    instructions: ii
                        .instructions
                        .iter()
                        .map(|i| InnerInstruction {
                            instruction: CompiledInstruction {
                                program_id_index: i.program_id_index as u8,
                                accounts: i.accounts.clone(),
                                data: i.data.clone(),
                            },
                            stack_height: i.stack_height,
                        })
                        .collect(),
                })
                .collect(),
        )
    };

    let log_messages = if p.log_messages_none {
        None
    } else {
        Some(p.log_messages.clone())
    };

    let token_balances = |v: &[yellowstone_grpc_proto::solana::storage::confirmed_block::TokenBalance]| {
        v.iter()
            .map(|b| TransactionTokenBalance {
                account_index: b.account_index as u8,
                mint: b.mint.clone(),
                ui_token_amount: match b.ui_token_amount.as_ref() {
                    Some(a) => UiTokenAmount {
                        ui_amount: Some(a.ui_amount),
                        decimals: a.decimals as u8,
                        amount: a.amount.clone(),
                        ui_amount_string: a.ui_amount_string.clone(),
                    },
                    None => UiTokenAmount {
                        ui_amount: None,
                        decimals: 0,
                        amount: String::new(),
                        ui_amount_string: String::new(),
                    },
                },
                owner: b.owner.clone(),
                program_id: b.program_id.clone(),
            })
            .collect::<Vec<_>>()
    };

    let rewards = p
        .rewards
        .iter()
        .map(|r| Reward {
            pubkey: r.pubkey.clone(),
            lamports: r.lamports,
            post_balance: r.post_balance,
            reward_type: match r.reward_type {
                0 => None,
                1 => Some(RewardType::Fee),
                2 => Some(RewardType::Rent),
                3 => Some(RewardType::Staking),
                4 => Some(RewardType::Voting),
                _ => None,
            },
            commission: r.commission.parse().ok(),
            commission_bps: r.commission_bps.parse().ok(),
        })
        .collect::<Vec<_>>();

    let return_data = if p.return_data_none {
        None
    } else {
        p.return_data.as_ref().map(|rd| TransactionReturnData {
            program_id: Pubkey::try_from(rd.program_id.as_slice()).unwrap_or_default(),
            data: rd.data.clone(),
        })
    };

    TransactionStatusMeta {
        status,
        fee: p.fee,
        pre_balances: p.pre_balances.clone(),
        post_balances: p.post_balances.clone(),
        inner_instructions,
        log_messages,
        pre_token_balances: Some(token_balances(&p.pre_token_balances)),
        post_token_balances: Some(token_balances(&p.post_token_balances)),
        rewards: Some(rewards),
        loaded_addresses: v0::LoadedAddresses {
            writable: keys_from_bytes(&p.loaded_writable_addresses).unwrap_or_default(),
            readonly: keys_from_bytes(&p.loaded_readonly_addresses).unwrap_or_default(),
        },
        return_data,
        compute_units_consumed: p.compute_units_consumed,
        cost_units: p.cost_units,
    }
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
            meta: info.meta.as_ref().map(to_official_meta),
            transaction: to_versioned_transaction(&proto)?,
        })
    }
}

impl From<SubscribeUpdateTransaction> for TransactionFormat {
    fn from(u: SubscribeUpdateTransaction) -> Self {
        TransactionFormat::try_from_update(u).expect("valid geyser transaction")
    }
}

// ── HTTP/2 keep-alive ────────────────────────────────────────────────────────
//
// ⚠️ tonic 的 keep_alive_while_idle(true) 只是「空闲时也发 PING」的开关，
// **真正决定发不发 PING 的是 http2_keep_alive_interval**。只设前者而不设
// 后者，一个 PING 都不会发 —— 这是个静默的 no-op，看起来配了心跳其实没有。
//
// 没有 PING 的后果：NAT / LB / 云安全组按空闲回收 TCP 时（常见 60s~350s），
// 连接变成半开（对端没发 FIN/RST），客户端的 stream.next() 会**永远挂住**，
// 既不报错也不重连，表现为「进程还活着但一笔交易都不推了」。
//
// 默认 30s / 10s：比常见空闲回收阈值短，一个心跳周期内就能判死半开连接，
// 让 stream 返回 Err，交给调用方的重连循环。
pub const DEFAULT_KEEP_ALIVE_INTERVAL_SECS: u64 = 30;
/// 发出 PING 后等 ACK 的上限；超时即判连接已死。
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_SECS: u64 = 10;

const ENV_KEEP_ALIVE_INTERVAL: &str = "GRPC_KEEP_ALIVE_INTERVAL_SECS";
const ENV_KEEP_ALIVE_TIMEOUT: &str = "GRPC_KEEP_ALIVE_TIMEOUT_SECS";

/// HTTP/2 心跳参数（秒级）。调用方一般不用手动构造，[YellowstoneGrpc::build_client]
/// 会用 [GrpcKeepAlive::from_env] 读环境变量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcKeepAlive {
    /// 两次 PING 之间的间隔。
    pub interval: Duration,
    /// 一次 PING 等 ACK 的超时。
    pub timeout: Duration,
}

impl Default for GrpcKeepAlive {
    fn default() -> Self {
        Self::from_secs(
            DEFAULT_KEEP_ALIVE_INTERVAL_SECS,
            DEFAULT_KEEP_ALIVE_TIMEOUT_SECS,
        )
    }
}

impl GrpcKeepAlive {
    /// 规范化：间隔至少 1s；超时夹在 1s..=interval。
    ///
    /// 超时大于间隔只会让 PING 在队列里堆积（每个周期都还没等到上一个 ACK），
    /// 没有任何收益，所以直接夹住。
    pub fn from_secs(interval_secs: u64, timeout_secs: u64) -> Self {
        let interval_secs = interval_secs.max(1);
        Self {
            interval: Duration::from_secs(interval_secs),
            timeout: Duration::from_secs(timeout_secs.clamp(1, interval_secs)),
        }
    }

    /// 从环境变量读取：
    /// - GRPC_KEEP_ALIVE_INTERVAL_SECS（默认 30）
    /// - GRPC_KEEP_ALIVE_TIMEOUT_SECS（默认 10）
    ///
    /// 变量名与 nonce_cache 共用，方便统一调。非法值（非正整数）回落默认。
    pub fn from_env() -> Self {
        Self::from_secs(
            env_secs(ENV_KEEP_ALIVE_INTERVAL, DEFAULT_KEEP_ALIVE_INTERVAL_SECS),
            env_secs(ENV_KEEP_ALIVE_TIMEOUT, DEFAULT_KEEP_ALIVE_TIMEOUT_SECS),
        )
    }
}

/// 解析正整数秒；空串 / 0 / 负数 / 非数字都返回 None。
fn parse_secs(raw: &str) -> Option<u64> {
    raw.trim().parse::<u64>().ok().filter(|v| *v > 0)
}

fn env_secs(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match parse_secs(&raw) {
            Some(v) => v,
            None => {
                log::warn!(
                    "⚠️ {}={:?} 非法（需要正整数秒），回落默认 {}s",
                    key, raw, default
                );
                default
            }
        },
    }
}

pub struct YellowstoneGrpc {
    endpoint: String,
    x_token: Option<String>,
    /// **服务端 → 客户端**方向的响应压缩编码；`None` = 不请求压缩（identity）。
    ///
    /// 默认 `None`，与旧版行为一致；由调用方显式 [`Self::gzip`] 打开
    /// （`transaction-monitor` 的订阅循环按 `YELLOWSTONE_GRPC_COMPRESSION_ENCODING`
    /// 决定，默认 gzip）。
    ///
    /// 对应 tonic 客户端的 `accept_compressed`：握手时发 `grpc-accept-encoding: gzip`，
    /// 服务端（yellowstone-geyser 默认 `compression.send = [gzip, zstd]`）才会压着回包。
    /// 服务端不支持时会忽略这个头、照样发明文，不会报错。
    accept_compression: Option<CompressionEncoding>,
}

impl YellowstoneGrpc {
    pub fn new(endpoint: String, x_token: Option<String>) -> Self {
        Self {
            endpoint,
            x_token,
            accept_compression: None,
        }
    }

    /// 请求 gzip 响应压缩（**只影响服务端 → 客户端**方向，省下行流量）。
    ///
    /// 不提供 `send_compressed`：客户端上行只有 SubscribeRequest / Ping，
    /// 几十字节，压了没收益；反而要求服务端开 `compression.accept`，
    /// 有些服务商为了省 CPU 会关掉，发了压缩请求会直接被拒。
    pub fn gzip(mut self) -> Self {
        self.accept_compression = Some(CompressionEncoding::Gzip);
        self
    }

    /// 关闭响应压缩（默认行为，保留给需要显式表达的调用方）。
    pub fn no_compression(mut self) -> Self {
        self.accept_compression = None;
        self
    }

    pub async fn build_client(self) -> Result<Arc<Mutex<GeyserGrpcClient>>, AppError> {
        let accept_compression = self.accept_compression;
        let mut builder = GeyserGrpcClient::build_from_shared(self.endpoint)?
            .x_token(self.x_token)?
            .tls_config(ClientTlsConfig::new().with_native_roots())?;

        // 只对「服务端 → 客户端」方向请求压缩，这是流量的大头（交易/账户推送）。
        if let Some(encoding) = accept_compression {
            builder = builder.accept_compressed(encoding);
        }

        // HTTP/2 心跳：必须「间隔 + 超时 + 空闲也发」三个一起配。
        // 少了 http2_keep_alive_interval，keep_alive_while_idle 是空操作，一个
        // PING 都不会发 —— 半开连接就会永远挂在 stream.next() 上（见文件头部说明）。
        let keep_alive = GrpcKeepAlive::from_env();
        log::info!(
            "💓 gRPC HTTP/2 心跳：间隔 {}s / 超时 {}s（空闲也发 PING）",
            keep_alive.interval.as_secs(),
            keep_alive.timeout.as_secs()
        );

        let client = builder
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(keep_alive.interval)
            .keep_alive_timeout(keep_alive.timeout)
            .keep_alive_while_idle(true)
            .timeout(Duration::from_secs(60))
            .connect()
            .await?;
        Ok(Arc::new(Mutex::new(client)))
    }
}

#[cfg(test)]
mod keep_alive_tests {
    use super::*;

    #[test]
    fn parse_secs_accepts_positive_integers() {
        assert_eq!(parse_secs("30"), Some(30));
        assert_eq!(parse_secs(" 7 "), Some(7));
    }

    #[test]
    fn parse_secs_rejects_garbage() {
        assert_eq!(parse_secs(""), None);
        assert_eq!(parse_secs("0"), None);
        assert_eq!(parse_secs("-1"), None);
        assert_eq!(parse_secs("1.5"), None);
        assert_eq!(parse_secs("abc"), None);
    }

    #[test]
    fn defaults_are_sane() {
        let ka = GrpcKeepAlive::default();
        assert_eq!(ka.interval, Duration::from_secs(DEFAULT_KEEP_ALIVE_INTERVAL_SECS));
        assert_eq!(ka.timeout, Duration::from_secs(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS));
        // 心跳间隔必须短于常见的 NAT/LB 空闲回收阈值（60s 起）。
        assert!(ka.interval < Duration::from_secs(60));
        assert!(ka.timeout < ka.interval);
    }

    #[test]
    fn from_secs_clamps_degenerate_values() {
        // 0 不能变成「每 tick 都发 PING」或「永远超时」。
        assert_eq!(GrpcKeepAlive::from_secs(0, 0).interval, Duration::from_secs(1));
        assert_eq!(GrpcKeepAlive::from_secs(0, 0).timeout, Duration::from_secs(1));
        // 超时 > 间隔：夹到间隔，避免 PING 排队堆积。
        let ka = GrpcKeepAlive::from_secs(5, 30);
        assert_eq!(ka.interval, Duration::from_secs(5));
        assert_eq!(ka.timeout, Duration::from_secs(5));
        // 合法值原样保留。
        let ka = GrpcKeepAlive::from_secs(30, 10);
        assert_eq!(ka.interval, Duration::from_secs(30));
        assert_eq!(ka.timeout, Duration::from_secs(10));
    }
}
