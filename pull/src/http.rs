//! HTTP/1.1 keep-alive 传输层：在可复用的 TCP 连接上跑 `GET`，读状态行与响应头，
//! 正文由调用方按 `Content-Length` 读完。连接池 [`Pool`] 收口借/还，
//! [`http_get`] 收口"复用的连接被对端悄悄关掉时换新重试一次"。

use crate::error::Error;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// 建连超时：远端在约定时间内没握上手就别耗着。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 单次读取之间允许的最长空闲；超过就认定这条连接已经哑掉（既不回数据也不断开）。
#[cfg(not(test))]
pub const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// 测试里缩到 500ms：好让 `debug.sh` 真跑一遍超时路径，而不必干等半分钟。
#[cfg(test)]
pub const READ_TIMEOUT: Duration = Duration::from_millis(500);

/// 取锁，中毒也照常返回：临界区里只有一次 `Vec` 增删，中毒后内部数据仍结构完整，
/// 继续用是安全的——与 `main.rs`、`file_cache.rs` 的 `lock` 语义对齐。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 可复用的 HTTP/1.1 keep-alive 连接池。
///
/// 池里存空闲的连接：[`Pool::acquire`] 借一条、[`Pool::release`] 还一条。并发拉取时
/// 每个任务各借一条，池空则新建；归还的连接被下一个 `acquire` 复用。同时在飞的连接数
/// 由调用方用 `buffer_unordered(N)` 的 N 控制——池本身不做上限，`N` 就是这个上限。
#[derive(Default)]
pub struct Pool {
    idle: Mutex<Vec<BufReader<TcpStream>>>,
}

impl Pool {
    /// 借一条连接：池里有空闲的就拿来用，没有就新建。返回 `(连接, 是否来自池)`——
    /// [`http_get`] 靠第二个值判断失败时要不要重试一次。
    async fn acquire(&self, host: &str) -> Result<(BufReader<TcpStream>, bool), Error> {
        // 临界区只有一次 `Vec::pop`：同步锁足够，省掉异步 Mutex 的状态机开销。
        // 先把 `pop` 的结果取出来、让临时 `MutexGuard` 在语句结束时释放，再判 `Some`；
        // 写成 `if let Some(conn) = lock(&self.idle).pop()` 会让 guard 一直活到整个
        // `if let` 表达式结束（clippy::significant_drop_in_scrutinee）。
        let idle = lock(&self.idle).pop();
        if let Some(conn) = idle {
            return Ok((conn, true));
        }
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(host))
            .await
            .map_err(|_| Error::Timeout { phase: "连接" })?
            .map_err(|source| Error::Connect {
                host: host.to_string(),
                source,
            })?;
        let _ = stream.set_nodelay(true);
        Ok((BufReader::new(stream), false))
    }

    /// 归还：正文按 `Content-Length` 精确读满、连接干净时才调。
    pub fn release(&self, conn: BufReader<TcpStream>) {
        lock(&self.idle).push(conn);
    }
}

/// 借/建一条连接，写 `GET` 请求，读状态行并跳过响应头；返回可继续读正文的 reader 与响应
/// 声明的 `Content-Length`，非 200 报错。`fetch_file`、`list_entries` 共有的请求前置收口于此。
///
/// 复用的连接若被服务端悄悄关掉（keep-alive 超时、对端 RST），下一次请求会在写或读状态行时
/// 失败——这时换一条新连接重试一次，不让一个已死的池连接把整次拉取带走。只重试一次、且只在
/// 确系复用时：新连接也失败就是真出错。超时不重试（服务端活着只是慢，不是连接死了）。
pub async fn http_get(
    pool: &Pool,
    host: &str,
    path: &str,
) -> Result<(BufReader<TcpStream>, Option<u64>), Error> {
    // `retried` 而不是"循环几次"：明确表示最多重试一次，二次进入循环时不再重试
    let mut retried = false;
    loop {
        let (mut reader, reused) = pool.acquire(host).await?;
        match request(&mut reader, host, path).await {
            Ok((200, content_length)) => return Ok((reader, content_length)),
            Ok((status, _)) => {
                return Err(Error::Http {
                    status,
                    path: path.to_string(),
                });
            }
            Err(error) if reused && !retried && !matches!(error, Error::Timeout { .. }) => {
                // 池里拿到的连接坏了：换新连接重试一次，`reader` 在这里被丢弃
                retried = true;
            }
            Err(error) => return Err(error),
        }
    }
}

/// 写一条 `GET` 请求并读出状态行与响应头。请求不带 `Connection` 头：HTTP/1.1 默认 keep-alive，
/// 连接因此可复用。建连后整条 `TcpStream` 直接交给 reader，**不做 `into_split`**：那样写半边
/// 会在请求发完后出作用域，`OwnedWriteHalf::drop` 顺手 `shutdown(Write)`，而这个提前的
/// half-close 会让服务端（salvo/hyper 的 `http1` 默认 `half_close = false`）在读到 EOF 时
/// 判定连接中断、丢掉还在飞的响应——几十 MB 的文件就只落下几 MB。标准客户端（curl、浏览器）
/// 也不 half-close。
async fn request(
    reader: &mut BufReader<TcpStream>,
    host: &str,
    path: &str,
) -> Result<(u16, Option<u64>), Error> {
    reader
        .get_mut()
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes())
        .await?;
    read_status(reader).await
}

/// 读状态行 + 跳过响应头，返回状态码与响应声明的 `Content-Length`。正文留给调用方接着读。
async fn read_status(reader: &mut BufReader<TcpStream>) -> Result<(u16, Option<u64>), Error> {
    // 状态行最长也就几十字节，一次给够，免得 `read_line` 中途扩容
    let mut status_line = String::with_capacity(64);
    read_line_in_time(reader, &mut status_line).await?;
    let status = status_code(&status_line).ok_or(Error::Malformed("状态行格式异常"))?;
    // 复用同一个 String 读响应头，免得每行各分配一次。
    let mut content_length = None;
    let mut line = String::new();
    loop {
        line.clear();
        let read = read_line_in_time(reader, &mut line).await?;
        if read == 0 || line.trim().is_empty() {
            break;
        }
        take_content_length(&line, &mut content_length)?;
    }
    Ok((status, content_length))
}

/// 读一行响应头，套上单次读取的空闲超时：远端接上了却一直不说话也不能挂死。
async fn read_line_in_time(
    reader: &mut BufReader<TcpStream>,
    line: &mut String,
) -> Result<usize, Error> {
    tokio::time::timeout(READ_TIMEOUT, reader.read_line(line))
        .await
        .map_err(|_| Error::Timeout {
            phase: "读取响应"
        })?
        .map_err(Error::from)
}

/// 这一行若是 `Content-Length`（名字大小写不敏感）就把值记下来。
///
/// 值不是合法数字直接报错：拿不到可信长度就没法判断正文有没有被截断，与其悄悄放过，
/// 不如当场把"对面发了个看不懂的长度"说出来。
fn take_content_length(line: &str, content_length: &mut Option<u64>) -> Result<(), Error> {
    let Some((name, value)) = line.split_once(':') else {
        return Ok(());
    };
    if !name.trim().eq_ignore_ascii_case("content-length") {
        return Ok(());
    }
    *content_length = Some(
        value
            .trim()
            .parse()
            .map_err(|_| Error::Malformed("Content-Length 不是合法数字"))?,
    );
    Ok(())
}

/// 从状态行 `HTTP/1.1 200 OK` 里取出状态码。
///
/// 先用 `memchr` 定位版本号后那个空格，再在剩下的一小段里取词：比
/// `split_whitespace().nth(1)` 少一整层 `Pattern` 与迭代器分发（基准见 `bench_status_code`）。
/// 仍按任意 ASCII 空白切分，与原来的宽容度一致。
fn status_code(line: &str) -> Option<u16> {
    let bytes = line.as_bytes();
    let rest = &bytes[memchr::memchr(b' ', bytes)? + 1..];
    let start = rest.iter().position(|b| !b.is_ascii_whitespace())?;
    let token = &rest[start..];
    let end = token
        .iter()
        .position(u8::is_ascii_whitespace)
        .unwrap_or(token.len());
    std::str::from_utf8(&token[..end]).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// 跑 `iters` 次取平均纳秒，先热身 `iters/10` 次。
    fn time<R>(iters: u32, f: impl Fn() -> R) -> f64 {
        use std::hint::black_box;
        use std::time::Instant;

        for _ in 0..iters / 10 {
            black_box(f());
        }
        let start = Instant::now();
        for _ in 0..iters {
            black_box(f());
        }
        start.elapsed().as_secs_f64() * 1e9 / f64::from(iters)
    }

    /// 基准：`memchr` 取状态码 vs `split_whitespace().nth(1)`，逐文件都会走一遍。
    ///
    /// `cargo test` 默认跑在 `opt-level = 0`：std 是预编译的优化产物而 `memchr` 不是，
    /// 那种 profile 下这一项偏向原实现；要看真实差距得加 `--release`。
    #[test]
    #[ignore = "微基准，需 cargo test --release -- --ignored 显式运行"]
    fn bench_status_code() {
        for line in ["HTTP/1.1 200 OK\r\n", "HTTP/1.1 404 Not Found\r\n"] {
            let old = || line.split_whitespace().nth(1)?.parse::<u16>().ok();
            assert_eq!(old(), status_code(line), "{line} 取值不一致");

            let old_ns = time(500_000, old);
            let memchr_ns = time(500_000, || status_code(line));
            println!(
                "基准 status_code（{}B）: split_whitespace {old_ns:.1} ns vs memchr {memchr_ns:.1} ns",
                line.len()
            );
            // 只卡数量级：未优化的测试 profile 抖动大，这里不追求证明「更快」
            assert!(
                memchr_ns < old_ns * 10.0,
                "memchr 版比原实现慢了一个数量级: {memchr_ns:.1} vs {old_ns:.1} ns"
            );
        }
    }
}
