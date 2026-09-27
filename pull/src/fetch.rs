//! 拉取操作：单文件下载 [`fetch_file`] 与目录列举 [`list_entries`]，都建在
//! [`crate::http`] 的 keep-alive 传输之上。正文按响应声明的 `Content-Length` 精确读满即止，
//! 读满的连接归还池子复用；读不满即截断，连接丢弃。落盘在 Linux/Android 且目标文件系统
//! 支持时走 `splice(2)` 零拷贝（见 `crate::splice`），否则退回用户态读写的同步搬运。

use crate::error::Error;
use crate::http::{Pool, READ_TIMEOUT, http_get};
use serde::Deserialize;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt as _, BufReader};

#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::splice::{Moved, transfer};

/// 每块搬运的字节数：一次同步 `read` + 一次同步 `write` 处理这么多，够摊薄系统调用。
const COPY_BUF: usize = 64 * 1024;

/// 响应没带 `Content-Length` 时的报错：正文边界无从得知，当场说清，不猜长度。
///
/// 这里不能退化成读到 EOF——请求不带 `Connection: close`，对端不会关连接，`read_to_end`
/// 只会空等到 `READ_TIMEOUT` 再报超时，还不如当场把话说清楚。`/api/list`（salvo 的 `Json`）
/// 与 `/pull`（`NamedFile`）都带长度，所以正常走不到这一支。
const NO_CONTENT_LENGTH: &str = "响应没有 Content-Length，无法确定正文边界";

/// `/api/list` 返回的一条条目。
///
/// `type` 缺字段按文件处理（与原先 `unwrap_or("file")` 一致）；`size` 仅文件有，目录为
/// `None`，用于"本地已存在且尺寸一致就跳过"。
#[derive(Deserialize)]
pub struct RemoteEntry {
    pub name: String,
    #[serde(rename = "type", default)]
    pub kind: String,
    pub size: Option<u64>,
}

impl RemoteEntry {
    pub fn is_dir(&self) -> bool {
        self.kind == "dir"
    }
}

/// `/api/list` 的响应体：只取 `entries`，其余字段（`path`/`lan_ip`/`port`）客户端用不到。
#[derive(Deserialize)]
struct ListResponse {
    entries: Vec<RemoteEntry>,
}

/// 取一层目录的条目：`GET /api/list[/<remote>]`。正文按 `Content-Length` 增量读满，连接干净
/// 归还池子复用；读不满即截断，连接丢弃。
pub async fn list_entries(
    pool: &mut Pool,
    host: &str,
    remote: &str,
) -> Result<Vec<RemoteEntry>, Error> {
    let path = if remote.is_empty() {
        "/api/list".to_string()
    } else {
        format!("/api/list/{}", encode_path(remote))
    };
    let (reader, declared) = http_get(pool, host, &path).await?;
    let len = declared.ok_or(Error::Malformed(NO_CONTENT_LENGTH))?;
    // 列表正文就几十 KB 出头，这里卡的是整段读完的总时长（不是空闲）。用 `take` 把读取截在
    // 声明的长度上，而不是先按这个长度开一块：对端报的数在读懂之前都不算数。
    let mut limited = reader.take(len);
    let mut body = Vec::new();
    tokio::time::timeout(READ_TIMEOUT, limited.read_to_end(&mut body))
        .await
        .map_err(|_| Error::Timeout {
            phase: "读取目录列表",
        })??;
    if body.len() as u64 != len {
        return Err(Error::Truncated {
            remote: remote.to_string(),
            want: len,
            got: body.len() as u64,
        });
    }
    pool.release(limited.into_inner());
    Ok(serde_json::from_slice::<ListResponse>(&body)?.entries)
}

/// 一次抓取里正文走的搬运方式。
///
/// 拉完给用户汇报「几个文件真的做了内核搬运、几个根本不用搬」时就按它分类。
#[derive(Clone, Copy, Debug)]
pub enum Via {
    /// 内核 `splice(2)` 零拷贝：socket → 管道 → 文件。
    Splice,
    /// 用户态 `read` + `write`：非 Linux/Android，或目标文件系统没有 `splice_write`。
    Copy,
    /// 正文全在 `BufReader` 的预读缓冲里，没有需要搬运的字节。
    Prebuffered,
}

/// 一次文件落盘的结果。
#[derive(Debug)]
pub struct Fetched {
    /// 落盘字节数。
    pub bytes: u64,
    /// 正文走的搬运方式。
    pub via: Via,
}

/// 拉一个文件到 `local`：正文按响应声明的 `Content-Length` 精确读满即停。
///
/// 读满后连接干净，归还池子给下一个文件复用；服务端提前 EOF（读到的字节数不足声明的长度）
/// 或半路超时/IO 出错，都把没写完的文件删掉再报错——宁可什么都没有，也不留一个看着完整
/// 其实残缺的文件。
pub async fn fetch_file(
    pool: &mut Pool,
    host: &str,
    remote: &str,
    local: &Path,
) -> Result<Fetched, Error> {
    let path = format!("/pull/{}", encode_path(remote));
    let (reader, declared) = http_get(pool, host, &path).await?;
    let want = declared.ok_or(Error::Malformed(NO_CONTENT_LENGTH))?;

    // `BufReader::into_inner` 会丢弃内部缓冲里已预读的字节，而读响应头时它通常已经
    // 预读了正文开头；先复制出来交给阻塞线程，避免丢掉正文的前几个字节。
    let buffered = reader.buffer().to_vec();
    let stream = reader.into_inner();

    let (copied, via, stream) =
        match copy_in_blocking(stream, buffered, local.to_path_buf(), want).await {
            Ok(landed) => landed,
            Err(error) => {
                discard(local).await;
                return Err(error);
            }
        };

    if copied != want {
        discard(local).await;
        return Err(Error::Truncated {
            remote: remote.to_string(),
            want,
            got: copied,
        });
    }
    // 把 socket 切回异步、包回 `BufReader` 归还复用。
    let stream = tokio::net::TcpStream::from_std(stream)?;
    pool.release(BufReader::new(stream));
    Ok(Fetched { bytes: copied, via })
}

/// 在阻塞线程里把正文从 socket 搬进文件，返回落盘字节数、搬运方式与归还的 socket。
///
/// 读 socket 与写文件都在同一个阻塞线程里用同步 IO 完成：tokio 的 `fs::File` 每次
/// `write` 都要把缓冲搬到 blocking pool，异步 socket 每次 `read` 都要过一遍 reactor
/// 并在 waker 上注册一次；大文件连续传输时这两笔每块固定开销会累加到明显可观的 CPU
/// 占用。整个循环收进一个 blocking 线程后，一次文件传输只跨线程两次（进、出），其余
/// 全是同步系统调用（`splice(2)` 或 `read`/`write`）。
///
/// `buffered` 是 `BufReader` 预读出来、还没被消耗的正文开头；`into_inner` 会把它丢掉，
/// 所以由调用方先取出来，这里负责先落盘再接着读。
async fn copy_in_blocking(
    stream: tokio::net::TcpStream,
    buffered: Vec<u8>,
    target: PathBuf,
    want: u64,
) -> Result<(u64, Via, std::net::TcpStream), Error> {
    tokio::task::spawn_blocking(move || {
        // `into_std` 只把 fd 转回 std，不改变阻塞模式；tokio 的 socket 是非阻塞的，
        // 要做同步读就得先切回阻塞。
        let stream = stream.into_std()?;
        stream.set_nonblocking(false)?;
        let (copied, via) = copy_sync(&stream, &buffered, &target, want)?;
        // 交还前切回非阻塞，否则 `from_std` 之后 reactor 会在错误的前提上注册 fd。
        stream.set_nonblocking(true)?;
        Ok::<_, Error>((copied, via, stream))
    })
    .await
    .map_err(|join| Error::Io(io::Error::other(join)))?
}

/// 同步地把正文搬进文件：读满 `want` 字节即停，或用完 socket 上的数据即停。
///
/// 读满是因为对端声明了 `Content-Length`，读多一个字节会把下一条响应的开头吃进缓冲；
/// 读不满则由调用方按截断处理。每次读取都套一个 `READ_TIMEOUT` 的空闲超时（由
/// `set_read_timeout` 挂在 socket 上），服务器接上却半路哑掉时不会把阻塞线程挂住。
fn copy_sync(
    stream: &std::net::TcpStream,
    buffered: &[u8],
    target: &Path,
    want: u64,
) -> Result<(u64, Via), Error> {
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let mut file = std::fs::File::create(target)?;

    // `BufReader` 预读出来的正文开头先落盘。对端若发多了（超过 `Content-Length`），
    // 多出的字节已经在 `BufReader` 里被吞掉，这里截到 `want` 就不会误当正文写下去。
    let take = buffered.len().min(want as usize);
    if take > 0 {
        file.write_all(&buffered[..take])?;
    }

    let (copied, via) = copy_body(stream, &file, want - take as u64)?;
    Ok((take as u64 + copied, via))
}

/// 把剩下的正文搬进文件（接着当前文件偏移写），返回落盘字节数与搬运方式。
///
/// 目标支持 `splice(2)` 时走内核零拷贝，否则退回用户态读写——两边都读到 `want` 字节即止、
/// 都用同一个 socket 空闲超时，调用方看到的字节数与截断语义完全一致。
fn copy_body(
    stream: &std::net::TcpStream,
    file: &std::fs::File,
    want: u64,
) -> Result<(u64, Via), Error> {
    // 正文已经全在 `BufReader` 的预读里（小文件都是这一支）就没什么可搬的，别白建一根管道
    if want == 0 {
        return Ok((0, Via::Prebuffered));
    }
    // 非 Linux/Android 没有 `splice(2)`，整段不编译，直接落到下面的用户态读写
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Moved::Done(copied) = transfer(stream, file, want)? {
        return Ok((copied, Via::Splice));
    }
    Ok((copy_read_write(stream, file, want)?, Via::Copy))
}

/// 用户态搬运：一次 `read` 加一次 `write` 处理 [`COPY_BUF`] 字节，读满 `want` 即停，或用完
/// socket 上的数据即停（读不满由调用方按截断处理）。
///
/// `Read`/`Write` 是实现在 `&TcpStream`/`&File` 上的（`read`/`write` 要 `&mut self`），所以
/// 两个入参的绑定取成可变，引用本身仍是共享的。
fn copy_read_write(
    mut stream: &std::net::TcpStream,
    mut file: &std::fs::File,
    want: u64,
) -> Result<u64, Error> {
    let mut buf = vec![0_u8; COPY_BUF];
    let mut total = 0_u64;
    while total < want {
        let room = (want - total).min(buf.len() as u64) as usize;
        let read = match stream.read(&mut buf[..room]) {
            Ok(0) => break,
            Ok(n) => n,
            // `set_read_timeout` 超时后 `read` 报 `WouldBlock`（Linux）或
            // `TimedOut`（部分平台），两种都当作读超时。
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                return Err(Error::Timeout {
                    phase: "读取正文"
                });
            }
            Err(error) => return Err(Error::Io(error)),
        };
        file.write_all(&buf[..read])?;
        total += read as u64;
    }
    Ok(total)
}

/// 删掉没写完整的本地文件。删不掉也不覆盖真正的错误，只在 stderr 上留一句。
async fn discard(local: &Path) {
    if let Err(error) = tokio::fs::remove_file(local).await {
        eprintln!("  清理 {} 失败：{error}", local.display());
    }
}

/// 把远端路径做百分号编码：保留 `A-Za-z0-9-_.~/` 与分隔符 `/`，其余按 UTF-8 字节转义。
/// 用于 `/files/<sub>/<name>` 与 `/api/list/<sub>` 这两类路径。
///
/// 转义直接查表手写两个 hex 字符，不走 `fmt::Write`：文件名带中文或空格时每个字节
/// 都会走一次格式化分发，这条路径在每个文件下载时都会经过。
fn encode_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(path.len());
    for &byte in path.as_bytes() {
        match byte {
            b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(char::from(byte));
            }
            _ => {
                out.push('%');
                out.push(char::from(HEX[(byte >> 4) as usize]));
                out.push(char::from(HEX[(byte & 0x0F) as usize]));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::TcpStream;

    /// 把请求头读到空行即止（GET 无正文）；`BufReader` 把整段请求吃进缓冲，读完恰好干净。
    async fn read_request(reader: &mut BufReader<TcpStream>) {
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            if line.trim().is_empty() {
                break;
            }
        }
    }

    #[test]
    fn encode_path_keeps_unreserved_and_slash() {
        assert_eq!(encode_path("sub/a_b-1.txt"), "sub/a_b-1.txt");
    }

    #[test]
    fn encode_path_percent_encodes_space_and_unicode() {
        assert_eq!(encode_path("a b.txt"), "a%20b.txt");
        assert_eq!(encode_path("中"), "%E4%B8%AD");
    }

    /// 服务端接上却一句话不说时，读取超时要把客户端放出来，并且不留半截文件。
    #[tokio::test]
    async fn fetch_file_times_out_on_a_silent_server() {
        use std::time::Duration;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // 收下请求就不吭声：既不回响应，也不断开
        let server = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let _ = conn.read(&mut request).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let target =
            std::env::temp_dir().join(format!("lanfile-pull-timeout-{}", std::process::id()));
        let mut pool = Pool::default();
        let error = fetch_file(&mut pool, &addr.to_string(), "x.bin", &target)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }), "{error}");
        assert!(!target.exists(), "超时后不该留半截文件");
        server.abort();
    }

    /// 响应头说好了长度，正文只给半截就哑掉：读取阶段同样要按「读取正文超时」报出来，
    /// 半截文件不能留下。Linux 上这条路径由 `splice` 的 `EAGAIN` 走到。
    #[tokio::test]
    async fn fetch_file_times_out_on_a_stalled_body() {
        use std::time::Duration;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // 报 100 字节正文，只发 10 字节，然后既不补也不断开
        let server = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let _ = conn.read(&mut request).await;
            conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
                .await
                .unwrap();
            conn.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        let target =
            std::env::temp_dir().join(format!("lanfile-pull-stall-{}", std::process::id()));
        let mut pool = Pool::default();
        let error = fetch_file(&mut pool, &addr.to_string(), "x.bin", &target)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Timeout {
                    phase: "读取正文"
                }
            ),
            "{error}"
        );
        assert!(!target.exists(), "超时后不该留半截文件");
        server.abort();
    }

    /// 两个文件走同一条 keep-alive 连接：服务端只 accept 一次，第二条请求复用第一条归还的连接。
    /// 正文按 `Content-Length` 精确读满即停，读多的一个字节会把下一条响应的开头吃掉——这条
    /// 测试盯住「读满即止」与「归还复用」两件事同时成立。
    ///
    /// `BufReader` 在读响应头时通常已经预读了正文开头：这条测试的响应头与正文都很短，
    /// 恰好把 `into_inner` 丢缓冲这个坑踩在路径上（正文 3 字节，一定落在 `BufReader` 的预读里）。
    #[tokio::test]
    async fn fetch_file_reuses_one_connection_across_files() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(conn);
            read_request(&mut reader).await;
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\naaa")
                .await
                .unwrap();
            read_request(&mut reader).await;
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nbbb")
                .await
                .unwrap();
        });

        let dir = std::env::temp_dir().join(format!("lanfile-pull-reuse-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f1 = dir.join("a.bin");
        let f2 = dir.join("b.bin");
        let mut pool = Pool::default();
        let first = fetch_file(&mut pool, &addr.to_string(), "a.bin", &f1)
            .await
            .unwrap();
        let second = fetch_file(&mut pool, &addr.to_string(), "b.bin", &f2)
            .await
            .unwrap();
        // 3 字节正文必然落在 `BufReader` 的预读里：既没走 splice 也没走用户态搬运
        assert!(
            matches!(first.via, Via::Prebuffered) && matches!(second.via, Via::Prebuffered),
            "预读缓冲里的正文被算成了搬运"
        );
        assert_eq!(std::fs::read(&f1).unwrap(), b"aaa");
        assert_eq!(std::fs::read(&f2).unwrap(), b"bbb");
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 大正文（远超 `BufReader` 的 8 KiB 预读）整段走内核 `splice` 通道：内容要一字节不差，
    /// 而且读满 `Content-Length` 即止——连接仍然干净得能接着拉第二个大文件。
    #[tokio::test]
    async fn fetch_file_moves_a_large_body_over_one_connection() {
        const LEN: usize = 200_000;
        let first: Vec<u8> = (0..LEN as u32)
            .map(|index| (index % 251) as u8 + 1)
            .collect();
        let second: Vec<u8> = (0..LEN as u32)
            .map(|index| (index % 241) as u8 + 2)
            .collect();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (body1, body2) = (first.clone(), second.clone());
        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(conn);
            for body in [&body1, &body2] {
                read_request(&mut reader).await;
                let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                reader.get_mut().write_all(head.as_bytes()).await.unwrap();
                reader.get_mut().write_all(body).await.unwrap();
            }
        });

        let dir = std::env::temp_dir().join(format!("lanfile-pull-large-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f1 = dir.join("big1.bin");
        let f2 = dir.join("big2.bin");
        let mut pool = Pool::default();
        let copied = fetch_file(&mut pool, &addr.to_string(), "big1.bin", &f1)
            .await
            .unwrap();
        assert_eq!(
            copied.bytes, LEN as u64,
            "落盘字节数与 Content-Length 不一致"
        );
        // 200 KB 正文远超预读缓冲，必然落进 splice 或用户态读写其中一条
        assert!(
            !matches!(copied.via, Via::Prebuffered),
            "预读缓冲之外的正文被算成了无需搬运"
        );
        fetch_file(&mut pool, &addr.to_string(), "big2.bin", &f2)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&f1).unwrap(), first);
        assert_eq!(std::fs::read(&f2).unwrap(), second);
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
