//! 流式下载：一次 HTTP 事务把整棵目录树搬下来。
//!
//! 整个流程跑在一个阻塞线程里：同步 socket + 同步文件 IO，不走 `tokio::fs`。
//! `tokio::fs` 的每次 `File::create` / `write_all` / `close` 和 `create_dir_all` 都是一次
//! `spawn_blocking` 派发；12.4 万文件 + 9.6 万目录量级下这几十万次派发比正文本身还贵。
//!
//! 协议：服务端 `/api/stream/<sub>` 边遍历边发，每个条目前 1 字节类型（目录 0 / 文件 1 /
//! EOF 2），目录只有 NUL 结尾的路径，文件是 NUL 结尾的路径 + 8 字节小端长度 + 正文。
//! HTTP/1.1 chunked 编码由 [`ChunkedReader`] 就地解码。

use std::io::BufRead;
use std::{
    fs::File,
    io::{self, BufReader, Write as _},
    net::TcpStream,
    path::Path,
    time::Duration,
};

use crate::error::Error;
use crate::fetch::encode_path;
use crate::http::READ_TIMEOUT;

/// 一次系统调用搬这么多，与服务端 `ZIP_CHUNK` 同量级。
const READ_BUF: usize = 64 * 1024;

/// 建连超时（仅 `fetch_stream` 在 async 侧用一次）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// 一次流式拉取的统计。
#[derive(Default)]
pub struct StreamStats {
    pub files: u64,
    pub dirs: u64,
    pub bytes: u64,
}

/// 从 `host` 拉 `remote` 子树到本地 `target`（`target` 已经是落盘根目录，不含 basename 层）。
pub async fn fetch_stream(host: &str, remote: &str, target: &Path) -> Result<StreamStats, Error> {
    // 建连留在 async 侧：tokio 的 connect 带超时、走运行时解析器；连上之后整条流程交给
    // 一个阻塞线程，后面不再有任何 async 边界。
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect(host))
        .await
        .map_err(|_| Error::Timeout { phase: "连接" })?
        .map_err(|source| Error::Connect {
            host: host.to_string(),
            source,
        })?;
    let _ = stream.set_nodelay(true);
    let stream = stream.into_std()?;

    let host_owned = host.to_string();
    let remote_owned = remote.to_string();
    let target_owned = target.to_path_buf();
    tokio::task::spawn_blocking(move || {
        fetch_stream_blocking(stream, &host_owned, &remote_owned, &target_owned)
    })
    .await
    .map_err(|join| Error::Io(io::Error::other(join)))?
}

/// 阻塞线程内完成「发请求 → 读响应头 → 逐条解码 → 落盘」全流程。
fn fetch_stream_blocking(
    mut stream: TcpStream,
    host: &str,
    remote: &str,
    target: &Path,
) -> Result<StreamStats, Error> {
    // tokio 的 socket 是非阻塞的，切回阻塞模式才能用同步 IO
    stream.set_nonblocking(false)?;
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(READ_TIMEOUT));

    let path = format!("/api/stream/{}", encode_path(remote));
    stream.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
    )?;

    // BufReader 从头到尾包着 socket：先读状态行与响应头，再继续读 body。
    // BufReader 预读的字节不会被丢，body 从预读缓冲的正确位置接着读。
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    let status = read_response_head(&mut reader)?;
    if status != 200 {
        return Err(Error::Http { status, path });
    }

    std::fs::create_dir_all(target)?;
    let mut chunked = ChunkedReader::new(reader);
    let mut stats = StreamStats::default();
    let mut buf = vec![0_u8; READ_BUF];
    let mut path_buf = Vec::with_capacity(256);

    loop {
        // 1 字节类型；干净 EOF 视作收尾
        let mut kind = [0_u8; 1];
        match chunked.read_exact(&mut kind) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
        if kind[0] == 2 {
            break;
        }

        // 以 NUL 结尾的相对路径
        path_buf.clear();
        chunked.read_until_nul(&mut path_buf)?;
        if path_buf.is_empty() || path_buf.last() != Some(&0) {
            return Err(Error::Malformed("流式协议：路径未正常终止"));
        }
        path_buf.pop();

        // 防御性检查：远端不该发来绝对路径或 `..`
        if path_buf.starts_with(b"/") || path_buf.split(|b| *b == b'/').any(|p| p == b"..") {
            return Err(Error::Malformed("远端返回了非法的相对路径"));
        }
        let rel = std::str::from_utf8(&path_buf)
            .map_err(|_| Error::Malformed("远端返回的路径不是合法 UTF-8"))?;

        match kind[0] {
            0 => {
                std::fs::create_dir_all(target.join(rel))?;
                stats.dirs += 1;
            }
            1 => {
                let mut size_buf = [0_u8; 8];
                chunked.read_exact(&mut size_buf)?;
                let size = u64::from_le_bytes(size_buf);

                let file_path = target.join(rel);
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let mut file = File::create(&file_path)?;
                let mut remaining = size;
                while remaining > 0 {
                    let to_read = remaining.min(buf.len() as u64) as usize;
                    chunked.read_exact(&mut buf[..to_read])?;
                    file.write_all(&buf[..to_read])?;
                    remaining -= to_read as u64;
                }
                stats.files += 1;
                stats.bytes += size;
            }
            _ => return Err(Error::Malformed("远端返回了未知的条目类型")),
        }
    }
    Ok(stats)
}

/// 读状态行 + 跳响应头，返回状态码。响应头里的 `Content-Length` 用不到（服务端发的是
/// chunked，没有长度），所以整个头读完就丢。
fn read_response_head(reader: &mut BufReader<TcpStream>) -> Result<u16, Error> {
    let mut line = String::with_capacity(64);
    reader.read_line(&mut line)?;
    let status = parse_status(&line).ok_or(Error::Malformed("状态行格式异常"))?;
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
    }
    Ok(status)
}

/// 从 `HTTP/1.1 200 OK` 里取状态码。与 `http.rs` 的实现一致（`memchr` 定位版本号后的空格）。
fn parse_status(line: &str) -> Option<u16> {
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

/// 最小 HTTP/1.1 chunked 解码器：把 body 的 chunk 分帧还原成连续字节流。
///
/// 只需要 `read_exact` 与 `read_until_nul` 两个入口：协议本身是定长的类型字节 + NUL 结尾
/// 路径 + 定长长度 + 定长正文，没有"读到某个分隔符为止"的用法（除了路径 NUL）。
///
/// 同步版本：内部全部是 `std::io::Read` 的阻塞调用，不涉及 waker、不涉及状态机。
struct ChunkedReader<R> {
    inner: R,
    /// chunk 帧头的复用缓冲。
    ///
    /// `BufRead::read_line` 会向 `String` 追加，所以每次读之前 `clear()`。挂在结构体上而不是
    /// 在 `next_chunk` 里现开：一次 17 GB 的流会调 `next_chunk` 上万次，每次一个
    /// `String::with_capacity(32)` 在总开销里能看见。
    line: String,
    /// 当前 chunk 还剩多少字节没交给调用方
    remaining: usize,
    /// 已经读到终止 chunk（0 长度）
    done: bool,
}

impl<R: BufRead> ChunkedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            // 帧头最长也就几十字节，一次给够，之后 `clear` 只清长度不还容量
            line: String::with_capacity(32),
            remaining: 0,
            done: false,
        }
    }

    /// 读满 `buf`。流正常结束（终止 chunk）后如果还要读，报 `UnexpectedEof`。
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            if self.remaining == 0 && (self.done || !self.next_chunk()?) {
                self.done = true;
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "流式响应提前结束",
                ));
            }
            let to_read = (buf.len() - filled).min(self.remaining);
            self.inner.read_exact(&mut buf[filled..filled + to_read])?;
            filled += to_read;
            self.remaining -= to_read;
            if self.remaining == 0 {
                // 每个 chunk 的数据后跟一个 CRLF，读掉它才对得上下一段帧头
                let mut crlf = [0_u8; 2];
                self.inner.read_exact(&mut crlf)?;
            }
        }
        Ok(())
    }

    /// 读到 NUL（含）为止，追加进 `out`。流结束前没遇到 NUL 则报 `Malformed`。
    ///
    /// 逐字节读是因为 NUL 可能落在 chunk 边界上，用 `read_until` 会跨过帧头继续读；而路径
    /// 一般只有几十字节，相对每次调用后紧跟的定长头/正文读来说可忽略。
    fn read_until_nul(&mut self, out: &mut Vec<u8>) -> Result<(), Error> {
        loop {
            if self.remaining == 0 && (self.done || !self.next_chunk()?) {
                self.done = true;
                return Err(Error::Malformed("流式协议：路径未正常终止"));
            }
            let mut found = false;
            while self.remaining > 0 {
                let mut byte = [0_u8; 1];
                self.inner.read_exact(&mut byte)?;
                self.remaining -= 1;
                out.push(byte[0]);
                if byte[0] == 0 {
                    found = true;
                    break;
                }
            }
            if self.remaining == 0 {
                let mut crlf = [0_u8; 2];
                self.inner.read_exact(&mut crlf)?;
            }
            if found {
                return Ok(());
            }
        }
    }

    /// 尝试读下一个 chunk 的帧头；返回 `false` 表示收到终止 chunk（0 长度）。
    fn next_chunk(&mut self) -> io::Result<bool> {
        self.line.clear();
        self.inner.read_line(&mut self.line)?;
        let trimmed = self.line.trim_end_matches(['\r', '\n']);
        // 忽略 chunk extension：`<size>;ext=...`
        let hex = trimmed.split(';').next().unwrap_or("");
        let size = usize::from_str_radix(hex, 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "chunked 长度非法"))?;
        if size == 0 {
            // 读掉 trailer（可能没有 trailer，只有一个空行）；同一个 `line` 缓冲复用
            loop {
                self.line.clear();
                let read = self.inner.read_line(&mut self.line)?;
                if read == 0 || self.line == "\r\n" || self.line == "\n" {
                    break;
                }
            }
            return Ok(false);
        }
        self.remaining = size;
        Ok(true)
    }
}
