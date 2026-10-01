//! 分片流下载：一条连接按 manifest 索引接收一组文件，正文走 `splice(2)` 零拷贝落盘。
//!
//! 服务端 `/stream-batch/<sub>` 只发文件记录：每条为类型 1、NUL 结尾的路径、8 字节小端
//! 长度、正文。响应没有
//! `Content-Length`，靠 `Connection: close` 后的 EOF 结束。
//!
//! 整条流程在一个阻塞线程里同步执行：同步 socket + 同步文件 IO，不走 `tokio::fs`。
//! `tokio::fs` 的每次 `File::create` / `write_all` / `close` 和 `create_dir_all` 都是一次
//! `spawn_blocking` 派发；12.4 万文件 + 9.6 万目录量级下这几十万次派发比正文本身还贵。
//! 也不经过 chunked 解码——正文直接从 socket `splice` 进文件。
//!
//! 头部分（类型字节、NUL 结尾路径、8 字节长度）通过一个用户态缓冲批量读取：逐字节
//! `read_exact` 对每个文件会产生十几次系统调用，循环十万文件就是几百万次，在回环或
//! 局域网上远超正文本身的开销；缓冲后头部分只在缓冲耗尽时读一次 socket。文件正文不再
//! 从缓冲里搬——缓冲区里已有的那点正文先落盘，剩余的直接 `splice` 进文件。

use std::{
    collections::HashMap,
    fs::File,
    io::{self, IoSlice, Read as _, Write as _},
    net::TcpStream,
    path::Path,
};

use crate::error::Error;
use crate::fetch::{Via, encode_path};
use crate::http::{CONNECT_TIMEOUT, READ_TIMEOUT, status_code};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::splice::{Moved, transfer};

/// 流式响应头部分的读取缓冲大小。
///
/// 头部分（类型 + 路径 + 长度）每条不到一百字节，一次 `read` 能覆盖几十条；正文超过
/// 这个缓冲时剩余的走 `splice`，缓冲区里的那部分照常落盘。
const STREAM_READ_BUF: usize = 64 * 1024;

/// 一次流式拉取的统计。
#[derive(Default)]
pub struct StreamStats {
    pub files: u64,
    pub bytes: u64,
    pub spliced: u64,
    pub copied: u64,
    pub prebuffered: u64,
}

impl StreamStats {
    pub const fn merge(&mut self, other: &Self) {
        self.files += other.files;
        self.bytes += other.bytes;
        self.spliced += other.spliced;
        self.copied += other.copied;
        self.prebuffered += other.prebuffered;
    }
}

pub async fn fetch_stream_shard(
    host: &str,
    remote: &str,
    target: &Path,
    entries: Vec<(String, u64)>,
) -> Result<StreamStats, Error> {
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
    let request_body_len = entries.iter().map(|(path, _)| path.len() + 1).sum();
    let mut request_body = Vec::with_capacity(request_body_len);
    for (path, _) in &entries {
        request_body.extend_from_slice(path.as_bytes());
        request_body.push(0);
    }
    let expected = entries.into_iter().collect::<HashMap<_, _>>();
    let request_path = format!("/stream-batch/{}", encode_path(remote));
    tokio::task::spawn_blocking(move || {
        fetch_stream_blocking(
            stream,
            &host_owned,
            &request_path,
            &request_body,
            &remote_owned,
            &target_owned,
            expected,
        )
    })
    .await
    .map_err(|join| Error::Io(io::Error::other(join)))?
}

/// 阻塞线程内完成「发请求 → 读响应头 → 逐条解码 → 落盘」全流程。
fn fetch_stream_blocking(
    mut stream: TcpStream,
    host: &str,
    request_path: &str,
    request_body: &[u8],
    remote: &str,
    target: &Path,
    mut expected: HashMap<String, u64>,
) -> Result<StreamStats, Error> {
    // tokio 的 socket 是非阻塞的，切回阻塞模式才能用同步 IO
    stream.set_nonblocking(false)?;
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(READ_TIMEOUT));

    let request = format!(
        "GET {request_path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        request_body.len()
    );
    write_all_vectored(&mut stream, request.as_bytes(), request_body)?;

    let mut reader = BufferedSocket::new(&stream);
    let status = reader.read_response_head()?;
    if status != 200 {
        return Err(Error::Http {
            status,
            path: request_path.to_string(),
        });
    }

    std::fs::create_dir_all(target)?;
    let mut stats = StreamStats::default();
    let mut path_buf = Vec::with_capacity(256);
    let mut copy_buf = Vec::new();

    loop {
        // 1 字节类型；干净 EOF 视作收尾
        let mut kind = [0_u8; 1];
        match reader.read_exact(&mut kind) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // 读 NUL 结尾的相对路径
        path_buf.clear();
        reader.read_until_nul(&mut path_buf)?;
        if path_buf.len() > 8192 {
            return Err(Error::Malformed("远端返回的路径过长"));
        }
        // 防御性检查：远端不该发来绝对路径或 `..`
        if path_buf.starts_with(b"/") || path_buf.split(|&b| b == b'/').any(|p| p == b"..") {
            return Err(Error::Malformed("远端返回了非法的相对路径"));
        }
        let rel = std::str::from_utf8(&path_buf)
            .map_err(|_| Error::Malformed("远端返回的路径不是合法 UTF-8"))?;

        match kind[0] {
            0 => {
                return Err(Error::Malformed("分片流包含目录记录"));
            }
            1 => {
                let mut size_buf = [0_u8; 8];
                reader.read_exact(&mut size_buf)?;
                let size = u64::from_le_bytes(size_buf);
                check_expected(&mut expected, rel, size)?;
                let file_path = target.join(rel);
                let file = File::create(&file_path)?;
                match stream_file_content(
                    &stream,
                    &file,
                    size,
                    remote,
                    rel,
                    &mut reader,
                    &mut copy_buf,
                ) {
                    Ok(via) => {
                        match via {
                            Via::Splice => stats.spliced += 1,
                            Via::Copy => stats.copied += 1,
                            Via::Prebuffered => stats.prebuffered += 1,
                        }
                        stats.files += 1;
                        stats.bytes += size;
                    }
                    Err(error) => {
                        drop(file);
                        let _ = std::fs::remove_file(&file_path);
                        return Err(error);
                    }
                }
            }
            _ => return Err(Error::Malformed("远端返回了未知的条目类型")),
        }
    }
    check_missing(&expected)?;
    Ok(stats)
}

/// Writes both request parts without copying them into one contiguous buffer.
fn write_all_vectored(stream: &mut TcpStream, head: &[u8], body: &[u8]) -> io::Result<()> {
    let mut head_start = 0;
    let mut body_start = 0;
    while head_start < head.len() {
        let written = stream.write_vectored(&[
            IoSlice::new(&head[head_start..]),
            IoSlice::new(&body[body_start..]),
        ])?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let head_written = written.min(head.len() - head_start);
        head_start += head_written;
        body_start += written - head_written;
    }
    if body_start < body.len() {
        stream.write_all(&body[body_start..])?;
    }
    Ok(())
}

fn check_expected(expected: &mut HashMap<String, u64>, rel: &str, size: u64) -> Result<(), Error> {
    let want = expected.remove(rel).ok_or_else(|| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("分片流包含清单外的路径：{rel}"),
        ))
    })?;
    if size != want {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("分片流文件大小与清单不一致：{rel} 应得 {want} 字节，收到 {size} 字节"),
        )));
    }
    Ok(())
}

fn check_missing(expected: &HashMap<String, u64>) -> Result<(), Error> {
    if expected.is_empty() {
        return Ok(());
    }
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "分片流缺少清单中的 {} 个文件，首个为 {}",
            expected.len(),
            expected.keys().next().map_or("", String::as_str)
        ),
    )))
}

/// 把下一段 `size` 字节从 socket 搬进文件。
///
/// 缓冲区里已有的正文先落盘（通常是上一次 `fill` 顺手读进来的），剩余的直接
/// `splice(2)` 进文件，不再经过用户态。
fn stream_file_content(
    mut socket: &TcpStream,
    mut file: &File,
    size: u64,
    remote: &str,
    rel: &str,
    reader: &mut BufferedSocket<'_>,
    copy_buf: &mut Vec<u8>,
) -> Result<Via, Error> {
    if size == 0 {
        return Ok(Via::Prebuffered);
    }

    // 先把缓冲区里已有的那截正文落盘。缓冲区里可能只装了正文的一部分（大文件），
    // 也可能装了全部（小文件）——`take` 取两者的最小值。
    let from_buf = reader.available().min(size as usize);
    if from_buf > 0 {
        reader.consume_to(from_buf, file)?;
    }
    let remaining = size - from_buf as u64;
    if remaining == 0 {
        return Ok(Via::Prebuffered);
    }

    // 剩下的正文不在缓冲区里，直接从 socket 搬。
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Moved::Done(copied) = transfer(socket, file, remaining)? {
        if copied == remaining {
            return Ok(Via::Splice);
        }
        return Err(Error::Truncated {
            remote: format!("{remote}/{rel}"),
            want: size,
            got: size - remaining + copied,
        });
    }

    // 非 Linux/Android，或目标文件系统不支持 splice_write：用户态读写兜底
    let mut remaining = remaining;
    if copy_buf.len() != STREAM_READ_BUF {
        copy_buf.resize(STREAM_READ_BUF, 0);
    }
    let buf = &mut copy_buf[..];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = socket.read(&mut buf[..want])?;
        if n == 0 {
            return Err(Error::Truncated {
                remote: format!("{remote}/{rel}"),
                want: size,
                got: size - remaining,
            });
        }
        file.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    Ok(Via::Copy)
}

/// 带用户态缓冲的 socket 读取器。
///
/// 头部分（类型字节、NUL 结尾路径、8 字节长度）逐字节读取会产生大量 `read` 系统调用：
/// 路径平均几十字节，一千万个文件就是几亿次。这里一次 `read` 预读一整块到用户态，头部分
/// 全在内存里解析；缓冲区耗尽才再读一次 socket。
///
/// 正文不从这里走：`stream_file_content` 会先把缓冲区里已有的正文落盘，剩余的直接
/// `splice(2)`——缓冲区只服务头部分。
struct BufferedSocket<'a> {
    socket: &'a TcpStream,
    buf: Vec<u8>,
    start: usize,
    end: usize,
}

impl<'a> BufferedSocket<'a> {
    fn new(socket: &'a TcpStream) -> Self {
        Self {
            socket,
            buf: vec![0_u8; STREAM_READ_BUF],
            start: 0,
            end: 0,
        }
    }

    /// Reads a response head into the existing buffer, leaving any pre-read
    /// body bytes available for the record decoder.
    fn read_response_head(&mut self) -> Result<u16, Error> {
        let mut search_from = self.start;
        let head_end = loop {
            let found = self.buf[search_from..self.end]
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|at| search_from + at + 4);
            if let Some(head_end) = found {
                break head_end;
            }
            if self.end == self.buf.len() {
                self.buf.resize(self.buf.len() * 2, 0);
            }
            let read = self
                .socket
                .read(&mut self.buf[self.end..])
                .map_err(Error::from)?;
            if read == 0 {
                return Err(Error::Malformed("响应头提前结束"));
            }
            search_from = self.end.saturating_sub(3).max(search_from);
            self.end += read;
        };
        let head = &self.buf[self.start..head_end];
        let Some(line_end) = head.windows(2).position(|window| window == b"\r\n") else {
            return Err(Error::Malformed("状态行未终止"));
        };
        let line = std::str::from_utf8(&head[..line_end])
            .map_err(|_| Error::Malformed("状态行不是 UTF-8"))?;
        let status = status_code(line).ok_or(Error::Malformed("状态行格式异常"))?;
        self.start = head_end;
        Ok(status)
    }

    /// 从 socket 读一块进缓冲区；返回是否读到数据（`false` 表示 EOF）。
    fn fill(&mut self) -> io::Result<bool> {
        self.start = 0;
        self.end = self.socket.read(&mut self.buf)?;
        Ok(self.end > 0)
    }

    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut written = 0;
        while written < out.len() {
            if self.start == self.end && !self.fill()? {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let n = (self.end - self.start).min(out.len() - written);
            out[written..written + n].copy_from_slice(&self.buf[self.start..self.start + n]);
            self.start += n;
            written += n;
        }
        Ok(())
    }

    /// 读到 NUL（不含）为止，把 NUL 之前的字节追加进 `out`。
    fn read_until_nul(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        loop {
            if self.start == self.end && !self.fill()? {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let search = &self.buf[self.start..self.end];
            // `memchr` 一次扫完整块缓冲：路径平均几十字节，逐字节比较在内层循环里
            if let Some(pos) = memchr::memchr(0, search) {
                out.extend_from_slice(&search[..pos]);
                self.start += pos + 1;
                return Ok(());
            }
            out.extend_from_slice(search);
            self.start = self.end;
        }
    }

    /// 缓冲区里还有多少字节没被消费。
    const fn available(&self) -> usize {
        self.end - self.start
    }

    /// 把缓冲区里最多 `n` 字节写进 `file`，返回实际写入的字节数。
    fn consume_to(&mut self, n: usize, file: &File) -> io::Result<usize> {
        let take = self.available().min(n);
        // `&File` 实现了 `Write`（`File` 的 `impl Write for &File`），这里借一个
        // 可变的 `&File` 就能直接写，不需要 `try_clone` 出一份新 fd
        let mut writer = file;
        writer.write_all(&self.buf[self.start..self.start + take])?;
        self.start += take;
        Ok(take)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn stream_file_content_rejects_partial_body() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).unwrap();
        let (mut peer, _) = listener.accept().unwrap();

        let payload = b"abc";
        peer.write_all(payload).unwrap();
        drop(peer);

        let root =
            std::env::temp_dir().join(format!("lanfile-stream-truncated-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let file_path = root.join("partial");
        let file = File::create(&file_path).unwrap();
        let mut reader = BufferedSocket::new(&stream);

        let mut copy_buf = Vec::new();
        let error = stream_file_content(
            &stream,
            &file,
            4,
            "remote",
            "partial",
            &mut reader,
            &mut copy_buf,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::Truncated {
                want: 4,
                got: 3,
                ..
            }
        ));

        drop(file);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
