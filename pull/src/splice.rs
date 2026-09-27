//! socket → 文件的内核零拷贝搬运：`splice(2)` 把正文经一根管道直接送进文件，不进用户态。
//!
//! `splice` 要求两端至少一端是管道，所以一趟搬运是两次系统调用：socket → 管道、管道 → 文件。
//! 写文件那一头走不走得通取决于文件系统有没有实现 `splice_write`（ext4、xfs、btrfs、tmpfs 都有，
//! procfs 之类没有），因此先拿空管道探一次能力，探不通就把正文原样留给调用方的用户态读写——
//! 这条路径只是加速，不支持不影响正确性。
//!
//! 管道按 blocking 线程复用：`pipe_with` 一次要分配两个 fd 与一份内核 pipe 结构，一根管道在
//! 同一个线程上可以跨多次 `transfer` 复用，一趟拉取下来只建一次。

use crate::error::Error;
use rustix::fd::OwnedFd;
use rustix::io::Errno;
use rustix::pipe::{PipeFlags, SpliceFlags, pipe_with, splice};
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::net::TcpStream;

/// 一轮搬多少：管道容量（默认 64 KiB）就是单次 `splice` 的上限，取同一个数即可；与用户态
/// 路径的块大小一致，"读满 `want` 即止"的语义两边相同。
const CHUNK: usize = 64 * 1024;

/// 搬运结果。
pub enum Moved {
    /// 正文已搬进文件（或用完对端数据提前停下），给的是本次落盘字节数。
    Done(u64),
    /// 这套文件系统没有 `splice_write`，正文一个字节都没动过。
    Unsupported,
}

thread_local! {
    /// 本 blocking 线程复用的管道：socket 与文件之间的中转缓冲。
    ///
    /// 一次 `lanfile get` 里所有文件都由 tokio 的 blocking 线程池处理，池里线程数远少于
    /// 文件数，因此同一线程上的复用命中率很高。管道里可能因为上次传输中途失败而残留数据，
    /// 出错时不归还，直接丢弃重建。
    static PIPE: RefCell<Option<(OwnedFd, OwnedFd)>> = const { RefCell::new(None) };
}

/// 把正文从 `stream` 搬进 `file`（接着当前文件偏移写），最多 `want` 字节。
///
/// 返回 [`Moved::Unsupported`] 时 socket 上一个字节都没被读走，调用方可以原样改用用户态读写。
pub fn transfer(stream: &TcpStream, file: &File, want: u64) -> Result<Moved, Error> {
    PIPE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let (pipe_read, pipe_write) = match slot.take() {
            Some(pipe) => pipe,
            None => pipe_with(PipeFlags::CLOEXEC).map_err(io::Error::from)?,
        };
        match transfer_inner(stream, &pipe_read, &pipe_write, file, want) {
            Ok(moved) => {
                // `Done` 已把所有字节从管道里 drain 走，`Unsupported` 一个字节都没写，
                // 两种情况管道里都是空的，可以安全复用。
                *slot = Some((pipe_read, pipe_write));
                Ok(moved)
            }
            // 出错时管道里可能残留字节：与其猜测状态，不如丢弃重建
            Err(error) => Err(error),
        }
    })
}

/// [`transfer`] 去掉管道获取与归还的部分。
fn transfer_inner(
    stream: &TcpStream,
    pipe_read: &OwnedFd,
    pipe_write: &OwnedFd,
    file: &File,
    want: u64,
) -> Result<Moved, Error> {
    if !supported(pipe_read, file) {
        return Ok(Moved::Unsupported);
    }

    let mut total = 0_u64;
    while total < want {
        let room = (want - total).min(CHUNK as u64) as usize;
        let read = match splice(stream, None, pipe_write, None, room, SpliceFlags::MOVE) {
            // 对端提前收尾：剩下的交给调用方按截断处理
            Ok(0) => break,
            Ok(n) => n,
            // socket 上挂了 `SO_RCVTIMEO`（`copy_sync` 的 `set_read_timeout`），
            // 对端哑掉时 `splice` 和 `read` 一样报 `EAGAIN`
            Err(Errno::AGAIN) => {
                return Err(Error::Timeout {
                    phase: "读取正文"
                });
            }
            Err(error) => return Err(Error::Io(error.into())),
        };
        total += read as u64;
        drain(pipe_read, file, read)?;
    }
    Ok(Moved::Done(total))
}

/// 探测 `file` 支不支持 `splice_write`：空管道上做一次非阻塞 `splice`，支持则只会因为管道空
/// 拿到 `EAGAIN`；不支持时内核在 `warn_unsupported` 里回 `EINVAL`。
///
/// 除 `EAGAIN` 之外一律当作不支持：这里只是挑快路，拿不准就退用户态读写，不值得让一次探测
/// 把整次拉取带走。
fn supported(pipe_read: &OwnedFd, file: &File) -> bool {
    matches!(
        splice(pipe_read, None, file, None, 1, SpliceFlags::NONBLOCK),
        Err(Errno::AGAIN)
    )
}

/// 把管道里的 `want` 字节全部写进文件。管道里已经躺着这么多字节，`splice` 不会再等数据。
fn drain(pipe_read: &OwnedFd, file: &File, want: usize) -> Result<(), Error> {
    let mut done = 0_usize;
    while done < want {
        match splice(pipe_read, None, file, None, want - done, SpliceFlags::MOVE) {
            // 管道里明明有数据却一个字节都没搬走：与其空转，不如当场报错
            Ok(0) => {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "管道到文件的 splice 没搬走任何字节",
                )));
            }
            Ok(n) => done += n,
            Err(error) => return Err(Error::Io(error.into())),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};

    /// 建一条本地 TCP 连接，把 `payload` 从对端写进去，返回读端。
    ///
    /// 对端放在线程里写：正文大于发送缓冲时 `write_all` 会等到读端开始收才返回，同一条线程
    /// 里先写完再读会自己把自己堵死。
    fn connected(payload: &[u8]) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stream = TcpStream::connect(addr).unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let payload = payload.to_vec();
        let _writer = std::thread::spawn(move || {
            let _ = peer.write_all(&payload);
        });
        stream
    }

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lanfile-pull-splice-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn cleanup(path: &Path) {
        if let Some(dir) = path.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// 支持的 fs 上，splice 通道要把正文一个字节不差地搬进文件。
    ///
    /// 正文 200 KB 大于单次上限（管道容量 64 KiB），一定走多轮；`transfer` 拿到的字节数、
    /// 落盘内容都要对得上。
    #[test]
    fn transfer_moves_the_body_into_the_file() {
        let payload: Vec<u8> = (0..200_000_u32)
            .map(|index| (index % 251) as u8 + 1)
            .collect();
        let stream = connected(&payload);
        let path = temp_path("body.bin");
        let file = File::create(&path).unwrap();
        let want = payload.len() as u64;

        match transfer(&stream, &file, want).unwrap() {
            Moved::Done(copied) => {
                assert_eq!(copied, want, "搬运字节数与正文长度不一致");
                drop(file);
                assert_eq!(std::fs::read(&path).unwrap(), payload);
            }
            // 这套文件系统不支持 `splice_write`：没有 splice 可测，用户态路径另测
            Moved::Unsupported => eprintln!("{} 不支持 splice_write，跳过", path.display()),
        }
        cleanup(&path);
    }

    /// 目标探不通时（这里是只读打开的普通文件），必须原样报"不支持"，且正文一个字节都不能
    /// 从 socket 里被读走——用户态路径要靠它接着读。
    #[test]
    fn an_unsupported_target_leaves_the_body_untouched() {
        let payload = b"0123456789";
        let stream = connected(payload);
        let path = temp_path("readonly.bin");
        std::fs::write(&path, b"").unwrap();
        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();

        assert!(
            matches!(
                transfer(&stream, &file, payload.len() as u64).unwrap(),
                Moved::Unsupported
            ),
            "只读目标不该被当成支持 splice_write"
        );
        let mut rest = Vec::new();
        stream
            .take(payload.len() as u64)
            .read_to_end(&mut rest)
            .unwrap();
        assert_eq!(rest, payload, "探测把正文吃掉了");
        cleanup(&path);
    }
}
