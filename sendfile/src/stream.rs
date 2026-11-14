//! The transport stream that turns placeholder body bytes into `sendfile(2)`.

use std::{
    fs::File,
    io::{self, IoSlice},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::body::{Plan, SendfileSlot};

/// Marks the end of an HTTP/1 response head.
const HEAD_END: &[u8; 4] = b"\r\n\r\n";

/// A transport that can move file bytes to the peer without a userspace copy.
///
/// Implemented for [`tokio::net::TcpStream`]; tests substitute an in-memory
/// target so the state machine can be exercised without a real socket.
pub trait SendfileTarget: AsyncWrite + Unpin {
    /// Transfers up to `count` bytes of `file` starting at `offset`.
    ///
    /// `Err(WouldBlock)` means the transport is momentarily full. It does **not**
    /// arrange a wakeup — the bytes move through a bare `sendfile(2)` syscall, so
    /// nothing in the `Poll` machinery is touched — which is why the caller has to
    /// call [`SendfileTarget::poll_writable`] before parking the task.
    fn try_sendfile(&self, file: &File, offset: u64, count: usize) -> io::Result<usize>;

    /// Registers the current task to be woken once the transport is writable.
    ///
    /// `Poll::Ready(Ok(()))` means writability is already available again, so the
    /// caller should retry its syscall instead of parking.
    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// Writes response-head bytes that the body follows immediately.
    ///
    /// The default writes them straight through. A transport that can hold them
    /// back until the body arrives overrides this so the head and the body leave
    /// as one segment instead of two; that halves the packet count of a small
    /// response, which is where a loopback benchmark spends most of its time.
    fn poll_write_more(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.get_mut()).poll_write(cx, buf)
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl SendfileTarget for tokio::net::TcpStream {
    fn try_sendfile(&self, file: &File, offset: u64, count: usize) -> io::Result<usize> {
        self.try_io(tokio::io::Interest::WRITABLE, || {
            let mut offset = offset;
            rustix::fs::sendfile(self, file, Some(&mut offset), count).map_err(io::Error::from)
        })
    }

    fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_write_ready(cx)
    }

    /// Writes the head with `MSG_MORE` so the `sendfile(2)` that follows lands in
    /// the same segment.
    ///
    /// Without it the head leaves as its own segment and the body as a second one:
    /// a 600B download costs four segments per request instead of two, which is
    /// most of the gap to a server that buffers head and body together.
    fn poll_write_more(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stream: &Self = self.get_mut();
        loop {
            let sent = stream.try_io(tokio::io::Interest::WRITABLE, || {
                rustix::net::send(stream, buf, rustix::net::SendFlags::MORE)
                    .map_err(io::Error::from)
            });
            match sent {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    match stream.poll_write_ready(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                sent => return Poll::Ready(sent),
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
impl SendfileTarget for tokio::net::TcpStream {
    fn try_sendfile(&self, _file: &File, _offset: u64, _count: usize) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    fn poll_writable(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::from(io::ErrorKind::Unsupported)))
    }
}

/// What the bytes handed to the stream are taken to mean.
enum State {
    /// Ordinary pass-through.
    Idle,
    /// Scanning for the end of the response head, which precedes the placeholders.
    Head { carry: [u8; 3], len: u8 },
    /// Every byte handed over now stands for file content.
    Sending,
}

/// A transport stream that serves an armed [`SendfileSlot`] with `sendfile(2)`.
///
/// Hyper writes a response head and then the response body. When a handler has
/// armed the slot, the body it produced is a placeholder: this stream passes the
/// head through untouched, then replaces every following byte with a real
/// `sendfile(2)` of the same length. Byte counts and ordering are preserved
/// exactly, so Hyper's framing, `Content-Length` accounting and keep-alive are
/// unaffected.
pub struct SendfileStream<S> {
    inner: S,
    slot: Arc<SendfileSlot>,
    state: State,
    /// Head bytes taken from Hyper but not yet accepted by the socket.
    owed: Vec<u8>,
    plan: Option<Plan>,
}

impl<S> SendfileStream<S> {
    /// Wraps `inner` and shares `slot` with the handler that will arm it.
    #[must_use]
    pub const fn new(inner: S, slot: Arc<SendfileSlot>) -> Self {
        Self {
            inner,
            slot,
            state: State::Idle,
            owed: Vec::new(),
            plan: None,
        }
    }
}

/// Moves up to `len` placeholder bytes to the socket as file content, stopping
/// early when the transport is momentarily full.
///
/// Returns how many bytes were transferred, which can be less than `len` (zero
/// included) on a full transport. No waker is registered here:
/// [`SendfileTarget::poll_writable`] is called by `write_placeholders` before it
/// parks, while `write_head` reports the short write so Hyper retries at once.
///
/// Errors if `len` exceeds the plan's remaining bytes, which would mean sending
/// file bytes this response never promised.
fn transfer(target: &impl SendfileTarget, plan: &mut Plan, len: usize) -> io::Result<usize> {
    // 越界判定收在这里：两个调用点本来各判一次，收口之后不会再漏判
    if len as u64 > plan.remaining {
        return Err(io::Error::other("sendfile body exceeds its content length"));
    }
    let mut done = 0;
    while done < len {
        match target.try_sendfile(&plan.file, plan.offset, len - done) {
            // The file shrank under us, so the promised `Content-Length` can no
            // longer be met: fail the connection rather than hang the client.
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file ended before its content length",
                ));
            }
            Ok(written) => {
                plan.offset += written as u64;
                plan.remaining -= written as u64;
                done += written;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => {
                // 文件系统或内核不支持 sendfile：当前连接按原方式结束，之后的请求
                // 由 should_upgrade 分流到用户态正文，不会反复卡进同一个系统调用。
                if matches!(
                    error.kind(),
                    io::ErrorKind::Unsupported | io::ErrorKind::InvalidInput
                ) {
                    crate::policy::disable();
                }
                return Err(error);
            }
        }
    }
    Ok(done)
}

fn write_zero() -> io::Error {
    io::Error::from(io::ErrorKind::WriteZero)
}

/// Index in `buf` of the last byte of the first head terminator ending there.
///
/// `carry` holds up to three trailing bytes from earlier calls, so a terminator
/// split across writes is still found. A terminator that starts in `carry` ends
/// within the first `HEAD_END.len() - 1` bytes of `buf`, so only that little window
/// has to be joined; the rest of the head is one vectorized scan over `buf`.
fn find_head_end(carry: &[u8], buf: &[u8]) -> Option<usize> {
    let carried = carry.len();
    if carried > 0 {
        let mut joined = [0_u8; 2 * HEAD_END.len() - 2];
        let take = buf.len().min(HEAD_END.len() - 1);
        joined[..carried].copy_from_slice(carry);
        joined[carried..carried + take].copy_from_slice(&buf[..take]);
        let window = &joined[..carried + take];
        // At most six bytes: memmem's search setup costs more than scanning these few
        // windows directly (measured ~2.6x slower), so this branch stays on std.
        if let Some(start) = window.windows(HEAD_END.len()).position(|w| w == HEAD_END) {
            return Some(start + HEAD_END.len() - 1 - carried);
        }
    }
    memchr::memmem::find(buf, HEAD_END).map(|start| start + HEAD_END.len() - 1)
}

/// The last up-to-three bytes of `carry` followed by `buf`.
fn trailing(carry: &[u8], buf: &[u8]) -> ([u8; 3], u8) {
    let total = carry.len() + buf.len();
    let take = total.min(3);
    let mut out = [0_u8; 3];
    for (index, slot) in out[..take].iter_mut().enumerate() {
        let at = total - take + index;
        *slot = if at < carry.len() {
            carry[at]
        } else {
            buf[at - carry.len()]
        };
    }
    (out, take as u8)
}

impl<S: SendfileTarget> SendfileStream<S> {
    /// Pushes head bytes already taken from Hyper to the socket.
    fn drain_owed(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.owed.is_empty() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.owed) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(write_zero())),
                Poll::Ready(Ok(written)) => {
                    self.owed.drain(..written);
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    /// Returns to pass-through once the plan's last byte has reached the socket.
    fn retire(&mut self) {
        if self.plan.as_ref().is_some_and(|plan| plan.remaining == 0) {
            self.plan = None;
            self.state = State::Idle;
        }
    }

    /// Writes the response head and, when it ends here, the placeholders after it.
    fn write_head(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let (carry, carry_len) = match self.state {
            State::Head { carry, len } => (carry, len as usize),
            State::Idle | State::Sending => {
                return Poll::Ready(Err(io::Error::other("sendfile stream state lost")));
            }
        };

        let (head_len, found) = match find_head_end(&carry[..carry_len], buf) {
            Some(index) => (index + 1, true),
            None => (buf.len(), false),
        };

        let mut written = 0;
        while written < head_len {
            match Pin::new(&mut self.inner).poll_write_more(cx, &buf[written..head_len]) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(write_zero())),
                Poll::Ready(Ok(count)) => written += count,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                // 唯一的提前退出：没写完就是被阻塞（循环跑完即 `written == head_len`）
                Poll::Pending => break,
            }
        }
        if written < head_len {
            self.owed.extend_from_slice(&buf[written..head_len]);
        }

        if found {
            // 拿到计划前先别碰 state：拿不到 plan 时这次调用以错误结束，状态机
            // 仍停留在 `Head`，下一次调用从 carry 续上，不会有中间态泄漏到字段里。
            let Some(plan) = self.slot.take_plan() else {
                return Poll::Ready(Err(io::Error::other("sendfile plan disappeared")));
            };
            self.plan = Some(plan);
            self.state = State::Sending;
        } else {
            let (next, len) = trailing(&carry[..carry_len], &buf[..head_len]);
            self.state = State::Head { carry: next, len };
        }

        // `!found` 时 `head_len` 必然是 `buf.len()`（见上面 `find_head_end` 的
        // 取值），所以 `head_len == buf.len()` 已覆盖“还没找到结尾”。这里只剩两种
        // 情况需要早退：head 没写完（被阻塞），或本次调用已经没有 body 字节可发。
        if written < head_len || head_len == buf.len() {
            return Poll::Ready(Ok(head_len));
        }

        let rest = buf.len() - head_len;
        let done = {
            // 只借 `plan` 一个字段，不再把整个 `self` 解构：`&self.inner` 与
            // `&mut self.plan` 是不同字段，借用检查器允许同时存在。
            let Some(plan) = self.plan.as_mut() else {
                return Poll::Ready(Err(io::Error::other("sendfile plan disappeared")));
            };
            match transfer(&self.inner, plan, rest) {
                Ok(done) => done,
                Err(error) => return Poll::Ready(Err(error)),
            }
        };
        self.retire();
        Poll::Ready(Ok(head_len + done))
    }

    /// Replaces `buf` with file content of the same length.
    fn write_placeholders(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let len = buf.len();
        loop {
            let done = {
                let Some(plan) = self.plan.as_mut() else {
                    return Poll::Ready(Err(io::Error::other("sendfile plan disappeared")));
                };
                match transfer(&self.inner, plan, len) {
                    Ok(done) => done,
                    Err(error) => return Poll::Ready(Err(error)),
                }
            };
            if done > 0 {
                self.retire();
                return Poll::Ready(Ok(done));
            }
            // `sendfile(2)` is a bare syscall, so a full socket leaves no waker
            // behind. Parking here without registering one would strand the
            // response until some unrelated event happened to wake the task.
            match self.inner.poll_writable(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: SendfileTarget> AsyncWrite for SendfileStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // `ready!` 负责把 `Pending` 原样返回（挂起时保留 waker），`if let` 只处理错误；
        // 展开后的机器码与原先的三分支 `match` 完全一致。
        if let Err(error) = ready!(this.drain_owed(cx)) {
            return Poll::Ready(Err(error));
        }

        if matches!(this.state, State::Idle) {
            if !this.slot.is_armed() {
                return Pin::new(&mut this.inner).poll_write(cx, buf);
            }
            this.state = State::Head {
                carry: [0; 3],
                len: 0,
            };
        }

        if matches!(this.state, State::Sending) {
            this.write_placeholders(cx, buf)
        } else {
            this.write_head(cx, buf)
        }
    }

    /// 顺着 slice 走同一个状态机，一次调用能把 Hyper 攒下的多个 iovec 全部推进。
    ///
    /// 这里不做真正的 `writev`：占位字节必须逐个交给 `sendfile(2)`，状态机本来就是按字节流
    /// 处理的，系统调用次数不会变。省下来的是 Hyper 的 flush 循环——`is_write_vectored()` 为
    /// 真时它走 `Queue` 策略，只写第一个 slice 会让同一个响应多转一圈（`chunks_vectored`、
    /// 零初始化的 iovec 数组、`advance` 各再来一遍），顺着写下去则一圈推到写不动为止。
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut written = 0;
        for buf in bufs.iter().filter(|buf| !buf.is_empty()) {
            let buf: &[u8] = buf;
            match Pin::new(&mut *this).poll_write(cx, buf) {
                Poll::Ready(Ok(count)) => {
                    written += count;
                    // 没吃满这个 slice：写不动了，后面的 slice 也一定写不动
                    if count < buf.len() {
                        break;
                    }
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {
                    // 一个字节都没写出去时必须原样返回 `Pending`（waker 已注册），
                    // 谎报 `Ok(0)` 会让 Hyper 判成 `WriteZero`
                    return if written == 0 {
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(written))
                    };
                }
            }
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = ready!(this.drain_owed(cx)) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = ready!(this.drain_owed(cx)) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for SendfileStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn head_end_is_found_across_the_carry_boundary() {
        // Terminator split 3/1 between the carry and the new bytes.
        assert_eq!(find_head_end(b"\r\n\r", b"\nbody"), Some(0));
        assert_eq!(find_head_end(b"ab\r", b"\n\r\nrest"), Some(2));
    }

    #[test]
    fn head_end_is_found_inside_one_buffer() {
        // The index of the terminator's final byte, so the head is one longer.
        assert_eq!(find_head_end(&[], b"HTTP/1.1 200 OK\r\n\r\nbody"), Some(18));
    }

    #[test]
    fn head_end_absent_reports_none() {
        assert_eq!(find_head_end(&[], b"HTTP/1.1 200 OK\r\n"), None);
        assert_eq!(find_head_end(b"\r\n", b""), None);
    }

    #[test]
    fn trailing_keeps_the_last_three_bytes() {
        assert_eq!(trailing(b"ab", b"cdef"), (*b"def", 3));
        assert_eq!(trailing(&[], b"a"), (*b"a\0\0", 1));
        assert_eq!(trailing(b"abc", b""), (*b"abc", 3));
    }
}

/// Drives the whole state machine against an in-memory transport, so the paths a
/// real socket rarely reaches — a head split across writes and a stalled head
/// write — are exercised too.
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod transport_tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        cell::{Cell, RefCell},
        fs::File,
        io::{self, IoSlice},
        os::unix::fs::FileExt,
        pin::Pin,
        rc::Rc,
        sync::Arc,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncWrite, AsyncWriteExt};

    use super::{SendfileStream, SendfileTarget};
    use crate::body::SendfileSlot;

    /// Everything the transport was asked to send, in order.
    #[derive(Clone, Default)]
    struct Record {
        out: Rc<RefCell<Vec<u8>>>,
        /// Bytes the stream handed over as a response head, which the transport is
        /// allowed to hold back until the body follows.
        head: Rc<RefCell<Vec<u8>>>,
        /// Stall the next write once, to force the stream to hold head bytes.
        stall_once: Rc<Cell<bool>>,
        sendfile_calls: Rc<Cell<usize>>,
        /// Make every `sendfile` report a full socket, as a slow peer would.
        block_sendfile: Rc<Cell<bool>>,
        /// How often the stream asked to be woken once the socket drains.
        writable_polls: Rc<Cell<usize>>,
    }

    struct FakeTarget(Record);

    impl AsyncWrite for FakeTarget {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.0.stall_once.replace(false) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.0.out.borrow_mut().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl SendfileTarget for FakeTarget {
        fn try_sendfile(&self, file: &File, offset: u64, count: usize) -> io::Result<usize> {
            self.0.sendfile_calls.set(self.0.sendfile_calls.get() + 1);
            if self.0.block_sendfile.get() {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            let mut buf = vec![0_u8; count.min(64 * 1024)];
            let read = file.read_at(&mut buf, offset)?;
            self.0.out.borrow_mut().extend_from_slice(&buf[..read]);
            Ok(read)
        }

        fn poll_writable(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.0.writable_polls.set(self.0.writable_polls.get() + 1);
            if self.0.block_sendfile.get() {
                // A socket that stays full: the caller must park here.
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_write_more(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.0.stall_once.replace(false) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.0.out.borrow_mut().extend_from_slice(buf);
            self.0.head.borrow_mut().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
    }

    const HEAD: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 150000\r\n\r\n";
    const OFFSET: u64 = 1000;
    const LEN: u64 = 150_000;

    fn armed_stream(name: &str) -> (Record, SendfileStream<FakeTarget>, Vec<u8>) {
        let dir = std::env::temp_dir().join(format!("lanfile-sendfile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let payload: Vec<u8> = (0..200_000_u32)
            .map(|index| (index % 251) as u8 + 1)
            .collect();
        std::fs::write(&path, &payload).unwrap();

        let slot = Arc::new(SendfileSlot::new());
        // `arm` 返回的 `SendfileBody` 是交给 Hyper 的占位体；这里不经过 Hyper，
        // 直接丢弃即可——计划已经存进 slot，`take_plan` 仍然拿得到
        slot.arm(Arc::new(File::open(&path).unwrap()), OFFSET, LEN)
            .unwrap();

        let record = Record::default();
        let stream = SendfileStream::new(FakeTarget(record.clone()), slot);
        (record, stream, payload)
    }

    /// Writes `HEAD` and the placeholder frames a body of `LEN` bytes produces.
    ///
    /// The fake transport shares its record through `Rc`, so the future is
    /// deliberately not `Send`; these tests never cross threads.
    #[allow(clippy::future_not_send)]
    async fn write_response(stream: &mut SendfileStream<FakeTarget>) {
        stream.write_all(&HEAD[..HEAD.len() - 2]).await.unwrap();
        stream.write_all(&HEAD[HEAD.len() - 2..]).await.unwrap();
        let placeholders = vec![0_u8; 50_000];
        for _ in 0..3 {
            stream.write_all(&placeholders).await.unwrap();
        }
        stream.flush().await.unwrap();
    }

    #[tokio::test]
    async fn placeholders_become_file_content_at_the_requested_offset() {
        let (record, mut stream, payload) = armed_stream("offset.bin");
        write_response(&mut stream).await;

        let out = record.out.borrow();
        assert_eq!(&out[..HEAD.len()], HEAD);
        assert_eq!(
            &out[HEAD.len()..],
            &payload[OFFSET as usize..(OFFSET + LEN) as usize]
        );
        assert!(record.sendfile_calls.get() > 0, "no sendfile was issued");
    }

    #[tokio::test]
    async fn the_head_is_handed_over_with_more_so_it_shares_a_segment_with_the_body() {
        let (record, mut stream, _) = armed_stream("coalesced.bin");
        write_response(&mut stream).await;

        assert_eq!(
            record.head.borrow().as_slice(),
            HEAD,
            "the response head was not written as a head that the body may join"
        );
    }

    #[tokio::test]
    async fn a_stalled_head_write_is_finished_before_the_file_content() {
        let (record, mut stream, payload) = armed_stream("stalled.bin");
        // The first head write returns `Pending`, so the stream has to hold the
        // head itself and push it out before any file byte.
        record.stall_once.set(true);
        write_response(&mut stream).await;

        let out = record.out.borrow();
        assert_eq!(
            &out[..HEAD.len()],
            HEAD,
            "head bytes were lost or reordered"
        );
        assert_eq!(
            &out[HEAD.len()..],
            &payload[OFFSET as usize..(OFFSET + LEN) as usize]
        );
    }

    #[tokio::test]
    async fn a_blocked_sendfile_registers_a_wakeup() {
        let (record, mut stream, _) = armed_stream("blocked.bin");
        // A peer that stops reading fills the socket, so every `sendfile` fails.
        record.block_sendfile.set(true);

        let stalled = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            write_response(&mut stream),
        )
        .await;
        assert!(
            stalled.is_err(),
            "a write must not finish while the socket stays full"
        );
        assert!(
            record.writable_polls.get() > 0,
            "a blocked sendfile parked the task without registering a wakeup"
        );
    }

    /// Hyper 的 `Queue` 策略会把响应头与紧随其后的占位字节攒进同一次
    /// `poll_write_vectored`：一次调用必须把能写下去的 slice 全部推进。
    #[tokio::test]
    async fn a_vectored_write_covers_every_slice_it_can() {
        let (record, mut stream, _) = armed_stream("vectored.bin");
        let placeholders = vec![0_u8; 4096];
        let bufs = [IoSlice::new(HEAD), IoSlice::new(&placeholders)];

        let written =
            std::future::poll_fn(|cx| Pin::new(&mut stream).poll_write_vectored(cx, &bufs))
                .await
                .unwrap();

        assert_eq!(
            written,
            HEAD.len() + placeholders.len(),
            "the vectored write stopped after the first slice"
        );
        let out = record.out.borrow();
        assert_eq!(&out[..HEAD.len()], HEAD);
        assert_eq!(out.len(), HEAD.len() + placeholders.len());
        assert!(record.sendfile_calls.get() > 0, "no sendfile was issued");
    }

    /// 一个字节都没写出去时必须原样返回 `Pending`（waker 已注册）：报 `Ok(0)` 会被 Hyper
    /// 判成 `WriteZero`，连接会被掐断。
    #[tokio::test]
    async fn a_stalled_vectored_write_reports_pending() {
        let (record, mut stream, _) = armed_stream("vectored-stall.bin");
        let mut cx = Context::from_waker(std::task::Waker::noop());

        // 第一次调用：响应头写不出去，被 `owed` 接管，所以这次调用仍然算“已消费”
        record.stall_once.set(true);
        let head_only = [IoSlice::new(HEAD)];
        let first = Pin::new(&mut stream).poll_write_vectored(&mut cx, &head_only);
        assert!(
            matches!(first, Poll::Ready(Ok(count)) if count == HEAD.len()),
            "a stalled head should be taken over, not reported as an error"
        );
        assert!(record.out.borrow().is_empty());

        // 第二次调用：补写 `owed` 时又写不动，一个字节都没出去，必须是 `Pending`
        record.stall_once.set(true);
        let placeholders = vec![0_u8; 4096];
        let next = [IoSlice::new(&placeholders)];
        let second = Pin::new(&mut stream).poll_write_vectored(&mut cx, &next);
        assert!(
            second.is_pending(),
            "a write that moved nothing must report `Pending`, not `Ok(0)`"
        );
        assert!(record.out.borrow().is_empty());
    }
}
