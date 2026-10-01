#[cfg(not(any(target_os = "linux", target_os = "android")))]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    cell::{Cell, RefCell},
    fmt::{self, Write as _},
    io::{self, IsTerminal},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use chrono::Local;
use lanfile::{
    AccessLine, AccessLog, LINE_STACK, build_router, render_line, render_line_stack, serve,
};
use tracing_subscriber::{
    EnvFilter,
    fmt::{format::Writer, time::FormatTime},
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

thread_local! {
    /// The last second formatted and the text for it.
    ///
    /// Every log line carries a timestamp and the access log writes one line per
    /// request, so `chrono`'s `strftime` ends up costing more than the rest of the
    /// line put together; what it returns only changes once a second. Reading the
    /// second off the coarse clock also avoids the real `clock_gettime` on every
    /// call; the timezone conversion inside `Local::now` still happens once per
    /// refresh, but a refresh is at most one per second instead of one per line.
    static STAMP: RefCell<(i64, String)> = const { RefCell::new((i64::MIN, String::new())) };

    /// 直写访问日志退回 `String` 拼行（快路径在栈上）时用的缓冲，按线程复用，省掉每请求一次分配。
    static LINE: RefCell<String> = const { RefCell::new(String::new()) };
}

/// `LINE` 这份复用缓冲的保留上限。
///
/// 超长路径（hyper 允许约 400 KiB 的请求行）会把缓冲撑大，而 `clear` 只清长度、不还容量：
/// 留着它就等于每个 worker 线程永久占住一块大内存，这种行宁愿丢掉缓冲重新分配。
const LINE_KEEP_MAX: usize = 8 * 1024;

/// The current second, used to tell whether the cached timestamp is stale.
///
/// `SystemTime::now()` is a real `clock_gettime` system call on a machine whose
/// clocksource is `hpet` (measured at 1223 ns here), while `CLOCK_REALTIME_COARSE`
/// reads the value the kernel already maintains for the current tick (3.3 ns).
/// The log timestamp only carries whole seconds, so the tick granularity of the
/// coarse clock is more than enough and its seconds match `SystemTime::now`'s.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn current_second() -> i64 {
    rustix::time::clock_gettime(rustix::time::ClockId::RealtimeCoarse).tv_sec
}

/// Platforms without `CLOCK_REALTIME_COARSE` fall back to a real clock read.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn current_second() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(i64::MIN, |since_epoch| {
            i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
        })
}

/// 当前秒的时间戳文本，每秒只格式化一次，同一秒内的请求直接复用缓存。
fn with_stamp<R>(use_stamp: impl FnOnce(&str) -> R) -> R {
    let second = current_second();
    STAMP.with(|stamp| {
        let mut stamp = stamp.borrow_mut();
        if stamp.0 != second {
            stamp.0 = second;
            stamp.1.clear();
            // 往 `String` 里写不会失败，真失败了也只是这一秒的时间戳空着。
            let _ = write!(stamp.1, "{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
        }
        use_stamp(&stamp.1)
    })
}

struct LoggerFormatter;

impl FormatTime for LoggerFormatter {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        with_stamp(|stamp| w.write_str(stamp))
    }
}

/// 单片日志缓冲攒够这么多字节就叫醒写线程，不必再等一个 [`LOG_INTERVAL`]。
const LOG_BATCH: usize = 64 * 1024;

/// 写线程的等待上限：即使没攒够一批，也要在这个时间内把已有的日志送出去。
const LOG_INTERVAL: Duration = Duration::from_millis(100);

/// 缓冲的上限。写线程跟不上时（stdout 是慢终端之类）超出的日志直接丢掉：丢日志总好过
/// 把内存吃光，也好过把 worker 卡在一个永远写不完的 stdout 上。它是所有分片加起来的上限。
const LOG_PENDING_MAX: usize = 4 * 1024 * 1024;

/// 日志缓冲的分片数：每个线程固定用其中一片。
///
/// 原设计里所有 worker 每请求都往同一个 `Mutex<Vec<u8>>` 的尾部追加一行，16 个 worker 抢
/// 同一把锁、写同一条缓存行。本机实测分片后每请求内核态少 0.4~0.7 µs（futex）、运行队列
/// 等待少 1.7 µs，用户态少 0.2~0.4 µs：每行只碰本线程那一片，锁不跨核，缓存行留在本核。
const SHARDS: usize = 32;

/// 每片的上限：所有分片加起来仍是 [`LOG_PENDING_MAX`]。
const LOG_PENDING_MAX_PER_SHARD: usize = LOG_PENDING_MAX / SHARDS;

thread_local! {
    /// 本线程固定使用的分片号，第一次用到时才分配。
    ///
    /// 用 `thread_local!` 而不是 `thread::current().id()`：前者是一次 TLS 读，后者要取线程
    /// 元数据再哈希一遍，比它要省掉的那次加锁还贵。
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// 下一个要分配的线程分片号。
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

/// 取本线程的分片号，第一次调用时从 [`NEXT_SHARD`] 领一个。
///
/// 线程数超过 [`SHARDS`] 时会有多个线程共用一片，那只是退回"这一片上仍有争用"，不影响正确性。
fn shard_index() -> usize {
    SHARD.with(|slot| {
        let current = slot.get();
        if current != usize::MAX {
            return current;
        }
        let index = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
        slot.set(index);
        index
    })
}

/// 访问日志的落地缓冲。
///
/// `tracing` 仍然负责格式化与 `RUST_LOG` 过滤，只有"把字节送到 stdout"这一步换成了攒批：
/// 日志先追加进来，攒够 [`LOG_BATCH`] 或等满 [`LOG_INTERVAL`] 才由写线程一次写出。
///
/// 换掉 `tracing_appender::non_blocking` 是因为它每行都往 channel 发一条消息，唤醒一个
/// 阻塞在 `recv` 的后台线程再让它重新 park——每行两次 futex。访问日志每请求一行，手机上
/// 实测这要花掉每请求约 10 µs CPU（其中约 8 µs 在内核态），9 字节小文件的吞吐因此只有
/// Go 的三分之二。攒批之后每请求只剩一次加锁与一次 `memcpy`。
struct LogSink {
    /// 已经格式化好、还没写出去的字节，按线程分片。
    pending: [Mutex<Vec<u8>>; SHARDS],
    /// 写线程睡着时用来叫醒它。
    ready: Condvar,
    /// 串行化"取出并写出"，并作为写线程等待 [`LOG_INTERVAL`] 时的锁。
    writing: Mutex<()>,
}

/// 进程唯一的日志出口。
///
/// 写线程、`tracing` 的 `MakeWriter` 和直写路径都借用同一个 `&'static` 引用，不必再为
/// 三处共享包一层 `Arc`：`Mutex`、`Condvar` 与 `Vec` 的构造函数都是 `const fn`，整个
/// 结构可以直接放进 `static`。
static SINK: LogSink = LogSink::new();

/// `tracing` 要的 `MakeWriter` 是一个能返回 `io::Write` 的 `Fn() -> W`。
///
/// 这里返回的 `&'static LogSink` 就是 [`LogSink`] 自己实现的那个 `io::Write`，因此
/// `tracing` 的每个事件直接写进同一个攒批缓冲，不必再包 `Arc` 或 `Mutex`。
fn sink_writer() -> &'static LogSink {
    &SINK
}

/// 取锁时忽略中毒。
///
/// 中毒只说明有线程在临界区里 panic 过，而临界区里只有一次 `Vec` 追加或一次取出，数据
/// 仍然完整；日志不该因为一次 panic 就永久停摆，所以取回内部数据继续用。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl LogSink {
    const fn new() -> Self {
        Self {
            pending: [const { Mutex::new(Vec::new()) }; SHARDS],
            ready: Condvar::new(),
            writing: Mutex::new(()),
        }
    }

    /// 往本线程的分片里追加一段日志，返回是否已经攒够一批（`true` 时调用方该叫醒写线程）。
    ///
    /// 分片已经到 [`LOG_PENDING_MAX_PER_SHARD`] 时这一行直接丢掉，返回 `false`。
    /// 未满时分片只要达到 [`LOG_BATCH`] 就返回 `true`，由调用方唤醒写线程。
    fn append(&self, shard: usize, buf: &[u8]) -> bool {
        let mut pending = lock(&self.pending[shard]);
        if pending.len() >= LOG_PENDING_MAX_PER_SHARD {
            return false;
        }
        pending.extend_from_slice(buf);
        pending.len() >= LOG_BATCH
    }

    /// 追加一段日志；攒够一批就叫醒写线程。
    fn push(&self, buf: &[u8]) {
        if self.append(shard_index(), buf) {
            self.ready.notify_one();
        }
    }

    /// 取出各分片里的字节并写出；返回这一轮是否真的写出去过东西。
    ///
    /// 返回值就是"还有没有积压"的判据：`false` 意味着此刻所有分片都空，调用方可以去
    /// [`Condvar::wait_timeout`] 上睡 [`LOG_INTERVAL`]。这样写线程每轮只需按分片走一遍，
    /// 不必先跑一趟专门"是否全空"的预检（那一趟同样要逐个锁分片，等于把加锁次数翻倍）。
    ///
    /// 取出与写出都在 `writing` 下完成：写出者只有一个，顺序即取出顺序。只按
    /// `writing` -> `pending` 的顺序取锁，`push` 只碰自己的分片，不会死锁。
    fn drain(&self, out: &mut impl io::Write, spare: &mut Vec<u8>) -> io::Result<bool> {
        let _writing = lock(&self.writing);
        let mut wrote = false;
        for shard in &self.pending {
            // 交换出的缓冲写完清空后继续给下一片用，锁随即释放：`write_all` 在锁外
            // 做，分片上的追加不会被这次可能很慢的写出堵住，也不必每批重新分配。
            let mut batch = std::mem::take(spare);
            std::mem::swap(&mut batch, &mut *lock(shard));
            if !batch.is_empty() {
                out.write_all(&batch)?;
                wrote = true;
            }
            batch.clear();
            *spare = batch;
        }
        Ok(wrote)
    }

    /// 写线程：攒够一批会被 [`Self::push`] 的调用方叫醒，否则最多等 [`LOG_INTERVAL`]。
    fn run(&self, out: &mut impl io::Write) {
        let mut spare = Vec::new();
        loop {
            // 先把已有的刷出去。刷到东西就接着刷（可能又有新的追加进来），刷空了才去等。
            // 这样"检查是否全空"与"等待"之间没有窗口：睡下去之前最后一次 flush 已经
            // 确认过所有分片都是空的。
            match self.drain(out, &mut spare) {
                Ok(true) => continue,
                Ok(false) => {}
                // stdout 已经写不动了（管道对端消失之类），再试也没有意义
                Err(_) => break,
            }
            let writing = lock(&self.writing);
            // 锁在 `wait_timeout` 返回时已经释放；结果本身不重要，丢掉即可。
            // 叫醒与这里检查"全空"之间不是原子的，所以最坏也就是多等一个
            // `LOG_INTERVAL`，日志不会丢。
            let _ = self.ready.wait_timeout(writing, LOG_INTERVAL);
        }
    }
}

impl io::Write for &LogSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // 真正的写出交给写线程：这里若写出去，每个事件都会退化成一次系统调用
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // `lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]`：拉取子命令，目录整棵
    // 镜像、单文件直落。直链看 URL 里的远端：`http://h/files/<sub>`、`http://h/pull/<sub>` 当
    // 文件，`http://h/api/zip/<sub>`、`http://h/api/list/<sub>`、`http://h/#<sub>` 当目录，其余
    // 非空路径（`http://h/<sub>`，如 `/.pi`）就是远端本身、文件还是目录交给服务端探测。
    // 在日志/服务端那套初始化之前就分流出去——它只是个 HTTP 客户端，用 eprintln 报进度即可。
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).is_some_and(|arg| arg == "get") {
        match lanfile_pull::run(&argv[2..]).await {
            Ok(()) => return,
            Err(error) => {
                eprintln!("lanfile get: {error}");
                std::process::exit(1);
            }
        }
    }

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let is_terminal = std::io::stdout().is_terminal();
    if let Err(error) = thread::Builder::new()
        .name("access-log".to_owned())
        .spawn(|| SINK.run(&mut std::io::stdout()))
    {
        // 没有写线程，缓冲就只会涨；这里必须直接失败，不能带着一个永不落地的日志跑
        eprintln!("无法启动日志写线程: {error}");
        std::process::exit(1);
    }

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_timer(LoggerFormatter)
        .with_ansi(is_terminal)
        .with_writer(sink_writer as fn() -> &'static LogSink)
        .init();

    let (port, dir) = parse_args(argv.into_iter().skip(1));
    let root = std::fs::canonicalize(&dir).unwrap_or_else(|error| {
        tracing::error!("无法访问目录 {:?}: {error}", dir);
        // 进程马上退出，这一行不能留在缓冲里
        let _ = io::Write::flush(&mut &SINK);
        std::process::exit(1);
    });

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("serving {} on http://{addr}", root.display());
    let access_log = Arc::new(access_log(is_terminal));
    let router = build_router(root.clone(), port, Arc::clone(&access_log));

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("无法监听 {addr}: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = serve(listener, root, access_log, router).await {
        eprintln!("监听 {addr} 出错: {error}");
        std::process::exit(1);
    }
    // 退出前把最后一批日志写出去
    let _ = io::Write::flush(&mut &SINK);
}

/// 访问日志的出口。
///
/// 必须在 `tracing_subscriber::fmt()` 装好之后调用，否则 [`AccessLog::enabled`] 问不到过滤器。
fn access_log(is_terminal: bool) -> AccessLog {
    if !AccessLog::enabled() {
        return AccessLog::Off;
    }
    if is_terminal {
        // 终端下要 `tracing` 上 ANSI 颜色，格式也归它管
        return AccessLog::Tracing;
    }
    // 非终端直写：绕开 `tracing` 的分发与 fmt 层，每请求省约 1.2 µs 用户态
    AccessLog::Direct(Box::new(|line: &AccessLine<'_>| {
        with_stamp(|stamp| {
            // 快路径：整行拼进栈缓冲，绕开 `fmt::Formatter` 的逐字段分发。
            let mut stack = [0_u8; LINE_STACK];
            if let Some(len) = render_line_stack(stamp, line, &mut stack) {
                SINK.push(&stack[..len]);
                return;
            }
            // 长路径或 IPv6：退回 `String`，多长都能拼。
            LINE.with(|buf| {
                let mut buf = buf.borrow_mut();
                buf.clear();
                buf.push_str(stamp);
                // 写 `String` 不会失败；这里仍按 `is_ok` 收口，是为了让容量回收
                // 在两条支路上都会跑到——不能因为一次（不可能发生的）格式化失败
                // 就把一块被撑大的缓冲永久留在本线程上。
                if render_line(line, &mut buf).is_ok() {
                    SINK.push(buf.as_bytes());
                }
                if buf.capacity() > LINE_KEEP_MAX {
                    *buf = String::new();
                }
            });
        });
    }))
}

fn parse_args<I>(args: I) -> (u16, PathBuf)
where
    I: IntoIterator<Item = String>,
{
    let mut port: u16 = 8000;
    let mut dir = PathBuf::from(".");
    for arg in args {
        if let Ok(parsed) = arg.parse() {
            port = parsed;
        } else {
            dir = PathBuf::from(arg);
        }
    }
    (port, dir)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        io::{self, Write as _},
        path::PathBuf,
        sync::Arc,
    };

    use super::{LOG_BATCH, LOG_PENDING_MAX_PER_SHARD, LogSink, parse_args};

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn defaults_without_args() {
        let (port, dir) = parse_args(args(&[]));
        assert_eq!(port, 8000_u16);
        assert_eq!(dir, PathBuf::from("."));
    }

    #[test]
    fn single_path_arg_is_served_directory() {
        let (port, dir) = parse_args(args(&["/srv/www"]));
        assert_eq!(port, 8000_u16);
        assert_eq!(dir, PathBuf::from("/srv/www"));
    }

    #[test]
    fn single_port_arg_keeps_default_directory() {
        let (port, dir) = parse_args(args(&["8080"]));
        assert_eq!(port, 8080_u16);
        assert_eq!(dir, PathBuf::from("."));
    }

    #[test]
    fn port_arg_sets_port_with_explicit_directory() {
        let (port, dir) = parse_args(args(&["8080", "/srv/www"]));
        assert_eq!(port, 8080_u16);
        assert_eq!(dir, PathBuf::from("/srv/www"));
    }

    #[test]
    fn arg_order_does_not_matter() {
        let (port, dir) = parse_args(args(&["/srv/www", "8080"]));
        assert_eq!(port, 8080_u16);
        assert_eq!(dir, PathBuf::from("/srv/www"));
    }

    #[test]
    fn last_path_arg_wins() {
        let (port, dir) = parse_args(args(&["/srv/www", "/data"]));
        assert_eq!(port, 8000_u16);
        assert_eq!(dir, PathBuf::from("/data"));
    }

    /// 记下收到的字节，然后让第一次写出就报错，使 [`LogSink::run`] 的循环退出。
    struct StopAfterFirstWrite(Vec<u8>);

    impl io::Write for StopAfterFirstWrite {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(buf);
            Err(io::Error::other("stop"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sink_keeps_lines_buffered_until_the_threshold() {
        let sink = LogSink::new();
        assert!(!sink.append(0, b"one\n"));
        assert!(!sink.append(0, b"two\n"));

        // 没攒够一批，所以一行都还没写出去；内容与顺序原样留着
        let mut out = Vec::new();
        let mut spare = Vec::new();
        let wrote = sink.drain(&mut out, &mut spare).unwrap();
        assert!(wrote, "有积压时 flush 必须报告写过东西");
        assert_eq!(out.as_slice(), b"one\ntwo\n");
    }

    #[test]
    fn flush_reports_when_every_shard_is_empty() {
        let sink = LogSink::new();
        let mut out = Vec::new();
        let mut spare = Vec::new();
        assert!(
            !sink.drain(&mut out, &mut spare).unwrap(),
            "全空时 flush 必须报告没东西可写"
        );
        assert!(out.is_empty());
    }

    #[test]
    fn sink_asks_for_a_wakeup_once_a_batch_is_full() {
        let sink = LogSink::new();
        let almost = vec![b'x'; LOG_BATCH - 1];
        assert!(!sink.append(0, &almost));
        assert!(sink.append(0, b"x"));
        // 阈值按片算：别的片攒得再多也不该替这一片凑数
        assert!(!sink.append(1, b"x"));
    }

    #[test]
    fn sink_writes_each_batch_only_once() {
        let sink = LogSink::new();
        let batch = vec![b'a'; LOG_BATCH];
        sink.append(0, &batch);

        let mut out = Vec::new();
        let mut spare = Vec::new();
        sink.drain(&mut out, &mut spare).unwrap();
        sink.drain(&mut out, &mut spare).unwrap();
        assert_eq!(out.len(), LOG_BATCH);
    }

    #[test]
    fn sink_drops_lines_once_the_buffer_is_full() {
        let sink = LogSink::new();
        let full = vec![b'x'; LOG_PENDING_MAX_PER_SHARD];
        sink.append(0, &full);
        assert!(!sink.append(0, b"dropped\n"));

        let mut out = Vec::new();
        let mut spare = Vec::new();
        sink.drain(&mut out, &mut spare).unwrap();
        assert_eq!(out, full);
    }

    #[test]
    fn sink_writer_thread_writes_a_full_batch() {
        let sink = Arc::new(LogSink::new());
        // 走 `io::Write` 才会叫醒写线程
        let batch = vec![b'a'; LOG_BATCH];
        let mut sink_writer: &LogSink = &sink;
        sink_writer.write_all(&batch).unwrap();

        let recorded = std::thread::spawn(move || {
            let mut out = StopAfterFirstWrite(Vec::new());
            sink.run(&mut out);
            out.0
        })
        .join()
        .unwrap();
        assert_eq!(recorded, batch);
    }
}
