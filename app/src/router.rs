use std::{
    fmt::{self, Write as _},
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
};

use lanfile_assets::static_routes;
use lanfile_list::list_routes;
use salvo::{
    http::{Version, header::CONTENT_LENGTH},
    prelude::*,
};

#[path = "fast.rs"]
mod fast;

pub use fast::serve;

/// 访问日志的 target。`tracing` 默认取模块路径，这个文件就是 lib 的根，所以是 `lanfile`。
pub const ACCESS_LOG_TARGET: &str = module_path!();

/// 访问日志的内容，一条 `Display` 就能渲染成整行。
///
/// 原来是六个字段（`?ip, %method, ...`）：`tracing` 为每个字段都要走一遍 visitor
/// （`record_debug`/`record_str`/`record_u64` 各一次），再逐字段拼分隔符。手机上实测这条
/// 日志的用户态开销约 1.9 µs，而 Go 那边一次 `fmt.Sprintf` 只要 0.4 µs。合成一条消息后
/// 只剩一次 `write!`，而字段值的格式化仍然只发生在事件真的启用时——宏把消息放在 enabled
/// 判断里面，所以 `RUST_LOG=off` 时一个字节都不会拼。
pub struct AccessLine<'a> {
    ip: Option<IpAddr>,
    method: &'a str,
    path: &'a str,
    version: Version,
    status: u16,
    size: &'a str,
}

impl fmt::Display for AccessLine<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "access ip={:?} method={} path={} version={:?} status={} size={}",
            self.ip, self.method, self.path, self.version, self.status, self.size
        )
    }
}

/// 访问日志的出口。
pub enum AccessLog {
    /// `RUST_LOG` 把这条日志关掉了，一行都不写。
    Off,
    /// 交给 `tracing`：stdout 是终端时只有它会写 ANSI 颜色，格式也归它管。
    Tracing,
    /// 直接把整行写进日志缓冲，绕开 `tracing` 的分发与 fmt 层。
    ///
    /// 桌面 release 上实测：一个只有消息、没有字段的 `tracing` 事件每请求要 1.57 µs 用户态，
    /// 全是分发与 fmt 层的固定开销；直写只剩拼行本身。
    Direct(Box<dyn Fn(&AccessLine<'_>) + Send + Sync>),
}

impl AccessLog {
    /// 现在要不要记访问日志。
    ///
    /// `tracing` 的过滤器在 `init` 之后不会再变，所以问一次就够。唯一的例外是 `RUST_LOG`
    /// 里的 span 条件（`[span{field=value}]`），它依赖每条事件所在的 span，这里快照的结果
    /// 可能与逐事件判断不同；访问日志不在任何 span 里，用不到那种写法。
    #[must_use]
    pub fn enabled() -> bool {
        tracing::enabled!(target: ACCESS_LOG_TARGET, tracing::Level::INFO)
    }
}

/// 按 `tracing` 的格式拼出一整行（不含时间戳）：级别右对齐到 5、target、消息、换行。
///
/// 只给 [`AccessLog::Direct`] 用。格式一旦与 `tracing` 漂移，`direct_line_matches_tracing`
/// 就会失败——这是绕过 `tracing` 必须付的保险费。
pub fn render_line(line: &AccessLine<'_>, out: &mut String) -> fmt::Result {
    writeln!(out, "  INFO {ACCESS_LOG_TARGET}: {line}")
}

/// 直写快路径在栈上拼行用的缓冲大小。
///
/// 常见的 `/files/<相对路径>` 整行都在这之内；放不下（长路径）就退回 [`render_line`]。
pub const LINE_STACK: usize = 256;

/// 定长缓冲的写入器：越界即返回 `false`，由调用方退回 [`render_line`]。
struct StackWriter<'a> {
    out: &'a mut [u8],
    len: usize,
}

impl StackWriter<'_> {
    fn push(&mut self, bytes: &[u8]) -> bool {
        let end = self.len + bytes.len();
        if end > self.out.len() {
            return false;
        }
        self.out[self.len..end].copy_from_slice(bytes);
        self.len = end;
        true
    }

    const fn push_byte(&mut self, byte: u8) -> bool {
        if self.len == self.out.len() {
            return false;
        }
        self.out[self.len] = byte;
        self.len += 1;
        true
    }

    /// 十进制无符号整数，不经过 `fmt`。
    ///
    /// 用后置判零的 `loop` 而不是 `while value > 0`：后者遇到 `0` 会一位都不写、
    /// 输出空串，而这个函数至少要写出一位数字（`status` 与 IPv4 八位组都可能为 0）。
    fn push_uint(&mut self, mut value: u32) -> bool {
        let mut digits = [0_u8; 10];
        let mut at = digits.len();
        loop {
            at -= 1;
            digits[at] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.push(&digits[at..])
    }
}

/// `Option<IpAddr>` 的 `{:?}` 输出。
///
/// IPv6 的 RFC 5952 压缩不值得手写，遇到就返回 `false` 让整行走慢路径。
fn push_ip(out: &mut StackWriter<'_>, ip: Option<IpAddr>) -> bool {
    match ip {
        None => out.push(b"None"),
        Some(IpAddr::V4(addr)) => {
            let octets = addr.octets();
            out.push(b"Some(")
                && out.push_uint(u32::from(octets[0]))
                && out.push_byte(b'.')
                && out.push_uint(u32::from(octets[1]))
                && out.push_byte(b'.')
                && out.push_uint(u32::from(octets[2]))
                && out.push_byte(b'.')
                && out.push_uint(u32::from(octets[3]))
                && out.push_byte(b')')
        }
        Some(IpAddr::V6(_)) => false,
    }
}

/// `Version` 的 `{:?}` 输出，与 `http` crate 的实现逐字节一致。未知版本退回慢路径。
fn push_version(out: &mut StackWriter<'_>, version: Version) -> bool {
    let text = match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2.0",
        Version::HTTP_3 => "HTTP/3.0",
        _ => return false, // 未知版本退回慢路径
    };
    out.push(text.as_bytes())
}

/// 直写快路径：把整行（含时间戳）直接拼进定长缓冲，绕开 `fmt::Formatter` 的逐字段分发。
///
/// 输出与 `tracing` 逐字节一致（`direct_line_matches_tracing` 把关）；放不下长路径、或碰上
/// 不值得手写的字段（IPv6）时返回 `None`，由调用方退回 [`render_line`]。
pub fn render_line_stack(stamp: &str, line: &AccessLine<'_>, out: &mut [u8]) -> Option<usize> {
    let mut out = StackWriter { out, len: 0 };
    let fits = out.push(stamp.as_bytes())
        && out.push(b"  INFO ")
        && out.push(ACCESS_LOG_TARGET.as_bytes())
        && out.push(b": access ip=")
        && push_ip(&mut out, line.ip)
        && out.push(b" method=")
        && out.push(line.method.as_bytes())
        && out.push(b" path=")
        && out.push(line.path.as_bytes())
        && out.push(b" version=")
        && push_version(&mut out, line.version)
        && out.push(b" status=")
        && out.push_uint(u32::from(line.status))
        && out.push(b" size=")
        && out.push(line.size.as_bytes())
        && out.push_byte(b'\n');
    fits.then_some(out.len)
}

/// 访问日志的 hoop。
///
/// 用结构体而不是自由函数，是为了把出口挂在 handler 上；塞进 `Depot` 的话每请求都要多一次
/// 类型查找。
struct AccessLogHandler {
    access_log: Arc<AccessLog>,
}

#[handler]
impl AccessLogHandler {
    async fn handle(
        &self,
        req: &mut Request,
        depot: &mut Depot,
        res: &mut Response,
        ctrl: &mut FlowCtrl,
    ) {
        ctrl.call_next(req, depot, res).await;
        log_access(&self.access_log, req, res);
    }
}

/// 记一条访问日志。salvo 的 hoop 与 hyper 快路径共用这一个出口。
pub fn log_access(access_log: &AccessLog, req: &Request, res: &Response) {
    // `Off` 时 salvo 侧根本不挂这个 hoop，所以这道早退只可能由快路径走到：快路径没有
    // `tracing` 的过滤器兜底，少了它就会在 `RUST_LOG=off` 时照样拼行、照样写日志
    if matches!(access_log, AccessLog::Off) {
        return;
    }
    let method = req.method().as_str();
    let path = req.uri().path();
    let ip = req.remote_addr().ip();
    let version = req.version();
    let status = match res.status_code {
        Some(code) => code.as_u16(),
        None => 200,
    };
    let size = match res
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
    {
        Some(size) => size,
        None => "-",
    };

    let line = AccessLine {
        ip,
        method,
        path,
        version,
        status,
        size,
    };
    if let AccessLog::Direct(write) = access_log {
        write(&line);
    } else {
        tracing::info!("{line}");
    }
}

#[must_use]
pub fn build_router(root: PathBuf, port: u16, access_log: Arc<AccessLog>) -> Router {
    // 日志关掉时连 hoop 都不挂：hoop 是 `#[async_trait]`，每请求要装箱一个 future 再
    // 动态分发一次，而它在 `Off` 下什么都不做。挂上与否的语义完全相同。
    let router = if matches!(*access_log, AccessLog::Off) {
        Router::new()
    } else {
        Router::new().hoop(AccessLogHandler { access_log })
    };
    router.push(static_routes()).push(list_routes(root, port))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        fmt, io,
        sync::{Arc, Mutex},
    };

    use salvo::http::Version;
    use tracing_subscriber::fmt::{MakeWriter, format::Writer, time::FormatTime};

    use super::{ACCESS_LOG_TARGET, AccessLine, LINE_STACK, render_line, render_line_stack};

    /// 固定时间戳，好让直写与 `tracing` 的输出只差这个前缀。
    struct FixedStamp;

    impl FormatTime for FixedStamp {
        fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
            w.write_str("STAMP")
        }
    }

    /// 把 `tracing` 的输出收进内存，好在测试里和直写快路径逐字节比对。
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// 直写快/慢两条路拼出来的字节必须和 `tracing` 写出来的一模一样，否则绕过它就等于悄悄改了
    /// 日志格式。`tracing` 升级导致格式漂移时这里会失败。
    fn assert_matches_tracing(line: &AccessLine<'_>) {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(Capture(Arc::clone(&captured)))
            .with_ansi(false)
            .with_timer(FixedStamp)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: ACCESS_LOG_TARGET, "{line}");
        });
        let text = String::from_utf8(captured.lock().unwrap().clone()).unwrap();

        let mut stack = [0_u8; LINE_STACK];
        let len = render_line_stack("STAMP", line, &mut stack).unwrap();
        assert_eq!(text.as_bytes(), &stack[..len], "栈上拼行与 tracing 不一致");

        let mut direct = String::new();
        render_line(line, &mut direct).unwrap();
        assert_eq!(
            text.strip_prefix("STAMP").unwrap(),
            direct,
            "String 拼行与 tracing 不一致"
        );
    }

    #[test]
    fn direct_line_matches_tracing() {
        assert_matches_tracing(&AccessLine {
            ip: Some("127.0.0.1".parse().unwrap()),
            method: "GET",
            path: "/files/one.bin",
            version: Version::HTTP_11,
            status: 206,
            size: "9",
        });
        assert_matches_tracing(&AccessLine {
            ip: None,
            method: "POST",
            path: "/api/zip/target",
            version: Version::HTTP_2,
            status: 200,
            size: "-",
        });
    }

    /// 长路径放不下时快路径必须认输，交给 `String` 那条路。
    #[test]
    fn stack_line_gives_up_when_full() {
        let path = "a".repeat(LINE_STACK);
        let line = AccessLine {
            ip: Some("127.0.0.1".parse().unwrap()),
            method: "GET",
            path: &path,
            version: Version::HTTP_11,
            status: 200,
            size: "1",
        };
        let mut stack = [0_u8; LINE_STACK];
        assert!(render_line_stack("STAMP", &line, &mut stack).is_none());
    }

    /// IPv6 不手写，交给 `String` 那条路。
    #[test]
    fn stack_line_gives_up_on_ipv6() {
        let line = AccessLine {
            ip: Some("::1".parse().unwrap()),
            method: "GET",
            path: "/files/one.bin",
            version: Version::HTTP_11,
            status: 200,
            size: "1",
        };
        let mut stack = [0_u8; LINE_STACK];
        assert!(render_line_stack("STAMP", &line, &mut stack).is_none());
    }
}
