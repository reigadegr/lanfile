//! `/files`、`/pull`、`/stream-batch` 的 hyper 快路径。
//!
//! salvo 的 `HyperHandler` 每请求要做一整套：按 `Host` 重建 `Uri`、往 `Extensions` 里插
//! `ConnCtrl`、把路径 `to_owned`、构造 `PathState`、跑一遍路由匹配、重建 handler 链
//! （链上每个 handler 都是 `#[async_trait]`，各要装箱一个 future），最后再把整个 future
//! 装箱。按调用点归因，这一圈是每请求 15 次堆分配加三次异步跳转，而 `/files` 与 `/pull`
//! 都用不到路由与任何中间件。
//!
//! 所以这里自己跑 accept 循环：`/files/*` 直接构造 salvo 的 `Request`/`Response` 调用
//! [`ServeFiles::serve`]、`/pull/*` 调用不缓存的 [`ServeFiles::serve_raw`]（都不经过
//! `dyn Handler`，不装箱），其余路径原样交给 salvo 的 `HyperHandler`。HTTP/1 的配置直接
//! 用 salvo 的 [`HttpBuilder::new`]——`Server::new` 用的就是它，所以连接层行为与原来完全一致。
//!
//! `/stream-batch` 走得更远：accept 后先 `peek` 一眼请求行，命中就完全绕开 hyper，
//! 直接往 socket 写响应头、按请求体索引逐文件 `sendfile(2)`。
//! HTTP/1.1 下这是吃上 `sendfile` 的唯一方式：只要经过 hyper 的 body，
//! 无 `Content-Length` 就会走 chunked 分帧，零拷贝无从谈起。

use std::{
    borrow::Cow,
    future::Future,
    io::{self, Read as _},
    net::TcpStream as StdTcpStream,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use lanfile_assets::ServeFiles;
use lanfile_sendfile::{SendfileSlot, SendfileStream};
use salvo::{
    Depot, Request, Response, Router, Service,
    catcher::Catcher,
    conn::{ConnCtrl, HttpBuilder, SocketAddr, StraightStream},
    http::{Method, StatusCode, body::ResBody, uri::Scheme},
    hyper::{
        Request as HyperRequest, Response as HyperResponse, body::Incoming,
        service::Service as HyperService,
    },
    routing::decode_url_path,
};
use tokio::net::TcpListener;

use crate::{AccessLog, log_access};

/// salvo 的响应 future 类型，与 `HyperHandler` 的 `Future` 完全一致。
type BoxedFuture =
    Pin<Box<dyn Future<Output = Result<HyperResponse<ResBody>, salvo::hyper::Error>> + Send>>;

/// 快路径认的两条端点前缀。
///
/// 用枚举而不是 `&'static str`：`sub_path` 剥前缀时只需要一个编译期常量，而 `route_path`
/// 的匹配天然保证"路由判过的前缀"和"要剥的前缀"是同一个——两者不会再各自看一个字符串。
#[derive(Copy, Clone)]
enum Prefix {
    Files,
    Pull,
}

impl Prefix {
    /// 这条端点要剥掉的前缀。
    const fn as_str(self) -> &'static str {
        match self {
            Self::Files => "/files/",
            Self::Pull => "/pull/",
        }
    }
}

/// 剥掉 [`route_path`] 判过的前缀，去掉开头的斜杠，再按 salvo 的规则解码。
///
/// 调用方必须传 [`route_path`] 返回的那条前缀。两者看的是同一个 `Uri`——salvo 的
/// `Request::from_hyper` 把 `uri` 原样搬进 `Request`——所以前缀一定对得上，这里不再自己判一遍。
/// 切点也由该前缀保证落在字符边界上。
///
/// 末尾带斜杠的请求在 salvo 那边匹配不上（实测 `/files/f.bin/` 是 404），这里用空串表示，
/// `ServeFiles` 同样会把它判成 404。
fn sub_path(path: &str, prefix: Prefix) -> Cow<'_, str> {
    let rest = path[prefix.as_str().len()..].trim_start_matches('/');
    if rest.ends_with('/') {
        return Cow::Borrowed("");
    }
    decode_url_path(rest)
}

/// 这条路径归快路径的哪条端点；`None` 表示不归快路径管、交给 salvo。
///
/// 两条前缀都是编译期常量，`starts_with` 会被折成一次直接比较（基准见 `bench_prefix_match`）。
fn route_path(path: &str) -> Option<Prefix> {
    if path.starts_with(Prefix::Files.as_str()) {
        Some(Prefix::Files)
    } else if path.starts_with(Prefix::Pull.as_str()) {
        Some(Prefix::Pull)
    } else {
        None
    }
}

/// `/files` 与 `/pull` 走快路径，其余路径交给 salvo。
///
/// 回退那一侧存成闭包：`Service::hyper_handler` 返回的 `HyperHandler` 在 salvo 里不可命名
/// （`service` 模块是私有的）。闭包每连接建一次，之后每请求只是一次间接调用。
struct FastService {
    files: Arc<ServeFiles>,
    access_log: Arc<AccessLog>,
    fallback: Box<dyn Fn(HyperRequest<Incoming>) -> BoxedFuture + Send + Sync>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    /// 本连接的 sendfile 槽位：handler 用它 arm 文件，transport stream 用它取走 plan
    slot: Arc<SendfileSlot>,
}

impl HyperService<HyperRequest<Incoming>> for FastService {
    type Response = HyperResponse<ResBody>;
    type Error = salvo::hyper::Error;
    type Future = BoxedFuture;

    fn call(&self, req: HyperRequest<Incoming>) -> Self::Future {
        // 分流只判前缀，真正的取值留到下面 `sub_path` 做一次。前缀本身要带进去：
        // 这里已经判过它，`sub_path` 就不必再判一遍
        let Some(prefix) = route_path(req.uri().path()) else {
            return (self.fallback)(req);
        };
        let files = Arc::clone(&self.files);
        let access_log = Arc::clone(&self.access_log);
        let slot = Arc::clone(&self.slot);
        let local_addr = self.local_addr.clone();
        let remote_addr = self.remote_addr.clone();
        Box::pin(async move {
            let mut request = Request::from_hyper(req, Scheme::HTTP);
            *request.local_addr_mut() = local_addr;
            *request.remote_addr_mut() = remote_addr;
            let mut res = Response::new();
            // 一次把容量留够：`HeaderMap` 逐个 insert 会反复扩容，实测每请求 4 次分配
            res.headers_mut().reserve(8);

            let method = request.method();
            let is_head = method == Method::HEAD;
            if is_head || method == Method::GET {
                // 借自 `request`，不再单独分配：`decode_url_path` 在没有 `%` 时就是借用。
                // 求值放进这个分支里——非 GET/HEAD 只会返回 404，根本用不到子路径
                let sub = sub_path(request.uri().path(), prefix);
                match prefix {
                    Prefix::Pull => files.serve_raw(&sub, &request, &mut res, Some(&slot)).await,
                    Prefix::Files => files.serve(&sub, &request, &mut res, Some(&slot)).await,
                }
            } else {
                res.status_code(StatusCode::NOT_FOUND);
            }

            // 与 salvo 的 `Service` 完全一致地补错误页：状态码是 4xx/5xx 且没写出响应体时
            // 跑一遍 catcher。`!is_head` 放最前面短路：HEAD 不补体（RFC 9110 §9.3.2），
            // 就不该为它白算后面两项
            if !is_head
                && res
                    .status_code
                    .is_some_and(|code| code.is_client_error() || code.is_server_error())
                && (res.body.is_none() || res.body.is_error())
            {
                // `Depot` 只有补错误页时才用得到，正常 200 路径不必每请求建一次
                let mut depot = Depot::new();
                Catcher::default()
                    .catch(&mut request, &mut depot, &mut res, ConnCtrl::new())
                    .await;
            }

            // 必须放在 catcher 之后：salvo 的 hoop 也是在整条链跑完后才记日志，错误页的
            // Content-Length 那时才写上去
            log_access(&access_log, &request, &res);
            Ok(res.into_hyper())
        })
    }
}

/// accept 出错后的退避时间，取值与 salvo 的 `Server` 一致。
const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);
const STREAM_BATCH_PREFIX: &str = "/stream-batch/";
const STREAM_BATCH_READ_BUF: usize = 4096;
const HEAD_END: &[u8; 4] = b"\r\n\r\n";

/// Returns the target of an HTTP request line.
fn request_target(line: &[u8]) -> Option<&[u8]> {
    let method_end = line.iter().position(|&byte| byte == b' ')?;
    let rest = &line[method_end + 1..];
    let target_end = rest.iter().position(|&byte| byte == b' ')?;
    Some(&rest[..target_end])
}

/// 看请求行是不是 `/stream-batch/...`。
///
/// 用 `peek` 不消耗 socket 缓冲；是就交给自定义处理，否则原样交回给 hyper。
/// 带超时，避免客户端连上却不发请求时卡住 accept 循环。
async fn is_stream_batch_request(conn: &tokio::net::TcpStream) -> bool {
    let mut buf = [0_u8; 1024];
    let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(500), conn.peek(&mut buf)).await
    else {
        return false;
    };
    let Some(line_end) = buf[..n].windows(2).position(|w| w == b"\r\n") else {
        return false;
    };
    let line = &buf[..line_end];
    request_target(line).is_some_and(|target| target.starts_with(STREAM_BATCH_PREFIX.as_bytes()))
}

/// 处理一条 `/stream-batch/` 连接：读请求头、解析路径、读请求体并发送文件。
async fn handle_stream_connection(
    conn: tokio::net::TcpStream,
    root: Arc<PathBuf>,
) -> io::Result<()> {
    let _ = conn.set_nodelay(true);
    let stream = conn.into_std()?;
    tokio::task::spawn_blocking(move || handle_stream_blocking(stream, &root))
        .await
        .map_err(io::Error::other)?
}

fn handle_stream_blocking(mut stream: StdTcpStream, root: &Path) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));

    let (head, head_len) = read_request_head(&mut stream)?;
    if head_len == 0 {
        return Ok(());
    }

    // 解析请求行，取路径
    let Some(line_end) = head[..head_len].windows(2).position(|w| w == b"\r\n") else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "请求行未终止"));
    };
    let line = &head[..line_end];
    let Some(target) = request_target(line) else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "请求行格式异常"));
    };
    let path = std::str::from_utf8(target)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "请求路径不是 UTF-8"))?;
    let Some(encoded_target) = path.strip_prefix(STREAM_BATCH_PREFIX) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "路径不是 stream-batch 端点",
        ));
    };

    let decoded = decode_url_path(encoded_target);
    let body = read_request_body(&mut stream, &head, head_len)?;
    lanfile_list::serve_stream_batch(&mut stream, root, &decoded, &body)
}

/// Reads the request head in blocks and returns all bytes read plus the length
/// of the head itself. Bytes after the terminator may already be in the buffer
/// and are left for the body reader.
fn read_request_head(stream: &mut StdTcpStream) -> io::Result<(Vec<u8>, usize)> {
    let mut head = Vec::with_capacity(STREAM_BATCH_READ_BUF);
    let mut chunk = [0_u8; STREAM_BATCH_READ_BUF];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Ok((Vec::new(), 0));
        }
        let search_start = head.len().saturating_sub(HEAD_END.len() - 1);
        head.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_head_end(&head, search_start) {
            return Ok((head, end + 1));
        }
    }
}

fn find_head_end(input: &[u8], from: usize) -> Option<usize> {
    input
        .get(from..)?
        .windows(HEAD_END.len())
        .position(|window| window == HEAD_END)
        .map(|at| from + at + HEAD_END.len() - 1)
}

fn read_request_body(
    stream: &mut StdTcpStream,
    head: &[u8],
    head_len: usize,
) -> io::Result<Vec<u8>> {
    let mut length = None;
    for line in head[..head_len].split(|&byte| byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.len() >= 15 && line[..15].eq_ignore_ascii_case(b"Content-Length:") {
            if length.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Content-Length 重复",
                ));
            }
            let value = std::str::from_utf8(line[15..].trim_ascii())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Content-Length 不是文本"))?
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Content-Length 非法"))?;
            length = Some(value);
        }
    }
    let length = length.unwrap_or(0);
    let received = head.get(head_len..).unwrap_or_default();
    let take = received.len().min(length);
    let mut body = Vec::with_capacity(length);
    body.extend_from_slice(&received[..take]);
    body.resize(length, 0);
    stream.read_exact(&mut body[take..])?;
    Ok(body)
}

/// 跑 accept 循环，把每条连接交给 [`FastService`]。
///
/// 取代原来的 `Server::new(acceptor).serve(router)`。连接本身仍按 sendfile 的要求包装
/// （`TCP_NODELAY`、槽位），否则零拷贝体没有槽位可用。accept 与单条连接的准备出错都只
/// 影响那一条连接（照 `Server` 的做法退避重试），不会像 `?` 那样把整个进程带走。
pub async fn serve(
    listener: TcpListener,
    root: PathBuf,
    access_log: Arc<AccessLog>,
    router: Router,
) -> io::Result<()> {
    let builder = Arc::new(HttpBuilder::new());
    let service = Service::new(router);
    let files = Arc::new(ServeFiles::new(root.clone()));
    let root = Arc::new(root);
    loop {
        let (conn, remote_addr) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                // 瞬时错误不该带走整个服务，最典型的是 fd 耗尽（`FileCache` 最多占 512 个）
                tracing::error!(error = ?error, "接受连接失败");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let local_addr: SocketAddr = match conn.local_addr() {
            Ok(local_addr) => local_addr.into(),
            // 已经 accept 到的连接取不到本地地址很反常，同样退避后继续，避免忙等
            Err(error) => {
                tracing::error!(error = ?error, "取本地地址失败");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let remote_addr: SocketAddr = remote_addr.into();
        // HTTP/1.1 把一个响应写成「响应头」+「body」两次写。body 小于 MSS 时 Nagle 会压住
        // 第二次写，直到对端的 delayed ACK 超时（Linux 约 40ms），小响应因此每个都平白多出
        // 40ms。Go 的 net 包默认就打开 TCP_NODELAY，这里对齐。
        if let Err(error) = conn.set_nodelay(true) {
            // 丢的只是这条连接的延迟优化：Nagle 设不上不影响正确性，连接照样服务
            tracing::debug!(error = ?error, "设置 TCP_NODELAY 失败");
        }

        // /stream-batch 分流：peek 一眼请求行，命中就完全绕开 hyper
        if is_stream_batch_request(&conn).await {
            let root = Arc::clone(&root);
            tokio::spawn(async move {
                if let Err(error) = handle_stream_connection(conn, root).await {
                    tracing::debug!("stream-batch 连接出错: {error}");
                }
            });
            continue;
        }

        let slot = Arc::new(SendfileSlot::new());
        let stream = SendfileStream::new(conn, Arc::clone(&slot));
        // 一条连接只建一份 `ConnCtrl`，与 salvo 的 `TcpAcceptor` 一样：`HyperHandler` 会把它
        // 插进每个请求的 extensions，handler 拿到的必须就是驱动这条连接的那一份，
        // `abort()`/`graceful_shutdown()`/`relax_timeouts()` 才会真的作用到这条连接上
        let conn_ctrl = ConnCtrl::new();
        let io = StraightStream::new(stream, None, conn_ctrl.clone(), None);
        let handler = service.hyper_handler(
            local_addr.clone(),
            remote_addr.clone(),
            Scheme::HTTP,
            None,
            conn_ctrl.clone(),
            None,
        );
        let fast = FastService {
            files: Arc::clone(&files),
            access_log: Arc::clone(&access_log),
            fallback: Box::new(move |req| handler.call(req)),
            local_addr,
            remote_addr,
            slot,
        };
        let builder = Arc::clone(&builder);
        tokio::spawn(async move {
            if let Err(error) = builder
                .serve_connection(io, fast, None, conn_ctrl, None)
                .await
            {
                tracing::debug!("连接出错: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::borrow::Cow;
    use std::io::Write as _;
    use std::net::TcpListener;

    use super::{Prefix, read_request_body, read_request_head, route_path, sub_path};

    /// 快路径只认这两条前缀：`/pull` 必须由它自己服务，落到 salvo 就丢了零拷贝与不缓存的收益
    #[test]
    fn route_path_claims_files_and_pull_only() {
        assert!(matches!(route_path("/files/f.bin"), Some(Prefix::Files)));
        assert!(matches!(
            route_path("/files/sub/g.txt"),
            Some(Prefix::Files)
        ));
        assert!(matches!(route_path("/pull/f.bin"), Some(Prefix::Pull)));
        assert!(matches!(route_path("/pull/sub/g.txt"), Some(Prefix::Pull)));
        // 少一个斜杠或别的路径都不归快路径管
        assert!(route_path("/files").is_none());
        assert!(route_path("/pull").is_none());
        assert!(route_path("/api/list").is_none());
        assert!(route_path("/stream-batch/foo").is_none()); // stream-batch 走 peek 分流，不进 hyper
        assert!(route_path("/static/x.css").is_none());
        assert!(route_path("/").is_none());
    }

    /// 这些取值是拿旧二进制实测出来的：每个用例的注释是它当时的响应。
    ///
    /// 只喂 [`route_path`] 认得的路径：前缀由它判过，`sub_path` 自己不再判。
    #[test]
    fn sub_path_matches_the_router() {
        let cases: &[(&str, &str)] = &[
            ("/files/f.bin", "f.bin"),
            // 开头的空段被跳过
            ("/files//f.bin", "f.bin"),
            ("/files/./f.bin", "./f.bin"),
            ("/files/sub//g.txt", "sub//g.txt"),
            ("/files/sub/../f.bin", "sub/../f.bin"),
            // 百分号解码，但不把 `+` 当空格
            ("/files/a%20b.txt", "a b.txt"),
            ("/files/a+b.txt", "a+b.txt"),
            ("/files/%2e%2e/f.bin", "../f.bin"),
            // 末尾斜杠：salvo 那边匹配不上，这里用空串表达同一个 404
            ("/files/", ""),
            ("/files/f.bin/", ""),
            ("/files//", ""),
        ];
        for (path, expected) in cases {
            assert_eq!(
                &*sub_path(path, Prefix::Files),
                *expected,
                "路径 {path} 的子路径取值不对"
            );
        }
    }

    /// `route_path` 给出的前缀必须正好是 `sub_path` 要剥的那条——两者看的是同一个 `Uri`
    #[test]
    fn route_path_prefix_is_what_sub_path_strips() {
        let cases = [
            ("/files/f.bin", "f.bin"),
            ("/files/sub/g.txt", "sub/g.txt"),
            ("/files/a%20b.txt", "a b.txt"),
            ("/pull/f.bin", "f.bin"),
        ];
        for (path, expected) in cases {
            let prefix = route_path(path).unwrap();
            assert_eq!(&*sub_path(path, prefix), expected, "路径 {path} 的切点不对");
        }
    }

    /// `/pull` 与 `/files` 共用同一套取值规则，只是前缀不同
    #[test]
    fn pull_sub_path_uses_the_same_rules() {
        assert_eq!(
            &*sub_path("/pull/sub/a%20b.txt", Prefix::Pull),
            "sub/a b.txt"
        );
        assert_eq!(&*sub_path("/pull/", Prefix::Pull), "");
        assert_eq!(&*sub_path("/pull/f.bin/", Prefix::Pull), "");
    }

    #[test]
    fn sub_path_borrows_when_nothing_is_encoded() {
        assert!(matches!(
            sub_path("/files/f.bin", Prefix::Files),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn request_body_beyond_read_buffer_is_completed() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let body = vec![7_u8; 16 * 1024];
        let expected = body.clone();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (head, head_len) = read_request_head(&mut stream).unwrap();
            read_request_body(&mut stream, &head, head_len).unwrap()
        });
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .write_all(
                format!(
                    "GET /stream-batch/sub HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .unwrap();
        client.write_all(&body).unwrap();
        drop(client);

        assert_eq!(server.join().unwrap(), expected);
    }

    /// 基准：前缀匹配各写法的耗时。
    ///
    /// `route_path` 用的是常量 needle 的 `starts_with("/files/")`：rustc 会把常量前缀折成一次
    /// 直接比较，优化构建下它与 `is_prefix` 都在 1 ns 上下打平，而未优化时 `starts_with` 明显
    /// 更快，所以 `route_path` 保持 `starts_with`。`memmem::find` 也在对照里：它要扫完整条路径
    /// 才能判定"子串不在开头"，慢一个数量级。
    ///
    /// `cargo test` 默认跑在 `opt-level = 0`：std 与 libc 都是预编译的优化产物而 `memchr` 不是，
    /// 那种 profile 下打印出来的数会偏向现实现，要看真实差距得加 `--release`。
    #[test]
    #[ignore = "微基准，需 cargo test --release -- --ignored 显式运行"]
    fn bench_prefix_match() {
        use std::hint::black_box;
        use std::time::Instant;

        fn time<R>(iters: u32, f: impl Fn() -> R) -> f64 {
            for _ in 0..iters / 10 {
                black_box(f());
            }
            let start = Instant::now();
            for _ in 0..iters {
                black_box(f());
            }
            start.elapsed().as_secs_f64() * 1e9 / f64::from(iters)
        }

        for path in ["/files/f.bin", "/files/sub/deeper/dir/c.bin"] {
            // 常量 needle：`route_path` 的情形
            let sw = time(500_000, || path.starts_with("/files/"));
            let ip = time(500_000, || {
                memchr::arch::all::is_prefix(path.as_bytes(), b"/files/")
            });
            let mm = time(500_000, || {
                memchr::memmem::find(path.as_bytes(), b"/files/") == Some(0)
            });
            println!(
                "基准 前缀分流 常量（{}B）: starts_with {sw:.1} ns | is_prefix {ip:.1} ns | memmem {mm:.1} ns",
                path.len()
            );
            assert_eq!(
                path.starts_with("/files/"),
                memchr::arch::all::is_prefix(path.as_bytes(), b"/files/"),
                "{path} 两种写法取值不一致"
            );
            assert!(
                mm > sw,
                "memmem 不该比 starts_with 快: {mm:.1} vs {sw:.1} ns"
            );
        }
    }
}
