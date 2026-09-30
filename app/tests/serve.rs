#![allow(clippy::unwrap_used)]

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use lanfile::{AccessLog, build_router, serve};
use salvo::{
    prelude::*,
    test::{ResponseExt, TestClient},
};

static COUNTER: AtomicU32 = AtomicU32::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lanfile-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn root(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn api_router(root: PathBuf) -> Arc<Router> {
    Arc::new(build_router(root, 8000, Arc::new(AccessLog::Tracing)))
}

// ---- JSON API tests ----

#[tokio::test]
async fn api_list_returns_json_for_root() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("a.txt"), "abc").unwrap();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/list")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));

    let body = res.take_string().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["path"], "/");
    assert_eq!(json["port"], 8000);
    assert!(json["lan_ip"].is_null() || json["lan_ip"].is_string());
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);

    let by_name: std::collections::HashMap<&str, &serde_json::Value> = entries
        .iter()
        .map(|e| (e["name"].as_str().unwrap(), e))
        .collect();
    let a = by_name.get("a.txt").unwrap();
    assert_eq!(a["type"], "file");
    assert_eq!(a["size"], 3);
    assert!(a["modified"].as_str().is_some());

    let sub = by_name.get("sub").unwrap();
    assert_eq!(sub["type"], "dir");
    assert!(sub["size"].is_null());
}

#[tokio::test]
async fn api_list_shows_dot_files() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join(".hidden"), "secret").unwrap();
    std::fs::write(dir.root().join("visible.txt"), "abc").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/list")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));

    let body = res.take_string().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);

    let by_name: std::collections::HashMap<&str, &serde_json::Value> = entries
        .iter()
        .map(|e| (e["name"].as_str().unwrap(), e))
        .collect();
    assert!(by_name.contains_key(".hidden"));
    assert!(by_name.contains_key("visible.txt"));
}

#[cfg(unix)]
#[tokio::test]
async fn api_list_hides_symlinks() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("real.txt"), "real").unwrap();
    std::os::unix::fs::symlink(dir.root().join("real.txt"), dir.root().join("alias.txt")).unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/list")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));

    let body = res.take_string().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "real.txt");
    assert!(!body.contains("alias.txt"));
}

#[tokio::test]
async fn api_list_lists_subdirectory() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub").join("inner.txt"), "xyz").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/list/sub")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));

    let body = res.take_string().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["path"], "/sub");
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "inner.txt");
    assert_eq!(entries[0]["type"], "file");
}

#[tokio::test]
async fn api_list_returns_404_for_missing_directory() {
    let dir = TestDir::new();
    let router = api_router(dir.root().to_path_buf());
    let res = TestClient::get("http://127.0.0.1:5800/api/list/nope")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn api_manifest_returns_flat_tree_metadata() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub").join("empty")).unwrap();
    std::fs::write(dir.root().join("sub").join("a.txt"), "abc").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/manifest/sub")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));

    let body = res.take_string().await.unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    let by_path: std::collections::HashMap<&str, &serde_json::Value> = entries
        .iter()
        .map(|entry| (entry["path"].as_str().unwrap(), entry))
        .collect();
    let file = by_path.get("a.txt").unwrap();
    assert_eq!(file["type"], "file");
    assert_eq!(file["size"], 3);
    let dir = by_path.get("empty").unwrap();
    assert_eq!(dir["type"], "dir");
    assert!(dir["size"].is_null());
}

// ---- /files 与 /pull：这两条端点由 app 的 hyper 快路径在 salvo 路由之前直接服务 ----
// 以下测试都起真正的 `serve`（快路径），用裸 TCP 打过去，测的就是生产里真正跑的那条路，
// 不再经 salvo 路由登记 /files、/pull（生产里 salvo 那边永远收不到这两条）。

/// 发一条任意方法/路径的请求，靠 `Connection: close` 收到 EOF 为止，再切成响应头与正文。
/// 这样 HEAD（无正文）、304（无正文）、普通 GET 都用同一套读法，不必各自猜正文长度。
async fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra: &str,
) -> (String, Vec<u8>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra}Connection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(buf.len());
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let body = buf.get(split + 4..).unwrap_or(&[]).to_vec();
    (head, body)
}

/// 响应头里的状态行（第一行）。
fn status_line(head: &str) -> &str {
    head.split("\r\n").next().unwrap_or(head)
}

/// 响应头里某个字段的值（名字大小写不敏感）。
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .find(|(n, _)| n.trim().eq_ignore_ascii_case(name))
        .map(|(_, v)| v.trim())
}

/// `/files` 走快路径：点文件、隐藏目录里的成员照常服务，HEAD 与 GET 同状态码但不带正文。
#[tokio::test]
async fn files_endpoint_serves_special_names() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join(".hidden"), "secret").unwrap();
    std::fs::create_dir_all(dir.root().join(".git")).unwrap();
    std::fs::write(dir.root().join(".git/config"), "secret-config").unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (head, body) = http_request(addr, "GET", "/files/.hidden", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(body, b"secret");

    let (head, body) = http_request(addr, "GET", "/files/.git/config", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(body, b"secret-config");

    let (head, body) = http_request(addr, "HEAD", "/files/.hidden", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 200"), "{head}");
    assert!(body.is_empty(), "HEAD 不该写出正文");

    server.abort();
}

/// `/files` 走快路径：缺失文件、目录、路径穿越、符号链接一律 404；非 GET/HEAD 也是 404。
#[tokio::test]
async fn files_endpoint_rejects_invalid_paths() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub/inner.txt"), "xyz").unwrap();
    #[cfg(unix)]
    {
        std::fs::write(dir.root().join("real.txt"), "real").unwrap();
        std::os::unix::fs::symlink(dir.root().join("real.txt"), dir.root().join("alias.txt"))
            .unwrap();
    }
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let mut paths = vec![
        "/files/nope.txt",
        "/files/sub",
        "/files/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
    ];
    #[cfg(unix)]
    paths.push("/files/alias.txt");

    for path in paths {
        let (head, _) = http_request(addr, "GET", path, "").await;
        assert!(
            status_line(&head).starts_with("HTTP/1.1 404"),
            "{path} 应当 404：{head}"
        );
    }

    let (head, _) = http_request(addr, "POST", "/files/sub/inner.txt", "").await;
    assert!(
        status_line(&head).starts_with("HTTP/1.1 404"),
        "POST 应当 404：{head}"
    );

    server.abort();
}

/// `/pull` 走快路径：正文与 `/files` 一致，但类型固定 `application/octet-stream`，且不编码
/// `ETag`、`Last-Modified`、`Content-Disposition`；HEAD 同 GET 状态码；缺失文件 404。
#[tokio::test]
async fn pull_endpoint_serves_file_with_lean_headers() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("hello.txt"), "hello world").unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (head, body) = http_request(addr, "GET", "/pull/hello.txt", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 200"), "{head}");
    assert_eq!(
        header(&head, "content-type"),
        Some("application/octet-stream"),
        "/pull 类型固定成 octet-stream，省掉建响应体时那次嗅探 pread：{head}"
    );
    for name in ["etag", "last-modified", "content-disposition"] {
        assert!(
            header(&head, name).is_none(),
            "/pull 不该编码 {name}：{head}"
        );
    }
    assert_eq!(body, b"hello world");

    let (head, body) = http_request(addr, "HEAD", "/pull/hello.txt", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 200"), "{head}");
    assert!(body.is_empty(), "HEAD 不该写出正文");

    let (head, _) = http_request(addr, "GET", "/pull/nope.txt", "").await;
    assert!(status_line(&head).starts_with("HTTP/1.1 404"), "{head}");

    server.abort();
}

/// 第二次请求同一个文件会走命中缓存的路径：缓存里复用的 `ETag` 等头必须与未命中时现算的
/// 一模一样，带回这个 `ETag` 再请求也必须仍然是 304。
#[tokio::test]
async fn files_cache_hit_sends_the_same_headers() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("hello.txt"), "hello world").unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (missed, _) = http_request(addr, "GET", "/files/hello.txt", "").await;
    assert!(status_line(&missed).starts_with("HTTP/1.1 200"), "{missed}");
    let (hit, _) = http_request(addr, "GET", "/files/hello.txt", "").await;
    assert!(status_line(&hit).starts_with("HTTP/1.1 200"), "{hit}");
    for name in [
        "etag",
        "content-disposition",
        "content-type",
        "last-modified",
        "content-length",
    ] {
        let missed_header = header(&missed, name);
        assert!(missed_header.is_some(), "未命中也应当带上 {name}：{missed}");
        assert_eq!(
            missed_header,
            header(&hit, name),
            "命中与未命中的 {name} 必须一致"
        );
    }

    let etag = header(&missed, "etag").unwrap();
    let (head, body) = http_request(
        addr,
        "GET",
        "/files/hello.txt",
        &format!("If-None-Match: {etag}\r\n"),
    )
    .await;
    assert!(status_line(&head).starts_with("HTTP/1.1 304"), "{head}");
    assert!(body.is_empty(), "304 不该有正文");

    server.abort();
}

// ---- sendfile tests ----

/// 9 MiB of non-zero, non-repeating bytes.
///
/// A placeholder leak would surface as zeros, and a mis-ordered or duplicated
/// range would surface as a byte mismatch, so an exact comparison proves the
/// response really came from `sendfile(2)`.
///
/// The size has to exceed the placeholder buffer's frame length (4 MiB) so the
/// body spans several frames: that is what exercises the stream's cross-frame
/// offset and remaining-length accounting. Raise it whenever that buffer grows.
fn sendfile_payload() -> Vec<u8> {
    (0..9 * 1024 * 1024_u32)
        .map(|index| (index % 251) as u8 + 1)
        .collect()
}

/// A few hundred non-zero bytes, for the same reason as [`sendfile_payload`].
fn small_payload() -> Vec<u8> {
    (0..700_u32).map(|index| (index % 251) as u8 + 1).collect()
}

/// Downloads `name` over a real connection and returns the head and body.
async fn download(addr: std::net::SocketAddr, name: &str, extra: &str) -> (String, Vec<u8>) {
    use tokio::io::AsyncWriteExt;

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /files/{name} HTTP/1.1\r\nHost: localhost\r\n{extra}Connection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    read_response(&mut stream).await
}

/// 起一个真正的 sendfile 服务：直接复用生产里的 `serve`，它自己跑 accept 循环、
/// 给每条连接装上 `SendfileStream` 并把槽位交给 handler。
async fn serve_with_sendfile(root: PathBuf) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let access_log = Arc::new(AccessLog::Tracing);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        // 服务循环不返回；这里忽略它的 `io::Result`
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (addr, server)
}

/// Reads one response, using `Content-Length` to find the end of the body.
async fn read_response(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    use tokio::io::AsyncReadExt;

    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let read = stream.read(&mut byte).await.unwrap();
        assert_ne!(read, 0, "connection closed while reading the head");
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let len = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0_u8; len];
    stream.read_exact(&mut body).await.unwrap();
    (head, body)
}

#[tokio::test]
async fn large_file_is_served_byte_for_byte_over_sendfile() {
    let dir = TestDir::new();
    let payload = sendfile_payload();
    std::fs::write(dir.root().join("big.bin"), &payload).unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (head, body) = download(addr, "big.bin", "").await;
    server.abort();

    assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
    assert!(
        head.to_ascii_lowercase()
            .contains(&format!("content-length: {}", payload.len())),
        "head: {head}"
    );
    assert_eq!(body, payload, "body must be the exact file contents");
}

#[tokio::test]
async fn small_file_is_served_byte_for_byte_over_sendfile() {
    let dir = TestDir::new();
    let payload = small_payload();
    std::fs::write(dir.root().join("small.bin"), &payload).unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (head, body) = download(addr, "small.bin", "").await;
    server.abort();

    assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
    // The body is a placeholder unless the transport really sent the file, so
    // an exact match also proves small files take the `sendfile` path.
    assert_eq!(body, payload, "small files must also be served by sendfile");
}

/// A run of small responses must not pay a per-response stall.
///
/// 一串小响应不能每个都卡一下。
///
/// The transport writes the head and the body as two separate writes, and when
/// the body is smaller than the MSS Nagle holds the second write back until the
/// peer's delayed ACK fires (about 40ms on Linux); the measured median for a
/// small response went from 0.2ms to 43ms. A fresh connection stays in Linux's
/// quick-ACK mode and hides the stall, so this reuses one keep-alive connection
/// to let the delayed ACK take effect.
///
/// 传输层把响应头与 body 分两次写出，当 body 小于 MSS 时 Nagle 会压住第二次写，
/// 直到对端 delayed ACK 超时（Linux 上约 40ms）；实测小响应中位数从 0.2ms 变成
/// 43ms。新连接仍处于 Linux 的 quick-ACK 模式，会掩盖这个停顿，所以这里复用同一条
/// keep-alive 连接，让 delayed ACK 生效。
#[tokio::test]
async fn small_response_body_is_not_held_by_nagle() {
    use tokio::io::AsyncWriteExt;

    let payload = small_payload();
    let dir = TestDir::new();
    std::fs::write(dir.root().join("small.bin"), &payload).unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request =
        b"GET /files/small.bin HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";

    // 10 healthy responses take about 2ms; a stall per response takes about 430ms.
    let started = std::time::Instant::now();
    for _ in 0..10 {
        stream.write_all(request).await.unwrap();
        let (head, body) = read_response(&mut stream).await;
        assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
        assert_eq!(body, payload);
    }
    let elapsed = started.elapsed();

    server.abort();

    assert!(
        elapsed < std::time::Duration::from_millis(50),
        "10 keep-alive responses took {elapsed:?}: Nagle is holding every small body until the delayed ACK fires"
    );
}

#[tokio::test]
async fn range_request_over_sendfile_returns_the_exact_slice() {
    let dir = TestDir::new();
    let payload = sendfile_payload();
    std::fs::write(dir.root().join("big.bin"), &payload).unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    // 6 MiB wide, so the slice crosses a 4 MiB placeholder frame boundary and the
    // stream's offset arithmetic is exercised across frames rather than within one.
    let (start, end) = (1_000_000_usize, 7_000_000_usize);
    let (head, body) = download(
        addr,
        "big.bin",
        &format!("Range: bytes={start}-{}\r\n", end - 1),
    )
    .await;
    server.abort();

    assert!(head.starts_with("HTTP/1.1 206"), "head: {head}");
    assert_eq!(body, payload[start..end].to_vec());
}

#[tokio::test]
async fn sendfile_response_keeps_the_connection_reusable() {
    use tokio::io::AsyncWriteExt;

    let dir = TestDir::new();
    let payload = sendfile_payload();
    std::fs::write(dir.root().join("big.bin"), &payload).unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /files/big.bin HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let (head, body) = read_response(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
    assert_eq!(body, payload);

    // The stream must be back to pass-through for the next response on the same
    // connection, or the JSON below would be swallowed as file content.
    stream
        .write_all(b"GET /api/list HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let (head, body) = read_response(&mut stream).await;
    server.abort();

    assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["path"], "/");
}

// ---- Frontend page tests ----

#[tokio::test]
async fn root_returns_html_page() {
    let dir = TestDir::new();
    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/").send(router).await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_string().await.unwrap();
    assert!(body.contains("<title>文件浏览</title>"));
    assert!(body.contains("/static/style.css"));
    assert!(body.contains("/static/app.js"));
}

#[tokio::test]
async fn static_serves_css() {
    let dir = TestDir::new();
    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/static/style.css")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_string().await.unwrap();
    assert!(body.contains("border-box"));
}

// ---- Zip download tests ----

#[tokio::test]
async fn api_zip_streams_folder() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("a.txt"), "hello").unwrap();
    std::fs::write(dir.root().join("sub/b.txt"), "world").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/zip")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_string().await.unwrap();
    assert!(body.starts_with("PK\x03\x04"));
    assert!(body.contains("a.txt"));
    assert!(body.contains("sub/b.txt"));
}

/// 统计 `needle` 在 `haystack` 中出现的次数。
fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

/// 分块流水线必须把每个文件的内容完整、连续地写进 zip。
///
/// 载荷大于单个读取分块（256 KiB），两段内容互不相同：内容被截断、分块串到别的
/// 条目上、或条目整个丢失，都会让下面的断言失败。空文件走的是「只有 `FileStart` 与
/// `FileEnd`、没有 `Chunk`」的路径，单独断言它的条目仍在。
#[tokio::test]
async fn api_zip_streams_each_file_intact() {
    const CHUNKED_SIZE: usize = 3 * 512 * 1024;

    let dir = TestDir::new();
    let a: Vec<u8> = (0..CHUNKED_SIZE as u32)
        .map(|index| (index % 251) as u8 + 1)
        .collect();
    let b: Vec<u8> = a.iter().map(|byte| byte.wrapping_add(100)).collect();
    std::fs::write(dir.root().join("a.bin"), &a).unwrap();
    std::fs::write(dir.root().join("b.bin"), &b).unwrap();
    std::fs::write(dir.root().join("empty.bin"), b"").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/zip")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_bytes(None).await.unwrap();

    // Stored 压缩，文件内容原样落在 zip 流里
    assert_eq!(
        count_occurrences(&body, &a),
        1,
        "a.bin 的内容必须完整且只出现一次"
    );
    assert_eq!(
        count_occurrences(&body, &b),
        1,
        "b.bin 的内容必须完整且只出现一次"
    );
    assert!(
        count_occurrences(&body, b"empty.bin") >= 1,
        "空文件也应当保留条目"
    );
}

#[tokio::test]
async fn api_zip_returns_404_for_missing() {
    let dir = TestDir::new();
    let router = api_router(dir.root().to_path_buf());
    let res = TestClient::get("http://127.0.0.1:5800/api/zip/nope")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn api_zip_rejects_path_traversal() {
    let dir = TestDir::new();
    let router = api_router(dir.root().to_path_buf());
    let res = TestClient::get("http://127.0.0.1:5800/api/zip/%2e%2e")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn api_zip_includes_dot_files_and_dirs() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join(".hidden"), "secret").unwrap();
    std::fs::create_dir_all(dir.root().join(".git")).unwrap();
    std::fs::write(dir.root().join(".git/config"), "cfg").unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/zip")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_string().await.unwrap();
    assert!(body.contains(".hidden"), "body: {body}");
    assert!(body.contains(".git/config"), "body: {body}");
}

#[tokio::test]
async fn api_zip_preserves_empty_directory() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("empty")).unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/zip")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let body = res.take_string().await.unwrap();
    assert!(body.contains("empty/"), "body: {body}");
}

#[cfg(unix)]
#[tokio::test]
async fn api_zip_skips_unreadable_directory() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("locked")).unwrap();
    std::fs::write(dir.root().join("locked/secret.txt"), "secret").unwrap();
    std::fs::set_permissions(
        dir.root().join("locked"),
        std::fs::Permissions::from_mode(0o000),
    )
    .unwrap();

    let router = api_router(dir.root().to_path_buf());
    let mut res = TestClient::get("http://127.0.0.1:5800/api/zip")
        .send(router)
        .await;
    assert_eq!(res.status_code, Some(StatusCode::OK));
    let _ = res.take_string().await;

    // 恢复权限，避免 TestDir 清理失败
    std::fs::set_permissions(
        dir.root().join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
}

// ---- lanfile get 递归拉取 ----

/// `lanfile get` 把远端一棵小树镜像到本地：起一个真实监听的 lanfile，
/// 用 `lanfile_pull::run` 拉 `sub` 子树，验证内容落进 `local/sub/`（套一层远端目录名）、
/// 逐文件与原内容一致；再拉第二次验证"本地已存在且尺寸一致就跳过"不破坏已有文件。
#[tokio::test]
async fn get_subcommand_mirrors_a_tree() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("a.txt"), "aaa").unwrap();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub").join("b.txt"), "bbbb").unwrap();
    std::fs::create_dir_all(dir.root().join("sub").join("deeper")).unwrap();
    std::fs::write(
        dir.root().join("sub").join("deeper").join("c.bin"),
        vec![1_u8, 2, 3, 4, 5],
    )
    .unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let local = TestDir::new();
    let base = format!("http://{addr}");
    let local_arg = local.root().to_string_lossy().into_owned();
    lanfile_pull::run(&[base.clone(), "sub".to_string(), local_arg.clone()])
        .await
        .unwrap();

    // run 会在 local 下以远端目录名套一层：内容落进 local/sub/，而非直接散进 local
    let mirror = local.root().join("sub");
    assert_eq!(std::fs::read(mirror.join("b.txt")).unwrap(), b"bbbb");
    assert_eq!(
        std::fs::read(mirror.join("deeper").join("c.bin")).unwrap(),
        vec![1_u8, 2, 3, 4, 5]
    );
    // 根下的 a.txt 不该被拉进 sub 的镜像
    assert!(!local.root().join("a.txt").exists());

    // 第二次拉取：本地已存在且尺寸一致，应跳过，文件内容不变
    lanfile_pull::run(&[base, "sub".to_string(), local_arg])
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(mirror.join("deeper").join("c.bin")).unwrap(),
        vec![1_u8, 2, 3, 4, 5]
    );

    server.abort();
}

/// `lanfile get` 给一个文件名（而非目录）时，顶层 `/api/list` 返回 404 后要改走 `/pull`
/// 把文件拉下来，而不是直接失败。
#[tokio::test]
async fn get_on_a_file_downloads_it() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("claude-code.tgz"), "not a dir").unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let base = format!("http://{addr}");
    let dst = TestDir::new();
    lanfile_pull::run(&[
        base,
        "claude-code.tgz".into(),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();

    let got = std::fs::read(dst.root().join("claude-code.tgz")).unwrap();
    assert_eq!(got, b"not a dir");

    server.abort();
}

/// `lanfile get http://h/files/<sub>` 直链：当文件拉，落进 local（默认当前目录）。
#[tokio::test]
async fn get_file_direct_link_downloads_it() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("boards.md"), "# boards").unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let url = format!("http://{addr}/files/boards.md");
    let dst = TestDir::new();
    lanfile_pull::run(&[url, dst.root().to_string_lossy().into()])
        .await
        .unwrap();

    let got = std::fs::read(dst.root().join("boards.md")).unwrap();
    assert_eq!(got, b"# boards");

    server.abort();
}

/// `lanfile get` 给一个根本不存在的名字时，目录与文件都 404，报成"远端不存在"。
#[tokio::test]
async fn get_missing_remote_reports_not_found() {
    let dir = TestDir::new();
    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let base = format!("http://{addr}");
    let error = lanfile_pull::run(&[base, "nope".into()]).await.unwrap_err();
    let msg = error.to_string();
    assert!(msg.contains("不存在"), "报错该说明\"不存在\"，实际: {msg}");
    assert!(msg.contains("nope"), "报错该带上远端名，实际: {msg}");

    server.abort();
}

/// `--flat`：不套 basename 一层，目录内容直接落进 local，而非 local/<远端名>/。
#[tokio::test]
async fn get_flat_does_not_nest() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub").join("b.txt"), "bbbb").unwrap();
    std::fs::create_dir_all(dir.root().join("sub").join("deeper")).unwrap();
    std::fs::write(
        dir.root().join("sub").join("deeper").join("c.bin"),
        vec![1_u8, 2, 3, 4, 5],
    )
    .unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let base = format!("http://{addr}");
    let dst = TestDir::new();
    lanfile_pull::run(&[
        base,
        "sub".into(),
        dst.root().to_string_lossy().into(),
        "--flat".into(),
    ])
    .await
    .unwrap();

    // 内容直接落进 dst，不再有 dst/sub/ 这一层
    assert!(!dst.root().join("sub").exists());
    assert_eq!(std::fs::read(dst.root().join("b.txt")).unwrap(), b"bbbb");
    assert_eq!(
        std::fs::read(dst.root().join("deeper").join("c.bin")).unwrap(),
        vec![1_u8, 2, 3, 4, 5]
    );

    server.abort();
}

/// 清单直链的 `--flat` 语义与逐个拉取一致：不套远端目录名这一层。
#[tokio::test]
async fn get_manifest_direct_link_flat_does_not_nest() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub").join("a.txt"), "abc").unwrap();
    std::fs::write(dir.root().join("sub").join("b.bin"), [1, 2, 3]).unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dst = TestDir::new();
    lanfile_pull::run(&[
        format!("http://{addr}/#sub"),
        dst.root().to_string_lossy().into(),
        "--flat".into(),
    ])
    .await
    .unwrap();

    assert!(!dst.root().join("sub").exists());
    assert_eq!(std::fs::read(dst.root().join("a.txt")).unwrap(), b"abc");
    assert_eq!(std::fs::read(dst.root().join("b.bin")).unwrap(), [1, 2, 3]);

    server.abort();
}

/// 分片流由 manifest 建目录，正文流只发文件；空目录和点文件不能在这一步丢掉。
#[tokio::test]
async fn get_stream_shards_preserve_manifest_structure() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub").join("empty")).unwrap();
    std::fs::create_dir_all(dir.root().join("sub").join("deep")).unwrap();
    std::fs::write(dir.root().join("sub").join(".hidden"), "secret").unwrap();
    std::fs::write(dir.root().join("sub").join("deep").join("a.bin"), [1, 2, 3]).unwrap();
    std::fs::write(dir.root().join("sub").join("b.txt"), "abc").unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dst = TestDir::new();
    lanfile_pull::run(&[
        format!("http://{addr}/#sub"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();

    let mirror = dst.root().join("sub");
    assert!(mirror.join("empty").is_dir());
    assert_eq!(std::fs::read(mirror.join(".hidden")).unwrap(), b"secret");
    assert_eq!(
        std::fs::read(mirror.join("deep").join("a.bin")).unwrap(),
        [1, 2, 3]
    );
    assert_eq!(std::fs::read(mirror.join("b.txt")).unwrap(), b"abc");

    std::fs::write(mirror.join("deep").join("a.bin"), [9, 9, 9]).unwrap();
    std::fs::write(mirror.join("b.txt"), "too-long").unwrap();
    lanfile_pull::run(&[
        format!("http://{addr}/#sub"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();

    assert_eq!(
        std::fs::read(mirror.join("deep").join("a.bin")).unwrap(),
        [9, 9, 9]
    );
    assert_eq!(std::fs::read(mirror.join("b.txt")).unwrap(), b"abc");

    server.abort();
}

#[tokio::test]
async fn stream_batch_accepts_empty_index_request() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("a.txt"), "abc").unwrap();
    let (addr, server) = serve_with_sendfile(dir.root().to_path_buf()).await;

    let (head, body) = http_request(addr, "GET", "/stream-batch/", "").await;
    assert!(status_line(&head).contains("200"));
    assert!(body.is_empty());

    server.abort();
}

/// 文件夹直链 `http://h/api/zip/<sub>`、`http://h/#<sub>`：当目录整棵拉，落盘语义与
/// `lanfile get http://h <sub>` 一致（默认套一层）。
#[tokio::test]
async fn get_dir_direct_link_pulls_the_tree() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub").join("deeper")).unwrap();
    std::fs::write(dir.root().join("sub").join("b.txt"), "bbbb").unwrap();
    std::fs::write(
        dir.root().join("sub").join("deeper").join("c.bin"),
        vec![1_u8, 2, 3, 4, 5],
    )
    .unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // /api/zip/<sub> 与 /#<sub> 两种写法都要认。
    for url in [
        format!("http://{addr}/api/zip/sub"),
        format!("http://{addr}/#sub"),
    ] {
        let dst = TestDir::new();
        lanfile_pull::run(&[url, dst.root().to_string_lossy().into()])
            .await
            .unwrap();

        let mirror = dst.root().join("sub");
        assert_eq!(std::fs::read(mirror.join("b.txt")).unwrap(), b"bbbb");
        assert_eq!(
            std::fs::read(mirror.join("deeper").join("c.bin")).unwrap(),
            vec![1_u8, 2, 3, 4, 5]
        );
    }

    server.abort();
}

/// 路径直链 `http://h/<sub>`（如 `/.pi`）：路径就是远端，只拉那棵子树。
/// 回归用：早先无法识别的路径会静默退回裸 host、remote 缺省为空＝拉根，把整棵 share
/// 拖下来（拉根现已禁、会直接报错，但路径认成远端仍是正解）；这里显式断言只拉了子树。
#[tokio::test]
async fn get_path_only_url_pulls_that_subtree_not_the_root() {
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("sub").join("b.txt"), "bbbb").unwrap();
    std::fs::write(dir.root().join("root-only.txt"), "root").unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dst = TestDir::new();
    lanfile_pull::run(&[
        format!("http://{addr}/sub"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();

    assert_eq!(
        std::fs::read(dst.root().join("sub").join("b.txt")).unwrap(),
        b"bbbb"
    );
    assert!(!dst.root().join("lanfile-root").exists());
    assert!(!dst.root().join("root-only.txt").exists());

    server.abort();
}

/// 拉根被禁：`lanfile get http://h`（不给 remote）在连服务端前就报错，啥也不下载。
#[tokio::test]
async fn get_root_is_rejected() {
    let dir = TestDir::new();
    std::fs::write(dir.root().join("root-only.txt"), "root").unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dst = TestDir::new();
    // 不给 remote ＝ 拉根：必须在连服务端前报错，且不碰本地。
    let error = lanfile_pull::run(&[format!("http://{addr}")])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("根目录"),
        "应报“根目录”相关错误，实得: {error}"
    );
    assert_eq!(std::fs::read_dir(dst.root()).unwrap().count(), 0);

    server.abort();
}

/// 大文件必须整段落盘。
///
/// 回归用：客户端原来把 `TcpStream` `into_split`，写半边在请求发完后就出作用域，
/// `OwnedWriteHalf::drop` 会 `shutdown(Write)`。服务端走的 salvo/hyper 默认
/// `half_close = false`，读到这个 EOF 会判定连接中断、丢掉还在飞的响应，
/// 于是 29MB 的文件只落下 3.75MB。两条路径共用 `fetch_file`，一起盯住。
#[tokio::test]
async fn get_large_file_is_not_truncated() {
    let payload: Vec<u8> = (0_u8..=250).cycle().take(8 * 1024 * 1024).collect();
    let dir = TestDir::new();
    std::fs::create_dir_all(dir.root().join("sub")).unwrap();
    std::fs::write(dir.root().join("big.bin"), &payload).unwrap();
    std::fs::write(dir.root().join("sub").join("big.bin"), &payload).unwrap();

    let root = dir.root().to_path_buf();
    let access_log = Arc::new(AccessLog::Off);
    let router = build_router(root.clone(), 8000, Arc::clone(&access_log));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = serve(listener, root, access_log, router).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 单文件直链
    let dst = TestDir::new();
    lanfile_pull::run(&[
        format!("http://{addr}/files/big.bin"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();
    let got = std::fs::read(dst.root().join("big.bin")).unwrap();
    assert_eq!(got.len(), payload.len(), "单文件落盘长度不对");
    assert!(got == payload, "单文件内容不一致");

    // 目录递归（用户报的那条路径）
    let dst = TestDir::new();
    lanfile_pull::run(&[
        format!("http://{addr}/api/zip/sub"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap();
    let got = std::fs::read(dst.root().join("sub").join("big.bin")).unwrap();
    assert_eq!(got.len(), payload.len(), "递归落盘长度不对");
    assert!(got == payload, "递归内容不一致");

    server.abort();
}

/// 服务端谎报长度（声明 100 字节、只发 10 字节就断开）时必须报错，而不是把半截文件
/// 当完整文件留下。
#[tokio::test]
async fn get_truncated_transfer_is_reported_and_discarded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = conn.read(&mut request).await;
        let _ = conn
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n0123456789",
            )
            .await;
        drop(conn);
    });

    let dst = TestDir::new();
    let error = lanfile_pull::run(&[
        format!("http://{addr}/files/x.bin"),
        dst.root().to_string_lossy().into(),
    ])
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("100"),
        "报错要带上应得长度：{error}"
    );
    assert!(
        error.to_string().contains("10"),
        "报错要带上实收长度：{error}"
    );
    assert!(!dst.root().join("x.bin").exists(), "半截文件不能留在盘上");
    server.abort();
}
