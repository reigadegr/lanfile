use std::{
    fs::File,
    io::{Read as _, Write as _},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[cfg(any(target_os = "linux", target_os = "android"))]
use std::mem::MaybeUninit;

use arc_swap::ArcSwap;
use async_zip::{Compression, ZipEntryBuilder, tokio::write::ZipFileWriter};
use futures_lite::io::AsyncWriteExt;
#[cfg(any(target_os = "linux", target_os = "android"))]
use rustix::fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RawDir};
use salvo::{
    http::header::{CONTENT_DISPOSITION, CONTENT_TYPE, HeaderValue},
    prelude::*,
    routing::filters,
};
use serde::Serialize;

mod ip;
mod zip;

use ip::detect_lan_ip;

struct LanIpCache {
    ip: Option<String>,
    fetched_at: Option<Instant>,
}

#[derive(Serialize)]
struct ListEntry {
    name: String,
    #[serde(rename = "type")]
    entry_type: &'static str,
    size: Option<u64>,
    modified: String,
}

#[derive(Serialize)]
struct ListResponse {
    path: String,
    lan_ip: Option<String>,
    port: u16,
    entries: Vec<ListEntry>,
}

#[derive(Serialize)]
struct ManifestEntry {
    path: String,
    #[serde(rename = "type")]
    entry_type: &'static str,
    size: Option<u64>,
}

#[derive(Serialize)]
struct ManifestResponse {
    entries: Vec<ManifestEntry>,
}

struct ListApi {
    root: PathBuf,
    port: u16,
    lan_ip: ArcSwap<LanIpCache>,
}

impl ListApi {
    #[must_use]
    fn new(root: PathBuf, port: u16) -> Self {
        Self {
            root,
            port,
            lan_ip: ArcSwap::new(Arc::new(LanIpCache {
                ip: None,
                fetched_at: None,
            })),
        }
    }

    async fn get_lan_ip(&self) -> Option<String> {
        let cache = self.lan_ip.load();
        if cache
            .fetched_at
            .is_some_and(|t| t.elapsed() < Duration::from_secs(1))
        {
            return cache.ip.clone();
        }
        // 枚举网络接口是阻塞的系统调用，放到阻塞线程池，避免拖慢异步 worker
        let new_ip = tokio::task::spawn_blocking(detect_lan_ip)
            .await
            .ok()
            .flatten();
        self.lan_ip.store(Arc::new(LanIpCache {
            ip: new_ip.clone(),
            fetched_at: Some(Instant::now()),
        }));
        new_ip
    }
}

/// 解析请求路径对应的绝对目录。
fn resolve_under(root: &Path, sub: &str) -> Option<PathBuf> {
    let canonical = root.join(sub).canonicalize().ok()?;
    canonical.starts_with(root).then_some(canonical)
}

/// 目录条目排序：目录在前，同类按名称升序。
fn sort_list_entries(entries: &mut [ListEntry]) {
    entries.sort_unstable_by(|a, b| {
        let a_dir = a.entry_type == "dir";
        let b_dir = b.entry_type == "dir";
        b_dir.cmp(&a_dir).then_with(|| a.name.cmp(&b.name))
    });
}

/// 目录里的一条原始条目：平台原语交给共用逻辑的全部信息。
struct RawEntry {
    /// 条目名（已按 `to_string_lossy` 处理非 UTF-8 字节）
    name: String,
    /// 是否目录
    is_dir: bool,
    /// 文件长度，目录上的取值无意义
    size: u64,
    /// 修改时间；`/api/list` 输出时才格式化
    modified: Option<chrono::DateTime<chrono::Utc>>,
}

/// 平台原语：把 `dir` 下每一条要展示的条目交给 `emit`。
///
/// 契约（[`list_directory`] 完全建立在这三条上）：
/// - 打不开目录返回 `None`；
/// - 不交出 `.` 与 `..`，也不交出符号链接；
/// - 单个条目读不出来就跳过它，绝不因此放弃整次列举。
///
/// 逐条回调而不是先攒成 `Vec`：原版就是一个 `Vec`、一次分配，中间再攒一层等于
/// 每个目录多一次堆分配；回调也让名字的 `String` 从原语直接移进调用方的 `Vec`，
/// 中间没有第二次搬运。闭包会被单态化，机器码与手写展开一致。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn raw_dir_entries(dir: &Path, mut emit: impl FnMut(RawEntry)) -> Option<()> {
    // 1. openat 打开目录 fd
    //    OFlags::DIRECTORY 隐含 is_dir 检查，省 1 次 stat
    let dirfd = rfs::openat(
        rfs::CWD,
        dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .ok()?;

    // 2. RawDir 用栈缓冲遍历（零堆分配，vs std read_dir 内部 Vec）
    let mut buf = [MaybeUninit::<u8>::uninit(); 8192];
    let mut raw_dir = RawDir::new(&dirfd, &mut buf);

    while let Some(entry) = raw_dir.next() {
        let Ok(entry) = entry else {
            continue;
        };
        // 3. 名字按原始字节读取，后续 statat 与 String 分配复用
        let name_cstr = entry.file_name();
        let name_bytes = name_cstr.to_bytes();
        // RawDir 原样返回 . 与 ..，需显式跳过
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        // 4. statat 相对 dirfd 获取类型、size 与 mtime
        let Ok(stat) = rfs::statat(&dirfd, name_cstr, AtFlags::SYMLINK_NOFOLLOW) else {
            continue;
        };
        // 5. 直接读 st_mtime；manifest 不需要展示时间，避免在遍历时格式化和分配
        let actual_ft = FileType::from_raw_mode(stat.st_mode);
        if actual_ft.is_symlink() {
            continue;
        }
        let modified = chrono::DateTime::from_timestamp(stat.st_mtime, 0);
        emit(RawEntry {
            // 名字只分配一次 String（vs 原先 to_string_lossy + to_string 两次分配）
            name: String::from_utf8_lossy(name_bytes).into_owned(),
            is_dir: actual_ft.is_dir(),
            #[allow(clippy::cast_sign_loss)]
            size: stat.st_size as u64,
            modified,
        });
    }
    Some(())
}

/// 非 Linux/Android（Windows、macOS 等）下的目录遍历：`rustix::fs` 的 Linux 专用接口
/// 不可用，改用 `std::fs`；契约与 Linux 版本一致。
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn raw_dir_entries(dir: &Path, mut emit: impl FnMut(RawEntry)) -> Option<()> {
    let read_dir = std::fs::read_dir(dir).ok()?;
    for entry in read_dir {
        let Ok(entry) = entry else {
            continue;
        };
        let Ok(metadata) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        let ft = metadata.file_type();
        if ft.is_symlink() {
            continue;
        }
        let modified = metadata
            .modified()
            .ok()
            .map(chrono::DateTime::<chrono::Utc>::from);
        emit(RawEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            is_dir: ft.is_dir(),
            size: metadata.len(),
            modified,
        });
    }
    Some(())
}

/// 枚举目录并返回排序后的条目；路径非法、非目录或打不开返回 `None`。
///
/// 平台差异全部收在 [`raw_dir_entries`] 这一条原语里，剩下的取舍（目录不带 size）
/// 与排序两个平台共用同一份代码；原语按回调逐条交来，这里边收边填，不攒中间 `Vec`。
///
/// 闭包按值传：`impl FnMut` 收的就是所有权，按值传时参数类型能从 `FnMut(RawEntry)`
/// 直接推出来；写成 `&mut |entry| …` 则要先过一层 `&mut Closure: FnMut` 的 impl，
/// 闭包自身参数类型在这个位置推不出来（E0282）。两者单态化后机器码一致。
///
/// 全程是同步阻塞的 fs 操作，应由调用方放进 `spawn_blocking`，避免拖慢异步 worker。
fn list_directory(root: &Path, path: &str) -> Option<Vec<ListEntry>> {
    let dir = resolve_under(root, path)?;
    let mut list_entries: Vec<ListEntry> = Vec::with_capacity(64);
    raw_dir_entries(&dir, |entry| {
        let is_dir = entry.is_dir;
        let size = if is_dir { None } else { Some(entry.size) };
        list_entries.push(ListEntry {
            name: entry.name,
            entry_type: if is_dir { "dir" } else { "file" },
            size,
            modified: entry
                .modified
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
                .unwrap_or_default(),
        });
    })?;
    sort_list_entries(&mut list_entries);
    Some(list_entries)
}

/// 递归收集整棵树的元数据，不打开文件正文。
fn manifest_directory(root: &Path, path: &str) -> Option<Vec<ManifestEntry>> {
    let dir = resolve_under(root, path)?;
    let mut entries = Vec::new();
    collect_manifest(&dir, "", &mut entries)?;
    Some(entries)
}

fn collect_manifest(dir: &Path, prefix: &str, entries: &mut Vec<ManifestEntry>) -> Option<()> {
    raw_dir_entries(dir, |entry| {
        let path = if prefix.is_empty() {
            entry.name.clone()
        } else {
            format!("{prefix}/{}", entry.name)
        };
        if entry.is_dir {
            entries.push(ManifestEntry {
                path: path.clone(),
                entry_type: "dir",
                size: None,
            });
            let child = dir.join(&entry.name);
            // 打不开的子目录沿用流式遍历语义：跳过该目录，继续拉其他内容。
            let _ = collect_manifest(&child, &path, entries);
        } else {
            entries.push(ManifestEntry {
                path,
                entry_type: "file",
                size: Some(entry.size),
            });
        }
    })?;
    Some(())
}

#[handler]
impl ListApi {
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn handle(&self, req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        let path = req.param::<String>("path").unwrap_or_default();
        let root = self.root.clone();
        // 空路径时 `format!("/{path}")` 就是 `"/"`，不必再分一条分支
        let display_path = format!("/{path}");

        // 目录枚举是阻塞的 fs 操作，整体放进阻塞线程池
        let Ok(listed) = tokio::task::spawn_blocking(move || list_directory(&root, &path)).await
        else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            return;
        };
        let Some(entries) = listed else {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        };

        let response = ListResponse {
            path: display_path,
            lan_ip: self.get_lan_ip().await,
            port: self.port,
            entries,
        };
        res.render(Json(response));
    }
}

struct ManifestApi {
    root: PathBuf,
}

#[handler]
impl ManifestApi {
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn handle(&self, req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        let path = req.param::<String>("path").unwrap_or_default();
        let root = self.root.clone();
        let Ok(manifest) =
            tokio::task::spawn_blocking(move || manifest_directory(&root, &path)).await
        else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            return;
        };
        let Some(entries) = manifest else {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        };
        res.render(Json(ManifestResponse { entries }));
    }
}

// ---- zip 打包流水线 ----

/// zip 打包流水线里，阻塞遍历线程发往异步写线程的一条消息。
///
/// 文件内容由遍历线程自己 `read` 后分块发出，异步侧不再碰 `tokio::fs`：
/// 每次读盘只花一次 `read`，不必再付一次 `spawn_blocking` 派发，
/// 也不必让 tokio 先把数据读进自己的缓冲、再整块拷到调用方的缓冲。
///
/// 协议：`FileStart` 之后只会跟同一文件的若干 `Chunk`，直到 `FileEnd`；装得下一块的文件
/// 直接发一条自足的 `Whole`，不再走 `FileStart`/`Chunk`/`FileEnd` 三段。
/// `Chunk` 与 `Whole` 的 `data` 始终保持 `ZIP_CHUNK` 满长（便于消费侧原样归还后复用），
/// 有效字节数分别是 `Chunk.len` 与 `Whole.len`。
enum Item {
    Dir {
        name: String,
    },
    /// 小于一块的文件：一次读全，异步侧走 `write_entry_whole`
    Whole {
        name: String,
        data: Vec<u8>,
        len: usize,
    },
    FileStart {
        name: String,
    },
    Chunk {
        data: Vec<u8>,
        len: usize,
    },
    FileEnd,
}

/// 每次读盘发送的字节数，也是流水线的拷贝粒度。
const ZIP_CHUNK: usize = 262_144;

/// 有界队列深度；内存上界约为 `(ZIP_QUEUE + 2) * ZIP_CHUNK`（约 2.5 MiB）——
/// 消息队列最多压 `ZIP_QUEUE` 块，再加上生产、消费两侧各自手上的一块。
const ZIP_QUEUE: usize = 8;

/// 交给 channel 之前的攒批大小。
///
/// `async_zip` 写一个条目会按字段分成很多次小写（本地头、数据、数据描述符、中央目录…），
/// 每次小写经 channel 都会变成一个 chunked 分帧加一次 `sendto`——实测一个 604 字节的
/// 归档打出了 134 次 `sendto`，内核态因此占了 74%。先在内存里攒够再交出去。
const ZIP_FLUSH: usize = 64 * 1024;

struct ZipApi {
    root: PathBuf,
}

impl ZipApi {
    #[must_use]
    const fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

#[handler]
impl ZipApi {
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn handle(&self, req: &mut Request, _depot: &mut Depot, res: &mut Response) {
        let path = req.param::<String>("path").unwrap_or_default();
        let root = self.root.clone();

        // 解析路径 + 类型判断是阻塞 fs 操作，放进阻塞线程池
        let resolved = tokio::task::spawn_blocking(move || {
            let canonical = resolve_under(&root, &path)?;
            canonical.is_dir().then_some(canonical)
        })
        .await;

        // 拆成两次 let-else：500 与 404 两条分支彼此独立，比原来的三分支 match 更直白
        let Ok(resolved) = resolved else {
            res.status_code(StatusCode::INTERNAL_SERVER_ERROR);
            return;
        };
        let Some(canonical) = resolved else {
            res.status_code(StatusCode::NOT_FOUND);
            return;
        };

        let folder_name = zip::folder_name(&canonical);
        res.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
        if let Ok(val) = HeaderValue::from_str(&zip::content_disposition(&folder_name)) {
            res.headers_mut().insert(CONTENT_DISPOSITION, val);
        }

        // 边遍历边流式打包，不先把整棵树攒进内存：
        // - 阻塞遍历线程自己读文件内容，通过有界 channel 逐块发 Item（有界 = 内存封顶）
        // - 异步写 zip 在 tokio 里，逐条收 Item 写入
        // - 空缓冲经 free channel 回传复用，见 send_file_chunks
        let (item_tx, mut item_rx) = tokio::sync::mpsc::channel::<Item>(ZIP_QUEUE);
        let (free_tx, mut free_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(ZIP_QUEUE);
        tokio::task::spawn_blocking(move || {
            zip::walk(&canonical, &folder_name, &mut |entry| match entry {
                zip::Entry::Dir { name } => item_tx.blocking_send(Item::Dir { name }).is_ok(),
                zip::Entry::File { file, size, name } => {
                    send_one_file(&item_tx, &mut free_rx, file, size, name)
                }
            });
        });

        let tx = res.channel();
        tokio::spawn(async move {
            let mut writer =
                ZipFileWriter::with_tokio(tokio::io::BufWriter::with_capacity(ZIP_FLUSH, tx));
            while let Some(item) = item_rx.recv().await {
                match item {
                    Item::Dir { name } => {
                        // 目录条目：名字以 / 结尾、置 S_IFDIR 权限位，解压后保留空目录结构
                        let dir = ZipEntryBuilder::new(name.into(), Compression::Stored)
                            .unix_permissions(0o40755);
                        if writer.write_entry_whole(dir, &[]).await.is_err() {
                            return;
                        }
                    }
                    Item::Whole { name, data, len } => {
                        let entry = ZipEntryBuilder::new(name.into(), Compression::Stored);
                        if writer.write_entry_whole(entry, &data[..len]).await.is_err() {
                            return;
                        }
                        // 池满（消费快于生产）就丢弃，只是少一次复用，不影响正确性
                        let _ = free_tx.try_send(data);
                    }
                    Item::FileStart { name } => {
                        let entry = ZipEntryBuilder::new(name.into(), Compression::Stored);
                        let Ok(mut ew) = writer.write_entry_stream(entry).await else {
                            return;
                        };
                        // 遍历线程保证 FileStart 与 FileEnd 之间只会出现 Chunk；
                        // 收到 FileEnd 或 channel 关闭（遍历线程已退出）都收尾。
                        // write_all 返回时数据已被拷进 body，缓冲可以安全归还复用。
                        while let Some(Item::Chunk { data, len }) = item_rx.recv().await {
                            if ew.write_all(&data[..len]).await.is_err() {
                                return;
                            }
                            // 池满（消费快于生产）就丢弃，只是少一次复用，不影响正确性
                            let _ = free_tx.try_send(data);
                        }
                        if ew.close().await.is_err() {
                            return;
                        }
                    }
                    // 按协议不会单独出现：Chunk / FileEnd 已在上面就地消费
                    Item::Chunk { .. } | Item::FileEnd => {}
                }
            }
            // `close` 把中央目录写完并把内部 writer 还回来，还得再 flush 一次，
            // 否则攒在 `BufWriter` 里的尾巴会随任务结束一起丢掉
            if let Ok(mut buffered) = writer.close().await {
                let _ = futures_lite::AsyncWriteExt::flush(&mut buffered).await;
            }
        });
    }
}

// ---- /stream-batch：走快路径的清单分片流端点，正文 sendfile(2) ----

/// 协议标记：文件 1，路径以 NUL 结尾，随后跟 8 字节小端长度 + 正文。
const STREAM_FILE: u8 = 1;

/// 服务端头部缓冲区的大小，也是「小文件直接塞进缓冲区」的上限。
///
/// 头部分（类型字节 + NUL 结尾路径 + 8 字节长度）每条不到一百字节，逐条 `write_all`
/// 会把一个 8 万文件的树打成几十万次 `write` 系统调用——在回环/局域网上远超正文开销。
/// 这里把所有头部分和小于阈值的小文件内容都攒进一个缓冲区，凑够一次写出。
const STREAM_BUF: usize = 64 * 1024;

/// 小于这个大小的文件不单独走 `sendfile`，内容读进 [`STREAM_BUF`] 与头部一起发出。
///
/// `sendfile` 对小文件是净亏：一次 `sendfile` 调用的固定开销远大于它省下的那次用户态
/// 拷贝（回环上尤其明显，内核本来就要把数据拷进接收端 socket 缓冲）。小文件走
/// 「读进用户态缓冲 + 与头部一起 write」，一次 `write` 能覆盖几十条记录。
const SENDFILE_THRESHOLD: u64 = 64 * 1024;

/// 遍历线程交给发送线程的最多条目数。
///
/// 队列有界，既限制同时打开的文件 fd，也让发送慢时反过来压住目录遍历。64 个条目足以
/// 覆盖网络侧正在写的一批记录，同时不会在深树上占住过多 fd。
const STREAM_ENTRY_QUEUE: usize = 64;

/// `/stream-batch` 发送线程拿到的一条已打开文件。
struct StreamFile {
    file: File,
    size: u64,
    name: String,
}

/// 从文件开头读满 `buf`，不改动文件偏移量（Unix 用 `pread`，非 Unix 复制 fd 后用 `seek`）。
///
/// `/stream-batch` 的小文件走这里：`openat(dirfd, ...)` 拿到的 fd 是共享给 `sendfile` 的，
/// 用 `pread` 读不会移动偏移量；非 Unix 没有 `pread`，复制一份 fd 后 `seek` 到 0 再读。
fn read_file_prefix(file: &File, buf: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, 0)
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut owned = file.try_clone()?;
        owned.seek(SeekFrom::Start(0))?;
        owned.read_exact(buf)
    }
}

/// 服务 `/stream-batch/<sub>`：按请求体中的 manifest 索引发文件。
///
/// 目录由客户端根据 manifest 创建，这里不发送目录记录。请求体每条记录为
/// `路径 NUL`，服务端不再为每个分片重复遍历目录树。
pub fn serve_stream_batch(
    socket: &mut TcpStream,
    root: &Path,
    sub: &str,
    body: &[u8],
) -> std::io::Result<()> {
    match parse_stream_batch_body(body) {
        Ok(entries) => serve_stream_batch_inner(socket, root, sub, &entries),
        Err(_) => socket.write_all(
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ),
    }
}

fn parse_stream_batch_body(body: &[u8]) -> std::io::Result<Vec<String>> {
    let invalid =
        |message: &'static str| std::io::Error::new(std::io::ErrorKind::InvalidData, message);
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < body.len() {
        let Some(nul) = body[offset..].iter().position(|&byte| byte == 0) else {
            return Err(invalid("stream-batch 路径缺少 NUL"));
        };
        let path_end = offset + nul;
        let path = std::str::from_utf8(&body[offset..path_end])
            .map_err(|_| invalid("stream-batch 路径不是 UTF-8"))?;
        if path.is_empty() {
            return Err(invalid("stream-batch 路径为空"));
        }
        entries.push(path.to_owned());
        offset = path_end + 1;
    }
    Ok(entries)
}

fn send_batch_entries(target: &Path, entries: &[String], entry_tx: &mpsc::SyncSender<StreamFile>) {
    for rel in entries {
        let Some((file, size)) = zip::open_file_under(target, rel) else {
            continue;
        };
        let entry = StreamFile {
            file,
            size,
            name: rel.clone(),
        };
        if entry_tx.send(entry).is_err() {
            break;
        }
    }
}

fn serve_stream_batch_inner(
    socket: &mut TcpStream,
    root: &Path,
    sub: &str,
    entries: &[String],
) -> std::io::Result<()> {
    let target = resolve_under(root, sub).filter(|path| path.is_dir());
    let Some(target) = target else {
        socket.write_all(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    };

    // 生产线程把已打开的文件交过来；发送回调出错时返回 `false` 并退出接收循环。
    let mut error: Option<std::io::Error> = None;
    // 头 + 小文件内容的攒批缓冲。小于阈值的小文件内容也读进来，跟头一起写出。
    let mut head_buf: Vec<u8> = Vec::with_capacity(STREAM_BUF);
    head_buf.extend_from_slice(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
    );
    let mut copy_buf = Vec::new();

    let mut send_entry = |entry: StreamFile| {
        if error.is_some() {
            return false;
        }
        // 内层闭包返回 `Result`，`?` 就能用了
        let result: std::io::Result<()> = (|| {
            // 缓冲区够大就先刷一次：即使条目都是长路径，它也不会无界增长
            if head_buf.len() >= STREAM_BUF / 2 {
                socket.write_all(&head_buf)?;
                head_buf.clear();
            }

            let StreamFile { file, size, name } = entry;
            let rel = name.trim_start_matches('/');
            // 头部三个 write_all 合并进缓冲区一次写出
            head_buf.push(STREAM_FILE);
            head_buf.extend_from_slice(rel.as_bytes());
            head_buf.push(0);
            head_buf.extend_from_slice(&size.to_le_bytes());

            if size < SENDFILE_THRESHOLD {
                // 小文件：内容读进缓冲区，与头部一起发。
                // 先把 head_buf 扩到够长，再 `read_exact_at` 填进去。
                let start = head_buf.len();
                head_buf.resize(start + size as usize, 0);
                read_file_prefix(&file, &mut head_buf[start..])?;
                // 单个小文件可能把缓冲区顶满，顺手刷一次；不刷下一轮开头也会刷
                if head_buf.len() >= STREAM_BUF {
                    socket.write_all(&head_buf)?;
                    head_buf.clear();
                }
            } else {
                // 大文件：头部先出去，正文交给 sendfile。
                // 先刷头部能保证长度那 8 字节跟头部分一起落到
                // 对端缓冲，不会跟随后的 sendfile 抢一个 MSS 段。
                socket.write_all(&head_buf)?;
                head_buf.clear();
                sendfile_all(socket, &file, size, &mut copy_buf)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => true,
            Err(e) => {
                error = Some(e);
                false
            }
        }
    };

    let (entry_tx, entry_rx) = mpsc::sync_channel(STREAM_ENTRY_QUEUE);
    let producer_panicked = thread::scope(|scope| {
        let producer = scope.spawn(move || {
            send_batch_entries(&target, entries, &entry_tx);
            true
        });

        while let Ok(entry) = entry_rx.recv() {
            if !send_entry(entry) {
                break;
            }
        }
        // 先丢掉接收端，阻塞在 send 上的生产线程才能拿到错误并停止。
        drop(entry_rx);
        producer.join().is_err()
    });
    if producer_panicked && error.is_none() {
        error = Some(std::io::Error::other(
            "stream-batch producer thread panicked",
        ));
    }

    // 收尾：把缓冲区里最后那点没写出去的头/内容刷出去
    if error.is_none()
        && !head_buf.is_empty()
        && let Err(e) = socket.write_all(&head_buf)
    {
        error = Some(e);
    }

    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// 把 `file` 从 `offset` 起、共 `remaining` 字节写到 socket。
///
/// `sendfile`/`splice` 不可用时的用户态兜底：非 Linux 平台直接用它，Linux/Android 上
/// 文件系统不支持 `sendfile`（fuse 类挂载等会回 `EINVAL`）时也回退到这里。
///
/// 用 `pread`（Unix）/ `try_clone` + `seek`（其他）读文件，不动共享的文件偏移量：
/// `/stream-batch` 的 fd 是 `openat` 拿到的，可能还有其他引用；移动偏移量会互相干扰。
fn copy_user(
    mut socket: &TcpStream,
    file: &File,
    offset: u64,
    remaining: u64,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut offset = offset;
    let mut remaining = remaining;
    if buf.len() != STREAM_BUF {
        buf.resize(STREAM_BUF, 0);
    }
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = read_at(file, &mut buf[..want], offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "文件在发送途中缩短",
            ));
        }
        socket.write_all(&buf[..n])?;
        offset += n as u64;
        remaining -= n as u64;
    }
    Ok(())
}

/// 从文件 `offset` 处读最多 `buf.len()` 字节，返回实读字节数。不改动文件偏移量。
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut owned = file.try_clone()?;
        owned.seek(SeekFrom::Start(offset))?;
        owned.read(buf)
    }
}

/// 用 `sendfile(2)` 把 `file` 全部 `size` 字节写到 socket；不可用时回退到用户态读写。
///
/// 编译期：非 Linux/Android 直接走 [`copy_user`]。
/// 运行时：Linux/Android 上若 `sendfile` 因为文件系统不支持而回 `EINVAL`/`ENOSYS`/
/// `EOPNOTSUPP`，已经搬走的字节数保留在 `offset` 里，剩下的交给 [`copy_user`] 继续——
/// 这样即便根目录挂在 fuse 上，`/stream-batch` 也不会中途断连。
#[cfg(any(target_os = "linux", target_os = "android"))]
fn sendfile_all(
    socket: &TcpStream,
    file: &File,
    size: u64,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    let mut offset = 0_u64;
    let mut remaining = size;
    while remaining > 0 {
        match rustix::fs::sendfile(socket, file, Some(&mut offset), remaining as usize) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "文件在发送途中缩短",
                ));
            }
            Ok(n) => remaining -= n as u64,
            Err(rustix::io::Errno::INTR) => (),
            // 文件系统不支持 sendfile（fuse 类挂载、部分网络文件系统等）：回退到
            // 用户态读写，从已经搬走的 offset 接着发，已经写出去的头不会被破坏
            Err(e)
                if e == rustix::io::Errno::INVAL
                    || e == rustix::io::Errno::NOSYS
                    || e == rustix::io::Errno::OPNOTSUPP =>
            {
                return copy_user(socket, file, offset, remaining, buf);
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn sendfile_all(
    socket: &TcpStream,
    file: &File,
    size: u64,
    buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    copy_user(socket, file, 0, size, buf)
}

/// 发一个文件：装得下一块就走整条目写入，否则流式分块。返回是否应继续遍历。
///
/// 整条目写入把大小与 CRC 直接写进本地头，省掉流式那条数据描述符和收尾往返。
fn send_one_file(
    item_tx: &tokio::sync::mpsc::Sender<Item>,
    free_rx: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    file: File,
    size: u64,
    name: String,
) -> bool {
    let mut f = file;
    // 内核顺序读提示：扩大预读窗口，大文件连续传输更快；仅设置标志、立即返回
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let _ = rustix::fs::fadvise(&f, 0, None, rustix::fs::Advice::Sequential);
    let is_small = size < ZIP_CHUNK as u64;
    if is_small {
        let (buf, len) = read_first_chunk(&mut f, free_rx);
        if len < ZIP_CHUNK {
            return item_tx
                .blocking_send(Item::Whole {
                    name,
                    data: buf,
                    len,
                })
                .is_ok();
        }
        // 读满说明文件在 fstat 之后长大了：已读那块先发出去再接着流式读完，
        // 直接退回流式会把这一段丢掉
        if item_tx.blocking_send(Item::FileStart { name }).is_err() {
            return false;
        }
        if item_tx
            .blocking_send(Item::Chunk { data: buf, len })
            .is_err()
        {
            return false;
        }
    } else if item_tx.blocking_send(Item::FileStart { name }).is_err() {
        return false;
    }
    send_file_chunks(item_tx, free_rx, &mut f, ZIP_CHUNK)
        && item_tx.blocking_send(Item::FileEnd).is_ok()
}

/// 读第一块：从空闲池取一块满长缓冲，读到读满或读到 EOF 为止。
///
/// 返回的有效长度等于 `ZIP_CHUNK` 时说明文件一块装不下（或它在 `fstat` 之后长大了），
/// 调用方应继续走流式路径。
fn read_first_chunk(
    file: &mut File,
    free_rx: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> (Vec<u8>, usize) {
    let mut buf = free_rx.try_recv().unwrap_or_else(|_| vec![0_u8; ZIP_CHUNK]);
    let mut len = 0;
    while len < ZIP_CHUNK {
        match file.read(&mut buf[len..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => len += n,
        }
    }
    (buf, len)
}

/// 阻塞线程内顺序读文件，按 `chunk_size` 分块发往异步侧；返回 `false` 表示 channel 已关闭、应停止遍历。
///
/// 缓冲优先取 `free_rx` 里消费侧归还的空缓冲，取不到才新建。`vec![0u8; n]` 走
/// `alloc_zeroed`，实测 256 KiB 一次约 2.1 µs、其中 98% 是清零；归还的缓冲保持满长，
/// 所以复用既省掉分配也省掉清零，且读入前不需要 `resize`（那等于把清零做回来）。
/// 读错与读到 EOF 都按读完了收尾（返回 `true`），不再继续遍历。
fn send_file_chunks(
    tx: &tokio::sync::mpsc::Sender<Item>,
    free_rx: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
    file: &mut File,
    chunk_size: usize,
) -> bool {
    loop {
        let mut buf = free_rx
            .try_recv()
            .unwrap_or_else(|_| vec![0_u8; chunk_size]);
        // 归还的缓冲始终满长，这里只读不截断，才能原样复用
        debug_assert_eq!(buf.len(), chunk_size);
        match file.read(&mut buf) {
            Ok(0) | Err(_) => return true,
            Ok(n) => {
                if tx.blocking_send(Item::Chunk { data: buf, len: n }).is_err() {
                    return false;
                }
            }
        }
    }
}

#[must_use]
pub fn list_routes(root: PathBuf, port: u16) -> Router {
    Router::new()
        .push(
            Router::with_path("/api/list/{**path}")
                .filter(filters::get())
                .goal(ListApi::new(root.clone(), port)),
        )
        .push(
            Router::with_path("/api/zip/{**path}")
                .filter(filters::get())
                .goal(ZipApi::new(root.clone())),
        )
        .push(
            Router::with_path("/api/manifest/{**path}")
                .filter(filters::get())
                .goal(ManifestApi { root }),
        )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{fs::File, io::Write as _, path::Path, time::Instant};

    use tokio::sync::mpsc;

    use super::{Item, ZIP_QUEUE, parse_stream_batch_body, send_file_chunks};

    const FILE_SIZE: usize = 64 * 1024 * 1024;

    #[test]
    fn parse_stream_batch_body_accepts_compact_entries() {
        let mut body = Vec::new();
        for path in ["a.txt", "dir/b.bin"] {
            body.extend_from_slice(path.as_bytes());
            body.push(0);
        }

        assert_eq!(
            parse_stream_batch_body(&body).unwrap(),
            vec!["a.txt".to_owned(), "dir/b.bin".to_owned()]
        );
    }

    #[test]
    fn parse_stream_batch_body_rejects_bad_entries() {
        for body in [&b"a.txt"[..], &b"\0"[..]] {
            assert!(parse_stream_batch_body(body).is_err());
        }
    }

    /// 复刻流水线的读取侧：阻塞线程分块读文件 → 有界 channel → 异步侧消费并归还缓冲。
    /// 生产路径固定用 `ZIP_CHUNK`，这里开放 `chunk_size` 只为观察分块大小对吞吐的影响。
    async fn copy_throughput(chunk_size: usize, path: &Path) -> f64 {
        let (tx, mut rx) = mpsc::channel::<Item>(ZIP_QUEUE);
        let (free_tx, mut free_rx) = mpsc::channel::<Vec<u8>>(ZIP_QUEUE);
        let path = path.to_path_buf();
        let producer = tokio::task::spawn_blocking(move || {
            let mut f = File::open(&path).unwrap();
            let _ = send_file_chunks(&tx, &mut free_rx, &mut f, chunk_size);
        });

        let start = Instant::now();
        while let Some(Item::Chunk { data, .. }) = rx.recv().await {
            let _ = free_tx.try_send(data);
        }
        let elapsed = start.elapsed().as_secs_f64();
        producer.await.unwrap();

        FILE_SIZE as f64 / (1024.0 * 1024.0) / elapsed
    }

    #[tokio::test]
    #[ignore = "微基准，需 cargo test --release -- --ignored 显式运行"]
    async fn zip_copy_throughput_by_buffer_size() {
        let path = std::env::temp_dir().join(format!("lanfile-perf-{}", std::process::id()));
        let mut f = File::create(&path).unwrap();
        let chunk = vec![0xABu8; 1024 * 1024];
        let mut remaining = FILE_SIZE;
        while remaining > 0 {
            f.write_all(&chunk).unwrap();
            remaining -= chunk.len();
        }
        drop(f);

        let mut total_64k = 0.0;
        let mut total_256k = 0.0;

        let mut total_512k = 0.0;
        let mut total_1024k = 0.0;

        for _ in 0..10 {
            total_64k += copy_throughput(64 * 1024, &path).await;
            total_256k += copy_throughput(256 * 1024, &path).await;
            total_512k += copy_throughput(512 * 1024, &path).await;
            total_1024k += copy_throughput(1024 * 1024, &path).await;
        }
        let avg_64k = total_64k / 10.0;
        let avg_256k = total_256k / 10.0;

        let avg_512k = total_512k / 10.0;
        let avg_1024k = total_1024k / 10.0;

        println!("zip 拷贝吞吐 64KB 缓冲: {avg_64k:.1} MB/s");
        println!("zip 拷贝吞吐 256KB 缓冲: {avg_256k:.1} MB/s");

        println!("zip 拷贝吞吐 512KB 缓冲: {avg_512k:.1} MB/s");
        println!("zip 拷贝吞吐 1024KB 缓冲: {avg_1024k:.1} MB/s");

        std::fs::remove_file(&path).unwrap();

        assert!(
            avg_256k >= avg_64k * 0.8,
            "256KB 缓冲吞吐不应显著低于 64KB: {avg_64k:.1} vs {avg_256k:.1} MB/s",
        );
    }
}
