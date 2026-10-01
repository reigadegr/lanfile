//! 已打开文件的缓存。
//!
//! 每个请求都要按路径 `open` 一次文件，而这条路径上还有 `canonicalize`（对每一层路径各一次
//! `readlink`）、`fstat`、`fadvise` 和字符集嗅探的一次 `pread`。把已经打开、已经 `fstat` 过
//! 并验证过的 fd 连同它的元数据与解析出来的 `Content-Type` 按请求路径缓存起来，命中时只克隆
//! 几个 `Arc`：上面那几次系统调用一个都不用做，过期后也只需先补一次 `lstat` 校验（见下）。
//!
//! 缓存里存的是 **fd 而不是内容**，读到的永远是文件当前的内容；命中要求 `(ino, size, mtime)`
//! 与请求时 `lstat` 到的元数据完全一致，所以文件被改动、替换或删除都会未命中并重新解析。
//! `Content-Length`、`Last-Modified`、`ETag` 都取自这份元数据，而它正是命中时那个 fd 自己的
//! `fstat` 结果。
//!
//! 那次校验用的 `lstat` 是每请求一次系统调用，在这台没有 TSC 的机器上要 1.03 µs（占每请求
//! CPU 的 3%）。所以校验结果带一个 [`REVALIDATE_MILLIS`] 的时间戳：这段时间内同一路径直接
//! 返回缓存，连 `lstat` 都不做；过期后第一个请求重新校验并把时间戳刷新回来。代价是最多这么长
//! 的陈旧窗口——文件被原地改写或被 rename 替换后，最长这段时间内仍按旧元数据与旧 fd 响应。
//! 其中"原地改写"会让 `Content-Length` 与正文长度对不上：文件在有效期内被截断时，客户端会
//! 收到一个短正文并看到连接被关闭。校验与发送之间本来就有同类竞态，TTL 只是把这个窗口从微秒
//! 级拉宽到最长 1 秒。
//!
//! 并发上用 [`SHARDS`] 把一把全局锁拆成多把：路径按哈希固定落在其中一片，请求只在这一片上
//! 竞争，锁里只做查找与拷贝。条目与响应体共享同一个 fd（`Arc<File>`），命中与写入都不再
//! `dup`：sendfile 带显式 offset，共享文件描述符本来就是安全的。
//!
//! 淘汰按 LRU：每片记一个自增序号，命中时刷新该条的序号，满了就淘汰序号最小的那条。每片只有
//! [`CAPACITY_PER_SHARD`] 条，线性扫一遍比维护链表简单得多；也避免了"满了全清"把热文件一起
//! 丢掉——下载大量不同文件时，全清会让命中率归零，缓存反而比不缓存慢。

use std::path::Path;
use std::{
    fs::{File, Metadata},
    hash::{Hash, Hasher},
    os::unix::fs::MetadataExt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use hashbrown::HashMap;
use hashbrown::hash_map::RawEntryMut;
use lanfile_namedfile::FileMeta;
use rustc_hash::{FxBuildHasher, FxHasher};

use crate::CachedHeaders;

/// 用 `FxHash` 的 HashMap：与原来 `rustc_hash::FxHashMap` 同样是 FxHasher，但底座是
/// hashbrown 而不是 std，从而有 `raw_entry_mut` 可以复用预计算好的哈希。
type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// 分片数：把一把全局锁拆成 16 把
const SHARDS: usize = 16;
/// 每片的条目上限（总数 512 不变）：fd 数量必须有硬上限
const CAPACITY_PER_SHARD: usize = 32;
/// 校验结果的有效期（毫秒）：这段时间内同一路径不再 `lstat`。
///
/// 每请求一次 `statx` 在这台机器上要 1.03 µs，而下面这个 coarse 时钟只要 3 ns，所以只要一个
/// 文件在有效期内被请求两次以上，省下的就远多于多读一次时钟。取 1 秒是"陈旧窗口"与"校验
/// 频率"的折中：热文件每秒校验一次，与访问频率无关（NFS 默认的属性缓存是 3~60 秒）。
const REVALIDATE_MILLIS: i64 = 1000;

/// 单调粗时钟的毫秒值。
///
/// 必须用 coarse 变体：这台机器没有 TSC，非 coarse 的 `clock_gettime` 走不了 vDSO 的快路径
/// （实测 1.2 µs，比它要省掉的那次 `statx` 还贵），coarse 变体直接读 vvar（实测 3 ns）。
/// 单调时钟不会被改表，时间戳只会前进，两次读之间的差不会为负。
fn now_millis() -> i64 {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::MonotonicCoarse);
    now.tv_sec * 1000 + now.tv_nsec / 1_000_000
}

/// 取锁，中毒也照常返回：临界区里只有 `Vec`/`HashMap` 增删查与 `Arc` 克隆，
/// 中毒后内部数据仍结构完整，继续用是安全的——与 `main.rs` 的 `lock` 语义对齐。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Entry {
    /// 与响应体共享的 fd：命中时只克隆 `Arc`，不再 `dup`
    file: Arc<File>,
    /// 这个 fd 自己的 `fstat` 结果，命中时直接交给 `NamedFile`，省掉每请求一次 `fstat`
    metadata: FileMeta,
    /// 已经编码好的响应头。装一层 `Arc`：命中时只加一次引用计数，不再深拷贝 `Mime` 里的 `String`
    headers: Arc<CachedHeaders>,
    /// 拼好的绝对路径。它是 (root, sub) 的纯函数，存下来命中时就不必每请求再拼一次
    joined: Arc<Path>,
    /// 最近一次被用到的序号，淘汰时取最小的那条
    used: u64,
    /// 最近一次校验（[`FileCache::get`]）或写入（[`FileCache::insert`]）的时刻，
    /// 用来判断这个条目是否还在 [`REVALIDATE_MILLIS`] 的有效期内
    validated_at: i64,
}

/// 一片：条目表 + 该片自己的 LRU 序号（片内单调递增，不需要原子操作）
#[derive(Default)]
struct Shard {
    entries: FxHashMap<Box<str>, Entry>,
    clock: u64,
}

/// 请求路径 -> 已打开的文件。分片后每片一把锁。
#[derive(Default)]
pub struct FileCache {
    shards: [Mutex<Shard>; SHARDS],
}

/// 路径哈希：同一路径永远落在同一片，分片索引与 `HashMap` 查找共用同一份哈希。
fn path_hash(path: &str) -> u64 {
    let mut hasher = FxHasher::default();
    path.hash(&mut hasher);
    hasher.finish()
}

/// 根据哈希值计算落在哪个分片
#[inline]
const fn shard_index(hash: u64) -> usize {
    (hash as usize) % SHARDS
}

/// 一次命中交出去的四样东西：拼好的路径、fd、元数据、已经编码好的响应头。
pub struct CacheHit {
    pub joined: Arc<Path>,
    pub file: Arc<File>,
    pub metadata: FileMeta,
    pub headers: Arc<CachedHeaders>,
}

/// 从命中的条目里取出要交出去的东西，并刷新它的 LRU 序号
fn take(entry: &mut Entry, clock: u64) -> CacheHit {
    entry.used = clock;
    // 锁里只做拷贝：克隆三个 `Arc` 加一份纯数据的元数据，没有系统调用，也没有堆分配
    CacheHit {
        joined: Arc::clone(&entry.joined),
        file: Arc::clone(&entry.file),
        metadata: entry.metadata.clone(),
        headers: Arc::clone(&entry.headers),
    }
}

impl FileCache {
    const fn shard(&self, hash: u64) -> &Mutex<Shard> {
        &self.shards[shard_index(hash)]
    }

    /// 在分片里找到 `path` 对应的条目，交给 `check` 判断是否可用；可用就刷新 LRU 序号并取出。
    ///
    /// `check` 只在真正查到条目后调用一次，返回 `false` 表示这次不算命中。`get` 的
    /// `check` 还需要顺便刷新有效期，所以它拿到的是 `&mut Entry`。
    ///
    /// 闭包会被单态化并内联，因此两条调用路径（`get_fresh` / `get`）的机器码与原先
    /// 各自展开的实现一致。
    #[inline]
    fn lookup<F>(&self, path: &str, check: F) -> Option<CacheHit>
    where
        F: FnOnce(&mut Entry) -> bool,
    {
        let hash = path_hash(path);
        let mut shard = lock(self.shard(hash));
        shard.clock += 1;
        let clock = shard.clock;

        let entry = match shard
            .entries
            .raw_entry_mut()
            .from_hash(hash, |k| &**k == path)
        {
            RawEntryMut::Occupied(entry) => entry.into_mut(),
            RawEntryMut::Vacant(_) => return None,
        };
        if !check(entry) {
            return None;
        }
        let hit = take(entry, clock);
        drop(shard);
        Some(hit)
    }

    /// 还在有效期内就直接命中：连 `lstat` 都不做，省掉每请求一次系统调用。
    ///
    /// 这里**不刷新**时间戳：否则持续被请求的热文件永远等不到复校验，陈旧窗口就成了无界。
    /// 复校验由 [`Self::get`] 做，它命中时会把时间戳刷新到当前时刻。
    #[must_use]
    pub fn get_fresh(&self, path: &str) -> Option<CacheHit> {
        self.lookup(path, |entry| {
            now_millis() - entry.validated_at < REVALIDATE_MILLIS
        })
    }

    /// 命中时返回独立的 fd、它的元数据与已经编码好的响应头，调用方会把 fd 交给 `NamedFile` 消费掉。
    ///
    /// 只有 `ino`、大小与修改时间都与本次 `lstat` 的结果一致才算命中；命中即刷新有效期。
    #[must_use]
    pub fn get(&self, path: &str, metadata: &Metadata) -> Option<CacheHit> {
        self.lookup(path, |entry| {
            if entry.metadata.ino() != metadata.ino()
                || entry.metadata.len() != metadata.len()
                || (entry.metadata.mtime(), entry.metadata.mtime_nsec())
                    != (metadata.mtime(), metadata.mtime_nsec())
            {
                return false;
            }
            entry.validated_at = now_millis();
            true
        })
    }

    /// 未命中时把刚打开并已 `fstat` 的文件放进缓存：缓存自己留一份 fd，调用方那份继续用。
    ///
    /// `metadata` 必须是这个 fd 自己的 `fstat` 结果（而不是路径的 `lstat`）：命中时它会被
    /// 直接当作文件的元数据使用，两者必须是同一个 inode 的属性。`headers` 里的 `ETag` 与
    /// `Content-Disposition` 必须是从同一份元数据算出来的，否则命中时会给出错的响应头。
    pub fn insert(
        &self,
        path: &str,
        joined: Arc<Path>,
        file: Arc<File>,
        metadata: FileMeta,
        headers: Arc<CachedHeaders>,
    ) {
        let hash = path_hash(path);
        let mut shard = lock(self.shard(hash));

        shard.clock += 1;
        let clock = shard.clock;

        // 淘汰下来的 `Entry` 先攥着，等出了锁再丢：它那份 `Arc<File>` 很可能就是最后一个
        // 引用，一丢就要 `close(2)`，不该把这个系统调用放进临界区
        let mut evicted = None;
        // 一次 `raw_entry_mut` 走完：已存在就就地更新（不分配新 key、不淘汰），
        // 不存在才走淘汰+插入。复用 `hash` 避免再哈希一遍 key。
        match shard
            .entries
            .raw_entry_mut()
            .from_hash(hash, |k| &**k == path)
        {
            RawEntryMut::Occupied(entry) => {
                let entry = entry.into_mut();
                entry.file = file;
                entry.metadata = metadata;
                entry.headers = headers;
                entry.joined = joined;
                entry.used = clock;
                entry.validated_at = now_millis();
            }
            RawEntryMut::Vacant(_) => {
                // 满了就淘汰最久没被用到的那条，而不是把整片清空
                if shard.entries.len() >= CAPACITY_PER_SHARD {
                    evicted = shard
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.used)
                        .map(|(key, _)| key.clone())
                        .and_then(|oldest| shard.entries.remove(&oldest));
                }
                let key: Box<str> = path.into();
                shard.entries.insert(
                    key,
                    Entry {
                        file,
                        metadata,
                        headers,
                        joined,
                        used: clock,
                        validated_at: now_millis(),
                    },
                );
            }
        }
        drop(shard);
        drop(evicted);
    }

    /// 路径已经不存在了，顺手把占着的 fd 放掉。
    pub fn remove(&self, path: &str) {
        let hash = path_hash(path);
        let mut shard = lock(self.shard(hash));
        // 与 `insert` 的淘汰同理：条目带出锁外再丢，`close(2)` 不落在临界区里
        let dropped = shard.entries.remove(path);
        drop(shard);
        drop(dropped);
    }

    /// 总条目数，只有测试用得到
    #[cfg(test)]
    fn total_len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| lock(shard).entries.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mime::Mime;
    use salvo::http::{HeaderValue, headers::ETag};
    use std::io::Read as _;

    fn text_plain() -> Arc<Mime> {
        Arc::new(match "text/plain; charset=utf-8".parse() {
            Ok(mime) => mime,
            Err(_) => unreachable!("写死的类型应当能解析"),
        })
    }

    /// 测试用：只带类型，ETag 与 Content-Disposition 留空
    fn headers() -> Arc<CachedHeaders> {
        Arc::new(CachedHeaders {
            content_type: text_plain(),
            last_modified: None,
            etag: None,
            disposition: None,
        })
    }

    /// 测试用：写死的 ETag（解析不了就说明测试自己写错了）
    fn etag(value: &str) -> ETag {
        match value.parse() {
            Ok(etag) => etag,
            Err(_) => unreachable!("写死的 ETag 应当能解析"),
        }
    }

    /// 临时目录里的一个文件，Drop 时整个目录一起删掉
    struct Fixture {
        dir: std::path::PathBuf,
        path: std::path::PathBuf,
    }

    impl Fixture {
        fn new(tag: &str) -> std::io::Result<Self> {
            let dir = std::env::temp_dir()
                .join(format!("lanfile-file-cache-{tag}-{}", std::process::id()));
            std::fs::create_dir_all(&dir)?;
            let path = dir.join("file.txt");
            std::fs::write(&path, b"hello")?;
            Ok(Self { dir, path })
        }

        /// 打开文件并返回它的 fd 与 `fstat` 结果，模拟未命中时写入缓存的那份
        fn open(&self) -> std::io::Result<(File, FileMeta)> {
            let file = File::open(&self.path)?;
            let metadata = crate::fd_meta(&file)?;
            Ok((file, metadata))
        }

        fn lstat(&self) -> std::io::Result<Metadata> {
            std::fs::symlink_metadata(&self.path)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn read_all(file: &File) -> std::io::Result<String> {
        let mut text = String::new();
        let mut reader = file;
        reader.read_to_string(&mut text)?;
        Ok(text)
    }

    #[test]
    fn hits_while_the_file_is_unchanged() -> std::io::Result<()> {
        let fixture = Fixture::new("hit")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;

        cache.insert(
            "file.txt",
            Arc::from(Path::new("file.txt")),
            Arc::new(file),
            metadata,
            headers(),
        );

        let cached = cache.get("file.txt", &fixture.lstat()?);
        assert!(cached.is_some(), "元数据没变就应该命中");

        if let Some(CacheHit {
            file,
            metadata,
            headers,
            ..
        }) = cached
        {
            assert_eq!(read_all(&file)?, "hello");
            assert_eq!(metadata.len(), 5, "命中时给出的元数据就是那个 fd 的");
            assert_eq!(headers.content_type, text_plain(), "命中时类型也从缓存来");
        }

        assert!(
            cache.get("other.txt", &fixture.lstat()?).is_none(),
            "路径不同不该命中"
        );
        Ok(())
    }

    #[test]
    fn misses_after_the_file_changes() -> std::io::Result<()> {
        let fixture = Fixture::new("change")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;

        cache.insert(
            "file.txt",
            Arc::from(Path::new("file.txt")),
            Arc::new(file),
            metadata,
            headers(),
        );

        std::fs::write(&fixture.path, b"hello, world")?;
        assert!(
            cache.get("file.txt", &fixture.lstat()?).is_none(),
            "大小变了不该命中"
        );
        Ok(())
    }

    /// 文件被同大小、同时间戳的新文件替换掉（inode 变了），也必须未命中
    #[test]
    fn misses_after_the_file_is_replaced() -> std::io::Result<()> {
        let fixture = Fixture::new("replace")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;
        let original = fixture.lstat()?;

        cache.insert(
            "file.txt",
            Arc::from(Path::new("file.txt")),
            Arc::new(file),
            metadata,
            headers(),
        );

        let replacement = fixture.dir.join("replacement.txt");
        std::fs::write(&replacement, b"world")?;

        rustix::fs::utimensat(
            rustix::fs::CWD,
            &replacement,
            &rustix::fs::Timestamps {
                last_access: rustix::fs::Timespec {
                    tv_sec: original.atime(),
                    tv_nsec: original.atime_nsec(),
                },
                last_modification: rustix::fs::Timespec {
                    tv_sec: original.mtime(),
                    tv_nsec: original.mtime_nsec(),
                },
            },
            rustix::fs::AtFlags::empty(),
        )?;

        std::fs::rename(&replacement, &fixture.path)?;
        let replaced = fixture.lstat()?;

        assert_eq!(replaced.len(), 5, "替换文件的大小必须和原文件一样");
        assert!(
            cache.get("file.txt", &replaced).is_none(),
            "inode 变了就不该命中"
        );
        Ok(())
    }

    /// 已经编码好的 `ETag` 与 `Content-Disposition` 跟着条目一起存取，命中时原样拿回来
    #[test]
    fn keeps_the_encoded_headers() -> std::io::Result<()> {
        let fixture = Fixture::new("headers")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;

        let etag = etag("\"abc-1\"");
        let disposition = HeaderValue::from_static("inline");

        cache.insert(
            "file.txt",
            Arc::from(Path::new("file.txt")),
            Arc::new(file),
            metadata,
            Arc::new(CachedHeaders {
                content_type: text_plain(),
                last_modified: None,
                etag: Some(etag.clone()),
                disposition: Some(disposition.clone()),
            }),
        );

        let Some(CacheHit {
            headers: cached, ..
        }) = cache.get("file.txt", &fixture.lstat()?)
        else {
            panic!("元数据没变就应该命中");
        };

        assert_eq!(cached.etag, Some(etag), "命中时应当给出缓存里的 ETag");
        assert_eq!(
            cached.disposition,
            Some(disposition),
            "命中时应当给出缓存里的 Content-Disposition"
        );
        Ok(())
    }

    #[test]
    fn remove_drops_the_entry() -> std::io::Result<()> {
        let fixture = Fixture::new("remove")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;

        cache.insert(
            "file.txt",
            Arc::from(Path::new("file.txt")),
            Arc::new(file),
            metadata,
            headers(),
        );

        cache.remove("file.txt");
        assert!(cache.get("file.txt", &fixture.lstat()?).is_none());
        Ok(())
    }

    /// 有效期内不再校验；过期后回到逐次校验，而校验命中会把有效期刷新回来
    #[test]
    fn revalidates_at_most_once_per_interval() -> std::io::Result<()> {
        let fixture = Fixture::new("ttl")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;
        let path = "file.txt";

        cache.insert(
            path,
            Arc::from(Path::new(path)),
            Arc::new(file),
            metadata,
            headers(),
        );

        assert!(
            cache.get_fresh(path).is_some(),
            "刚写入的条目应当在有效期内"
        );
        assert!(cache.get_fresh("other.txt").is_none(), "路径不同不该命中");

        // 把时间戳往回拨到有效期的另一侧，等价于"1 秒过去了"
        let hash = path_hash(path);
        let Some(mut shard) = cache.shard(hash).lock().ok() else {
            unreachable!("锁不会中毒");
        };
        let Some(entry) = shard.entries.get_mut(path) else {
            unreachable!("刚插入的条目必须还在");
        };
        entry.validated_at -= REVALIDATE_MILLIS;
        drop(shard);

        assert!(
            cache.get_fresh(path).is_none(),
            "过了有效期就不该再走快路径"
        );
        assert!(
            cache.get(path, &fixture.lstat()?).is_some(),
            "元数据没变，逐次校验应当命中"
        );
        assert!(
            cache.get_fresh(path).is_some(),
            "校验命中应当把有效期刷新回来"
        );
        Ok(())
    }

    /// 路径数量远超容量时，条目总数必须有硬上限，且最后插入的那条仍然在
    #[test]
    fn stays_bounded_under_many_paths() -> std::io::Result<()> {
        let fixture = Fixture::new("bound")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;
        let file = Arc::new(file);

        for i in 0..2000 {
            let path = format!("path-{i}");
            cache.insert(
                &path,
                Arc::from(Path::new(&path)),
                Arc::clone(&file),
                metadata.clone(),
                headers(),
            );
        }

        assert!(
            cache.total_len() <= SHARDS * CAPACITY_PER_SHARD,
            "条目总数不能超过上限"
        );
        assert!(
            cache.get("path-1999", &fixture.lstat()?).is_some(),
            "最后插入的那条必须还在"
        );
        Ok(())
    }

    /// 淘汰的是最久没被用到的那条，而不是把整片清空
    #[test]
    fn evicts_the_least_recently_used() -> std::io::Result<()> {
        let fixture = Fixture::new("lru")?;
        let cache = FileCache::default();
        let (file, metadata) = fixture.open()?;
        let file = Arc::new(file);

        // 凑够同一片里的 CAPACITY_PER_SHARD + 1 条路径，把这一片填满
        let shard = shard_index(path_hash("same-shard-0"));
        let mut paths = Vec::new();
        let mut i = 0;

        while paths.len() <= CAPACITY_PER_SHARD {
            let path = format!("same-shard-{i}");
            if shard_index(path_hash(&path)) == shard {
                paths.push(path);
            }
            i += 1;
        }

        let target = paths[0].clone();
        let Some(extra) = paths.pop() else {
            unreachable!("至少有一条用于触发淘汰");
        };

        for path in &paths {
            cache.insert(
                path,
                Arc::from(Path::new(path)),
                Arc::clone(&file),
                metadata.clone(),
                headers(),
            );
        }

        assert!(
            cache.get(&target, &fixture.lstat()?).is_some(),
            "刚插入的应当命中"
        );

        cache.insert(
            &extra,
            Arc::from(Path::new(&extra)),
            Arc::clone(&file),
            metadata,
            headers(),
        );

        assert!(
            cache.get(&target, &fixture.lstat()?).is_some(),
            "刚用过的不能被淘汰"
        );
        assert!(
            cache.get(&paths[1], &fixture.lstat()?).is_none(),
            "最久没被用到的应当被淘汰"
        );
        Ok(())
    }
}
