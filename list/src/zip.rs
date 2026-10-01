use std::{fmt::Write as _, fs::File, path::Path};

#[cfg(any(target_os = "linux", target_os = "android"))]
use std::mem::MaybeUninit;

#[cfg(any(target_os = "linux", target_os = "android"))]
use rustix::fs::{self as rfs, AtFlags, FileType, Mode, OFlags, RawDir};

/// zip 归档中的一条记录：普通文件或目录（目录条目用于保留空目录结构）。
pub enum Entry {
    File {
        /// 已用 `openat(dirfd, name, O_RDONLY|O_CLOEXEC|O_NOFOLLOW)` 打开。
        /// 相对父目录解析，省掉全路径逐层查找。
        file: File,
        /// 文件字节数（来自 `fstat`）。
        size: u64,
        /// zip 内路径（含 prefix）。
        name: String,
    },
    Dir {
        name: String,
    },
}

/// 按相对路径安全打开 root 内的普通文件，返回 fd 与 fstat 大小。
///
/// stream-batch 的文件列表来自客户端请求体；这里逐级 `openat(..., O_NOFOLLOW)`，
/// 避免请求中的路径或竞态出现的符号链接把读取范围带出 root。
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn open_file_under(dir: &Path, rel: &str) -> Option<(File, u64)> {
    let mut dirfd = rfs::openat(
        rfs::CWD,
        dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .ok()?;
    let (parents, file_name) = match rel.rsplit_once('/') {
        Some((parents, file_name)) => (parents, file_name),
        None => ("", rel),
    };
    if !parents.is_empty() {
        for component in parents.split('/') {
            if component.is_empty() {
                return None;
            }
            let child = rfs::openat(
                &dirfd,
                component,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .ok()?;
            dirfd = child;
        }
    }
    if file_name.is_empty() {
        return None;
    }
    let fd = rfs::openat(
        &dirfd,
        file_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .ok()?;
    let stat = rfs::fstat(&fd).ok()?;
    if !FileType::from_raw_mode(stat.st_mode).is_file() {
        return None;
    }
    #[allow(clippy::cast_sign_loss)]
    Some((File::from(fd), stat.st_size as u64))
}

/// Fallback for platforms without `openat2`. This confines the resolved path to
/// `dir`. Unlike the Unix fast path, it canonicalizes first and therefore follows
/// a final symlink whose target remains under `dir`.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn open_file_under(dir: &Path, rel: &str) -> Option<(File, u64)> {
    let path = dir.join(rel);
    let canonical = path.canonicalize().ok()?;
    if !canonical.starts_with(dir) {
        return None;
    }
    let file = File::open(canonical).ok()?;
    let metadata = file.metadata().ok()?;
    metadata.is_file().then(|| (file, metadata.len()))
}

/// 取目录名作为 zip 内根前缀（也用于 Content-Disposition 文件名）。
pub fn folder_name(dir: &Path) -> String {
    dir.file_name()
        .map_or_else(|| "root".into(), |n| n.to_string_lossy().into_owned())
}

/// 按名称排序目录条目，保证 zip 内顺序确定；两个平台的实现共用。
fn sort_by_name<T>(entries: &mut [(T, String)]) {
    entries.sort_unstable_by(|a, b| a.1.cmp(&b.1));
}

/// 深度优先遍历目录树，把每个条目交给 `on_entry`；`on_entry` 返回 `false` 时提前停止。
/// `RawDir` 零分配遍历 + `d_type` 免 stat；与 /api/list 一致包含 dotfile、跳过符号链接。
/// 每个目录的条目按名称排序，保证 zip 内顺序确定。
/// 目录不可读（无权限等）时跳过该目录，不中断整个打包。
/// 返回 `false` 表示回调要求提前停止。
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn walk(dir: &Path, prefix: &str, on_entry: &mut impl FnMut(Entry) -> bool) -> bool {
    let Ok(dirfd) = rfs::openat(
        rfs::CWD,
        dir,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    ) else {
        return true;
    };

    // 记录目录自身，空目录解压后也能保留，保证目录结构原样
    if !on_entry(Entry::Dir {
        name: format!("{prefix}/"),
    }) {
        return false;
    }

    let mut buf = [MaybeUninit::<u8>::uninit(); 8192];
    let mut raw_dir = RawDir::new(&dirfd, &mut buf);

    let mut entries: Vec<(FileType, String)> = Vec::new();
    while let Some(entry) = raw_dir.next() {
        let Ok(entry) = entry else {
            continue;
        };
        let name_bytes = entry.file_name().to_bytes();
        // 只跳过 . 与 ..，点开头的文件/目录一并打包
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        entries.push((entry.file_type(), name));
    }

    sort_by_name(&mut entries);

    for (ft, name) in entries {
        let path = dir.join(&name);
        let zip_name = format!("{prefix}/{name}");

        let actual_ft = if ft == FileType::Unknown {
            rfs::statat(&dirfd, &name, AtFlags::SYMLINK_NOFOLLOW)
                .map_or(ft, |s| FileType::from_raw_mode(s.st_mode))
        } else {
            ft
        };

        let keep_going = if actual_ft.is_dir() {
            walk(&path, &zip_name, on_entry)
        } else if actual_ft.is_file() {
            // 相对 dirfd 打开：一个组件的路径解析，省掉从 `/` 开始的逐层查找。
            // 失败就跳过这个文件，不中断整棵树。
            let opened = rfs::openat(
                &dirfd,
                &name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .ok()
            .and_then(|fd| rfs::fstat(&fd).ok().map(|stat| (fd, stat)));
            match opened {
                Some((fd, stat)) => on_entry(Entry::File {
                    file: fd.into(),
                    #[allow(clippy::cast_sign_loss)]
                    size: stat.st_size as u64,
                    name: zip_name,
                }),
                None => true,
            }
        } else {
            // 符号链接等其它类型：跳过
            true
        };
        if !keep_going {
            return false;
        }
    }
    true
}

/// 深度优先遍历目录树，把每个条目交给 `on_entry`；`on_entry` 返回 `false` 时提前停止。
/// 每个目录的条目按名称排序，保证 zip 内顺序确定。
/// 目录不可读（无权限等）时跳过该目录，不中断整个打包。
/// 返回 `false` 表示回调要求提前停止。
#[cfg(not(any(target_os = "linux", target_os = "android")))]
/// 非 Linux/Android（Windows、macOS 等）下的递归遍历：`rustix::fs` 的 Linux 专用接口不可用，改用 `std::fs`，行为与 Linux 版本一致。
pub fn walk(dir: &Path, prefix: &str, on_entry: &mut impl FnMut(Entry) -> bool) -> bool {
    // 目录不可读时不产出目录条目，与 Linux 版本 openat 失败时的行为保持一致
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return true;
    };

    if !on_entry(Entry::Dir {
        name: format!("{prefix}/"),
    }) {
        return false;
    }

    let mut entries: Vec<(PathBuf, String)> = Vec::new();
    for entry in read_dir {
        let Ok(entry) = entry else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        entries.push((entry.path(), name));
    }
    sort_by_name(&mut entries);

    for (path, name) in entries {
        let zip_name = format!("{prefix}/{name}");
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let ft = metadata.file_type();
        let keep_going = if ft.is_dir() {
            walk(&path, &zip_name, on_entry)
        } else if ft.is_file() {
            match File::open(&path) {
                Ok(file) => on_entry(Entry::File {
                    file,
                    size: metadata.len(),
                    name: zip_name,
                }),
                Err(_) => true,
            }
        } else {
            // 符号链接等其它类型：跳过
            true
        };
        if !keep_going {
            return false;
        }
    }
    true
}

/// 生成 RFC 5987 风格的 Content-Disposition 值：filename*=UTF-8''<pct>.zip
pub fn content_disposition(folder_name: &str) -> String {
    let mut out = String::from("attachment; filename*=UTF-8''");
    for &b in folder_name.as_bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if keep {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out.push_str(".zip");
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn tmp_root(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("lanfile-zip-{tag}-{}", std::process::id()))
    }

    /// 建一棵小目录树 root/{a, b, dir/{c, d, e}}，共 7 个条目（2 目录 + 5 文件）。
    fn make_tree(root: &Path) {
        std::fs::create_dir_all(root.join("dir")).unwrap();
        for name in ["a", "b"] {
            std::fs::write(root.join(name), "x").unwrap();
        }
        for name in ["c", "d", "e"] {
            std::fs::write(root.join("dir").join(name), "x").unwrap();
        }
    }

    #[test]
    fn walk_emits_all_entries_with_structure() {
        let root = tmp_root("full");
        let _ = std::fs::remove_dir_all(&root);
        make_tree(&root);

        let mut out = Vec::new();
        walk(&root, "root", &mut |e| {
            out.push(e);
            true
        });

        assert_eq!(out.len(), 7);
        // 目录结构原样：根目录与子目录条目都以 / 结尾，文件 zip 内路径带前缀
        assert!(matches!(&out[0], Entry::Dir { name } if name == "root/"));
        assert!(
            out.iter()
                .any(|e| matches!(e, Entry::Dir { name } if name == "root/dir/"))
        );
        for e in &out {
            match e {
                Entry::Dir { name } => assert!(name.ends_with('/'), "目录条目应以 / 结尾: {name}"),
                Entry::File { file, name, .. } => {
                    assert!(name.starts_with("root/"), "文件 zip 路径应带前缀: {name}");
                    // fd 仍然可读，说明 openat 拿到的就是文件本身
                    let meta = file.metadata().expect("fd 应当有元数据");
                    assert!(meta.is_file());
                }
            }
        }

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn walk_stops_when_callback_returns_false() {
        let root = tmp_root("stop");
        let _ = std::fs::remove_dir_all(&root);
        make_tree(&root);

        let mut count = 0;
        walk(&root, "root", &mut |_e| {
            count += 1;
            count < 4
        });

        assert_eq!(count, 4);

        let _ = std::fs::remove_dir_all(&root);
    }
}
