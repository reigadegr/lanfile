//! 命令行与 URL 解析：把 `lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]`
//! 解析成 [`Parsed`]（host、remote、kind、local、flat）。裸 host 与直链两种来源都收口在这里，
//! 是纯解析、无 IO。

use crate::error::Error;
use std::path::PathBuf;

/// 用法串：直链与裸 host 两种源共用同一个尾部（可选 `local_dir` 与 `--flat`）。
const USAGE: &str = "用法: lanfile get <base_url|直链> [<remote_dir>] [local_dir] [--flat]";

/// 解析后的参数。
pub struct Parsed {
    /// `http://<host>`，打印进度用。
    pub base: String,
    /// 已剥 scheme 的 `host[:port]`，建连用。
    pub host: String,
    /// 远端相对路径（已剥首尾斜杠）；解析时已拒绝拉根，故恒非空。
    pub remote: String,
    /// 已知 kind，还是得探测。
    pub kind: Kind,
    /// 本地落盘目录。
    pub local: PathBuf,
    /// 不套 basename 一层：目录内容直接落进 `local`（单文件本就不套层）。
    pub flat: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Kind {
    /// 裸 host + 名字：先试目录，404 再当文件。
    Auto,
    /// `/api/zip/`、`/api/list/` 直链：当目录（逐个文件拉）。
    Dir,
    /// `/files/`、`/pull/` 直链：直接当文件。
    File,
    /// `/#<sub>`：清单并发。
    Stream,
}

/// 解析命令行：`<base_url|直链> [remote] [local] [--flat]`。
pub fn parse_args(args: &[String]) -> Result<Parsed, Error> {
    if args.is_empty() {
        return Err(Error::Malformed(USAGE));
    }
    let src = parse_source(&args[0])?;
    let (pos, flat) = parse_trailing(&args[1..]);
    let (remote, kind, local) = if let Some(direct) = src.direct {
        // 直链：remote 已在 URL 里指明，后面只剩可选 local 与 flag。
        let local = match pos {
            [] => None,
            [s] => Some(PathBuf::from(s)),
            _ => return Err(Error::Malformed(USAGE)),
        };
        (direct.remote, direct.kind, local)
    } else {
        // 裸 host：[remote] [local]；remote 为空会在下面统一拒绝。
        if pos.len() > 2 {
            return Err(Error::Malformed(USAGE));
        }
        let remote = pos
            .first()
            .map(|s| s.trim_matches('/').to_string())
            .unwrap_or_default();
        let local = pos.get(1).map(PathBuf::from);
        (remote, Kind::Auto, local)
    };
    // 拉根（remote 为空）被禁：在连服务端前直接报错，避免误把整棵 share 拖下来。
    if remote.is_empty() {
        return Err(Error::Malformed(
            "不允许拉取根目录（整棵 share）；请指定子目录或直链，如 `lanfile get <base> sub`",
        ));
    }
    // 不给 local：命名远端/文件缺省当前目录。
    let local = local.unwrap_or_else(|| PathBuf::from("."));
    Ok(Parsed {
        base: src.base,
        host: src.host,
        remote,
        kind,
        local,
        flat,
    })
}

/// 末尾若是 `--flat`/`-f` 则取下，返回剩余位置参数与 flat 标志。
/// flag 只认末尾一个：按用户的写法，它跟在 `local_dir` 之后。
fn parse_trailing(args: &[String]) -> (&[String], bool) {
    match args.last() {
        Some(last) if last.as_str() == "--flat" || last.as_str() == "-f" => {
            (&args[..args.len() - 1], true)
        }
        _ => (args, false),
    }
}

/// 直链解析结果：URL 已指明远端路径与它是文件还是目录。
struct Direct {
    remote: String,
    kind: Kind,
}

/// URL 解析结果：`base`/`host` 永远有；`direct` 为 `None` 即裸 host（remote 留给位置参数）。
struct Source {
    base: String,
    host: String,
    direct: Option<Direct>,
}

/// 解析 URL：剥 scheme，分 `host`、路径与 fragment，再交给 [`direct_of`] 认直链。
fn parse_source(url: &str) -> Result<Source, Error> {
    let rest = url.strip_prefix("http://").ok_or(Error::Malformed(
        "base_url 必须以 http:// 开头（不支持 https）",
    ))?;
    // host 到第一个 `/`、`?` 或 `#` 为止——`http://h#frag` 这种没有 `/` 的写法也要切对。
    let host_end = rest
        .bytes()
        .position(|b| matches!(b, b'/' | b'?' | b'#'))
        .unwrap_or(rest.len());
    let host = rest[..host_end].to_string();
    let tail = &rest[host_end..];
    let after = tail.strip_prefix('/').unwrap_or(tail);
    // `#` 之后是 fragment；路径到 `#` 前的第一个 `?` 为止——`?` 在 `#` 之后时归 fragment。
    let (head, fragment) = after.split_once('#').unwrap_or((after, ""));
    let path = head.split_once('?').map_or(head, |(path, _)| path);
    Ok(Source {
        base: format!("http://{host}"),
        host,
        direct: direct_of(path, fragment),
    })
}

/// 认直链，给出 URL 里已经指明的那条远端路径：
/// - `/files/<sub>`、`/pull/<sub>` 当文件，`/api/zip/<sub>`、`/api/list/<sub>` 当目录；
/// - `/#<sub>` 走清单并发；
/// - 其余非空路径本身就是远端，kind 待探测——`http://h/.pi` 等价于 `lanfile get http://h .pi`；
/// - 只有空路径（`http://h`、`http://h/`）返回 `None`，remote 留给位置参数。
fn direct_of(path: &str, fragment: &str) -> Option<Direct> {
    if let Some(sub) = path
        .strip_prefix("files/")
        .or_else(|| path.strip_prefix("pull/"))
        .filter(|sub| !sub.is_empty())
    {
        return Some(Direct {
            remote: percent_decode(sub),
            kind: Kind::File,
        });
    }
    if let Some(sub) = path
        .strip_prefix("api/zip/")
        .or_else(|| path.strip_prefix("api/list/"))
        .filter(|sub| !sub.is_empty())
    {
        return Some(Direct {
            remote: percent_decode(sub),
            kind: Kind::Dir,
        });
    }
    // 站内直链 /#<sub>：path 为空（`/`、`/#<sub>`，或没写 `/` 的 `#<sub>`）。`#` 在 `?` 之前时
    // 用户多写的查询串也算 fragment（`/#sub?x=1`），一并切掉。默认走清单并发。
    if path.is_empty() {
        let sub = fragment.trim_start_matches('/');
        let sub = sub.split_once('?').map_or(sub, |(sub, _)| sub);
        if !sub.is_empty() {
            return Some(Direct {
                remote: percent_decode(sub),
                kind: Kind::Stream,
            });
        }
        return None;
    }
    // 其余非空路径本身就是远端，是文件还是目录留给 `/api/list` 探测。
    // 不能把这类路径退回裸 host：那会把用户指定的子树误解释成拉根请求。
    let sub = path.trim_matches('/');
    if sub.is_empty() {
        return None;
    }
    Some(Direct {
        remote: percent_decode(sub),
        kind: Kind::Auto,
    })
}

/// 百分号解码：把 `%XX` 还原成原字节，用于直链里 URL 编码过的子路径（解码后再交给
/// `crate::fetch::encode_path` 重新编码发请求，本地文件名取解码后的末段）。
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 <= bytes.len()
            && let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2]))
        {
            out.push(hi << 4 | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

const fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parse_args_bare_host_defaults_local_to_cwd() {
        let p = parse_args(&["http://h:1".into(), "sub".into()]).unwrap();
        assert_eq!(p.base, "http://h:1");
        assert_eq!(p.host, "h:1");
        assert_eq!(p.remote, "sub");
        assert_eq!(p.kind, Kind::Auto);
        // 不给 local：缺省当前目录；run 会再套 basename 一层，最终落成 ./sub/
        assert_eq!(p.local, PathBuf::from("."));
    }

    #[test]
    fn parse_args_bare_host_strips_slashes() {
        let p = parse_args(&["http://h:1/".into(), "/sub/deep/".into()]).unwrap();
        assert_eq!(p.remote, "sub/deep");
        assert_eq!(p.kind, Kind::Auto);
    }

    #[test]
    fn parse_args_bare_host_root_is_rejected() {
        // 不给 remote、给 / 或空串，都算拉根，一律在连服务端前拒绝。
        assert!(parse_args(&["http://h:1".into()]).is_err());
        assert!(parse_args(&["http://h:1".into(), "/".into()]).is_err());
        assert!(parse_args(&["http://h:1".into(), String::new()]).is_err());
    }

    #[test]
    fn parse_args_bare_host_explicit_local() {
        let p = parse_args(&["http://h:1".into(), "sub".into(), "./dst".into()]).unwrap();
        assert_eq!(p.local, PathBuf::from("./dst"));
    }

    #[test]
    fn parse_args_empty_errors() {
        assert!(parse_args(&[]).is_err());
    }

    #[test]
    fn parse_args_file_direct_link_defaults_local_to_cwd() {
        let p = parse_args(&["http://h:1/files/boards.md".into()]).unwrap();
        assert_eq!(p.base, "http://h:1");
        assert_eq!(p.host, "h:1");
        assert_eq!(p.remote, "boards.md");
        assert_eq!(p.kind, Kind::File);
        assert_eq!(p.local, PathBuf::from("."));
    }

    #[test]
    fn parse_args_pull_direct_link_is_file() {
        let p = parse_args(&["http://h:1/pull/a/b.txt".into()]).unwrap();
        assert_eq!(p.remote, "a/b.txt");
        assert_eq!(p.kind, Kind::File);
    }

    #[test]
    fn parse_args_direct_link_strips_query_and_fragment() {
        let p = parse_args(&["http://h:1/files/x.txt?v=1#frag".into()]).unwrap();
        assert_eq!(p.remote, "x.txt");
    }

    #[test]
    fn parse_args_fragment_after_query_direct_link_is_stream() {
        // `?` 在 `#` 之前：`?` 之后是查询串，fragment 仍要认出来。
        let p = parse_args(&["http://h:1?x=1#filerserve".into()]).unwrap();
        assert_eq!(p.host, "h:1");
        assert_eq!(p.remote, "filerserve");
        assert_eq!(p.kind, Kind::Stream);
    }

    #[test]
    fn parse_args_fragment_direct_link_strips_query() {
        // `#` 在 `?` 之前：查询串随 fragment 一起被切掉。
        let p = parse_args(&["http://h:1/#filerserve?x=1".into()]).unwrap();
        assert_eq!(p.remote, "filerserve");
        assert_eq!(p.kind, Kind::Stream);
    }

    #[test]
    fn parse_args_fragment_direct_link_is_stream() {
        let p = parse_args(&["http://h:1/#filerserve".into()]).unwrap();
        assert_eq!(p.remote, "filerserve");
        assert_eq!(p.kind, Kind::Stream);
        // 不写 `/`、不带路径的 `#<sub>` 同样认
        let p = parse_args(&["http://h:1#filerserve".into()]).unwrap();
        assert_eq!(p.remote, "filerserve");
        assert_eq!(p.kind, Kind::Stream);
    }

    #[test]
    fn parse_args_path_only_url_is_a_remote() {
        // 路径本身就是远端（kind 待探测）：http://h/.pi 等价于 lanfile get http://h .pi。
        let p = parse_args(&["http://h:1/.pi".into()]).unwrap();
        assert_eq!(p.remote, ".pi");
        assert_eq!(p.kind, Kind::Auto);
        // 多级路径整条都是远端；它后面的位置参数是 local。
        let p = parse_args(&["http://h:1/a/b".into(), "./dst".into()]).unwrap();
        assert_eq!(p.remote, "a/b");
        assert_eq!(p.kind, Kind::Auto);
        assert_eq!(p.local, PathBuf::from("./dst"));
    }

    #[test]
    fn parse_args_flat_flag_after_local() {
        let p = parse_args(&[
            "http://h:1".into(),
            "sub".into(),
            "./dst".into(),
            "--flat".into(),
        ])
        .unwrap();
        assert_eq!(p.remote, "sub");
        assert_eq!(p.local, PathBuf::from("./dst"));
        assert!(p.flat);
    }

    #[test]
    fn parse_args_flat_short_flag_no_local() {
        // -f 且不给 local：命名远端缺省当前目录。
        let p = parse_args(&["http://h:1".into(), "sub".into(), "-f".into()]).unwrap();
        assert_eq!(p.remote, "sub");
        assert_eq!(p.local, PathBuf::from("."));
        assert!(p.flat);
    }

    #[test]
    fn parse_args_flat_root_is_rejected() {
        // 拉根被禁，--flat 也救不回来。
        assert!(parse_args(&["http://h:1".into(), "--flat".into()]).is_err());
    }

    #[test]
    fn parse_args_file_direct_link_flat() {
        let p =
            parse_args(&["http://h:1/files/x".into(), "./dst".into(), "--flat".into()]).unwrap();
        assert_eq!(p.remote, "x");
        assert_eq!(p.kind, Kind::File);
        assert_eq!(p.local, PathBuf::from("./dst"));
        assert!(p.flat);
    }

    #[test]
    fn parse_args_no_flat_by_default() {
        let p = parse_args(&["http://h:1".into(), "sub".into(), "./dst".into()]).unwrap();
        assert!(!p.flat);
    }

    #[test]
    fn parse_args_zip_direct_link_is_dir() {
        let p = parse_args(&["http://h:1/api/zip/filerserve".into()]).unwrap();
        assert_eq!(p.host, "h:1");
        assert_eq!(p.remote, "filerserve");
        assert_eq!(p.kind, Kind::Dir);
        assert_eq!(p.local, PathBuf::from("."));
    }

    #[test]
    fn parse_args_list_direct_link_is_dir() {
        let p = parse_args(&["http://h:1/api/list/a/b".into()]).unwrap();
        assert_eq!(p.remote, "a/b");
        assert_eq!(p.kind, Kind::Dir);
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("boards.md"), "boards.md");
        assert_eq!(percent_decode("a%20b.txt"), "a b.txt");
        assert_eq!(percent_decode("%E4%B8%AD"), "中");
        assert_eq!(percent_decode("boards.md%20"), "boards.md ");
        // 非法 %XX 原样保留
        assert_eq!(percent_decode("a%2z.txt"), "a%2z.txt");
    }
}
