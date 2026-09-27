# lanfile

一个单二进制的局域网文件共享服务端，外加配套的 `get` 拉取客户端。服务端把一个目录用
HTTP 暴露给局域网：浏览器里能浏览、单文件能下、整棵目录能打包成 zip；`get` 客户端能把
远端目录整棵镜像到本地、或把单个文件直落下来。

- **服务端**：salvo 1.0 之上自写了一条 hyper 快路径，`/files`、`/pull` 不走 salvo 每请求
  的装箱与路由分发；`/pull` 用 `sendfile(2)` 零拷贝直出。
- **客户端**：只走服务端两个 GET 端点（`/api/list`、`/pull`），读 `Content-Length` 校验
  完整性、带连接与读取超时，不静默留下半截文件。
- **目标平台**：Linux / Android（`sendfile(2)`、`CLOCK_REALTIME_COARSE`、`fadvise`）；其它
  平台有回退但不是目标。

版本见 `Cargo.toml` 的 `workspace.package.version`。

## 服务端

```
lanfile [port] [dir]
```

- `port`：监听端口，缺省 `8000`。
- `dir`：要共享的目录，缺省 `.`（当前目录）。
- 监听地址固定 `0.0.0.0:port`。

例：`lanfile 9000 /srv/share`。

### 路由

| 路径 | 作用 |
| --- | --- |
| `GET /` | 内嵌的浏览器界面（`index.html`，中文） |
| `GET /static/<path>` | 界面静态资源（`style.css`、`app.js`，内嵌） |
| `GET /files/<path>` | 浏览器友好的文件下载：带 `ETag` / `Last-Modified` / `Content-Disposition`，fd 缓存；走快路径 |
| `GET /pull/<path>` | 拉取专用：不缓存、`application/octet-stream`、零拷贝 `sendfile(2)`；走快路径 |
| `GET /api/list/<path>` | 目录的 JSON 列表；不是目录则 404 |
| `GET /api/zip/<path>` | 把整棵目录流式打成 zip 下载 |

`/api/list` 的 JSON 形状：

```json
{
  "path": "share/sub",
  "lan_ip": "192.168.1.20",
  "port": 9000,
  "entries": [
    { "name": "a.txt", "type": "file", "size": 1234, "modified": "2025-01-01 12:00:00" },
    { "name": "sub",   "type": "dir",  "size": null,  "modified": "2025-01-01 12:00:00" }
  ]
}
```

## 客户端 `lanfile get`

```
lanfile get <base_url | 直链> [<remote_dir>] [local_dir] [--flat | -f]
```

两种源：

**裸 host** —— `lanfile get http://host <remote_dir> [local_dir] [--flat]`

- `remote_dir` 必给：拉整棵根被禁，会在连服务端前直接报错；`local_dir` 缺省时落到当前目录。
- 类型交给服务端探测：`/api/list` 返回 200 即目录、整棵镜像；404 则当文件、走 `/pull` 直落。

**直链** —— `lanfile get <url> [local_dir] [--flat]`，看 URL 里的远端：

- `http://host/files/<sub>`、`http://host/pull/<sub>` → 单文件，落到 `local_dir/<basename>`。
- `http://host/api/zip/<sub>`、`http://host/api/list/<sub>`、`http://host/#<sub>` → 目录，整棵镜像。
- 其余非空路径（如 `http://host/.pi`）→ 这条路径就是远端本身，文件还是目录交给服务端探测。
- 直链可省略 `local_dir`，默认当前目录。

`--flat` / `-f`（必须是最后一个参数）：只去掉最外层 `basename` 那层目录嵌套。单文件时是 no-op。

例：

```
lanfile get http://192.168.1.20:9000 docs                 # 镜像 docs/ 到当前目录
lanfile get http://192.168.1.20:9000 docs ~/dl            # 镜像 docs/ 到 ~/dl/docs
lanfile get http://192.168.1.20:9000/files/a.tgz          # 单文件直落 ./a.tgz
lanfile get http://192.168.1.20:9000/api/zip/docs ~/dl    # 目录递归拉取
```

客户端的几条保证：

- 正文按响应声明的 `Content-Length` 精确读满即停——长度就是读取的停止条件，读不满报
  `传输不完整` 并把半截文件删掉，不留看着完整其实残缺的文件；响应没带 `Content-Length`
  （自家服务端都会带）则当场报错并丢弃连接，不猜长度、也不读到 EOF（keep-alive 下那只
  会空等到读取超时）。
- 连接超时 10 秒、两次读取之间空闲超时 30 秒；服务器接上却不说话时不会把客户端挂死。
- `get` 不发 `User-Agent` / `Accept`、不跟重定向、只认 200、不做断点续传——只伺候自家服务端。
- 不拉根：`remote_dir` 为空（或不给、或 `/`）时直接报错、不连服务端——避免误把整棵 share 拖下来。

## 构建

发布二进制（项目自带的链接优化 + `patchelf` 清理）：

```
sh build_native_stable.sh release   # 产出 target/release/lanfile
```

最简方式：

```
cargo build --release               # 产出 target/release/lanfile
```

贡献者改动后自检（格式化 + clippy + 测试，全须全尾一次跑完）：

```
sh debug.sh
```

> 开发调试统一用 `sh debug.sh`；它已包含 `cargo fmt`、`cargo clippy --workspace
> --all --all-targets --all-features --no-deps` 与 `cargo test --workspace`。

## 性能要点

- `mimalloc` 做全局分配。
- `/files`、`/pull` 走自写的 hyper 快路径，绕过 salvo 每请求的路由匹配与 future 装箱。
- `/pull` 用 `sendfile(2)` 零拷贝，文件内容不进用户态。
- 访问日志分片（每线程一片）+ 批量攒 64KiB 再写 + 粗时钟（`CLOCK_REALTIME_COARSE`）打时间
  戳，每请求不再各分配一行。
- 目录打包 zip 流式产出，配合 `fadvise(SEQUENTIAL)` 预读。

## 已知限制

- 只支持 HTTP，没有 TLS / 代理 / 鉴权 / cookie。
- 服务端走 salvo 的 `HttpBuilder`，后者未暴露 `half_close`：任何在请求发完就 half-close 写
  半边的客户端（如 `nc -N`）可能收到被截断的响应。标准客户端（curl、浏览器、本项目的
  `get`）不 half-close，不受影响。
- 客户端不认 `Transfer-Encoding: chunked`：没有 `Content-Length` 的响应一律当场报错，读不
  到边界就直说，不猜（自家 `/pull` 带长度、不分块，所以无碍）。
- 客户端 v1 顺序拉取：共用一条 keep-alive 连接（按 `Content-Length` 精确读满即止，复用的
  连接被对端关掉会自动换新连接重试）；目录内单文件失败只告警并继续，
  进程退出码仍为 0（坏文件本身会大声报错，但脚本层仍算成功——若需要"有文件失败就整体
  失败"，目前要自行看 stderr）。
