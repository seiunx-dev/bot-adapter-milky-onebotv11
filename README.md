# milky-onebot11-bridge

使用 Rust 编写的 Milky → OneBot-11 桥接中间件。下游机器人按 OneBot-11 协议接入本桥，桥再通过 [`milky-rust-sdk`](https://crates.io/crates/milky-rust-sdk) 与上游 Milky 网关通信。

## 功能

- 上游连接：通过 WebSocket 接入 Milky 网关，REST 地址由 SDK 根据 WS 地址自动推导。
- 正向 WebSocket：
  - `/`（universal，可收可发）
  - `/api`、`/api/`（仅接收 API 调用）
  - `/event`、`/event/`（仅推送事件）
- 反向 WebSocket：
  - 支持 universal 单连接或 API/Event 双连接
  - 自动重连，按 OneBot-11 规范带上 `X-Self-ID`、`X-Client-Role`、可选 `Authorization: Bearer <token>` 头
- 可选 HTTP API：`POST /http/<action>`（也接受 `GET`）。
- 双向消息链路：`send_private_msg`、`send_group_msg`、`send_msg`。
- 查询与管理：`get_login_info`、`get_status`、`get_version_info`、`can_send_image`、`can_send_record`、`get_group_info`、`get_group_list`、`get_group_member_info`、`get_group_member_list`、`get_msg`、`delete_msg`、`set_friend_add_request`、`set_group_add_request`。
- 事件：私聊/群消息、戳一戳、好友/群请求、私聊/群撤回、lifecycle / heartbeat。

## 构建

依赖最新稳定版 Rust（`dtolnay/rust-toolchain@stable`），edition 2024。

```bash
cargo build --release --locked
```

产物：`target/release/milky-ob11-bridge`。

## 快速开始

1. 复制配置模板：

   ```bash
   cp config.example.json config.json
   ```

2. 修改 `config.json` 中的 Milky WS 地址与 token。

3. 启动：

   ```bash
   cargo run -- --config config.json
   # 或运行已构建的二进制：
   ./target/release/milky-ob11-bridge --config config.json
   ```

   `--config` 默认值为 `config.json`，也可写作 `-config` / `-c`。

4. 让下游 OneBot-11 机器人连接：

   ```
   ws://<host>:<port>/
   ws://<host>:<port>/api
   ws://<host>:<port>/event
   ```

5. 启用 HTTP API 后可调用：

   ```
   POST http://<host>:<port>/http/send_group_msg
   ```

6. 启用反向 WebSocket 时，在配置中填写 `onebot.reverse.url` / `api_url` / `event_url` 与 `use_universal_client`。

## 配置

完整字段见 [`config.example.json`](./config.example.json)。配置文件采用严格 JSON 解析，未知字段会被拒绝。

主要字段：

- `milky.ws_endpoint` — Milky 网关 WebSocket 地址。REST 地址由 SDK 自动推导（`ws://` → `http://`，`wss://` → `https://`，路径替换为 `api/`），无需单独配置。
- `milky.token` — Milky 鉴权 token，留空表示无鉴权。
- `onebot.host` / `onebot.port` — OneBot-11 服务监听地址。
- `onebot.access_token` — 下游连接时的鉴权 token。
- `onebot.enable_http_api` / `enable_ws_api` / `enable_ws_event` / `enable_ws_universal` — 各端点开关。
- `onebot.reverse.*` — 反向 WS 配置。
- `bridge.message_format` — `array`（默认）或 `string`，决定事件中 `message` 字段的序列化方式。
- `bridge.heartbeat_interval_ms` — meta 心跳事件下发间隔。
- `bridge.log_level` — `trace` / `debug` / `info` / `warn` / `error`。
- `bridge.cache_size` — `MessageMap` LRU 容量。
- `bridge.self_id` — 留 0 时使用上游登录返回的 QQ 号；非 0 时强制覆盖。

## 开发

```bash
cargo test                                                   # 全量测试
cargo fmt --all                                              # 格式化
cargo clippy --all-targets --all-features -- -D warnings     # lint（CI 以 warnings 为错）
```

CI（`.github/workflows/ci.yml`）在每次 push 到 `main` / `rust` 与每个以它们为目标的 PR 上跑 fmt / check / clippy / test。Release 工作流（`.github/workflows/release.yml`）在推送 `v*` tag 时于各平台原生构建 linux-x64 / macos-arm64 / windows-x64 二进制，打包后连同 SHA256 校验文件发布到 GitHub Release；手动触发时只构建并上传 workflow artifact。

架构与扩展指南见 [`AGENTS.md`](./AGENTS.md)。

## 说明

- 默认 `message_format=array`，与内部消息 IR 对齐。array 与 string 两种形式均会自动 CQ-code 编解码。
- 部分高级管理接口尚未实现，未支持的 action 会返回 retcode `1503`。
