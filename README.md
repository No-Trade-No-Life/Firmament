# Firmament

Firmament 是一个面向研究与交易场景的数据仓库：实时数据落在 SQLite WAL 热库，历史数据以 Parquet 归档到 S3 冷库。行情、新闻与事件被挂上同一片"数据天穹"，供生态内的产品读取、分发与导出。

浏览器端提供目录授权同步：用户打开网页、选择一个本地目录，Web 用 File System Access API 把远端的部分数据同步到本地，并始终保持本地目录是 Firmament 的一个子集。

## 核心概念

- **数据集（dataset）**：一个按目录组织、可整包导出或同步的数据集合。每个数据集有一个层级标记：`hot`（实时，来自热库）或 `cold`（归档，来自冷库）。
- **导出（export）**：对单个文件的即取即用下载，适合研究时取一小段数据。
- **同步器（syncer）**：一份"数据集 → 本地目录"的同步配置（可带子目录前缀做子集）。同步在浏览器内完成：选择目录、授权、按远端清单（文件 + SHA-256 + 大小）增量下载。
- **本地状态**：同步器在被选目录里维护 `.firmament/sync.json`，记录上次同步的文件与哈希；远端删除的文件会在下一次同步时从本地清理。

## 数据架构

- **热库**：`~/.firmament/default.sqlite3`，启动时强制 `journal_mode=WAL`（允许读取在写入时继续进行）。
- **冷库**：Parquet 对象存于 S3 专用桶，按数据集前缀归档；冷数据在导出与同步时按需获取。

## 认证与 Linkit

- 前端通过 [Auth Mini](https://auth.ntnl.io) 登录（audience 为 `firma.ntnl.io`，并同时申请 `linkit.ntnl.io` 以复用 Linkit 集成会话）。
- 后端用 `auth-mini-axum` 校验 Auth Mini JWKS。第一个确认初始化的用户成为 `root_user_id`。
- Linkit 通知为每位用户独立配置的 Bot 凭证（`sk-…` Token）：保存后可用于发送同步与导出通知；Token 以 AES-256-GCM 加密存储。

## 本地开发

需要 Rust 1.93 与 Node.js 24：

```bash
cd web && npm ci && npm run build
cd .. && cargo test --all-targets --all-features
cargo run
```

服务监听 `127.0.0.1:8080`。SQLite 位于 `~/.firmament/default.sqlite3`。

## 发布

`main` 分支的每次合并会触发 release：构建前端与 Linux 二进制（`firmament-x86_64-unknown-linux-gnu.tar.gz`），产出一个 GitHub Release，并通过 AWS SSM 部署到 `firmament-prod` 实例；部署脚本以 `https://firma.ntnl.io/api/health` 做健康检查。
