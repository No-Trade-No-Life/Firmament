# Firmament

Firmament 是一位 **Single Truth Publisher**（单一真相发布方）：行情、新闻与事件被发布到同一片"数据天穹"上——**共享是默认的**，本地只是被授权的子集。作为面向研究与交易场景的数据仓库，实时数据落在 SQLite WAL 热库，历史数据以 Parquet 归档到 S3 冷库，供生态内的产品读取、分发与导出。

线上部署：**https://firma.ntnl.io**

浏览器端提供目录授权同步：用户打开网页、选择一个本地目录，Web 用 File System Access API 把远端的部分数据同步到本地，并始终保持本地目录是 Firmament 的一个子集。

## 数据从哪来

Firmament 是一块**黑板**：数据由**外部进程发布**——脚本、Cybion 任务或生态内的其他产品把数据发布到对应数据集之下。采集（抓取、轮询、调度）发生在发布者一侧，Firmament 自身不做采集，只负责保管（热库 / 冷库）与分发（导出 / 同步）。发布者不需要知道读取者，一次发布全生态可读。

治理上是**读开放、写受控**：读取在生态认证内默认开放；写入以署名为前提（写 token、root 审计）。面向外部发布者的**发布接口**（publish）是路线上的下一步；当前版本内置演示数据集，可先体验完整的读取链路。

## 核心概念

- **数据集（dataset）**：一个按目录组织、可整包导出或同步的数据集合。每个数据集有一个层级标记：`hot`（实时，来自热库）或 `cold`（归档，来自冷库）。
- **导出（export）**：对单个文件的即取即用下载，适合研究时取一小段数据。
- **同步器（syncer）**：一份"数据集 → 本地目录"的**订阅**配置（可带子目录前缀做子集）。同步在浏览器内完成：选择目录、授权、按远端清单（文件 + SHA-256 + 大小）增量下载。
- **本地状态**：同步器在被选目录里维护 `.firmament/sync.json`，记录上次同步的文件与哈希；远端删除的文件会在下一次同步时从本地清理。

## 数据架构

- **热库**：`~/.firmament/default.sqlite3`，启动时强制 `journal_mode=WAL`（允许读取在写入时继续进行）。
- **数据集目录**：`~/.firmament/datasets/`，清单（manifest）对热文件（本地目录）与冷文件（归档目录）生成路径、大小、SHA-256 与更新时间；导出与同步共用这一份清单，读者可凭它自行核对，无需信任平台。
- **冷库**：归档作业把表格文件（CSV → Parquet）按数据集前缀存入专用 S3 桶；冷文件在导出与同步时按需回源（root 可在数据集页触发归档）。

## 认证与 Linkit

- 前端通过 [Auth Mini](https://auth.ntnl.io) 登录（audience 为 `firma.ntnl.io`，并同时申请 `linkit.ntnl.io` 以复用 Linkit 集成会话）。
- 后端用 `auth-mini-axum` 校验 Auth Mini JWKS。第一个确认初始化的用户成为 `root_user_id`。
- Linkit 通知为每位用户独立配置的 Bot 凭证（`sk-…` Token）：保存后可用于发送同步与导出通知；Token 以 AES-256-GCM 加密存储，密钥文件 `~/.firmament/credential.key` 权限 0600。

## 本地开发

需要 Rust 1.93 与 Node.js 24：

```bash
cd web && npm ci && npm run build && cd ..
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
cargo run
```

服务监听 `127.0.0.1:8080`。SQLite 位于 `~/.firmament/default.sqlite3`。

## 发行与部署

`main` 分支的每次合并会触发 release：构建前端与 Linux 二进制（`firmament-x86_64-unknown-linux-gnu.tar.gz`，tag 形如 `v0.1.0-<run_number>`），产出一个 GitHub Release，并通过 AWS SSM 部署到 `firmament-prod` 实例；部署脚本以 `https://firma.ntnl.io/api/health` 做健康检查。

架构与设计决策见 [DESIGN.md](DESIGN.md)，产品定位见 [PRODUCT.md](PRODUCT.md)。
