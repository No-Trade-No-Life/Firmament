# Firmament

Firmament 是一位 **Single Truth Publisher**（单一真相发布方）：行情、新闻与事件被发布到同一片"数据天穹"上——**共享是默认的**，本地只是被授权的子集。作为面向研究与交易场景的数据仓库，实时数据落在 SQLite WAL 热库，历史数据以 Parquet 归档到 S3 冷库，供生态内的产品读取、分发与导出。

线上部署：**https://firma.ntnl.io**

浏览器端提供目录授权同步：用户打开网页、选择一个本地目录，Web 用 File System Access API 把远端的部分数据同步到本地，并始终保持本地目录是 Firmament 的一个子集。

## 数据从哪来

Firmament 是一块**黑板**：数据由**外部进程发布**——脚本、Cybion 任务或生态内的其他产品把数据发布到对应数据集之下。采集（抓取、轮询、调度）发生在发布者一侧，Firmament 自身不做采集，只负责保管（热库 / 冷库）与分发（导出 / 同步）。发布者不需要知道读取者，一次发布全生态可读。

治理上是**读开放、写受控**：读取在生态认证内默认开放；写入以署名为前提——root 创建数据集与**写入令牌**（write token），发布者通过发布接口写入，每次发布都记入审计。仓库里的 [`tools/okx-publisher`](tools/okx-publisher) 是一个可直接运行的参考发布者（OKX BTC 永续合约 1 分钟 K 线与资金费率）。

## 发布（写路径）

- **数据集**：root 在「数据集」页面创建；ID 由小写字母、数字与连字符组成。
- **写入令牌**：root 在「发布」页面创建；令牌只在创建时显示一次，库里只保存 SHA-256 哈希，可随时吊销。令牌就是署名，审计中能看到每条发布来自哪个发布者。
- **发布**：`PUT /api/v1/publish/{dataset_id}/{path}`，`Authorization: Bearer <写 token>`，请求体即文件字节（上限 32 MiB）。路径按组件校验：拒绝 `..`、绝对路径与点开头的隐藏段；重发同一路径会原子替换（临时文件 + rename），读者不会看到半成品。
- **审计**：每次发布记录发布者、数据集、路径、大小与 SHA-256；root 可在「发布」页面查看最近的发布记录。

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
- Linkit 通知由 Firmament 自动 ensure：为每位用户创建并维护一个 Linkit 机器人，无需手动配置；Bot Token 以 AES-256-GCM 加密存储，密钥文件 `~/.firmament/credential.key` 权限 0600，可用于发送同步与导出通知。

## 本地开发

需要 Rust 1.93 与 Node.js 24：

```bash
cd web && npm ci && npm run lint && npm test && npm run build && cd ..
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
cargo run
```

服务监听 `127.0.0.1:8080`。SQLite 位于 `~/.firmament/default.sqlite3`。

## Logo

圆穹顶 Logo 与 [NTNL 首页](https://www.ntnl.io/marks/firma.svg) 使用同一套 SVG 几何：半圆天穹、椭圆底环与内部经线，32 × 32 viewBox、1.8 单位圆角描边。应用内组件、静态 favicon 和随主题切换的动态 favicon 同步更新；保留现有浅色黑线、深色白线的主题行为。`web/tests/branding.test.mjs` 校验三处图形及两种主题的 favicon，防止只更新一处而产生不一致。静态 favicon URL 带版本号，避免 CDN 和浏览器在未登录时沿用旧图形；以后调整几何时同步更新 `web/index.html` 中的版本号。

## 发行与部署

`main` 分支的每次合并会触发 release：构建前端与 Linux 二进制（`firmament-x86_64-unknown-linux-gnu.tar.gz`，tag 形如 `v0.1.0-<run_number>`），产出一个 GitHub Release，并通过 AWS SSM 部署到 `firmament-prod` 实例；部署脚本以 `https://firma.ntnl.io/api/health` 做健康检查。

架构与设计决策见 [DESIGN.md](DESIGN.md)，产品定位见 [PRODUCT.md](PRODUCT.md)。
