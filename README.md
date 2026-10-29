# Firmament

Firmament 是一台**冷却器**：每个数据集就是一张表，一份 schema 同时有两种存储形态 ——
一个可写的**热库**和一个只追加的 **Parquet 冷库**。日历往前走，早于当前周期的行从热库
导出成 Parquet 进入冷库，热库只留当前周期。下游把冷库增量镜像到本地，用自己的程序读。

线上部署：**https://firma.ntnl.io**

```
数据集（一张表）  │←──── 冷：Parquet，只追加 ────│←── 热：SQLite，可写可改 ──→│
                                     冷却线 = 当前周期起点（按 cadence 在 UTC 切）
```

它不是网盘（没有任意路径，只有声明好的表），不是数据库服务（不承诺查询），也不是传输工具
（同步是镜像，不是通道）。

## 概念

- **数据集**：一张表的全部 —— 建库时声明的表结构、一个冷却节奏（`cadence`），
  以及由它派生出的冷库树。
- **表**：主键、时间列（`timestamp`）、冷库分区粒度（`hour` / `day` / `month`）与列定义，
  创建后不可变更。一个数据集只有一张表，热库里就是一张 `data` 表。
- **冷却节奏**：`daily` / `weekly`（周一起）/ `monthly`。时间一律按 UTC 理解，没有时区配置。
- **热库**：可写 `SQLite`，装当前日历周期的行；周期内随便改。
- **冷却线**：当前周期的 UTC 起点 —— `daily` → 今天 00:00Z，`weekly` → 本周一 00:00Z，
  `monthly` → 本月 1 日 00:00Z。早于它的行才可冷却。
- **冷却不清空热库**：只删掉已导出成冷库的那部分行，当前周期永远留在可写的热库里。
- **冷库**：不可变的 Parquet 树 + 每个批次一份 catalogue，写在 S3 上，可脱离应用独立读取。
- **分区粒度与冷却节奏正交**：每天冷却一次的数据也可以按小时或按月切分区。

## 写入

root 在「数据集」页创建数据集，一次声明冷却节奏与表结构（逐列用表格建模）。请求体例如：

```json
{
  "id": "okx-btc-swap",
  "name": "OKX BTC 永续",
  "cadence": "daily",
  "table": {
    "primary_key": ["symbol", "updated_at"],
    "time_column": "updated_at",
    "granularity": "day",
    "columns": [
      { "name": "symbol", "type": "text" },
      { "name": "updated_at", "type": "timestamp" },
      { "name": "close", "type": "real" }
    ]
  }
}
```

root 在「接入」页创建**写入令牌**（只在创建时显示一次，库里只存 SHA-256），发布者用它写行：

```bash
curl -X POST "https://firma.ntnl.io/api/v1/ingest/okx-btc-swap" \
  -H "Authorization: Bearer $FIRMA_WRITE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"upsert":[{"symbol":"BTC-USDT-SWAP","updated_at":"2026-10-01T00:00:00Z","close":78200.5}]}'
```

- 同主键再写一次即覆盖；`{"delete":[{主键...}]}` 只能在尚未冷却的行上删除。
- `updated_at` 是**信息产生的时间**，由写入方给：它决定冷却时机、Parquet 分区和冷库里的版本先后。
  回填过去的时间会在下一轮冷却；写未来时间会一直停在热库。
- 每次写入记入审计（发布者、数据集、行数）。

[`tools/okx-publisher`](tools/okx-publisher) 是一个可直接运行的参考发布者（OKX BTC 永续
1 分钟 K 线与资金费率，分别是两个数据集）。

## 冷却与冷库

后台每 60 秒推进一次，也可以在数据集页手动触发。流程是"认领 → 导出 → 校验 → 提交 → 清空"，
提交点是桶里的 `_catalog/{batch}.json`：提交前失败热库不动，提交后崩溃只会重复导出同一批。
一轮冷却只处理时间列早于冷却线的行，所以热库里始终留着当前周期。

分区桶按 UTC 切（`day=2026-10-01` 指 UTC 的那个日历日），粒度由表声明：

```
datasets/{dataset}/day=2026-10-01/000007-0000.parquet
datasets/{dataset}/_catalog/000007.json
```

契约很简单：

- 文件**写一次永不改写**，同主键的新版本追加成新文件。
- Parquet 里**只有声明的列**，没有隐藏列。
- 同一主键出现多版本时，取 `updated_at` 最大的那条。

## 同步（镜像）

`GET /api/v1/datasets/{id}/manifest?since=<batch>` 返回该批次之后的冷文件
（路径、行数、字节、SHA-256、时间范围）。浏览器同步器把清单增量下载到用户选择的目录，
本地布局与桶一致（`{dataset}/…`），落盘前校验 SHA-256。

增量按**批次**前进，不按时间水位 —— 数据时间是写入方给的，回填会让旧分区出现新文件，
按时间做的增量会永久漏数据。文件不可变，所以镜像只增不覆盖、断点可续；本地状态写在
所选目录的 `.firmament/sync.json`。

## 读取

- 热库：`GET /api/v1/datasets/{id}/rows?limit=1000`，只返回还在热库里的行（尚未冷却）。
- 全量：没有全量接口。全量 = 镜像下来的冷库，用自己的程序读。
- 没有冷热混合查询：边界就是公开的冷却线，要拼自己拼。

## 认证与 Linkit

- 前端通过 [Auth Mini](https://auth.ntnl.io) 登录（audience `firma.ntnl.io`，同时申请
  `linkit.ntnl.io` 以复用 Linkit 会话）。后端用 `auth-mini-axum` 校验 JWKS，第一个确认
  初始化的用户成为 `root_user_id`。
- Linkit 通知自动 ensure：为每位用户创建并维护一个机器人，Bot Token 以 AES-256-GCM
  加密存储，密钥文件 `~/.firmament/credential.key` 权限 0600。

## 状态文件

- `~/.firmament/default.sqlite3`：控制面（数据集、表结构、批次、冷库目录、令牌、审计）。
- `~/.firmament/hot/{dataset}.sqlite3`：每个数据集的热库（WAL，内含一张 `data` 表）。
- `~/.firmament/credential.key`：Linkit 令牌加密密钥。

## 本地开发

需要 Rust 1.93 与 Node.js 24：

```bash
cd web && pnpm install --frozen-lockfile && pnpm run lint && pnpm test && pnpm run build && cd ..
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
cargo run
```

服务监听 `127.0.0.1:8080`。冷却需要 S3 权限：实例通过 IAM instance profile 获得冷库桶的
读写权限，桶与区域在「系统资源」页配置。

## Logo

圆穹顶 Logo 与 [NTNL 首页](https://www.ntnl.io/marks/firma.svg) 使用同一套 SVG 几何：
半圆天穹、椭圆底环与内部经线，32 × 32 viewBox、1.8 单位圆角描边。应用内组件、静态
favicon 和随主题切换的动态 favicon 同步更新；保留现有浅色黑线、深色白线的主题行为。
`web/tests/branding.test.mjs` 校验三处图形及两种主题的 favicon，防止只更新一处而产生不一致。
静态 favicon URL 带版本号，避免 CDN 和浏览器在未登录时沿用旧图形；以后调整几何时同步
更新 `web/index.html` 中的版本号。

## 发行与部署

`main` 分支的每次合并会触发 release：构建前端与 Linux 二进制
（`firmament-x86_64-unknown-linux-gnu.tar.gz`，tag 形如 `v0.1.0-<run_number>`），产出一个
GitHub Release，并通过 AWS SSM 部署到 `firmament-prod` 实例；部署脚本以
`https://firma.ntnl.io/api/health` 做健康检查。

架构与设计决策见 [DESIGN.md](DESIGN.md)，产品定位见 [PRODUCT.md](PRODUCT.md)。
