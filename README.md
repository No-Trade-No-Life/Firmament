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
- **冷库**：一个桶一个 Parquet 对象的扁平树，写在 S3 上，可脱离应用独立读取。
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

冷却永远不在写入请求里发生：后台线程每 60 秒推进一轮，数据集页的「立即冷却」也只是把它
叫醒。流程按**桶**进行："认领 → 合并旧分区 → 导出 → 校验 → 登记 → 清空"。

分区桶按 UTC 切，粒度由表声明。**一个桶就是桶里的一个对象**：

```
datasets/{dataset}/2026-10-01.parquet        # 天
datasets/{dataset}/2026-10-01T13.parquet     # 小时
datasets/{dataset}/2026-10.parquet           # 月
```

契约很简单：

- **分区是整桶快照**：桶里只要多了一行（回填、改数），整个对象重写一次。
- **重写要提升修订号**：`revision` 数据集内单调递增，镜像据此判断"这份分区我是不是已经见过"。
- **重写先读后写**：对象存在却读不出来时这一轮直接失败，绝不拿热库的行覆盖历史。
- Parquet 里**只有声明的列**，没有隐藏列。
- 同一主键在桶内只保留一行，`updated_at` 最大者胜出。

一轮冷却按桶逐个提交、按桶清理热库，并且有预算（每轮最多 128 个分区 / 100 万行），所以
一次大回填不会把一轮冷却拖成几十分钟。崩溃恢复靠"释放认领"而不是删除：上一轮认领过的行
回到未认领状态，下一轮把那个桶从头重写一遍，重写是幂等的。

## 同步（镜像）

`GET /api/v1/datasets/{id}/manifest?since=<revision>` 返回修订号大于该值的分区
（路径、桶、行数、字节、SHA-256、时间范围）。浏览器同步器把清单增量下载到用户选择的目录，
本地布局与桶一致（`{dataset}/…`），落盘前校验 SHA-256。

增量按**修订**前进，不按时间水位 —— 数据时间是写入方给的，回填会让旧分区被重写，
按时间做的增量会永久漏数据。分区不是不可变的：同一路径的内容变了，`revision` 和
SHA-256 就变了，镜像据此重下这一块；两次同步之间被重写多次的分区只取最终状态。
本地状态写在所选目录的 `.firmament/sync.json`。

## 批量导入

`POST /api/v1/import/{dataset}`（写令牌、`Content-Type: application/octet-stream`）
直接上传一个 Parquet 对象，服务端按声明的列解码、清洗、写进热库，等下一轮冷却再落冷库。

```
curl -X POST "https://firma.ntnl.io/api/v1/import/quant-1m-bars" \
  -H "Authorization: Bearer $FIRMA_WRITE_TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @2026-10-01.parquet
```

- 列**按名字匹配**，必须覆盖声明列；多余的列会被忽略，缺少的列直接报错。
- 同一个对象（SHA-256 相同）重复上传是幂等的，返回 `skipped: true`。
- 建议**按桶上传**：一天一个对象。按品种逐个上传会让同一天的分区被重写多次。

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

- `~/.firmament/default.sqlite3`：控制面（数据集、表结构、冷分区、令牌、审计、导入账本）。
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
