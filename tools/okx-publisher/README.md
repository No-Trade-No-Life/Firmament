# OKX reference publisher

一个参考发布者：从 OKX 公开接口抓取 BTC 永续合约（默认 `BTC-USDT-SWAP`）的
1 分钟 K 线与资金费率，并写进 Firmament 数据集的表里。

Firmament 自身不做采集 —— 采集与调度都在发布者一侧。本目录提供一个最小、可直接
运行的实现（Node.js 18+，无第三方依赖），也可以作为其他发布者的模板。

## 数据集需要先声明这两张表

```json
[
  {
    "name": "klines",
    "primary_key": ["symbol", "updated_at"],
    "time_column": "updated_at",
    "granularity": "day",
    "columns": [
      { "name": "symbol", "type": "text" },
      { "name": "updated_at", "type": "timestamp" },
      { "name": "open", "type": "real" },
      { "name": "high", "type": "real" },
      { "name": "low", "type": "real" },
      { "name": "close", "type": "real" },
      { "name": "volume", "type": "real" }
    ]
  },
  {
    "name": "funding",
    "primary_key": ["symbol", "updated_at"],
    "time_column": "updated_at",
    "granularity": "day",
    "columns": [
      { "name": "symbol", "type": "text" },
      { "name": "updated_at", "type": "timestamp" },
      { "name": "funding_rate", "type": "real" }
    ]
  }
]
```

`updated_at` 是这条信息产生的时间（客户端给），它同时是热窗口冷却的轴和冷库
Parquet 的分区依据。只写入已确认（confirmed）的 K 线，因此同一分钟可以在当天被
反复覆盖 —— 同主键再写一次就是替换。

## 用法

```bash
node main.mjs --write-token <secret> --since 2026-09-01 [--until 2026-10-01]
```

| 参数 | 说明 |
| --- | --- |
| `--write-token` | Firmament 写入令牌（root 在「接入」页创建），必填 |
| `--since` | 窗口起点（含），UTC 日期，必填 |
| `--until` | 窗口终点（不含），默认当前时刻 |
| `--base-url` | Firmament 地址，默认 `http://127.0.0.1:8080` |
| `--dataset` | 数据集 ID，默认 `okx-btc-swap` |
| `--inst` | OKX 合约，默认 `BTC-USDT-SWAP` |
| `--okx-base` | OKX API 地址，默认 `https://www.okx.com` |
| `--pace-ms` | OKX 请求间隔，默认 150ms（约 6.7 请求/秒） |
| `--batch` | 每次 ingest 请求携带的行数，默认 1000 |
| `--dry-run` | 只抓取与统计，不写入 |

示例：

```bash
# 回补 2026 年 9 月整月
node main.mjs --write-token firmament_xxx --since 2026-09-01 --until 2026-10-01 \
  --base-url https://firma.ntnl.io

# 日常增量：昨天 00:00 之后（含当天尚未走完的分钟，可反复重跑）
node main.mjs --write-token firmament_xxx --since 2026-10-09 \
  --base-url https://firma.ntnl.io
```

## 行为说明

- 写入走 `POST /api/v1/ingest/{dataset}/{table}`，body 是
  `{"upsert":[{...}, ...]}`，按主键覆盖；热窗口内的行还可以用
  `{"delete":[{主键...}]}` 删除。
- 分页按 `after` 向前翻页（每页 100 条）；识别到分页未前进时会报错退出，避免死循环。
- 对 429/5xx 做有限重试（最多 5 次，线性退避）；其余错误直接退出。
- 写入与冷却解耦：写入只落在热窗口；跨过热窗口的行由 Firmament 自己冷却成 Parquet。
