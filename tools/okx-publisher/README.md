# OKX reference publisher

一个参考发布者：从 OKX 公开接口抓取 BTC 永续合约（默认 `BTC-USDT-SWAP`）的
1 分钟 K 线与资金费率，并通过 Firmament 的发布接口写入数据集。

Firmament 自身不做采集——采集、调度由发布者一侧负责。本目录提供一个最小的、
可直接运行的发布者实现（Node.js 18+，无第三方依赖），也可以作为其他发布者的模板。

## 数据集布局

```
klines-1m/YYYY-MM-DD.csv    1 分钟 K 线（每天一个文件）
funding-rate/YYYY-MM.csv    资金费率（每月一个文件）
```

CSV 均为 UTF-8、LF 换行，时间戳为 UTC（ISO 8601，分钟精度）：

- `klines-1m/2026-09-03.csv`：`timestamp,open,high,low,close,volume`
- `funding-rate/2026-09.csv`：`timestamp,funding_rate`

只写入已确认（confirmed）的 K 线，因此当天的文件是一个稳定前缀，可以在一天内
反复重发；已经完整的日子重复发布是无副作用的（同样的字节会被清单与归档识别）。

## 用法

```bash
node main.mjs --write-token <secret> --since 2026-09-01 [--until 2026-10-01]
```

| 参数 | 说明 |
| --- | --- |
| `--write-token` | Firmament 写入令牌（root 在「发布」页面创建），必填 |
| `--since` | 窗口起点（含），UTC 日期，必填 |
| `--until` | 窗口终点（不含），默认当前时刻 |
| `--base-url` | Firmament 地址，默认 `http://127.0.0.1:8080` |
| `--dataset` | 数据集 ID，默认 `okx-btc-swap` |
| `--inst` | OKX 合约，默认 `BTC-USDT-SWAP` |
| `--okx-base` | OKX API 地址，默认 `https://www.okx.com` |
| `--pace-ms` | OKX 请求间隔，默认 150ms（约 6.7 请求/秒） |
| `--dry-run` | 只抓取与渲染，不发布 |

示例：

```bash
# 回补 2026 年 9 月整月（作为归档批次）
node main.mjs --write-token firmament_xxx --since 2026-09-01 --until 2026-10-01 \
  --base-url https://firma.ntnl.io

# 日常增量：昨天 00:00 之后（包含今天的未完成文件，可反复重发）
node main.mjs --write-token firmament_xxx --since 2026-10-09 \
  --base-url https://firma.ntnl.io
```

## 行为说明

- 分页按 `after` 向前翻页（每页 100 条）；识别到分页未前进时会报错退出，避免死循环。
- 对 429/5xx 做有限重试（最多 5 次，线性退避）；其余错误直接退出。
- 每天/每月的文件是"当前窗口"的完整快照：当天（当月）文件会随重发增长，历史文件重发
  字节不变。
- 发布与归档解耦：发布只写热层；root 之后可以在 Firmament 里把数据集归档到冷库。
