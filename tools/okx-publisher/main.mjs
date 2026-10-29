#!/usr/bin/env node
// OKX reference publisher for Firmament.
//
// Firmament never collects anything by itself: it owns a writable hot store and
// cools it into Parquet. This script is the collecting half — it pulls
// BTC-USDT-SWAP perpetual-swap data from OKX's public API. A dataset is one
// table, so the two shapes live in two datasets:
//
//   <dataset>          klines  (symbol, updated_at, open, high, low, close, volume)
//   <funding-dataset>  funding (symbol, updated_at, funding_rate)
//
// `updated_at` is the instant the information was produced, which is what the
// cold layout is partitioned by. Re-sending the same primary key replaces the
// row, so the current minute can be overwritten as it settles.
//
// Usage:
//   node main.mjs --write-token <secret> --since 2026-09-01 [options]
//
//   --write-token <secret>   Firmament write token (required)
//   --since <YYYY-MM-DD>     window start, inclusive (required)
//   --until <YYYY-MM-DD>     window end, exclusive; defaults to now
//   --base-url <url>         Firmament base URL (default http://127.0.0.1:8080)
//   --dataset <id>           klines dataset id (default okx-btc-swap)
//   --funding-dataset <id>   funding dataset id (default <dataset>-funding)
//   --inst <instId>          OKX instrument (default BTC-USDT-SWAP)
//   --okx-base <url>         OKX API base (default https://www.okx.com)
//   --pace-ms <ms>           delay between OKX requests (default 150)
//   --batch <n>              rows per ingest request (default 1000)
//   --dry-run                collect and count without writing

import { setTimeout as sleep } from "node:timers/promises";

const BOOLEAN_FLAGS = new Set(["dry-run"]);

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (!flag.startsWith("--")) {
      throw new Error(`unexpected argument: ${flag}`);
    }
    const name = flag.slice(2);
    if (BOOLEAN_FLAGS.has(name)) {
      options[name] = true;
      continue;
    }
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`missing value for ${flag}`);
    }
    options[name] = value;
    index += 1;
  }
  return options;
}

function required(options, name) {
  const value = options[name];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`--${name} is required`);
  }
  return value;
}

function parseDateMs(value, name) {
  const ms = Date.parse(`${value}T00:00:00Z`);
  if (Number.isNaN(ms)) {
    throw new Error(`--${name} must be a date like 2026-09-01`);
  }
  return ms;
}

async function getText(url) {
  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      const response = await fetch(url, { headers: { accept: "application/json" } });
      if (response.status === 429 || response.status >= 500) {
        throw new Error(`HTTP ${response.status}`);
      }
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return await response.text();
    } catch (error) {
      if (attempt >= 5) {
        throw error;
      }
      await sleep(1000 * attempt);
    }
  }
}

async function okxGet(context, path, params) {
  const url = new URL(path, context.okxBase);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  const payload = JSON.parse(await getText(url));
  if (payload.code !== "0") {
    throw new Error(`OKX error on ${path}: ${payload.code} ${payload.msg}`);
  }
  return payload.data ?? [];
}

async function fetchCandles(context, inst, startMs, endMs) {
  const rows = new Map();
  let cursor = endMs;
  let pages = 0;
  while (true) {
    await sleep(context.pace);
    const page = await okxGet(context, "/api/v5/market/history-candles", {
      instId: inst,
      bar: "1m",
      after: String(cursor),
      limit: "100",
    });
    if (page.length === 0) {
      break;
    }
    pages += 1;
    for (const row of page) {
      const ts = Number(row[0]);
      if (row[8] === "1" && ts >= startMs && ts < endMs) {
        rows.set(ts, row);
      }
    }
    const oldest = Math.min(...page.map((row) => Number(row[0])));
    // OKX returns records strictly earlier than `after`; a page that does not
    // move the cursor would otherwise page forever.
    if (oldest >= cursor) {
      throw new Error("candle pagination did not advance");
    }
    if (oldest < startMs) {
      break;
    }
    cursor = oldest;
    if (pages % 50 === 0) {
      console.log(`  candles: ${rows.size} rows so far, back to ${isoMinute(oldest)}`);
    }
  }
  return [...rows.values()].sort((left, right) => Number(left[0]) - Number(right[0]));
}

async function fetchFunding(context, inst, startMs, endMs) {
  const records = new Map();
  let cursor = endMs;
  while (true) {
    await sleep(context.pace);
    const page = await okxGet(context, "/api/v5/public/funding-rate-history", {
      instId: inst,
      after: String(cursor),
      limit: "100",
    });
    if (page.length === 0) {
      break;
    }
    for (const record of page) {
      const ts = Number(record.fundingTime);
      if (ts >= startMs && ts < endMs) {
        records.set(ts, record);
      }
    }
    const oldest = Math.min(...page.map((record) => Number(record.fundingTime)));
    if (oldest >= cursor) {
      throw new Error("funding pagination did not advance");
    }
    if (oldest < startMs) {
      break;
    }
    cursor = oldest;
  }
  return [...records.values()].sort(
    (left, right) => Number(left.fundingTime) - Number(right.fundingTime),
  );
}

function isoMinute(ts) {
  return new Date(ts).toISOString().replace(/\.\d{3}Z$/, "Z");
}

function candleRow(inst, row) {
  return {
    symbol: inst,
    updated_at: isoMinute(Number(row[0])),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]),
  };
}

function fundingRow(inst, record) {
  return {
    symbol: inst,
    updated_at: isoMinute(Number(record.fundingTime)),
    funding_rate: Number(record.fundingRate),
  };
}

async function ingest(context, dataset, label, rows) {
  const url = `${context.baseUrl}/api/v1/ingest/${dataset}`;
  let written = 0;
  for (let index = 0; index < rows.length; index += context.batch) {
    const chunk = rows.slice(index, index + context.batch);
    const response = await fetch(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${context.writeToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ upsert: chunk }),
    });
    if (!response.ok) {
      const detail = await response.text().catch(() => "");
      throw new Error(`ingest ${label} failed: HTTP ${response.status} ${detail}`);
    }
    written += chunk.length;
    console.log(`  ${label}: ${written}/${rows.length} rows`);
  }
  return written;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const startMs = parseDateMs(required(options, "since"), "since");
  const endMs = options.until === undefined ? Date.now() : parseDateMs(options.until, "until");
  if (endMs <= startMs) {
    throw new Error("--until must be later than --since");
  }
  const context = {
    baseUrl: (options["base-url"] ?? "http://127.0.0.1:8080").replace(/\/$/, ""),
    dataset: options.dataset ?? "okx-btc-swap",
    fundingDataset: options["funding-dataset"] ?? `${options.dataset ?? "okx-btc-swap"}-funding`,
    writeToken: required(options, "write-token"),
    okxBase: options["okx-base"] ?? "https://www.okx.com",
    pace: Number(options["pace-ms"] ?? 150),
    batch: Number(options.batch ?? 1000),
    dryRun: options["dry-run"] === true,
  };
  const instrument = options.inst ?? "BTC-USDT-SWAP";
  if (!Number.isFinite(context.pace) || context.pace < 0) {
    throw new Error("--pace-ms must be a non-negative number");
  }
  if (!Number.isInteger(context.batch) || context.batch < 1) {
    throw new Error("--batch must be a positive integer");
  }

  console.log(`window ${isoMinute(startMs)} → ${isoMinute(endMs)} (UTC)`);
  const candles = await fetchCandles(context, instrument, startMs, endMs);
  const funding = await fetchFunding(context, instrument, startMs, endMs);
  console.log(`collected ${candles.length} candles and ${funding.length} funding points`);

  const klines = candles.map((row) => candleRow(instrument, row));
  const fundingRows = funding.map((record) => fundingRow(instrument, record));
  if (context.dryRun) {
    console.log(`dry-run complete: ${klines.length} klines and ${fundingRows.length} funding rows`);
    return;
  }
  const writtenKlines = await ingest(context, context.dataset, "klines", klines);
  const writtenFunding = await ingest(context, context.fundingDataset, "funding", fundingRows);
  console.log(
    `done: ${writtenKlines} klines into ${context.dataset} and ${writtenFunding} funding rows into ${context.fundingDataset}`,
  );
}

main().catch((error) => {
  console.error(`okx-publisher: ${error.message}`);
  process.exitCode = 1;
});
