import 'dotenv/config';
import fetch from 'node-fetch';
import dayjs from 'dayjs';
import { getFmpDailySeries } from '../lib/fmp.js';

const BASE = process.env.BASE_URL || 'http://localhost:3000';
const FMP_KEY = process.env.FMP_API_KEY || process.env.FMP_KEY || '';

const TICKER = (process.env.TEST_TICKER || 'NVDA').toUpperCase();
const BASELINE_DATE = process.env.TEST_DATE || '2024-01-02'; // 請選擇至少已經過去 6 個月的日期
const MONTHS_AHEAD = Number(process.env.TEST_HORIZON_MONTHS || 6);
const TARGET_TOL = Number(process.env.TEST_TARGET_TOL || 0.05); // 允許偏差 5%

async function analyzeOnce(ticker, date) {
  const payload = {
    ticker,
    date,
    mode: 'full'
  };
  const res = await fetch(`${BASE}/api/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || j.error) {
    const msg = j?.error || `HTTP ${res.status}`;
    throw new Error(`analyze failed: ${msg}`);
  }
  return j;
}

async function fetchSeriesWindow(ticker, fromDate, toDate) {
  if (!FMP_KEY) {
    throw new Error('Missing FMP_API_KEY / FMP_KEY for historical series');
  }
  const all = await getFmpDailySeries(ticker, FMP_KEY, 900);
  const from = dayjs(fromDate);
  const to = dayjs(toDate);
  return all.filter(row => {
    const d = dayjs(row.date);
    return d.isSame(from, 'day') || d.isAfter(from, 'day') && (d.isBefore(to, 'day') || d.isSame(to, 'day'));
  });
}

function summarizeTargetHit({ baselinePrice, targetPrice, windowRows }) {
  if (!baselinePrice || !targetPrice || !windowRows?.length) {
    return { hit: false, max: null, min: null };
  }
  let max = -Infinity;
  let min = Infinity;
  for (const row of windowRows) {
    const c = Number(row.close || row.price || row.high || row.low);
    if (!Number.isFinite(c)) continue;
    if (c > max) max = c;
    if (c < min) min = c;
  }
  if (!Number.isFinite(max) || !Number.isFinite(min)) {
    return { hit: false, max: null, min: null };
  }
  const up = targetPrice > baselinePrice;
  const down = targetPrice < baselinePrice;
  let hit = false;
  if (up) {
    hit = max >= targetPrice * (1 - TARGET_TOL);
  } else if (down) {
    hit = min <= targetPrice * (1 + TARGET_TOL);
  }
  return { hit, max, min };
}

async function main() {
  console.log(`Self-test (target price, ${MONTHS_AHEAD}m horizon) for ${TICKER} @ ${BASELINE_DATE}`);
  const base = dayjs(BASELINE_DATE);
  if (!base.isValid()) {
    throw new Error(`Invalid TEST_DATE: ${BASELINE_DATE}`);
  }
  const horizonEnd = base.add(MONTHS_AHEAD, 'month').format('YYYY-MM-DD');

  const result = await analyzeOnce(TICKER, BASELINE_DATE);
  const action = result?.analysis?.action || {};
  const priceMeta = result?.fetched?.finnhub_summary?.price_meta || {};
  const baselinePrice = Number(priceMeta.value ?? result?.inputs?.price?.value);
  const targetPrice = Number(action.target_price);

  console.log(`  baseline price: ${baselinePrice || 'N/A'}`);
  console.log(`  target price:   ${targetPrice || 'N/A'}`);

  const windowRows = await fetchSeriesWindow(TICKER, BASELINE_DATE, horizonEnd);
  console.log(`  historical rows between ${BASELINE_DATE} and ${horizonEnd}: ${windowRows.length}`);

  const summary = summarizeTargetHit({ baselinePrice, targetPrice, windowRows });
  console.log(`  window max: ${summary.max != null ? summary.max.toFixed(2) : 'N/A'}`);
  console.log(`  window min: ${summary.min != null ? summary.min.toFixed(2) : 'N/A'}`);
  console.log(`  target tolerance: ${(TARGET_TOL * 100).toFixed(1)}%`);
  console.log(`  hit within ${MONTHS_AHEAD} months? ${summary.hit ? 'YES' : 'NO'}`);
}

main().catch(err => {
  console.error('❌ Target self-test failed:', err.message);
  process.exit(1);
});
