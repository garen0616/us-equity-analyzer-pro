import fetch from 'node-fetch';
import dayjs from 'dayjs';
import fs from 'fs';

const API_BASE = process.env.BACKTEST_API || 'http://localhost:3000';
const FMP_KEY = process.env.FMP_API_KEY || process.env.FMP_KEY || 'TDc1M5BjkEmnB57iOmmfvi8QdBdRLYFA';
const OUTPUT_DIR = process.env.BACKTEST_OUTPUT_DIR || 'output';
const OUTPUT_JSON = `${OUTPUT_DIR}/backtest.json`;
const OUTPUT_CSV = `${OUTPUT_DIR}/backtest.csv`;
const MAX_ACTUAL_LOOKAHEAD_DAYS = Number(process.env.BACKTEST_ACTUAL_LOOKAHEAD_DAYS || 7);
const BASELINE_START = process.env.BACKTEST_START_DATE || '2024-01-01';
const BASELINE_END = process.env.BACKTEST_END_DATE || '2025-11-01';
const TICKERS = (process.env.BACKTEST_TICKERS || 'NVDA,AAPL,MSFT,AMZN,GOOGL').split(',').map(s=>s.trim()).filter(Boolean);

function* generateMonthlyDates(startStr, endStr){
  let cursor = dayjs(startStr);
  const end = dayjs(endStr);
  while(!cursor.isAfter(end)){
    yield cursor.format('YYYY-MM-DD');
    cursor = cursor.add(1,'month');
  }
}

async function callApi(path, payload){
  const res = await fetch(`${API_BASE}${path}`,{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  if(!res.ok){
    const text = await res.text();
    throw new Error(`${path} ${res.status} ${text}`);
  }
  return res.json();
}

async function fetchActualPrice(symbol, targetDate){
  let cursor = dayjs(targetDate);
  for(let attempt=0; attempt<=MAX_ACTUAL_LOOKAHEAD_DAYS; attempt++){
    const queryDate = cursor.format('YYYY-MM-DD');
    const url = new URL('https://financialmodelingprep.com/stable/historical-price-eod/light');
    url.searchParams.set('symbol', symbol);
    url.searchParams.set('from', queryDate);
    url.searchParams.set('to', queryDate);
    url.searchParams.set('apikey', FMP_KEY);
    try{
      const res = await fetch(url);
      if(!res.ok){
        throw new Error(String(res.status));
      }
      const data = await res.json();
      const rows = Array.isArray(data?.historical) ? data.historical : Array.isArray(data) ? data : [];
      if(rows.length){
        const match = rows.find(r=>r.date===queryDate) || rows[0];
        const price = match?.close ?? match?.price ?? match?.open;
        if(Number.isFinite(price)) return { price, asOf: queryDate };
      }
    }catch(err){
      console.warn('[fetchActualPrice]', symbol, queryDate, err.message);
    }
    cursor = cursor.add(1,'day');
  }
  return { price: null, asOf: null };
}

function appendRow(rows, row){
  rows.push(row);
  fs.mkdirSync(OUTPUT_DIR,{recursive:true});
  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(rows,null,2));
}

function exportCsv(rows){
  const header = 'ticker,baselineDate,nextDate,rating,target,actual,actual_date,delta_pct\n';
  const data = rows.map(r=>[
    r.ticker,
    r.baselineDate,
    r.nextDate,
    r.rating || '',
    r.target ?? '',
    r.actual ?? '',
    r.actualDate || '',
    r.delta!=null ? r.delta.toFixed(2) : ''
  ].join(','));
  fs.writeFileSync(OUTPUT_CSV, header + data.join('\n'));
}

async function main(){
  let rows = [];
  if(fs.existsSync(OUTPUT_JSON)){
    try{ rows = JSON.parse(fs.readFileSync(OUTPUT_JSON,'utf8')); }
    catch{ rows = []; }
  }
  const processed = new Set(rows.map(r=>`${r.ticker}_${r.baselineDate}`));

  const baselineDates = Array.from(generateMonthlyDates(BASELINE_START, BASELINE_END));
  for(const ticker of TICKERS){
    for(const baselineDate of baselineDates){
      const key = `${ticker}_${baselineDate}`;
      if(processed.has(key)) continue;
      try{
        await callApi('/api/reset-cache',{ ticker, date: baselineDate });
        const analysis = await callApi('/api/analyze',{ ticker, date: baselineDate });
        const action = analysis?.analysis?.action || {};
        const target = Number(action.target_price) ?? null;
        const rating = action.rating || null;
        const nextDate = dayjs(baselineDate).add(1,'month').format('YYYY-MM-DD');
        const { price: actual, asOf: actualDate } = await fetchActualPrice(ticker, nextDate);
        const delta = (actual!=null && target!=null)
          ? ((actual - target) / target) * 100
          : null;
        const row = { ticker, baselineDate, nextDate, rating, target, actual, actualDate, delta, rationale: action.rationale || '' };
        appendRow(rows, row);
        processed.add(key);
        console.log('[backtest] stored', ticker, baselineDate, 'target', target, 'actual', actual, '@', actualDate);
        await new Promise(r=>setTimeout(r, 200));
      }catch(err){
        console.warn('[backtest] failed', ticker, baselineDate, err.message);
      }
    }
  }
  exportCsv(rows);
  console.log('Backtest completed. Rows:', rows.length);
}

main();
