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
const PARTIAL_MOVE_THRESHOLD = Number(process.env.BACKTEST_PARTIAL_THRESHOLD || 0.05);
const BACKTEST_MODE = (process.env.BACKTEST_MODE || 'build').toLowerCase();
const USE_RESET_CACHE = BACKTEST_MODE === 'build';

function* generateMonthlyDates(startStr, endStr){
  let cursor = dayjs(startStr);
  const end = dayjs(endStr);
  while(!cursor.isAfter(end)){
    yield cursor.format('YYYY-MM-DD');
    cursor = cursor.add(1,'month');
  }
}

function classifyRating(rating){
  if(!rating) return null;
  const text = rating.toString().toLowerCase();
  if(/(buy|long|outperform|overweight|accumulate|增持|買)/.test(text)) return 'bullish';
  if(/(sell|short|underperform|reduce|減持|賣)/.test(text)) return 'bearish';
  if(/(hold|neutral|market perform|equal weight|觀望|持有)/.test(text)) return 'neutral';
  return null;
}

function normalizeBandValue(raw, segment){
  let v = Number(raw);
  if(!Number.isFinite(v) || v <= 0) return null;
  if(v > 1) v = v / 100;
  const isSmallCap = (segment || '').toLowerCase() === 'small_cap';
  const min = isSmallCap ? 0.03 : 0.02;
  const max = isSmallCap ? 0.07 : 0.05;
  if(v < min) v = min;
  if(v > max) v = max;
  return v;
}

function resolveBandPct(action, segment){
  const band = action?.target_band || {};
  const candidates = [
    normalizeBandValue(band.band_pct, segment),
    normalizeBandValue(band.upper_pct, segment),
    normalizeBandValue(band.lower_pct, segment)
  ].filter(val=>Number.isFinite(val) && val>0);
  if(candidates.length) return Math.max(...candidates);
  const isSmallCap = (segment || '').toLowerCase() === 'small_cap';
  return isSmallCap ? 0.06 : 0.04;
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

const monthlyCache = new Map();

async function fetchMonthlyStats(symbol, baselineDate){
  const monthStart = dayjs(baselineDate).add(1,'month').startOf('month');
  const monthEnd = monthStart.endOf('month');
  const cacheKey = `${symbol}_${monthStart.format('YYYY-MM')}`;
  if(monthlyCache.has(cacheKey)) return monthlyCache.get(cacheKey);
  const url = new URL('https://financialmodelingprep.com/stable/historical-price-eod/light');
  url.searchParams.set('symbol', symbol);
  url.searchParams.set('from', monthStart.format('YYYY-MM-DD'));
  url.searchParams.set('to', monthEnd.format('YYYY-MM-DD'));
  url.searchParams.set('apikey', FMP_KEY);
  let payload = { rows: [], monthHigh:null, monthLow:null, rangeMid:null, avgClose:null };
  try{
    const res = await fetch(url);
    if(!res.ok) throw new Error(String(res.status));
    const data = await res.json();
    const rowsRaw = Array.isArray(data?.historical) ? data.historical : Array.isArray(data) ? data : [];
    const rows = rowsRaw
      .map(row=>({
        date: row.date,
        close: Number(row.close ?? row.price ?? row.open ?? null),
        high: Number(row.high ?? row.close ?? row.price ?? null),
        low: Number(row.low ?? row.close ?? row.price ?? null)
      }))
      .filter(row=>row.date && Number.isFinite(row.close));
    rows.sort((a,b)=> dayjs(a.date).valueOf() - dayjs(b.date).valueOf());
    let monthHigh = null;
    let monthLow = null;
    let sumClose = 0;
    rows.forEach(row=>{
      if(Number.isFinite(row.high)) monthHigh = monthHigh==null ? row.high : Math.max(monthHigh, row.high);
      if(Number.isFinite(row.low)) monthLow = monthLow==null ? row.low : Math.min(monthLow, row.low);
      sumClose += row.close;
    });
    const rangeMid = (monthHigh!=null && monthLow!=null) ? (monthHigh + monthLow) / 2 : null;
    const avgClose = rows.length ? sumClose / rows.length : null;
    payload = { rows, monthHigh, monthLow, rangeMid, avgClose };
  }catch(err){
    console.warn('[fetchMonthlyStats]', symbol, baselineDate, err.message);
  }
  monthlyCache.set(cacheKey, payload);
  return payload;
}

function appendRow(rows, row){
  rows.push(row);
  fs.mkdirSync(OUTPUT_DIR,{recursive:true});
  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(rows,null,2));
}

function exportCsv(rows){
  const header = 'ticker,baselineDate,nextDate,rating,target,actual,actual_date,delta_pct,baseline_price,month_high,month_low,range_mid,ma_avg,close_hit,range_mid_hit,intramonth_hit,ma_hit,partial_hit,hold_accuracy,hold_band_pct,hold_drift_flag\n';
  const data = rows.map(r=>[
    r.ticker,
    r.baselineDate,
    r.nextDate,
    r.rating || '',
    r.target ?? '',
    r.actual ?? '',
    r.actualDate || '',
    r.delta!=null ? r.delta.toFixed(2) : '',
    r.baselinePrice ?? '',
    r.monthHigh ?? '',
    r.monthLow ?? '',
    r.rangeMid ?? '',
    r.maAverage ?? '',
    r.closeHit ?? '',
    r.rangeMidHit ?? '',
    r.intramonthHit ?? '',
    r.maHit ?? '',
    r.partialHit ?? '',
    r.holdAccuracy ?? '',
    r.holdBandPct ?? '',
    r.holdDriftFlag ?? ''
  ].join(','));
  fs.writeFileSync(OUTPUT_CSV, header + data.join('\n'));
}

function createMetric(){ return { hit:0, total:0 }; }
function updateMetric(metric, value){
  if(metric && value!==null && value!==undefined){
    metric.total += 1;
    if(value === true) metric.hit += 1;
  }
}
function summarizeMetrics(rows){
  const metrics = {
    close: createMetric(),
    rangeMid: createMetric(),
    intramonth: createMetric(),
    ma: createMetric(),
    partial: createMetric(),
    hold: createMetric()
  };
  rows.forEach(row=>{
    updateMetric(metrics.close, row.closeHit);
    updateMetric(metrics.rangeMid, row.rangeMidHit);
    updateMetric(metrics.intramonth, row.intramonthHit);
    updateMetric(metrics.ma, row.maHit);
    updateMetric(metrics.partial, row.partialHit);
    updateMetric(metrics.hold, row.holdAccuracy);
  });
  return metrics;
}
function formatMetric(metric){
  if(!metric || !metric.total) return '0/0 (n/a)';
  return `${metric.hit}/${metric.total} (${(metric.hit / metric.total * 100).toFixed(1)}%)`;
}
function logMetrics(label, rows){
  const metrics = summarizeMetrics(rows);
  console.log(`[stats] ${label} -> close ${formatMetric(metrics.close)}, rangeMid ${formatMetric(metrics.rangeMid)}, intramonth ${formatMetric(metrics.intramonth)}, MA ${formatMetric(metrics.ma)}, partial ${formatMetric(metrics.partial)}, hold ${formatMetric(metrics.hold)}`);
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
        if(USE_RESET_CACHE){
          await callApi('/api/reset-cache',{ ticker, date: baselineDate });
        }
        const analysis = await callApi('/api/analyze',{ ticker, date: baselineDate });
        const action = analysis?.analysis?.action || {};
        const target = Number(action.target_price) ?? null;
        const rating = action.rating || null;
        const profile = analysis?.analysis?.profile || {};
        const baselinePrice = Number(analysis?.inputs?.price?.value ?? analysis?.fetched?.finnhub_summary?.quote?.c ?? null);
        const direction = classifyRating(rating);
        const bandPct = resolveBandPct(action, profile.segment);
        const nextDate = dayjs(baselineDate).add(1,'month').format('YYYY-MM-DD');
        const monthStats = await fetchMonthlyStats(ticker, baselineDate);
        const { price: actual, asOf: actualDate } = resolveActualFromRows(monthStats.rows, nextDate);
        const delta = (actual!=null && target!=null)
          ? ((actual - target) / target) * 100
          : null;
        const monthHigh = monthStats.monthHigh!=null ? Number(monthStats.monthHigh.toFixed(2)) : null;
        const monthLow = monthStats.monthLow!=null ? Number(monthStats.monthLow.toFixed(2)) : null;
        const rangeMid = monthStats.rangeMid!=null ? Number(monthStats.rangeMid.toFixed(2)) : null;
        const maAverage = monthStats.avgClose!=null ? Number(monthStats.avgClose.toFixed(2)) : null;
        const closeHit = (()=>{
          if(actual==null || baselinePrice==null) return null;
          if(direction === 'bullish') return actual >= baselinePrice;
          if(direction === 'bearish') return actual <= baselinePrice;
          if(direction === 'neutral' && Number.isFinite(bandPct)){
            return Math.abs((actual - baselinePrice) / baselinePrice) <= bandPct;
          }
          return null;
        })();
        const rangeMidHit = (()=>{
          if(rangeMid==null || baselinePrice==null) return null;
          if(direction === 'bullish') return rangeMid >= baselinePrice;
          if(direction === 'bearish') return rangeMid <= baselinePrice;
          if(direction === 'neutral' && Number.isFinite(bandPct)){
            return Math.abs((rangeMid - baselinePrice) / baselinePrice) <= bandPct;
          }
          return null;
        })();
        const intramonthHit = (()=>{
          if(target==null) return null;
          if(direction === 'bullish'){
            return monthHigh!=null ? monthHigh >= target : null;
          }
          if(direction === 'bearish'){
            return monthLow!=null ? monthLow <= target : null;
          }
          return null;
        })();
        const maHit = (()=>{
          if(maAverage==null || baselinePrice==null) return null;
          if(direction === 'bullish') return maAverage >= baselinePrice;
          if(direction === 'bearish') return maAverage <= baselinePrice;
          if(direction === 'neutral' && Number.isFinite(bandPct)){
            return Math.abs((maAverage - baselinePrice) / baselinePrice) <= bandPct;
          }
          return null;
        })();
        const partialHit = (()=>{
          if(baselinePrice==null) return null;
          if(direction === 'bullish'){
            return monthHigh!=null ? monthHigh >= baselinePrice * (1 + PARTIAL_MOVE_THRESHOLD) : null;
          }
          if(direction === 'bearish'){
            return monthLow!=null ? monthLow <= baselinePrice * (1 - PARTIAL_MOVE_THRESHOLD) : null;
          }
          return null;
        })();
        const holdAccuracy = direction === 'neutral' ? closeHit : null;
        const holdDriftFlag = direction === 'neutral' && actual!=null && baselinePrice!=null
          ? Math.abs((actual - baselinePrice) / baselinePrice) > 0.10
          : false;
        const row = {
          ticker,
          baselineDate,
          nextDate,
          rating,
          target,
          actual,
          actualDate,
          delta,
          rationale: action.rationale || '',
          baselinePrice,
          monthHigh,
          monthLow,
          rangeMid,
          maAverage,
          closeHit,
          rangeMidHit,
          intramonthHit,
          maHit,
          partialHit,
          holdAccuracy,
          holdBandPct: Number.isFinite(bandPct) ? bandPct : null,
          holdDriftFlag
        };
        appendRow(rows, row);
        processed.add(key);
        console.log('[backtest] stored', ticker, baselineDate, 'target', target, 'actual', actual, '@', actualDate);
        await new Promise(r=>setTimeout(r, 200));
      }catch(err){
        console.warn('[backtest] failed', ticker, baselineDate, err.message);
      }
    }
    const tickerRows = rows.filter(r=>r.ticker === ticker);
    logMetrics(ticker, tickerRows);
  }
  exportCsv(rows);
  logMetrics('Overall', rows);
  console.log('Backtest completed. Rows:', rows.length);
}

main();
function resolveActualFromRows(rows, targetDate){
  if(!Array.isArray(rows) || !rows.length) return { price:null, asOf:null };
  const exact = rows.find(row=>row.date === targetDate);
  if(exact) return { price: exact.close, asOf: exact.date };
  const target = dayjs(targetDate);
  const fallback = rows.find(row=>dayjs(row.date).isAfter(target) || dayjs(row.date).isSame(target, 'day'));
  if(fallback) return { price: fallback.close, asOf: fallback.date };
  return { price: rows[rows.length-1].close, asOf: rows[rows.length-1].date };
}
