import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import dayjs from 'dayjs';
import fetch from 'node-fetch';
import multer from 'multer';
import path from 'path';
import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import { getCIK, getRecentFilings } from './lib/sec.js';
import { fetchMDA } from './lib/secText.js';
import { getRecommendations, getEarnings, getQuote } from './lib/finnhub.js';
import { getAggregatedPriceTarget } from './lib/pricetarget.js';
import { analyzeWithLLM } from './lib/llm.js';
import { getHistoricalPrice } from './lib/historicalPrice.js';
import { getCachedAnalysis, saveAnalysisResult, deleteAnalysis, getStoredResult } from './lib/analysisStore.js';
import { buildNewsBundle } from './lib/news.js';
import { computeMomentumMetrics } from './lib/momentum.js';
import {
  getFmpQuote,
  getFmpBatchQuote,
  getFmpInstitutionalHolders,
  getFmpEarningsCallTranscript,
  getFmpPriceTargetSummary,
  getFmpAnalystEstimates,
  getFmpRatingsSnapshot,
  getFmpRatingsHistorical,
  getFmpGrades,
  getFmpGradesHistorical,
  getFmpGradesConsensus,
  getFmpAftermarketQuote,
  getFmpAftermarketTrades,
  getFmpInsiderTrading,
  getFmpInsiderStats,
  getFmpAnalystActions,
  getFmpEconomicCalendar,
  getFmpTreasuryCurve,
  getFmpMarketRiskPremium
} from './lib/fmp.js';
import { getYahooQuote } from './lib/yahoo.js';
import { clearCacheForTicker, getCache as readCache, setCache as writeCache } from './lib/cache.js';
import { summarizeMda } from './lib/mdaSummarizer.js';
import { summarizeCallTranscript } from './lib/callSummarizer.js';
import { enqueueJob } from './lib/jobQueue.js';
import { getAdaptiveLimits, recordUsage } from './lib/usageMonitor.js';

const app = express();
app.use(express.json());
app.use(morgan('dev'));
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const UA   = process.env.SEC_USER_AGENT || 'App/1.0 (email@example.com)';
const SEC_KEY = process.env.SEC_API_KEY || '';
const FH_KEY  = process.env.FINNHUB_KEY || '';
const AV_KEY  = process.env.ALPHAVANTAGE_KEY || '';
const PREMIUM_FMP_KEY_FALLBACK = 'TDc1M5BjkEmnB57iOmmfvi8QdBdRLYFA';
const FMP_KEY = process.env.FMP_API_KEY || process.env.FMP_KEY || PREMIUM_FMP_KEY_FALLBACK;
const OPENAI_KEY = process.env.OPENAI_API_KEY || '';
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-5';
const OPENAI_SECONDARY_MODEL = process.env.OPENAI_MODEL_SECONDARY || 'gpt-4o-mini';
const PRIMARY_MODEL_ALLOWLIST = new Set();
(process.env.OPENAI_MODEL_ALLOWLIST || `${OPENAI_MODEL},${OPENAI_SECONDARY_MODEL}`)
  .split(',')
  .map(v=>v.trim())
  .filter(Boolean)
  .forEach(val=>{
    PRIMARY_MODEL_ALLOWLIST.add(val);
    PRIMARY_MODEL_ALLOWLIST.add(val.toUpperCase());
    PRIMARY_MODEL_ALLOWLIST.add(val.toLowerCase());
  });
const BATCH_CONCURRENCY = Math.max(1, Number(process.env.BATCH_CONCURRENCY || 3));
const upload = multer({ storage: multer.memoryStorage(), limits:{ fileSize: 10 * 1024 * 1024 } });
const DAY_MS = 24 * 60 * 60 * 1000;
const REALTIME_RESULT_TTL_MS = Number(process.env.REALTIME_RESULT_TTL_HOURS || 12) * 60 * 60 * 1000;
const HISTORICAL_RESULT_TTL_MS = Number(process.env.HISTORICAL_RESULT_TTL_DAYS || 120) * DAY_MS;
const FILING_SUMMARY_TTL_MS = 180 * DAY_MS;
const RETRY_ATTEMPTS = Number(process.env.API_RETRY_ATTEMPTS || 3);
const RETRY_DELAY_MS = Number(process.env.API_RETRY_DELAY_MS || 1500);
const NEWS_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const MOMENTUM_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const THIRTEENF_CACHE_TTL_MS = 30 * DAY_MS;
const EARNINGS_CALL_TTL_MS = 30 * DAY_MS;
const ANALYST_AGGREGATE_TTL_MS = 2 * 60 * 60 * 1000;
const ANALYST_PRICE_TARGET_TTL_MS = 7 * DAY_MS;
const ANALYST_ESTIMATES_TTL_MS = 14 * DAY_MS;
const ANALYST_RATING_SNAPSHOT_TTL_MS = 7 * DAY_MS;
const ANALYST_RATING_HISTORY_TTL_MS = 30 * DAY_MS;
const ANALYST_GRADES_TTL_MS = 7 * DAY_MS;
const ANALYST_GRADES_HISTORY_TTL_MS = 14 * DAY_MS;
const ANALYST_GRADES_CONSENSUS_TTL_MS = 14 * DAY_MS;
const ANALYST_GRADES_LOOKBACK_DAYS = Number(process.env.ANALYST_GRADES_LOOKBACK_DAYS || 90);
const AFTERMARKET_CACHE_TTL_MS = Number(process.env.AFTERMARKET_CACHE_TTL_MS || 60) * 1000;
const INSIDER_CACHE_TTL_MS = Number(process.env.INSIDER_CACHE_TTL_HOURS || 3) * 60 * 60 * 1000;
const ANALYST_ACTION_CACHE_TTL_MS = Number(process.env.ANALYST_ACTION_CACHE_TTL_HOURS || 3) * 60 * 60 * 1000;
const MACRO_CACHE_TTL_MS = Number(process.env.MACRO_CACHE_TTL_MINUTES || 60) * 60 * 1000;
const MACRO_EVENT_LIMIT = Number(process.env.MACRO_EVENT_RENDER_LIMIT || 5);
const MACRO_EVENT_LOOKBACK_DAYS = Number(process.env.MACRO_EVENT_LOOKBACK_DAYS || 3);
const MACRO_EVENT_LOOKAHEAD_DAYS = Number(process.env.MACRO_EVENT_LOOKAHEAD_DAYS || 10);
const MAX_FILINGS_FOR_LLM = Math.max(1, Number(process.env.MAX_FILINGS_FOR_LLM || 2));
const NEWS_ARTICLE_LIMIT = Math.max(1, Number(process.env.NEWS_ARTICLE_LIMIT || 4));
const NEWS_EVENT_LIMIT = Math.max(1, Number(process.env.NEWS_EVENT_LIMIT || 3));
const NEWS_KEYWORD_LIMIT = Math.max(1, Number(process.env.NEWS_KEYWORD_LIMIT || 3));
const PREWARM_TICKERS = (process.env.PREWARM_TICKERS || '').split(',').map(t=>t.trim()).filter(Boolean);
const PREWARM_INTERVAL_HOURS = Number(process.env.PREWARM_INTERVAL_HOURS || 6);
const PREWARM_INCLUDE_LLM = process.env.PREWARM_INCLUDE_LLM === 'true';
const REALTIME_QUOTE_TTL_MS = Number(process.env.REALTIME_QUOTE_TTL_MS || 15) * 1000;

const realtimeQuoteCache = new Map();

function cacheRealtimeQuote(symbol, payload){
  if(!symbol || !payload) return;
  realtimeQuoteCache.set(symbol.toUpperCase(), { ...payload, ts: Date.now() });
}

function getCachedRealtimeQuote(symbol){
  if(!symbol) return null;
  const entry = realtimeQuoteCache.get(symbol.toUpperCase());
  if(!entry) return null;
  if(Date.now() - entry.ts > REALTIME_QUOTE_TTL_MS){
    realtimeQuoteCache.delete(symbol.toUpperCase());
    return null;
  }
  return entry;
}

async function prefetchBatchQuotes(symbols){
  if(!FMP_KEY) return;
  if(!Array.isArray(symbols) || !symbols.length) return;
  const pending = Array.from(new Set(
    symbols
      .map(sym=>String(sym || '').trim().toUpperCase())
      .filter(sym=>sym && !getCachedRealtimeQuote(sym))
  ));
  if(!pending.length) return;
  try{
    const rows = await getFmpBatchQuote(pending, FMP_KEY);
    rows.forEach(row=>{
      const symbol = row.symbol?.toUpperCase?.();
      const price = Number(row.price ?? row.c ?? row.currentPrice);
      if(!symbol || !Number.isFinite(price)) return;
      const asOf = row.timestamp
        ? dayjs.unix(Number(row.timestamp)).format('YYYY-MM-DD')
        : (row.date || row.lastUpdated || null);
      cacheRealtimeQuote(symbol, {
        price,
        asOf,
        source: 'fmp_batch_quote'
      });
    });
  }catch(err){
    console.warn('[FMP batch quote]', err.message);
  }
}

function resolveModelName(requested, opts={}){
  const preferSecondary = Boolean(opts.preferSecondary);
  const raw = typeof requested === 'string' ? requested.trim() : '';
  const lc = raw.toLowerCase();
  if(lc === 'auto' || raw === ''){
    return preferSecondary ? OPENAI_SECONDARY_MODEL : OPENAI_MODEL;
  }
  if(lc === 'secondary' || lc === 'fast'){
    return OPENAI_SECONDARY_MODEL;
  }
  if(PRIMARY_MODEL_ALLOWLIST.has(raw)){
    return raw;
  }
  if(PRIMARY_MODEL_ALLOWLIST.has(raw.toUpperCase())){
    return raw.toUpperCase();
  }
  if(PRIMARY_MODEL_ALLOWLIST.has(raw.toLowerCase())){
    return raw.toLowerCase();
  }
  if(preferSecondary) return OPENAI_SECONDARY_MODEL;
  return OPENAI_MODEL;
}

function resolveSecondaryModel(){
  return OPENAI_SECONDARY_MODEL;
}

function slimValue(value, key){
  if(value==null) return null;
  if(typeof value === 'string'){
    const limit = /summary|explanation|mda/i.test(key || '') ? 900 : 300;
    return value.length > limit ? `${value.slice(0, limit)}...` : value;
  }
  if(typeof value === 'number' && !Number.isFinite(value)) return null;
  if(typeof value === 'object' && !Array.isArray(value)){
    const out = {};
    for(const [k,v] of Object.entries(value)){
      const slimmed = slimValue(v, k);
      if(slimmed!=null){
        out[k] = slimmed;
      }
    }
    return Object.keys(out).length ? out : null;
  }
  if(Array.isArray(value)){
    const arr = value.map(item=>slimValue(item, key)).filter(item=>item!=null);
    return arr.length ? arr : null;
  }
  return value;
}

function buildSlimPayload(payload){
  if(!payload) return payload;
  return slimValue(payload, '');
}

function toBps(value, scale=10000){
  if(typeof value !== 'number' || Number.isNaN(value)) return null;
  return Math.round(value * scale);
}

function resolveQuarterYear(date){
  const parsed = dayjs(date);
  const month = parsed.month(); // 0-index
  const quarter = Math.floor(month / 3) + 1;
  const year = parsed.year();
  return { quarter, year };
}

function shiftQuarter(base, offset){
  let { quarter, year } = base;
  let steps = offset;
  while(steps !== 0){
    if(steps > 0){
      quarter++;
      if(quarter > 4){
        quarter = 1;
        year++;
      }
      steps--;
    }else{
      quarter--;
      if(quarter < 1){
        quarter = 4;
        year--;
      }
      steps++;
    }
  }
  return { quarter, year };
}

function formatMillions(val){
  const num = Number(val);
  if(!Number.isFinite(num)) return '';
  const abs = Math.abs(num);
  if(abs >= 1_000_000_000) return `${(num/1_000_000_000).toFixed(1)}B`;
  if(abs >= 1_000_000) return `${(num/1_000_000).toFixed(1)}M`;
  if(abs >= 1000) return `${(num/1000).toFixed(1)}K`;
  return num.toFixed(0);
}

function formatPercent(val, digits=1){
  const num = Number(val);
  if(!Number.isFinite(num)) return '';
  const pct = num > 1 ? num : num * 100;
  return `${pct.toFixed(digits)}%`;
}

function normalizeExtendedQuoteSnapshot(quote, trades){
  if(!quote) return null;
  const extended = {
    session: (quote.session || quote.type || 'afterhours').toLowerCase(),
    price: toFloat(quote.price ?? quote.last ?? quote.close),
    change: toFloat(quote.change ?? quote.priceChange ?? quote.delta),
    change_percent: toFloat(quote.changePercent ?? quote.change_percentage ?? quote.percentChange),
    volume: toFloat(quote.volume ?? quote.size ?? quote.totalVolume) ?? null,
    as_of: quote.timestamp
      ? dayjs(Number(quote.timestamp) * (quote.timestamp > 1e12 ? 1 : 1000)).toISOString()
      : (quote.date || quote.time || quote.lastTradeTime || null),
    source: 'fmp_aftermarket'
  };
  if(Array.isArray(trades) && trades.length){
    extended.recent_trades = trades.slice(0,3).map(trade=>({
      price: toFloat(trade.price ?? trade.executionPrice ?? trade.lastPrice),
      size: toFloat(trade.size ?? trade.quantity ?? trade.lastSize),
      venue: trade.exchange || trade.venue || null,
      timestamp: trade.timestamp
        ? dayjs(Number(trade.timestamp) * (trade.timestamp > 1e12 ? 1 : 1000)).toISOString()
        : (trade.date || trade.time || null)
    })).filter(item=> item.price!=null || item.size!=null);
  }
  return extended;
}

function summarizeInsiderActivity(trades=[], stats=[]){
  const latestStat = Array.isArray(stats) ? stats.find(Boolean) : null;
  const summary = latestStat ? {
    period: latestStat.period || latestStat.date || null,
    net_shares: toFloat(latestStat.netShares ?? latestStat.totalBoughtShares - latestStat.totalSoldShares),
    net_value: toFloat(latestStat.netTransactionValue ?? latestStat.totalBoughtValue - latestStat.totalSoldValue),
    buy_ratio: toFloat(latestStat.buyPercentage ?? latestStat.buySellRatio),
    total_trades: latestStat.totalTransactions ?? latestStat.total ?? null
  } : null;
  const recent = Array.isArray(trades)
    ? trades.slice(0,5).map(row=>({
        date: row.transactionDate || row.filingDate || row.date || null,
        insider: row.insiderName || row.reportedTitle || row.officerName || row.name || 'Insider',
        relation: row.position ?? row.relationship ?? '',
        type: row.transactionType || row.type || '',
        shares: toFloat(row.shares) ?? toFloat(row.quantity),
        price: toFloat(row.price ?? row.transactionPrice),
        value: toFloat(row.totalValue ?? (row.shares && row.price ? row.shares * row.price : null))
      }))
      .filter(item=>item.date || item.insider)
    : [];
  if(!summary && !recent.length) return null;
  const sentiment = summary?.net_shares!=null
    ? (summary.net_shares > 0 ? '連續淨買超' : summary.net_shares < 0 ? '持續淨賣超' : '買賣均衡')
    : null;
  return {
    summary_text: sentiment,
    stats: summary,
    recent
  };
}

function summarizeAnalystActions(rows=[]){
  if(!Array.isArray(rows) || !rows.length) return null;
  const now = dayjs();
  let upgrades7 = 0, downgrades7 = 0, upgrades30 = 0, downgrades30 = 0;
  const normalized = rows.slice(0,6).map(row=>{
    const date = dayjs(row.publishedDate || row.date || row.lastUpdated || row.effectiveDate);
    const days = date.isValid() ? now.diff(date, 'day') : null;
    const action = (row.action || row.grade || '').toLowerCase();
    if(days!=null){
      if(days <= 7){
        if(/upgrade|raise/i.test(row.action || row.toGrade || '')) upgrades7++;
        if(/downgrade|lower/i.test(row.action || row.toGrade || '')) downgrades7++;
      }
      if(days <= 30){
        if(/upgrade|raise/i.test(row.action || row.toGrade || '')) upgrades30++;
        if(/downgrade|lower/i.test(row.action || row.toGrade || '')) downgrades30++;
      }
    }
    return {
      date: date.isValid() ? date.format('YYYY-MM-DD') : (row.date || null),
      firm: row.firm || row.company || row.analystCompany || '',
      action: row.action || row.toGrade || '',
      from: row.fromGrade || row.oldGrade || '',
      to: row.toGrade || row.newGrade || '',
      price_target: toFloat(row.priceTarget) ?? toFloat(row.newPriceTarget)
    };
  });
  return {
    window_7d:{ upgrades: upgrades7, downgrades: downgrades7 },
    window_30d:{ upgrades: upgrades30, downgrades: downgrades30 },
    recent: normalized
  };
}

function pickLatest(rows){
  if(!Array.isArray(rows) || !rows.length) return null;
  return [...rows].sort((a,b)=> dayjs(b.date || b.publishedDate).valueOf() - dayjs(a.date || a.publishedDate).valueOf())[0];
}

function summarizeMacroSnapshot({ events=[], twoYear=[], tenYear=[], riskPremium=[] }){
  if(!events.length && !twoYear.length && !riskPremium.length) return null;
  const latest10 = pickLatest(tenYear);
  const latest2 = pickLatest(twoYear);
  const y10 = toFloat(latest10?.value ?? latest10?.close ?? latest10?.yield);
  const y2 = toFloat(latest2?.value ?? latest2?.close ?? latest2?.yield);
  const spread = (y10!=null && y2!=null) ? y10 - y2 : null;
  const curatedEvents = events
    .filter(ev=>ev && ev.event)
    .map(ev=>({
      date: ev.date || ev.publishedDate || ev.time || null,
      event: ev.event || ev.title || '',
      country: ev.country || ev.region || '',
      actual: ev.actual ?? ev.actualValue ?? null,
      consensus: ev.consensus ?? ev.estimate ?? null,
      previous: ev.previous ?? null,
      impact: ev.importance || ev.impact || null
    }))
    .slice(0, MACRO_EVENT_LIMIT);
  const latestRisk = pickLatest(riskPremium);
  return {
    yields:{
      y10,
      y2,
      spread,
      as_of: latest10?.date || latest2?.date || null
    },
    upcoming_events: curatedEvents,
    market_risk_premium: latestRisk ? {
      value: toFloat(latestRisk.value ?? latestRisk.marketRiskPremium ?? latestRisk.riskPremium),
      as_of: latestRisk.date || null
    } : null
  };
}

function parsePublishers(raw){
  if(!raw) return [];
  try{
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.slice(0, 10) : [];
  }catch{
    return [];
  }
}

function summarizePriceTargetSummary(row){
  if(!row) return null;
  return {
    last_month:{ count: row.lastMonthCount ?? null, avg: row.lastMonthAvgPriceTarget ?? null },
    last_quarter:{ count: row.lastQuarterCount ?? null, avg: row.lastQuarterAvgPriceTarget ?? null },
    last_year:{ count: row.lastYearCount ?? null, avg: row.lastYearAvgPriceTarget ?? null },
    all_time:{ count: row.allTimeCount ?? null, avg: row.allTimeAvgPriceTarget ?? null },
    publishers: parsePublishers(row.publishers)
  };
}

async function fetchAftermarketSnapshot(ticker){
  if(!FMP_KEY) return null;
  const cacheKey = `aftermarket_${ticker}`;
  const cached = await readCache(cacheKey, AFTERMARKET_CACHE_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const [quote, trades] = await Promise.all([
      getFmpAftermarketQuote(ticker, FMP_KEY),
      getFmpAftermarketTrades(ticker, FMP_KEY, { limit:3 })
    ]);
    const normalized = normalizeExtendedQuoteSnapshot(quote, trades);
    await writeCache(cacheKey, normalized || { __empty:true });
    return normalized;
  }catch(err){
    console.warn('[Aftermarket]', err.message);
    await writeCache(cacheKey, { __empty:true });
    return null;
  }
}

async function fetchInsiderSnapshot(ticker){
  if(!FMP_KEY) return null;
  const cacheKey = `insider_${ticker}`;
  const cached = await readCache(cacheKey, INSIDER_CACHE_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const [trades, stats] = await Promise.all([
      getFmpInsiderTrading({ symbol: ticker, limit: 10 }, FMP_KEY),
      getFmpInsiderStats({ symbol: ticker, period:'monthly', limit:2 }, FMP_KEY)
    ]);
    const summary = summarizeInsiderActivity(trades, stats);
    await writeCache(cacheKey, summary || { __empty:true });
    return summary;
  }catch(err){
    console.warn('[Insider]', err.message);
    await writeCache(cacheKey, { __empty:true });
    return null;
  }
}

async function fetchAnalystActionSnapshot(ticker){
  if(!FMP_KEY) return null;
  const cacheKey = `analyst_actions_${ticker}`;
  const cached = await readCache(cacheKey, ANALYST_ACTION_CACHE_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const actions = await getFmpAnalystActions({ symbol: ticker, limit: 20 }, FMP_KEY);
    const summary = summarizeAnalystActions(actions || []);
    await writeCache(cacheKey, summary || { __empty:true });
    return summary;
  }catch(err){
    console.warn('[AnalystActions]', err.message);
    await writeCache(cacheKey, { __empty:true });
    return null;
  }
}

async function fetchMacroSnapshot(baselineDate){
  if(!FMP_KEY) return null;
  const windowStart = dayjs(baselineDate).subtract(MACRO_EVENT_LOOKBACK_DAYS, 'day').format('YYYY-MM-DD');
  const windowEnd = dayjs(baselineDate).add(MACRO_EVENT_LOOKAHEAD_DAYS, 'day').format('YYYY-MM-DD');
  const cacheKey = `macro_${windowStart}_${windowEnd}`;
  const cached = await readCache(cacheKey, MACRO_CACHE_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const [events, tenYear, twoYear, mrp] = await Promise.all([
      getFmpEconomicCalendar({ from: windowStart, to: windowEnd }, FMP_KEY).catch(err=>{ console.warn('[Macro] calendar failed', err.message); return []; }),
      getFmpTreasuryCurve({ from: windowStart, to: windowEnd, maturity:'10year' }, FMP_KEY).catch(err=>{ console.warn('[Macro] 10y failed', err.message); return []; }),
      getFmpTreasuryCurve({ from: windowStart, to: windowEnd, maturity:'02year' }, FMP_KEY).catch(err=>{ console.warn('[Macro] 2y failed', err.message); return []; }),
      getFmpMarketRiskPremium({ from: windowStart, to: windowEnd }, FMP_KEY).catch(err=>{ console.warn('[Macro] mrp failed', err.message); return []; })
    ]);
    const summary = summarizeMacroSnapshot({ events, tenYear, twoYear, riskPremium: mrp });
    await writeCache(cacheKey, summary || { __empty:true });
    return summary;
  }catch(err){
    console.warn('[Macro] snapshot failed', err.message);
    await writeCache(cacheKey, { __empty:true });
    return null;
  }
}

async function fetchInstitutionalBase(ticker, baselineDate){
  const baseQuarter = resolveQuarterYear(baselineDate);
  const attemptOffsets = [0, -1, -2, -3];
  for(const offset of attemptOffsets){
    const target = offset === 0 ? baseQuarter : shiftQuarter(baseQuarter, offset);
    const cacheKey = `institutional_${ticker}_${target.year}q${target.quarter}`;
    const cached = await readCache(cacheKey, THIRTEENF_CACHE_TTL_MS);
    if(cached){
      if(cached.status === 'missing') continue;
      return cached;
    }
    try{
      const rows = await getFmpInstitutionalHolders({
        symbol: ticker,
        year: target.year,
        quarter: target.quarter,
        key: FMP_KEY
      });
      const summary = summarizeInstitutionalRows(rows);
      if(summary){
        summary.period = summary.period || `Q${target.quarter} ${target.year}`;
        await writeCache(cacheKey, summary);
        return summary;
      }
      await writeCache(cacheKey, { status:'missing' });
    }catch(err){
      console.warn('[Institutional]', err.message);
    }
  }
  return null;
}

function normalizeEstimateList(list, { take=5 }={}){
  if(!Array.isArray(list) || !list.length) return [];
  const sorted = [...list].sort((a,b)=> dayjs(a.date).valueOf() - dayjs(b.date).valueOf());
  return sorted.slice(0, take).map(item=>({
    date: item.date,
    period: item.period || '',
    revenueAvg: item.revenueAvg ?? null,
    revenueHigh: item.revenueHigh ?? null,
    revenueLow: item.revenueLow ?? null,
    epsAvg: item.epsAvg ?? null,
    epsHigh: item.epsHigh ?? null,
    epsLow: item.epsLow ?? null,
    numAnalystsRevenue: item.numAnalystsRevenue ?? null,
    numAnalystsEps: item.numAnalystsEps ?? null
  }));
}

function summarizeAnalystEstimates({ annual=[], quarterly=[] }={}){
  const upcomingQuarterly = normalizeEstimateList(quarterly, { take:4 });
  const forwardAnnual = normalizeEstimateList(annual, { take:4 });
  if(!upcomingQuarterly.length && !forwardAnnual.length) return null;
  return {
    quarterly: upcomingQuarterly,
    annual: forwardAnnual
  };
}

function summarizeRatings({ snapshot, historical }){
  if(!snapshot && (!Array.isArray(historical) || !historical.length)) return null;
  const histSorted = Array.isArray(historical)
    ? [...historical].sort((a,b)=> dayjs(b.date).valueOf() - dayjs(a.date).valueOf())
    : [];
  let trend = null;
  let trendDelta = null;
  let trendWindowDays = null;
  if(histSorted.length >= 2){
    const latest = histSorted[0];
    const anchor = histSorted.find((entry, idx)=>{
      if(idx === 0) return false;
      const diffDays = Math.abs(dayjs(latest.date).diff(dayjs(entry.date), 'day'));
      return diffDays >= 30;
    }) || histSorted[histSorted.length-1];
    if(latest?.overallScore!=null && anchor?.overallScore!=null){
      const diff = latest.overallScore - anchor.overallScore;
      trendDelta = diff;
      trendWindowDays = Math.abs(dayjs(latest.date).diff(dayjs(anchor.date), 'day'));
      trend = diff>0 ? 'up' : (diff<0 ? 'down' : 'flat');
    }
  }
  return {
    snapshot: snapshot ? {
      rating: snapshot.rating,
      overallScore: snapshot.overallScore,
      discountedCashFlowScore: snapshot.discountedCashFlowScore,
      returnOnEquityScore: snapshot.returnOnEquityScore,
      returnOnAssetsScore: snapshot.returnOnAssetsScore,
      debtToEquityScore: snapshot.debtToEquityScore,
      priceToEarningsScore: snapshot.priceToEarningsScore,
      priceToBookScore: snapshot.priceToBookScore
    } : null,
    historical: histSorted.slice(0,5).map(item=>({
      date: item.date,
      rating: item.rating,
      overallScore: item.overallScore
    })),
    trend,
    trend_delta: trendDelta,
    trend_window_days: trendWindowDays
  };
}

function summarizeGrades({ latest, historical, consensus }){
  const recentActions = Array.isArray(latest)
    ? latest.slice(0,5).map(item=>({
        date: item.date,
        gradingCompany: item.gradingCompany,
        previousGrade: item.previousGrade,
        newGrade: item.newGrade,
        action: item.action
      }))
    : [];
  let historicalCounts = null;
  if(Array.isArray(historical) && historical.length){
    const entry = historical[0];
    historicalCounts = {
      buy: entry.analystRatingsBuy ?? null,
      hold: entry.analystRatingsHold ?? null,
      sell: entry.analystRatingsSell ?? null,
      strongSell: entry.analystRatingsStrongSell ?? null
    };
  }
  return {
    recent_actions: recentActions,
    historical_counts: historicalCounts,
    consensus: consensus ? {
      strongBuy: consensus.strongBuy ?? null,
      buy: consensus.buy ?? null,
      hold: consensus.hold ?? null,
      sell: consensus.sell ?? null,
      strongSell: consensus.strongSell ?? null,
      consensus: consensus.consensus || null
    } : null
  };
}

function summarizeAnalystSignals({ summary, estimates, ratingsSnapshot, ratingsHistorical, grades, gradesHistorical, gradesConsensus }){
  const priceTargetSummary = summarizePriceTargetSummary(summary);
  const estimateSummary = summarizeAnalystEstimates(estimates || {});
  const ratingsSummary = summarizeRatings({ snapshot: ratingsSnapshot, historical: ratingsHistorical });
  const gradesSummary = summarizeGrades({ latest: grades, historical: gradesHistorical, consensus: gradesConsensus });
  const hasGrades = gradesSummary && (gradesSummary.recent_actions.length || gradesSummary.historical_counts || gradesSummary.consensus);
  if(!priceTargetSummary && !estimateSummary && !ratingsSummary && !hasGrades){
    return null;
  }
  return {
    price_target_summary: priceTargetSummary,
    estimates: estimateSummary,
    ratings: ratingsSummary,
    grades: gradesSummary
  };
}

function summarizeInstitutionalRows(payload){
  const rows = Array.isArray(payload)
    ? payload
    : (Array.isArray(payload?.holders) ? payload.holders : []);
  const summaryMeta = !Array.isArray(payload) ? payload?.summary : null;
  if((!rows || !rows.length) && !summaryMeta) return null;

  const normalized = Array.isArray(rows)
    ? rows
        .map(row=>{
          const name = row.investorname || row.investorName || row.holder || row.investor || '';
          if(!name) return null;
          const value = Number(row.value ?? row.marketValue ?? row.marketvalue ?? row.latestValue);
          const weightRaw = Number(row.weightPercentage ?? row.weightPercent ?? row.weight ?? row.portfolioPercent);
          const weight = Number.isFinite(weightRaw) ? (weightRaw > 1 ? weightRaw/100 : weightRaw) : null;
          const changeShares = Number(row.change ?? row.changeInShares ?? row.changeShares ?? row.change_shares);
          const reportDate = row.dateReported || row.latestQuarter || row.period || row.date;
          const changePercent = Number(row.changePercent ?? row.changePercentage);
          const shares = Number(row.shares ?? row.share);
          return {
            name,
            value: Number.isFinite(value) ? value : null,
            weight,
            changeShares: Number.isFinite(changeShares) ? changeShares : 0,
            changePercent: Number.isFinite(changePercent) ? changePercent : null,
            reportDate,
            shares: Number.isFinite(shares) ? shares : null
          };
        })
        .filter(Boolean)
    : [];
  normalized.sort((a,b)=> (b.value || 0) - (a.value || 0));
  const top = normalized.slice(0,5).map(item=>({
    name: item.name,
    value: item.value,
    weight: item.weight,
    change_shares: item.changeShares,
    change_percent: item.changePercent
  }));
  const netSharesFromRows = normalized.reduce((sum,item)=> sum + (item.changeShares || 0), 0);
  const netShares = Number.isFinite(Number(summaryMeta?.numberOf13FsharesChange))
    ? Number(summaryMeta.numberOf13FsharesChange)
    : netSharesFromRows;
  const signalLabel = netShares > 0 ? '加碼' : (netShares < 0 ? '減碼' : '持平');
  let latestDate = summaryMeta?.date || summaryMeta?.filingDate || null;
  normalized.forEach(item=>{
    if(item.reportDate && (!latestDate || dayjs(item.reportDate).isAfter(dayjs(latestDate)))){
      latestDate = item.reportDate;
    }
  });
  const summaryParts = [];
  summaryParts.push(`機構${signalLabel}${netShares ? `（淨變動 ${formatMillions(netShares)} 股）` : ''}`);
  if(summaryMeta?.investorsHolding!=null){
    summaryParts.push(`共有 ${summaryMeta.investorsHolding} 家機構持有`);
  }
  if(summaryMeta?.ownershipPercent!=null){
    summaryParts.push(`持股比重約 ${formatPercent(summaryMeta.ownershipPercent,1)}`);
  }
  if(Number.isFinite(summaryMeta?.totalInvested)){
    summaryParts.push(`總投資 ${formatMillions(summaryMeta.totalInvested)}`);
  }
  if(top[0]){
    const topWeight = top[0].weight!=null ? formatPercent(top[0].weight,1) : '';
    summaryParts.push(`${top[0].name} 約 ${formatMillions(top[0].value)} ${topWeight?`(${topWeight})`:''}`);
  }
  const metaStats = summaryMeta ? {
    investors_holding: summaryMeta.investorsHolding,
    ownership_percent: summaryMeta.ownershipPercent,
    total_invested: summaryMeta.totalInvested,
    new_positions: summaryMeta.newPositions,
    increased_positions: summaryMeta.increasedPositions,
    reduced_positions: summaryMeta.reducedPositions,
    closed_positions: summaryMeta.closedPositions,
    put_call_ratio: summaryMeta.putCallRatio,
    number_of_13f_shares: summaryMeta.numberOf13Fshares,
    period_date: summaryMeta.date
  } : null;
  return {
    as_of: latestDate,
    signal:{
      label: signalLabel,
      net_shares: netShares
    },
    top_holders: top,
    summary: summaryParts.filter(Boolean).join('；'),
    metrics: metaStats
  };
}

function toFloat(val){
  const num = Number(val);
  return Number.isFinite(num) ? num : null;
}

function calcGrowth(nextVal, prevVal){
  const nextNum = toFloat(nextVal);
  const prevNum = toFloat(prevVal);
  if(nextNum==null || prevNum==null || prevNum === 0) return null;
  return (nextNum - prevNum) / Math.abs(prevNum);
}

function trendArrow(trend){
  if(trend === 'up') return '↑';
  if(trend === 'down') return '↓';
  if(trend === 'flat') return '→';
  return null;
}

const GRADE_ORDER = {
  'strong buy': 6,
  'outperform': 5,
  'buy': 4,
  'overweight': 4,
  'accumulate': 4,
  'market perform': 3,
  'neutral': 3,
  'hold': 3,
  'equal weight': 3,
  'underperform': 2,
  'underweight': 2,
  'sell': 1
};

function normalizeGradeLabel(label){
  if(!label) return '';
  return String(label).trim().toLowerCase();
}

function deriveGradeDirection(prevGrade, nextGrade){
  const prev = GRADE_ORDER[normalizeGradeLabel(prevGrade)];
  const next = GRADE_ORDER[normalizeGradeLabel(nextGrade)];
  if(next == null || prev == null) return null;
  if(next > prev) return 'upgrade';
  if(next < prev) return 'downgrade';
  return null;
}

function buildGradeStats(grades){
  if(!grades) return null;
  const cutoff = dayjs().subtract(ANALYST_GRADES_LOOKBACK_DAYS, 'day');
  let upgrades = 0;
  let downgrades = 0;
  const actions = Array.isArray(grades.recent_actions) ? grades.recent_actions : [];
  actions.forEach(item=>{
    const date = item.date ? dayjs(item.date) : null;
    if(date?.isValid() && date.isBefore(cutoff)) return;
    const actionLabel = (item.action || '').toLowerCase();
    let direction = null;
    if(/upgrade|raised|overweight|buy/.test(actionLabel)) direction = 'upgrade';
    else if(/downgrade|lowered|underweight|sell/.test(actionLabel)) direction = 'downgrade';
    if(!direction){
      direction = deriveGradeDirection(item.previousGrade, item.newGrade);
    }
    if(direction === 'upgrade') upgrades++;
    if(direction === 'downgrade') downgrades++;
  });
  const consensus = grades.consensus || null;
  return {
    upgrades_90d: upgrades,
    downgrades_90d: downgrades,
    diff_90d: upgrades - downgrades,
    consensus_label: consensus?.consensus || null
  };
}

function buildEstimateStats(estimates){
  if(!estimates) return null;
  const quarterly = Array.isArray(estimates.quarterly) ? estimates.quarterly : [];
  const annual = Array.isArray(estimates.annual) ? estimates.annual : [];
  const nextQuarter = quarterly[0] || null;
  const prevQuarter = quarterly[1] || null;
  const nextYear = annual[0] || null;
  const prevYear = annual[1] || null;
  const summary = (entry)=> entry ? {
    period: entry.period || entry.date || null,
    eps_avg: toFloat(entry.epsAvg),
    revenue_avg: toFloat(entry.revenueAvg)
  } : { period:null, eps_avg:null, revenue_avg:null };
  return {
    quarterly:{
      ...summary(nextQuarter),
      eps_growth: calcGrowth(nextQuarter?.epsAvg, prevQuarter?.epsAvg),
      revenue_growth: calcGrowth(nextQuarter?.revenueAvg, prevQuarter?.revenueAvg)
    },
    annual:{
      ...summary(nextYear),
      eps_growth: calcGrowth(nextYear?.epsAvg, prevYear?.epsAvg),
      revenue_growth: calcGrowth(nextYear?.revenueAvg, prevYear?.revenueAvg)
    }
  };
}

function buildPriceTargetStats(summary){
  if(!summary) return null;
  const monthAvg = toFloat(summary.last_month?.avg);
  const quarterAvg = toFloat(summary.last_quarter?.avg);
  const yearAvg = toFloat(summary.last_year?.avg);
  const recentAvg = monthAvg ?? quarterAvg ?? yearAvg ?? toFloat(summary.all_time?.avg);
  return {
    month_avg: monthAvg,
    month_count: summary.last_month?.count ?? null,
    quarter_avg: quarterAvg,
    quarter_count: summary.last_quarter?.count ?? null,
    year_avg: yearAvg,
    year_count: summary.last_year?.count ?? null,
    recent_avg: recentAvg
  };
}

function buildRatingStats(ratings){
  if(!ratings) return null;
  return {
    latest: ratings.snapshot?.rating || null,
    score: toFloat(ratings.snapshot?.overallScore),
    trend: ratings.trend || null,
    trend_arrow: trendArrow(ratings.trend),
    trend_delta: toFloat(ratings.trend_delta),
    window_days: ratings.trend_window_days ?? null
  };
}

function buildAnalystMetrics(signals){
  if(!signals) return null;
  return {
    price_targets: buildPriceTargetStats(signals.price_target_summary),
    estimates: buildEstimateStats(signals.estimates),
    rating: buildRatingStats(signals.ratings),
    grades: buildGradeStats(signals.grades)
  };
}

function buildNewsSignal(bundle){
  if(!bundle) return null;
  const sentiment = bundle.sentiment || {};
  return {
    sentiment_label: sentiment.sentiment_label || null,
    sentiment_score: toFloat(sentiment.score),
    keywords: Array.isArray(bundle.keywords) ? bundle.keywords.slice(0,3) : [],
    event_count: Array.isArray(sentiment.supporting_events) ? sentiment.supporting_events.length : 0
  };
}

function buildInstitutionalSignal(institutional){
  if(!institutional) return null;
  const payload = {
    signal: institutional.signal?.label || null,
    net_shares: institutional.signal?.net_shares ?? null,
    ownership_percent: institutional.metrics?.ownership_percent ?? null,
    investors_holding: institutional.metrics?.investors_holding ?? null
  };
  if(institutional.insider_activity){
    payload.insider_net_shares = institutional.insider_activity.stats?.net_shares ?? null;
    payload.insider_summary = institutional.insider_activity.summary_text || null;
  }
  return payload;
}

function buildEarningsSignal(earningsCall){
  if(!earningsCall) return null;
  return {
    quarter: earningsCall.quarter || null,
    bullet_count: Array.isArray(earningsCall.bullets) ? earningsCall.bullets.length : 0,
    date: earningsCall.date || null
  };
}

function buildMacroSignal(macro){
  if(!macro) return null;
  return {
    yield_spread: toFloat(macro.yields?.spread),
    yield_10y: toFloat(macro.yields?.y10),
    events: Array.isArray(macro.upcoming_events)
      ? macro.upcoming_events.slice(0,3).map(ev=>({
          date: ev.date,
          event: ev.event,
          country: ev.country,
          impact: ev.impact
        }))
      : []
  };
}

function buildNumericPayload({ ticker, baselineDate, filings, priceMeta, analystMetrics, momentum, institutional, news, earningsCall, macro }){
  return {
    company: ticker,
    ticker,
    baseline_date: baselineDate,
    sec_filings: Array.isArray(filings)
      ? filings.slice(0, MAX_FILINGS_FOR_LLM).map(entry=>({
          form: entry.form,
          filingDate: entry.filingDate,
          reportDate: entry.reportDate
        }))
      : [],
    price: {
      value: toFloat(priceMeta?.value),
      as_of: priceMeta?.as_of || baselineDate,
      source: priceMeta?.source || null,
      extended: priceMeta?.extended ? {
        price: toFloat(priceMeta.extended.price),
        change: toFloat(priceMeta.extended.change),
        change_percent: toFloat(priceMeta.extended.change_percent),
        session: priceMeta.extended.session,
        as_of: priceMeta.extended.as_of
      } : null
    },
    analyst_metrics: analystMetrics || null,
    momentum: momentum ? {
      score: momentum.score ?? null,
      trend: momentum.trend || null,
      returns: momentum.returns ? {
        m3: toFloat(momentum.returns.m3),
        m6: toFloat(momentum.returns.m6),
        m12: toFloat(momentum.returns.m12)
      } : null
    } : null,
    institutional: buildInstitutionalSignal(institutional),
    news: buildNewsSignal(news),
    earnings_call: buildEarningsSignal(earningsCall),
    macro: buildMacroSignal(macro)
  };
}

function filingSummaryCacheKey(ticker, form, filingDate){
  return `filing_summary_${ticker}_${form}_${filingDate}`;
}

function finnhubSnapshotCacheKey(ticker, baselineDate){
  return `finnhub_snapshot_${ticker}_${baselineDate}`;
}

function newsCompactCacheKey(ticker, baselineDate, model){
  return `news_compact_${ticker}_${baselineDate}_${model}`;
}

function momentumCacheKey(ticker, baselineDate){
  return `momentum_compact_${ticker}_${baselineDate}`;
}

function resolveBatchConcurrency(mode){
  if(mode === 'metrics-only') return Math.max(1, Math.min(2, BATCH_CONCURRENCY));
  if(mode === 'cached-only') return Math.max(1, Math.floor(BATCH_CONCURRENCY / 2) || 1);
  return BATCH_CONCURRENCY;
}

function sleep(ms){
  return new Promise(resolve=>setTimeout(resolve, ms));
}

function isRetryableError(err){
  if(!err) return false;
  const status = err.response?.status;
  if(status && (status === 408 || status === 429 || status >= 500)) return true;
  const code = err.code;
  if(['ECONNRESET','ETIMEDOUT','ECONNABORTED','ENETUNREACH','EAI_AGAIN'].includes(code)) return true;
  const msg = err.message || '';
  return /timeout|socket hang up|temporarily unavailable/i.test(msg);
}

async function withRetries(task, attempts=RETRY_ATTEMPTS, delayMs=RETRY_DELAY_MS){
  let lastErr;
  for(let attempt=1; attempt<=Math.max(1, attempts); attempt++){
    try{
      return await task();
    }catch(err){
      lastErr = err;
      if(attempt === attempts || !isRetryableError(err)) throw err;
      await sleep(delayMs * attempt);
    }
  }
  throw lastErr;
}

function findStoredFiling(storedList, form, filingDate){
  if(!Array.isArray(storedList)) return null;
  return storedList.find(item=> item.form === form && item.filingDate === filingDate);
}

function compactRecommendation(reco){
  if(!Array.isArray(reco) || !reco.length) return null;
  const latest = reco.find(r=>r?.period) || reco[0];
  if(!latest) return null;
  const keepFields = ['buy','hold','sell','strongBuy','strongSell'];
  const snapshot = { period: latest.period };
  for(const field of keepFields){
    if(latest[field]!=null) snapshot[field] = latest[field];
  }
  if(latest.total && snapshot.total==null) snapshot.total = latest.total;
  return snapshot;
}

function compactEarningsRows(rows, limit=4){
  if(!Array.isArray(rows)) return [];
  return rows
    .slice(0, limit)
    .map(r=>({
      period: r.period,
      actual: r.actual,
      estimate: r.estimate,
      surprise: r.surprise,
      surprisePercent: r.surprisePercent,
      revenue: r.revenue,
      revenueEstimated: r.revenueEstimated
    }));
}

function compactQuote(quote){
  if(!quote) return null;
  return {
    c: quote.c ?? null,
    h: quote.h ?? null,
    l: quote.l ?? null,
    o: quote.o ?? null,
    pc: quote.pc ?? null
  };
}

function round(val, digits=4){
  if(typeof val !== 'number' || Number.isNaN(val)) return null;
  const factor = 10 ** digits;
  return Math.round(val * factor) / factor;
}

function compactMomentum(momentum){
  if(!momentum) return null;
  return {
    score: momentum.score,
    trend: momentum.trend,
    reference_date: momentum.reference_date,
    returns: momentum.returns ? {
      m3: round(momentum.returns.m3,4),
      m6: round(momentum.returns.m6,4),
      m12: round(momentum.returns.m12,4)
    } : null,
    moving_averages: momentum.moving_averages ? {
      ma20: round(momentum.moving_averages.ma20,2),
      ma50: round(momentum.moving_averages.ma50,2),
      ma200: round(momentum.moving_averages.ma200,2)
    } : null,
    rsi14: round(momentum.rsi14,2),
    atr14: round(momentum.atr14,2),
    volume_ratio: round(momentum.volume_ratio,2),
    price: round(momentum.price,2),
    price_vs_ma: momentum.price_vs_ma,
    etf: momentum.etf ? {
      symbol: momentum.etf.symbol,
      return3m: round(momentum.etf.return3m,4)
    } : null
  };
}

function compactArticles(articles, limit=5){
  if(!Array.isArray(articles)) return [];
  return articles.slice(0, limit).map(a=>({
    title: a.title,
    summary: a.summary,
    url: a.url,
    published_at: a.published_at,
    source: a.source,
    tags: a.tags,
    tone: a.tone
  }));
}

function compactNewsBundle(bundle){
  if(!bundle) return null;
  return {
    keywords: Array.isArray(bundle.keywords) ? bundle.keywords.slice(0,5) : [],
    articles: compactArticles(bundle.articles, 5),
    sentiment: bundle.sentiment ? {
      sentiment_label: bundle.sentiment.sentiment_label,
      summary: bundle.sentiment.summary,
      supporting_events: Array.isArray(bundle.sentiment.supporting_events)
        ? bundle.sentiment.supporting_events.slice(0,3)
        : []
    } : null
  };
}

function trimNewsForPayload(bundle, limit=NEWS_ARTICLE_LIMIT){
  if(!bundle) return null;
  const articles = Array.isArray(bundle.articles) ? bundle.articles : [];
  const sampledArticles = articles.slice(0, limit);
  const sentiment = bundle.sentiment || {};
  const supportingEvents = Array.isArray(sentiment.supporting_events)
    ? sentiment.supporting_events.slice(0, NEWS_EVENT_LIMIT).map(ev=>({
        title: ev.title || null,
        note: ev.reason ? ev.reason.slice(0, 120) : null
      }))
    : [];
  const topSources = Array.from(new Set(
    sampledArticles.map(a=>(a.source || '').toUpperCase()).filter(Boolean)
  )).slice(0,3);
  return {
    keywords: Array.isArray(bundle.keywords) ? bundle.keywords.slice(0, NEWS_KEYWORD_LIMIT) : [],
    sentiment_label: sentiment.sentiment_label || null,
    article_count: articles.length,
    event_count: supportingEvents.length,
    top_sources: topSources,
    events: supportingEvents
  };
}

function errRes(res, err){ console.error('❌', err); return res.status(500).json({error:String(err.message||err)}); }

async function mapWithConcurrency(items, limit, mapper){
  if(!Array.isArray(items) || !items.length) return [];
  const size = Math.max(1, Math.min(limit || 1, items.length));
  const results = new Array(items.length);
  let index = 0;
  const workers = Array.from({ length: size }, ()=>(async function worker(){
    while(true){
      const current = index++;
      if(current >= items.length) break;
      results[current] = await mapper(items[current], current);
    }
  })());
  await Promise.all(workers);
  return results;
}

async function performAnalysis(ticker, date, opts={}){
  const {
    preferCacheOnly=false,
    skipLlm=false,
    model,
    llmCacheTtlMs,
    preferSecondary=false
  } = opts;
  const parsedDate = dayjs(date);
  if(!parsedDate.isValid()) throw new Error('invalid date format');
  const baselineDate = parsedDate.format('YYYY-MM-DD');
  const upperTicker = ticker.toUpperCase();
  const isHistorical = parsedDate.isBefore(dayjs(), 'day');
  const analysisTtl = isHistorical ? HISTORICAL_RESULT_TTL_MS : REALTIME_RESULT_TTL_MS;
  const llmModel = resolveModelName(model, { preferSecondary });
  const secondaryModel = resolveSecondaryModel();
  const effectiveLlmCacheTtl = Number.isFinite(llmCacheTtlMs) ? llmCacheTtlMs : analysisTtl;
  const useSecondarySummaries = !skipLlm && Boolean(OPENAI_KEY);
  const cacheModelKey = skipLlm ? `${llmModel}__metrics` : `${llmModel}__full`;
  const adaptiveLimits = getAdaptiveLimits({ defaultFilings: MAX_FILINGS_FOR_LLM, defaultNews: NEWS_ARTICLE_LIMIT });
  const filingLimit = Math.max(1, adaptiveLimits.maxFilings);
  const effectiveNewsLimit = Math.max(1, adaptiveLimits.newsLimit);

  const cacheLookupKeys = skipLlm ? [cacheModelKey] : [cacheModelKey, llmModel];
  let cacheHit = null;
  for(const key of cacheLookupKeys){
    cacheHit = getCachedAnalysis({
      ticker: upperTicker,
      baselineDate,
      ttlMs: analysisTtl,
      model: key,
      requireAnalysis: !skipLlm
    });
    if(cacheHit){
      if(key !== cacheModelKey){
        saveAnalysisResult({ ticker: upperTicker, baselineDate, isHistorical, model: cacheModelKey, result: cacheHit });
      }
      return cacheHit;
    }
  }

  let storedRecord = getStoredResult({ ticker: upperTicker, baselineDate, model: cacheModelKey });
  let storedResult = storedRecord?.result || null;
  if(!storedResult && !skipLlm){
    storedRecord = getStoredResult({ ticker: upperTicker, baselineDate, model: llmModel });
    storedResult = storedRecord?.result || null;
  }
  const storedUpdatedAt = storedRecord?.updated_at || 0;
  const nowTs = Date.now();
  const storedFreshForAnalysis = storedRecord ? (nowTs - storedUpdatedAt) <= analysisTtl : false;
  const storedFinnhub = storedFreshForAnalysis ? storedResult?.fetched?.finnhub_summary : null;
  const storedSecFilings = storedFreshForAnalysis ? storedResult?.per_filing_summaries : null;
  const storedNewsFresh = storedRecord ? (nowTs - storedUpdatedAt) <= NEWS_CACHE_TTL_MS : false;
  const storedMomentumFresh = storedRecord ? (nowTs - storedUpdatedAt) <= MOMENTUM_CACHE_TTL_MS : false;
  const storedNews = storedNewsFresh ? storedResult?.news : null;
  const storedMomentum = storedMomentumFresh ? storedResult?.momentum : null;
  const storedInstitutional = storedFreshForAnalysis ? storedResult?.institutional : null;
  const storedEarningsCall = storedFreshForAnalysis ? storedResult?.earnings_call : null;
  if(preferCacheOnly && storedFreshForAnalysis && storedResult){
    return storedResult;
  }
  if(preferCacheOnly){
    throw new Error('cache_miss');
  }

  const cik = await getCIK(upperTicker, UA, SEC_KEY);
  const filings = await getRecentFilings(cik, baselineDate, UA, SEC_KEY);

  const finnhubPromise = (async ()=>{
    if(storedFinnhub) return storedFinnhub;
    const cacheContext = baselineDate;
    const finnhubCacheKey = finnhubSnapshotCacheKey(upperTicker, baselineDate);
    const cached = await readCache(finnhubCacheKey, analysisTtl);
    if(cached) return cached;
    const [recoRes, earnRes, quoteRes] = await Promise.allSettled([
      withRetries(()=>getRecommendations(upperTicker, FH_KEY, cacheContext)),
      withRetries(()=>getEarnings(upperTicker, FH_KEY, cacheContext)),
      withRetries(()=>getQuote(upperTicker, FH_KEY, cacheContext))
    ]);
    const finnhub = {
      recommendation: recoRes.status==='fulfilled'?recoRes.value:{ error:recoRes.reason.message },
      earnings:       earnRes.status==='fulfilled'?earnRes.value:{ error:earnRes.reason.message },
      quote:          quoteRes.status==='fulfilled'?quoteRes.value:{ error:quoteRes.reason.message }
    };
    let current = null;
    const priceMeta = {
      source: isHistorical ? 'historical_missing' : 'real-time_missing',
      as_of: isHistorical ? baselineDate : dayjs().format('YYYY-MM-DD'),
      kind: isHistorical ? 'historical' : 'real-time',
      value: null
    };

    if(isHistorical){
      try{
        const hist = await withRetries(()=>getHistoricalPrice(upperTicker, baselineDate, {
          fmpKey: FMP_KEY,
          finnhubKey: FH_KEY,
          alphaKey: AV_KEY,
        }), 2, RETRY_DELAY_MS);
        if(hist?.price!=null){
          current = hist.price;
          priceMeta.source = hist.source;
          priceMeta.as_of = hist.date || baselineDate;
        }
      }catch(err){
        console.warn('[HistoricalPrice]', err.message);
        priceMeta.source = 'real-time_fallback';
        priceMeta.kind = 'real-time';
      }
    }else{
      const cachedRealtime = getCachedRealtimeQuote(upperTicker);
      if(cachedRealtime){
        current = cachedRealtime.price;
        priceMeta.source = cachedRealtime.source || 'fmp_batch_quote';
        if(cachedRealtime.asOf) priceMeta.as_of = cachedRealtime.asOf;
      }
      if(FMP_KEY){
        try{
          if(current==null){
            const quote = await withRetries(()=>getFmpQuote(upperTicker, FMP_KEY));
            current = quote.price;
            priceMeta.source = 'fmp_quote';
            if(quote.asOf) priceMeta.as_of = quote.asOf;
            cacheRealtimeQuote(upperTicker, { price: current, asOf: quote.asOf, source: 'fmp_quote' });
          }
        }catch(err){ console.warn('[FMP Quote]', err.message); }
      }
      if(current==null){
        try{
          const yahoo = await withRetries(()=>getYahooQuote(upperTicker));
          current = yahoo.price;
          priceMeta.source = yahoo.source;
          if(yahoo.asOf) priceMeta.as_of = yahoo.asOf;
        }catch(err){ console.warn('[Yahoo Quote]', err.message); }
      }
      if(current==null){
        priceMeta.source = 'real-time_fallback';
      }
      priceMeta.kind = 'real-time';
    }

    if(current==null && FMP_KEY){
      try{
        const quote = await withRetries(()=>getFmpQuote(upperTicker, FMP_KEY));
        current = quote.price;
        priceMeta.source = 'fmp_quote';
        priceMeta.as_of = quote.asOf || priceMeta.as_of;
        priceMeta.kind = 'real-time';
        cacheRealtimeQuote(upperTicker, { price: current, asOf: quote.asOf, source: 'fmp_quote' });
      }catch(err){ console.warn('[FMP Quote fallback]', err.message); }
    }
    if(current==null){
      try{
        const yahoo = await withRetries(()=>getYahooQuote(upperTicker));
        current = yahoo.price;
        priceMeta.source = yahoo.source;
        priceMeta.as_of = yahoo.asOf || priceMeta.as_of;
        priceMeta.kind = 'real-time';
      }catch(err){ console.warn('[Yahoo Quote fallback]', err.message); }
    }

    priceMeta.value = current;
    const quote = { ...(finnhub.quote || {}), c: current };
    if(!isHistorical){
      try{
        const extended = await fetchAftermarketSnapshot(upperTicker);
        if(extended) priceMeta.extended = extended;
      }catch(err){
        console.warn('[Aftermarket attach]', err.message);
      }
    }

    let ptAgg;
    try{
      ptAgg = await getAggregatedPriceTarget(upperTicker, FH_KEY, AV_KEY, current, FMP_KEY, baselineDate);
    }catch(e){
      ptAgg = {
        source:'unavailable',
        error:e.message,
        targetHigh:null,
        targetLow:null,
        targetMean:null,
        targetMedian:null
      };
    }

    const snapshot = {
      recommendation: Array.isArray(finnhub.recommendation)
        ? compactRecommendation(finnhub.recommendation)
        : (finnhub.recommendation?.error ? { error: finnhub.recommendation.error } : null),
      earnings: Array.isArray(finnhub.earnings)
        ? compactEarningsRows(finnhub.earnings)
        : (finnhub.earnings?.error ? { error: finnhub.earnings.error } : []),
      quote: compactQuote(quote),
      price_target: ptAgg,
      price_meta: priceMeta
    };
    await writeCache(finnhubCacheKey, snapshot);
    return snapshot;
  })();

  const newsPromise = (async ()=>{
    const newsModelKey = skipLlm ? 'noai' : secondaryModel;
    const newsCacheKey = newsCompactCacheKey(upperTicker, baselineDate, newsModelKey);
    let newsCompact = storedNews || await readCache(newsCacheKey, NEWS_CACHE_TTL_MS);
    if(!newsCompact){
      const newsRaw = await buildNewsBundle({
        ticker: upperTicker,
        baselineDate,
        openKey: skipLlm ? null : OPENAI_KEY,
        model: secondaryModel,
        useLlm: !skipLlm,
        articleLimit: effectiveNewsLimit,
        finnhubKey: FH_KEY,
        fmpKey: FMP_KEY
      });
      newsCompact = compactNewsBundle(newsRaw);
      await writeCache(newsCacheKey, newsCompact);
    }
    return newsCompact;
  })();

  const momentumPromise = (async ()=>{
    const momentumKey = momentumCacheKey(upperTicker, baselineDate);
    let momentum = storedMomentum || await readCache(momentumKey, MOMENTUM_CACHE_TTL_MS);
    if(!momentum){
      const momentumRaw = await computeMomentumMetrics(upperTicker, baselineDate);
      momentum = compactMomentum(momentumRaw);
      if(momentum) await writeCache(momentumKey, momentum);
    }
    return momentum;
  })();

  const summaryTargets = filings.slice(0, filingLimit);
  const perFilingSummaries = await mapWithConcurrency(summaryTargets, 3, async (f)=>{
    const stored = findStoredFiling(storedSecFilings, f.form, f.filingDate);
    if(stored?.mda_summary){
      if(useSecondarySummaries && stored.summary_kind === 'fallback'){
        // allow regeneration with LLM if previously fallback
      }else{
        return stored;
      }
    }
    const cacheKey = filingSummaryCacheKey(upperTicker, f.form, f.filingDate);
    const cached = await readCache(cacheKey, FILING_SUMMARY_TTL_MS);
    if(cached?.mda_summary){
      const cachedKind = cached.summary_kind || 'llm';
      if(useSecondarySummaries && cachedKind === 'fallback'){
        // continue to regenerate with LLM
      }else{
        return {
          form:f.form,
          formLabel:f.formLabel,
          filingDate:f.filingDate,
          reportDate:f.reportDate,
          mda_summary: cached.mda_summary,
          mda_excerpt: cached.mda_excerpt,
          summary_kind: cachedKind
        };
      }
    }
    const mda = await fetchMDA(f.url, UA);
    let summaryBlock = { summary: mda.slice(0, 1200), kind: 'fallback' };
    try{
      summaryBlock = await summarizeMda({
        text: mda,
        openKey: OPENAI_KEY,
        model: secondaryModel,
        meta:{ ticker: upperTicker, form: f.form },
        useLlm: useSecondarySummaries
      });
    }catch(err){
      console.warn('[MDA Summary]', err.message);
    }
    const excerpt = summaryBlock.kind === 'fallback' ? mda.slice(0, 400) : undefined;
    const snapshot = {
      form:f.form,
      formLabel:f.formLabel,
      filingDate:f.filingDate,
      reportDate:f.reportDate,
      mda_summary: summaryBlock.summary,
      summary_kind: summaryBlock.kind
    };
    if(excerpt){
      snapshot.mda_excerpt = excerpt;
    }
    await writeCache(cacheKey, {
      mda_summary: snapshot.mda_summary,
      mda_excerpt: snapshot.mda_excerpt,
      summary_kind: snapshot.summary_kind,
      form:f.form,
      ticker: upperTicker
    });
    return snapshot;
  });
  const summaryIndex = new Map(perFilingSummaries.map(item=>{
    const key = `${item.form}_${item.filingDate}`;
    return [key, item];
  }));
  const perFiling = filings.map(f=>{
    const key = `${f.form}_${f.filingDate}`;
    if(summaryIndex.has(key)) return summaryIndex.get(key);
    return {
      form:f.form,
      formLabel:f.formLabel,
      filingDate:f.filingDate,
      reportDate:f.reportDate,
      mda_summary:null,
      summary_kind:null
    };
  });

  const institutionalPromise = (async ()=>{
    let base = storedInstitutional || await fetchInstitutionalBase(upperTicker, baselineDate);
    const [insider, analystActions] = await Promise.all([
      fetchInsiderSnapshot(upperTicker),
      fetchAnalystActionSnapshot(upperTicker)
    ]);
    if(!base && !insider && !analystActions) return null;
    const enriched = { ...(base || {}) };
    if(insider) enriched.insider_activity = insider;
    if(analystActions) enriched.analyst_actions = analystActions;
    return enriched;
  })();

  const earningsCallPromise = (async ()=>{
    if(storedEarningsCall) return storedEarningsCall;
    const base = resolveQuarterYear(baselineDate);
    const attempts = [base, shiftQuarter(base, -1)];
    for(const attempt of attempts){
      const callKey = `earnings_call_${upperTicker}_${attempt.year}q${attempt.quarter}`;
      const cached = await readCache(callKey, EARNINGS_CALL_TTL_MS);
      if(cached){
        if(cached.status === 'missing' && attempt !== attempts[attempts.length-1]) continue;
        return cached;
      }
      try{
        const rows = await getFmpEarningsCallTranscript({
          symbol: upperTicker,
          quarter: attempt.quarter,
          year: attempt.year,
          key: FMP_KEY
        });
        const transcript = Array.isArray(rows) && rows.length ? rows[0] : null;
        if(!transcript || !transcript.content){
          const placeholder = {
            quarter: `Q${attempt.quarter} ${attempt.year}`,
            summary: '近期無可用的 Earnings Call 逐字稿。',
            bullets: [],
            status: 'missing'
          };
          await writeCache(callKey, placeholder);
          continue;
        }
        const summary = await summarizeCallTranscript({
          text: transcript.content,
          openKey: OPENAI_KEY,
          model: secondaryModel,
          meta:{ ticker: upperTicker, quarter:`Q${attempt.quarter}`, year: attempt.year }
        });
        const payload = {
          quarter: `Q${attempt.quarter} ${attempt.year}`,
          date: transcript.date || transcript.transcriptDate || transcript.publishedDate || '',
          summary: summary.summary || '',
          bullets: summary.bullets || [],
          source: transcript.link || transcript.url || transcript.filingUrl || '',
          raw_excerpt: transcript.content.slice(0, 500)
        };
        await writeCache(callKey, payload);
        return payload;
      }catch(err){
        console.warn('[EarningsCall]', err.message);
      }
    }
    return null;
  })();

  const analystSignalsPromise = (async ()=>{
    if(!FMP_KEY) return null;
    const aggregateKey = `analyst_signals_${upperTicker}`;
    const aggregateCached = await readCache(aggregateKey, ANALYST_AGGREGATE_TTL_MS);
    if(aggregateCached) return aggregateCached;

    const fetchWithTtl = async ({ label, ttl, fetcher })=>{
      const key = `analyst_${label}_${upperTicker}`;
      const memo = await readCache(key, ttl);
      if(memo) return memo;
      try{
        const data = await fetcher();
        if(data) await writeCache(key, data);
        return data ?? null;
      }catch(err){
        console.warn(`[AnalystSignals:${label}]`, err.message);
        return null;
      }
    };

    const [summary, estimatesAnnual, estimatesQuarterly, ratingSnapshot, ratingHistorical, gradesLatest, gradesHistorical, gradesConsensus] = await Promise.all([
      fetchWithTtl({
        label:'pts',
        ttl: ANALYST_PRICE_TARGET_TTL_MS,
        fetcher: ()=>getFmpPriceTargetSummary(upperTicker, FMP_KEY)
      }),
      fetchWithTtl({
        label:'est_ann',
        ttl: ANALYST_ESTIMATES_TTL_MS,
        fetcher: ()=>getFmpAnalystEstimates({ symbol: upperTicker, period:'annual', limit:12 }, FMP_KEY)
      }),
      fetchWithTtl({
        label:'est_q',
        ttl: ANALYST_ESTIMATES_TTL_MS,
        fetcher: ()=>getFmpAnalystEstimates({ symbol: upperTicker, period:'quarter', limit:12 }, FMP_KEY)
      }),
      fetchWithTtl({
        label:'rating_snap',
        ttl: ANALYST_RATING_SNAPSHOT_TTL_MS,
        fetcher: ()=>getFmpRatingsSnapshot(upperTicker, FMP_KEY)
      }),
      fetchWithTtl({
        label:'rating_hist',
        ttl: ANALYST_RATING_HISTORY_TTL_MS,
        fetcher: ()=>getFmpRatingsHistorical({ symbol: upperTicker, limit:6 }, FMP_KEY)
      }),
      fetchWithTtl({
        label:'grades_latest',
        ttl: ANALYST_GRADES_TTL_MS,
        fetcher: ()=>getFmpGrades(upperTicker, FMP_KEY)
      }),
      fetchWithTtl({
        label:'grades_hist',
        ttl: ANALYST_GRADES_HISTORY_TTL_MS,
        fetcher: ()=>getFmpGradesHistorical({ symbol: upperTicker, limit:1 }, FMP_KEY)
      }),
      fetchWithTtl({
        label:'grades_cons',
        ttl: ANALYST_GRADES_CONSENSUS_TTL_MS,
        fetcher: ()=>getFmpGradesConsensus(upperTicker, FMP_KEY)
      })
    ]);

    const normalized = summarizeAnalystSignals({
      summary,
      estimates: { annual: estimatesAnnual, quarterly: estimatesQuarterly },
      ratingsSnapshot: ratingSnapshot,
      ratingsHistorical: ratingHistorical,
      grades: gradesLatest,
      gradesHistorical,
      gradesConsensus
    });
    if(normalized){
      await writeCache(aggregateKey, normalized);
      return normalized;
    }
    return null;
  })();

  const macroPromise = fetchMacroSnapshot(baselineDate);

  const [finnhubSnapshot, newsCompact, momentum, institutional, earningsCall, analystSignals, macroInsights] = await Promise.all([
    finnhubPromise,
    newsPromise,
    momentumPromise,
    institutionalPromise,
    earningsCallPromise,
    analystSignalsPromise,
    macroPromise
  ]);

  const priceMeta = finnhubSnapshot?.price_meta || {
    source: isHistorical ? 'historical_missing' : 'real-time_missing',
    as_of: isHistorical ? baselineDate : dayjs().format('YYYY-MM-DD'),
    kind: isHistorical ? 'historical' : 'real-time',
    value: finnhubSnapshot?.quote?.c ?? null
  };
  const ptAgg = finnhubSnapshot?.price_target || null;

  const analystMetrics = buildAnalystMetrics(analystSignals);
  const llmNews = trimNewsForPayload(newsCompact, effectiveNewsLimit);
  const llmPayload = buildNumericPayload({
    ticker: upperTicker,
    baselineDate,
    filings: perFiling,
    priceMeta,
    analystMetrics,
    momentum,
    institutional,
    news: llmNews,
    earningsCall,
    macro: macroInsights
  });
  let llm = null;
  let llmUsage = null;
  if(skipLlm){
    llm = storedResult?.analysis || null;
    llmUsage = storedResult?.llm_usage || null;
  }else{
    const llmTtlMs = Math.min(effectiveLlmCacheTtl, analysisTtl);
    const slimPayload = buildSlimPayload(llmPayload) || llmPayload;
    llm = await analyzeWithLLM(OPENAI_KEY, llmModel, slimPayload, {
      cacheTtlMs: llmTtlMs,
      promptVersion: 'profile_v2',
      fallbackModel: secondaryModel
    });
    llmUsage = llm?.__usage || null;
    if(llmUsage) recordUsage(llmUsage);
  }

  const result = {
    input:{ticker:upperTicker, date: baselineDate},
    fetched:{
      filings: filings.map(f=>({form:f.form, form_label:f.formLabel || f.form, filingDate:f.filingDate, reportDate:f.reportDate, url:f.url})),
      finnhub_summary: finnhubSnapshot
    },
    analysis: llm,
    llm_usage: llmUsage,
    analysis_model: llmModel,
    news: newsCompact,
    momentum,
    institutional,
    earnings_call: earningsCall,
    analyst_signals: analystSignals,
    per_filing_summaries: perFiling,
    analyst_metrics: analystMetrics,
    macro: macroInsights,
    inputs: llmPayload
  };
  saveAnalysisResult({ ticker: upperTicker, baselineDate, isHistorical, model: cacheModelKey, result });
  return result;
}

function normalizeDate(raw){
  if(raw==null) return '';
  if(typeof raw === 'number'){
    const date = new Date(Math.round((raw - 25569) * 86400 * 1000));
    return Number.isNaN(date.getTime()) ? '' : dayjs(date).format('YYYY-MM-DD');
  }
  if(raw instanceof Date) return dayjs(raw).format('YYYY-MM-DD');
  const str = String(raw).trim();
  if(!str) return '';
  const parsed = dayjs(str);
  if(parsed.isValid()) return parsed.format('YYYY-MM-DD');
  const alt = dayjs(new Date(str));
  return alt.isValid() ? alt.format('YYYY-MM-DD') : str;
}

function parseBatchFile(file){
  if(!file) throw new Error('缺少檔案');
  const ext = path.extname(file.originalname || '').toLowerCase();
  let rows = [];
  if(ext === '.csv'){
    const text = file.buffer.toString('utf8');
    rows = Papa.parse(text, { skipEmptyLines:false }).data;
  }else{
    const wb = XLSX.read(file.buffer, { type:'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(ws, { header:1, raw:false, defval:'' });
  }
  const tasks = [];
  for (const row of rows){
    if(!row || !row.length) continue;
    const ticker = String(row[0] ?? '').trim();
    const date = normalizeDate(row[1]);
    const model = String(row[2] ?? '').trim();
    if(/^(ticker|symbol)$/i.test(ticker) && /^date$/i.test(String(row[1] || ''))) continue;
    if(!ticker && !date) break;
    if(!ticker || !date) continue;
    tasks.push({ ticker, date, model });
  }
  return tasks;
}

app.post('/api/analyze', async (req,res)=>{
  const { ticker, date, model, analysis_model, mode } = req.body||{};
  if(!ticker||!date) return res.status(400).json({error:'ticker and date required'});
  const resolvedModel = resolveModelName(analysis_model || model);
  const modeKey = String(mode || '').toLowerCase();
  const preferCacheOnly = modeKey === 'cached-only';
  const deferredMode = modeKey === 'deferred';
  const skipLlm = modeKey === 'metrics-only' || deferredMode;
  try{
    const result = await performAnalysis(ticker, date, { model: resolvedModel, preferCacheOnly, skipLlm });
    res.json(result);
    if(deferredMode){
      enqueueJob(()=>performAnalysis(ticker, date, { model: resolvedModel, preferCacheOnly:false, skipLlm:false }))
        .catch(err=>console.warn('[deferred] background LLM failed', err.message));
    }
  }catch(err){
    if(err.message === 'cache_miss'){
      return res.status(409).json({ error:'cached result unavailable' });
    }
    return errRes(res, err);
  }
});

app.post('/api/reset-cache', async (req,res)=>{
  const { ticker, date, model, analysis_model } = req.body || {};
  if(!ticker || !date) return res.status(400).json({ error:'ticker and date required' });
  const normalizedDate = normalizeDate(date);
  if(!normalizedDate || !dayjs(normalizedDate).isValid()){
    return res.status(400).json({ error:'invalid date' });
  }
  const upperTicker = ticker.toUpperCase();
  const resolvedModel = resolveModelName(analysis_model || model);
  const variants = new Set([resolvedModel, `${resolvedModel}__full`, `${resolvedModel}__metrics`]);
  for(const variant of variants){
    deleteAnalysis({ ticker: upperTicker, baselineDate: normalizedDate, model: variant });
  }
  const clearedExact = clearCacheForTicker({ ticker: upperTicker, baselineDate: normalizedDate });
  const clearedAll = clearCacheForTicker({ ticker: upperTicker, includeAllDates: true });
  res.json({ ok:true, cleared_cache_files: clearedExact + clearedAll });
});

app.post('/api/batch', upload.single('file'), async (req,res)=>{
  try{
    const batchMode = String(req.query.mode || '').toLowerCase();
    const preferCacheOnly = batchMode === 'cached-only';
    const deferredMode = batchMode === 'deferred';
    const skipLlm = batchMode === 'metrics-only' || deferredMode;
    const concurrencyLimit = resolveBatchConcurrency(batchMode);
    const tasks = parseBatchFile(req.file);
    if(!tasks.length) return res.status(400).json({error:'檔案內沒有有效的 ticker/date 列'});
    await prefetchBatchQuotes(
      tasks
        .filter(task=>!dayjs(task.date).isBefore(dayjs(), 'day'))
        .map(task=>task.ticker)
    );
    const memo = new Map();
    const rows = await mapWithConcurrency(tasks, concurrencyLimit, async (task)=>{
      const resolvedModel = resolveModelName(task.model);
      const key = `${task.ticker.toUpperCase()}__${task.date}__${resolvedModel}__${batchMode}`;
      if(!memo.has(key)){
        memo.set(key, (async ()=>{
          try{
            const result = await performAnalysis(task.ticker, task.date, {
              model: resolvedModel,
              preferCacheOnly: preferCacheOnly && !skipLlm,
              skipLlm
            });
            if(deferredMode){
              enqueueJob(()=>performAnalysis(task.ticker, task.date, {
                model: resolvedModel,
                preferCacheOnly:false,
                skipLlm:false
              })).catch(err=>console.warn('[batch deferred]', err.message));
            }
            return { ok:true, result };
          }catch(error){
            return { ok:false, error };
          }
        })());
      }
      const outcome = await memo.get(key);
      if(!outcome.ok){
        const errMessage = outcome.error?.message === 'cache_miss'
          ? 'CACHE_ONLY：無可用快取'
          : outcome.error?.message;
        return {
          ticker: task.ticker.toUpperCase(),
          date: task.date,
          model: resolvedModel,
          current_price: '',
          llm_target_price: '',
          recommendation: `ERROR: ${errMessage}`,
          segment: '',
          quality_score: '',
          news_sentiment: '',
          momentum_score: '',
          trend_flag: '',
          institutional_signal: ''
        };
      }
      const result = outcome.result;
      const summary = result.fetched?.finnhub_summary || {};
      const profile = result.analysis?.profile;
      const newsSent = result.news?.sentiment;
      const momentum = result.momentum || {};
      const institutional = result.institutional;
      const earningsCall = result.earnings_call;
      const analystSignals = result.analyst_signals;
      const analystMetrics = result.analyst_metrics || buildAnalystMetrics(analystSignals);
      const ptSummary = analystSignals?.price_target_summary;
      const ratingSnapshot = analystSignals?.ratings?.snapshot;
      const gradeConsensus = analystSignals?.grades?.consensus;
      const recentAvgTarget = analystMetrics?.price_targets?.recent_avg ?? ptSummary?.last_month?.avg ?? '';
      const gradesDiff = analystMetrics?.grades?.diff_90d ?? '';
      const ratingTrendArrow = analystMetrics?.rating
        ? [analystMetrics.rating.latest || '', analystMetrics.rating.trend_arrow || ''].filter(Boolean).join(' ')
        : (analystSignals?.ratings?.trend || '');
      return {
        ticker: result.input.ticker,
        date: task.date,
        model: resolvedModel,
        current_price: summary.quote?.c ?? '',
        llm_target_price: result.analysis?.action?.target_price ?? '',
        recommendation: deferredMode ? 'DEFERRED' : (result.analysis?.action?.rating ?? ''),
        segment: profile?.segment_label || profile?.segment || '',
        quality_score: profile?.score ?? '',
        news_sentiment: newsSent?.sentiment_label || '',
        momentum_score: momentum.score ?? '',
        trend_flag: momentum.trend || '',
        institutional_signal: institutional?.signal?.label || institutional?.summary || '',
        pt_recent_month_avg: ptSummary?.last_month?.avg ?? '',
        pt_recent_quarter_avg: ptSummary?.last_quarter?.avg ?? '',
        analyst_rating: ratingSnapshot?.rating || '',
        analyst_rating_trend: analystSignals?.ratings?.trend || '',
        analyst_grade_consensus: gradeConsensus?.consensus || '',
        recent_avg_target: recentAvgTarget ?? '',
        grades_diff: gradesDiff ?? '',
        rating_trend: ratingTrendArrow || ''
      };
    });
    const fields = ['ticker','date','model','current_price','llm_target_price','recommendation','segment','quality_score','news_sentiment','momentum_score','trend_flag','institutional_signal','pt_recent_month_avg','pt_recent_quarter_avg','analyst_rating','analyst_rating_trend','analyst_grade_consensus','recent_avg_target','grades_diff','rating_trend'];
    const csv = Papa.unparse({
      fields,
      data: rows.map(r=>fields.map(f=>r[f]))
    });
    res.setHeader('Content-Type','text/csv');
    res.setHeader('Content-Disposition','attachment; filename="batch_results.csv"');
    res.send(csv);
  }catch(err){
    return errRes(res, err);
  }
});

// 自我測試
app.get('/selftest', async (req,res)=>{
  try{
    const t='NVDA'; const d=dayjs().format('YYYY-MM-DD');
    const r = await fetch(`http://localhost:${PORT}/api/analyze`,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ticker:t, date:d})
    });
    const j = await r.json();
    return res.status(r.status).json(j);
  }catch(err){ return errRes(res, err); }
});

app.listen(PORT, ()=> console.log(`🚀 http://localhost:${PORT}`));
schedulePrewarm();

function schedulePrewarm(){
  if(!PREWARM_TICKERS.length) return;
  const intervalMs = Math.max(1, PREWARM_INTERVAL_HOURS) * 60 * 60 * 1000;
  const run = async ()=>{
    for(const ticker of PREWARM_TICKERS){
      try{
        await performAnalysis(ticker, dayjs().format('YYYY-MM-DD'), {
          model: resolveModelName(),
          skipLlm: !PREWARM_INCLUDE_LLM
        });
      }catch(err){
        console.warn('[prewarm]', ticker, err.message);
      }
    }
  };
  run();
  setInterval(run, intervalMs);
}
