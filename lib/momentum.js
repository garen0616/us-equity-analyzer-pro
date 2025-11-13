import axios from 'axios';
import dayjs from 'dayjs';
import { getCache, setCache } from './cache.js';
import {
  getFmpDailySeries,
  getFmpTechnicalIndicator,
  getFmpStockEtfs,
  getFmpEtfSectorWeights,
  getFmpEtfCountryWeights
} from './fmp.js';
import { memoize } from './memoryCache.js';

const SERIES_CACHE_TTL = 24 * 60 * 60 * 1000; // 1 day
const METRIC_CACHE_TTL = 6 * 60 * 60 * 1000; // 6 hours
const ETF_RETURN_TTL = 12 * 60 * 60 * 1000;
const MAX_LOOKBACK_DAYS = Number(process.env.MOMENTUM_LOOKBACK_DAYS || 900);
const SERIES_MEM_TTL_MS = Number(process.env.MOMENTUM_SERIES_MEM_TTL_MS || 5 * 60 * 1000);
const SLICE_MEM_TTL_MS = Number(process.env.MOMENTUM_SLICE_MEM_TTL_MS || 5 * 60 * 1000);
const TECH_INDICATOR_TTL_MS = Number(process.env.MOMENTUM_INDICATOR_TTL_MS || 60 * 60 * 1000);
const ETF_LOOKUP_TTL_MS = Number(process.env.MOMENTUM_ETF_LOOKUP_TTL_MS || 12 * 60 * 60 * 1000);
const ETF_PROFILE_TTL_MS = Number(process.env.MOMENTUM_ETF_PROFILE_TTL_MS || 24 * 60 * 60 * 1000);
const FMP_KEY = process.env.FMP_API_KEY || process.env.FMP_KEY || '';

const ETF_MAP = [
  { etf: 'SOXX', tickers: ['NVDA','AMD','TSM','ASML','AVGO','QCOM','MU','INTC','SMCI'] },
  { etf: 'XLV', tickers: ['UNH','LLY','JNJ','ABBV','PFE','MRK','TMO'] },
  { etf: 'XLF', tickers: ['JPM','GS','BAC','C','MS','BLK'] },
  { etf: 'QQQ', tickers: ['AAPL','MSFT','GOOGL','META','AMZN','NFLX','ADBE','CRM','NOW','SNOW'] }
];

function toNumber(value){
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function cacheKey(prefix, symbol){
  return `${prefix}_${symbol}`;
}

function pickClosestRow(rows, baselineDate){
  if(!Array.isArray(rows) || !rows.length) return null;
  const target = dayjs(baselineDate);
  return rows.reduce((best, row)=>{
    if(!row?.date) return best;
    const rowDate = dayjs(row.date);
    if(!rowDate.isValid() || rowDate.isAfter(target)) return best;
    if(!best) return row;
    const bestDate = dayjs(best.date);
    return rowDate.isAfter(bestDate) ? row : best;
  }, null);
}

function normalizeIndicatorValue(row, type){
  if(!row) return null;
  const entries = Object.entries(row)
    .filter(([key])=> key !== 'date')
    .map(([key, value])=> [key.toLowerCase(), toNumber(value)]);
  const map = Object.fromEntries(entries);
  if(type === 'macd'){
    return {
      macd: map.macd ?? map.macdvalue ?? null,
      signal: map.macds ?? map.signal ?? null,
      histogram: map.macdh ?? map.histogram ?? map.macd_histogram ?? null
    };
  }
  if(type === 'ema') return map.ema ?? map[`ema${row.period}`] ?? firstFinite(entries);
  if(type === 'sma') return map.sma ?? map[`sma${row.period}`] ?? firstFinite(entries);
  if(type === 'rsi') return map.rsi ?? firstFinite(entries);
  if(type === 'adx') return map.adx ?? firstFinite(entries);
  return firstFinite(entries);
}

function firstFinite(entries){
  for(const [, value] of entries){
    if(Number.isFinite(value)) return value;
  }
  return null;
}

async function fetchTechnicalIndicatorSnapshot({ symbol, type, period=14, baselineDate }){
  if(!FMP_KEY) return null;
  const cacheId = cacheKey(`fmp_ti_${type}_${period}`, `${symbol}_${baselineDate}`);
  const cached = await getCache(cacheId, TECH_INDICATOR_TTL_MS);
  if(cached !== undefined) return cached;
  try{
    const rows = await getFmpTechnicalIndicator({ symbol, type, period }, FMP_KEY);
    const match = pickClosestRow(rows, baselineDate);
    const payload = match ? { date: match.date, type, value: normalizeIndicatorValue(match, type) } : null;
    await setCache(cacheId, payload);
    return payload;
  }catch(err){
    console.warn('[Momentum] technical indicator fetch failed', err.message);
    await setCache(cacheId, null);
    return null;
  }
}

async function fetchEtfCandidate(symbol){
  if(!FMP_KEY) return null;
  const cacheId = cacheKey('fmp_etf_pick', symbol);
  const cached = await getCache(cacheId, ETF_LOOKUP_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const rows = await getFmpStockEtfs(symbol, FMP_KEY);
    if(!Array.isArray(rows) || !rows.length){
      await setCache(cacheId, { __empty:true });
      return null;
    }
    const sorted = [...rows].sort((a,b)=>{
      const bw = toNumber(b.weightPercentage ?? b.weightPercent ?? b.assetExposure ?? b.asset ?? 0) || 0;
      const aw = toNumber(a.weightPercentage ?? a.weightPercent ?? a.assetExposure ?? a.asset ?? 0) || 0;
      return bw - aw;
    });
    const pick = sorted[0];
    await setCache(cacheId, pick || { __empty:true });
    return pick;
  }catch(err){
    console.warn('[Momentum] etf candidate fetch failed', err.message);
    await setCache(cacheId, { __empty:true });
    return null;
  }
}

async function fetchEtfWeights(symbol){
  if(!FMP_KEY || !symbol) return null;
  const cacheId = cacheKey('fmp_etf_weights', symbol);
  const cached = await getCache(cacheId, ETF_PROFILE_TTL_MS);
  if(cached) return cached.__empty ? null : cached;
  try{
    const [sectors, countries] = await Promise.all([
      getFmpEtfSectorWeights(symbol, FMP_KEY).catch(err=>{ console.warn('[Momentum] etf sector weights failed', err.message); return []; }),
      getFmpEtfCountryWeights(symbol, FMP_KEY).catch(err=>{ console.warn('[Momentum] etf country weights failed', err.message); return []; })
    ]);
    const payload = {
      sectors: Array.isArray(sectors) ? sectors.slice(0,5) : [],
      countries: Array.isArray(countries) ? countries.slice(0,5) : []
    };
    await setCache(cacheId, payload);
    return payload;
  }catch(err){
    console.warn('[Momentum] etf weights failed', err.message);
    await setCache(cacheId, { __empty:true });
    return null;
  }
}

async function buildEtfReference(symbol){
  const candidate = await fetchEtfCandidate(symbol);
  if(candidate){
    const etfSymbol = candidate.etfSymbol || candidate.symbol || candidate.etf || null;
    if(etfSymbol){
      const weights = await fetchEtfWeights(etfSymbol);
      return {
        symbol: etfSymbol,
        name: candidate.etfName || candidate.name || null,
        weight_in_symbol: toNumber(candidate.weightPercentage ?? candidate.weightPercent ?? candidate.assetExposure ?? candidate.asset ?? null),
        source: 'fmp',
        sectors: weights?.sectors || [],
        countries: weights?.countries || []
      };
    }
  }
  return {
    symbol: pickEtf(symbol),
    source: 'static'
  };
}

function buildIndicatorBundle({ rsiSnap, emaSnap, smaSnap, macdSnap }){
  const bundle = {};
  if(rsiSnap?.value!=null) bundle.rsi14 = rsiSnap.value;
  if(emaSnap?.value!=null) bundle.ema20 = emaSnap.value;
  if(smaSnap?.value!=null) bundle.sma50 = smaSnap.value;
  if(macdSnap?.value) bundle.macd = macdSnap.value;
  if(!Object.keys(bundle).length) return null;
  bundle.as_of = rsiSnap?.date || emaSnap?.date || smaSnap?.date || macdSnap?.date || null;
  return bundle;
}

async function getEtfReturn(symbol, baselineDate){
  if(!symbol) return null;
  const key = cacheKey('etf_ret', `${symbol}_${baselineDate}`);
  const cached = await getCache(key, ETF_RETURN_TTL);
  if(cached !== undefined && cached !== null) return cached;
  const sliced = await getSlicedSeries(symbol, baselineDate);
  const value = sliced && sliced.length>63 ? percentChange(sliced, 63) : null;
  await setCache(key, value);
  return value;
}

function pickEtf(symbol){
  const upper = symbol.toUpperCase();
  for(const bucket of ETF_MAP){
    if(bucket.tickers.includes(upper)) return bucket.etf;
  }
  return 'SPY';
}

function clamp(val, min, max){
  return Math.max(min, Math.min(max, val));
}

async function fetchFromAlpha(symbol){
  const apiKey = process.env.ALPHAVANTAGE_KEY;
  if(!apiKey) return null;
  const params = new URLSearchParams({
    function:'TIME_SERIES_DAILY_ADJUSTED',
    symbol,
    outputsize:'full',
    apikey: apiKey
  });
  const { data } = await axios.get(`https://www.alphavantage.co/query?${params.toString()}`,{ timeout:20000 });
  const series = data?.['Time Series (Daily)'];
  if(!series){
    const errMsg = data?.Note || data?.['Error Message'] || 'AlphaVantage no data';
    throw new Error(errMsg);
  }
  return Object.entries(series).map(([date, values])=>({
    date,
    close: Number(values['4. close'] ?? values['5. adjusted close'] ?? values['1. open']) || 0,
    high: Number(values['2. high']) || 0,
    low: Number(values['3. low']) || 0,
    volume: Number(values['6. volume']) || 0
  }));
}

async function fetchFromYahoo(symbol){
  const params = new URLSearchParams({
    range:'2y',
    interval:'1d'
  });
  const { data } = await axios.get(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?${params.toString()}`,{
    headers:{'User-Agent':'Mozilla/5.0'},
    timeout:20000
  });
  const result = data?.chart?.result?.[0];
  if(!result) throw new Error('Yahoo chart no data');
  const timestamps = result.timestamp || [];
  const close = result.indicators?.adjclose?.[0]?.adjclose || result.indicators?.quote?.[0]?.close || [];
  const high = result.indicators?.quote?.[0]?.high || [];
  const low = result.indicators?.quote?.[0]?.low || [];
  const volume = result.indicators?.quote?.[0]?.volume || [];
  return timestamps.map((ts,i)=>{
    const date = dayjs.unix(ts).format('YYYY-MM-DD');
    return {
      date,
      close: Number(close[i]) || Number(high[i]) || Number(low[i]) || 0,
      high: Number(high[i]) || Number(close[i]) || 0,
      low: Number(low[i]) || Number(close[i]) || 0,
      volume: Number(volume[i]) || 0
    };
  });
}

async function fetchFromFmp(symbol){
  const key = process.env.FMP_API_KEY || process.env.FMP_KEY;
  if(!key) return null;
  try{
    return await getFmpDailySeries(symbol, key, MAX_LOOKBACK_DAYS + 20);
  }catch(err){
    console.warn('[Momentum] fmp fetch failed', err.message);
    return null;
  }
}

async function fetchDailySeries(symbol){
  return memoize(`momentum_series_${symbol}`, SERIES_MEM_TTL_MS, async ()=>{
    const key = cacheKey('series', symbol);
    const cached = await getCache(key, SERIES_CACHE_TTL);
    if(cached) return cached;
    let rows = [];
    try{
      rows = await fetchFromFmp(symbol);
    }catch(err){
      console.warn('[Momentum] fmp fetch failed', err.message);
    }
    if(!rows || !rows.length){
      try{
        rows = await fetchFromAlpha(symbol);
      }catch(err){
        console.warn('[Momentum] alpha fetch failed', err.message);
      }
    }
    if(!rows || !rows.length){
      try{
        rows = await fetchFromYahoo(symbol);
      }catch(err){
        console.warn('[Momentum] yahoo fetch failed', err.message);
      }
    }
    if(!rows || !rows.length) return null;
    const sorted = rows.sort((a,b)=> dayjs(b.date).valueOf() - dayjs(a.date).valueOf()).slice(0, MAX_LOOKBACK_DAYS);
    await setCache(key, sorted);
    return sorted;
  });
}

function percentChange(series, idx){
  if(series.length <= idx || idx < 0) return null;
  const current = series[0];
  const base = series[idx];
  if(!current || !base || base.close === 0) return null;
  return (current.close / base.close) - 1;
}

function simpleMovingAverage(series, period){
  if(series.length < period) return null;
  const slice = series.slice(0, period);
  return slice.reduce((sum,row)=>sum+row.close,0)/period;
}

function calcRSI(series, period=14){
  if(series.length <= period) return null;
  let gains = 0; let losses = 0;
  for(let i=0;i<period;i++){
    const diff = series[i].close - series[i+1].close;
    if(diff>=0) gains += diff; else losses -= diff;
  }
  const avgGain = gains/period;
  const avgLoss = losses/period;
  if(avgLoss === 0) return 100;
  const rs = avgGain/avgLoss;
  return 100 - (100/(1+rs));
}

function calcATR(series, period=14){
  if(series.length <= period) return null;
  const trs = [];
  for(let i=0;i<period;i++){
    const current = series[i];
    const prev = series[i+1] || current;
    const tr = Math.max(
      current.high - current.low,
      Math.abs(current.high - prev.close),
      Math.abs(current.low - prev.close)
    );
    trs.push(tr);
  }
  return trs.reduce((a,b)=>a+b,0)/trs.length;
}

function avgVolume(series, period){
  if(series.length < period) return null;
  return series.slice(0, period).reduce((sum,row)=>sum+row.volume,0)/period;
}

function sliceByDate(series, baselineDate){
  if(!baselineDate) return series;
  const target = dayjs(baselineDate);
  const idx = series.findIndex(row=> dayjs(row.date).isSame(target,'day') || dayjs(row.date).isBefore(target,'day'));
  return idx>=0 ? series.slice(idx) : series;
}

async function getSlicedSeries(symbol, baselineDate){
  return memoize(`momentum_slice_${symbol}_${baselineDate}`, SLICE_MEM_TTL_MS, async ()=>{
    const series = await fetchDailySeries(symbol);
    return sliceByDate(series || [], baselineDate);
  });
}

export async function computeMomentumMetrics(symbol, baselineDate){
  try{
    const cacheId = cacheKey('momentum_metrics', `${symbol}_${baselineDate}`);
    const cached = await getCache(cacheId, METRIC_CACHE_TTL);
    if(cached) return cached;
    const sliced = await getSlicedSeries(symbol, baselineDate);
    if(!sliced?.length || sliced.length < 60) return null;
    const latest = sliced[0];
    const returns = {
      m3: percentChange(sliced, 63),
      m6: percentChange(sliced, 126),
      m12: percentChange(sliced, 252)
    };
    const ma20 = simpleMovingAverage(sliced, 20);
    const ma50 = simpleMovingAverage(sliced, 50);
    const ma200 = simpleMovingAverage(sliced, 200);
    const rsi14 = calcRSI(sliced,14);
    const atr14 = calcATR(sliced,14);
    const vol5 = avgVolume(sliced,5);
    const vol30 = avgVolume(sliced,30);
    const volumeRatio = (vol5 && vol30) ? vol5/vol30 : null;
    const rangeWindow = sliced.slice(0, 252);
    const yearHigh = rangeWindow.reduce((max,row)=>{
      const value = Number.isFinite(row?.high) ? row.high : row?.close;
      return (Number.isFinite(value) && value > max) ? value : max;
    }, 0);
    const yearLow = rangeWindow.reduce((min,row)=>{
      const value = Number.isFinite(row?.low) ? row.low : row?.close;
      if(!Number.isFinite(value)) return min;
      if(min===0) return value;
      return value < min ? value : min;
    }, 0);
    let fmpIndicatorBundle = null;
    if(FMP_KEY){
      const [rsiSnap, emaSnap, smaSnap, macdSnap] = await Promise.all([
        fetchTechnicalIndicatorSnapshot({ symbol, type:'rsi', period:14, baselineDate }),
        fetchTechnicalIndicatorSnapshot({ symbol, type:'ema', period:20, baselineDate }),
        fetchTechnicalIndicatorSnapshot({ symbol, type:'sma', period:50, baselineDate }),
        fetchTechnicalIndicatorSnapshot({ symbol, type:'macd', period:26, baselineDate })
      ]);
      fmpIndicatorBundle = buildIndicatorBundle({ rsiSnap, emaSnap, smaSnap, macdSnap });
    }
    const above50 = ma50!=null ? latest.close > ma50 : null;
    const above200 = ma200!=null ? latest.close > ma200 : null;

    let trend = '中性';
    if((above50 && above200) && returns.m3!=null && returns.m3 > 0.10) trend = '強勢';
    if((above50===false && above200===false) && returns.m3!=null && returns.m3 < -0.05) trend = '弱勢';

    let score = 50;
    if(returns.m3!=null) score += clamp(returns.m3*200, -20, 20);
    if(returns.m6!=null) score += clamp(returns.m6*150, -15, 15);
    if(returns.m12!=null) score += clamp(returns.m12*100, -10, 10);
    if(rsi14!=null) score += clamp((rsi14-50)/2, -10, 10);
    if(volumeRatio!=null) score += clamp((volumeRatio-1)*20, -10, 10);
    if(above50===true) score += 5; else if(above50===false) score -=5;
    if(above200===true) score += 5; else if(above200===false) score -=5;
    const momentumScore = Math.round(clamp(score,0,100));
    const atrPct = (atr14!=null && latest.close) ? atr14 / latest.close : null;

    const etfRef = await buildEtfReference(symbol);
    const etfSymbol = etfRef?.symbol || pickEtf(symbol);
    let etfReturn = null;
    try{
      etfReturn = await getEtfReturn(etfSymbol, baselineDate);
    }catch(err){
      console.warn('[Momentum] etf return failed', err.message);
    }
    const etf = {
      symbol: etfSymbol,
      name: etfRef?.name,
      source: etfRef?.source || 'static',
      weight_in_symbol: etfRef?.weight_in_symbol,
      return3m: etfReturn,
      sectors: etfRef?.sectors || [],
      countries: etfRef?.countries || []
    };

    const metrics = {
      score: momentumScore,
      trend,
      returns,
      price: latest.close,
      moving_averages:{ ma20, ma50, ma200 },
      rsi14,
      atr14,
      atr_pct: atrPct,
      volume_ratio: volumeRatio,
      price_vs_ma:{ above50, above200 },
      range_52w: (yearHigh || yearLow) ? {
        high: yearHigh || null,
        low: yearLow || null
      } : null,
      indicators:{
        native:{ rsi14, atr14, volume_ratio: volumeRatio, ma20, ma50, ma200 },
        fmp: fmpIndicatorBundle
      },
      etf,
      reference_date: latest.date
    };
    await setCache(cacheId, metrics);
    return metrics;
  }catch(err){
    console.warn('[Momentum] compute failed', err.message);
    return null;
  }
}
