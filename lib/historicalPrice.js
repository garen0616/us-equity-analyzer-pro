import axios from 'axios';
import dayjs from 'dayjs';
import { getCache, setCache } from './cache.js';
import { getFmpHistorical } from './fmp.js';

const MAX_LOOKBACK_DAYS = 7;
const FMP_MAX_RETRY = Number(process.env.FMP_RETRY_ATTEMPTS || 3);
const RETRY_DELAY_MS = Number(process.env.FMP_RETRY_DELAY_MS || 800);

function toNumber(value){
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function sleep(ms){
  return new Promise(resolve=>setTimeout(resolve, ms));
}

async function getCached(key, fetcher){
  const cached = await getCache(key);
  if(cached) return cached;
  const fresh = await fetcher();
  if(fresh) await setCache(key, fresh);
  return fresh;
}

async function fmpHistoricalWithRetry(symbol, date, key){
  const cacheKey = `hist_fmp_${symbol}_${date}`;
  return getCached(cacheKey, async ()=>{
    if(!key) throw new Error('Missing FMP key');
    let lastErr;
    for(let attempt=0; attempt<FMP_MAX_RETRY; attempt++){
      try{
        const hist = await getFmpHistorical(symbol, date, key);
        if(hist?.price!=null) return hist;
        lastErr = new Error('FMP historical returned empty');
      }catch(err){
        lastErr = err;
      }
      if(attempt < FMP_MAX_RETRY-1){
        await sleep(RETRY_DELAY_MS * Math.max(1, attempt+1));
      }
    }
    throw lastErr || new Error('FMP historical unavailable');
  });
}

async function yahooHistorical(symbol, date){
  const cacheKey = `hist_yahoo_${symbol}_${date}`;
  return getCached(cacheKey, async ()=>{
    const from = dayjs(date).startOf('day').unix();
    const to = dayjs(date).endOf('day').unix() + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&period1=${from}&period2=${to}&includePrePost=false&events=div%2Csplit`;
    const {data} = await axios.get(url,{ headers:{'User-Agent':'Mozilla/5.0'}, timeout:15000 });
    const result = data?.chart?.result?.[0];
    const close = result?.indicators?.quote?.[0]?.close?.[0];
    const price = toNumber(close);
    if(price!=null) return { price, source:'yahoo_chart', date };
    throw new Error('Yahoo chart no data');
  });
}

export async function getHistoricalPrice(symbol, date, keys){
  const errors = [];
  const target = dayjs(date);
  if(!target.isValid()) throw new Error('Invalid date for historical fetch');
  const fmpKey = keys?.fmpKey;

  for(let offset=0; offset<=MAX_LOOKBACK_DAYS; offset++){
    const currentDate = target.subtract(offset, 'day');
    const normalized = currentDate.format('YYYY-MM-DD');
    const dow = currentDate.day();
    if(dow === 0 || dow === 6){
      errors.push(`${normalized}:non-trading-day`);
      continue;
    }
    if(fmpKey){
      try{
        const fmp = await fmpHistoricalWithRetry(symbol, normalized, fmpKey);
        if(fmp?.price!=null) return fmp;
      }catch(err){
        errors.push(`${normalized}:FMP:${err.message}`);
      }
    }
    try{
      const yahoo = await yahooHistorical(symbol, normalized);
      if(yahoo?.price!=null) return yahoo;
    }catch(err){
      errors.push(`${normalized}:YAHOO:${err.message}`);
    }
  }
  throw new Error(errors.join(' | ') || 'No historical price source succeeded within lookback window (FMP + Yahoo)');
}
