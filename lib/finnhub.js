import axios from 'axios';
import { getCache, setCache } from './cache.js';
import { memoize } from './memoryCache.js';

const BASE = 'https://finnhub.io/api/v1';
const MEMORY_TTL_MS = Number(process.env.INMEM_API_TTL_MS || 30_000);

async function cachedGet(key, url){
  return memoize(`fh_mem_${key}`, MEMORY_TTL_MS, async ()=>{
    const c = await getCache(key);
    if(c) return c;
    const {data} = await axios.get(url,{timeout:15000});
    await setCache(key, data);
    return data;
  });
}

export async function getRecommendations(symbol, key, context='latest'){
  try{ return await cachedGet(`fh_reco_${symbol}_${context}`, `${BASE}/stock/recommendation?symbol=${symbol}&token=${key}`); }
  catch(err){ throw new Error(`[FINNHUB] ${err.response?.data?.error || err.message}`); }
}

export async function getEarnings(symbol, key, context='latest'){
  try{ return await cachedGet(`fh_earn_${symbol}_${context}`, `${BASE}/stock/earnings?symbol=${symbol}&token=${key}`); }
  catch(err){ throw new Error(`[FINNHUB] ${err.response?.data?.error || err.message}`); }
}

export async function getQuote(symbol, key, context='latest'){
  try{ return await cachedGet(`fh_quote_${symbol}_${context}`, `${BASE}/quote?symbol=${symbol}&token=${key}`); }
  catch(err){ throw new Error(`[FINNHUB] ${err.response?.data?.error || err.message}`); }
}

export async function getCompanyMetrics(symbol, key, metric='all'){
  const cacheKey = `fh_metric_${symbol}_${metric}`;
  try{
    return await cachedGet(cacheKey, `${BASE}/stock/metric?symbol=${symbol}&metric=${metric}&token=${key}`);
  }catch(err){
    throw new Error(`[FINNHUB] ${err.response?.data?.error || err.message}`);
  }
}
