import axios from 'axios';
import https from 'https';
import { getCache, setCache } from './cache.js';

const QUOTE_TTL_MS = Number(process.env.YAHOO_QUOTE_TTL_MS || 60 * 1000);
const SESSION_TTL_MS = Number(process.env.YAHOO_SESSION_TTL_MS || 60 * 60 * 1000);
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

function toNumber(value){
  if(value==null || value==='') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

let yahooSession = null;

function parseCookies(headers){
  const cookies = headers?.['set-cookie'];
  if(!Array.isArray(cookies) || !cookies.length) return null;
  const pairs = cookies.map(cookie=>cookie.split(';')[0]).filter(Boolean);
  return pairs.length ? pairs.join('; ') : null;
}

function rawHttpsGet({ hostname, path, headers }){
  return new Promise((resolve, reject)=>{
    const req = https.request({
      hostname,
      path,
      method:'GET',
      headers,
      maxHeaderSize: 65536
    }, res=>{
      const chunks = [];
      res.on('data', chunk=>chunks.push(chunk));
      res.on('end', ()=>{
        resolve({
          status: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks).toString('utf8')
        });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function refreshYahooSession(){
  const landing = await rawHttpsGet({
    hostname:'finance.yahoo.com',
    path:'/quote/%5EGSPC/',
    headers:{
      'User-Agent': UA,
      'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language':'en-US,en;q=0.9'
    }
  });
  const cookie = parseCookies(landing.headers);
  if(!cookie) throw new Error('Yahoo session cookie missing');
  const crumbResp = await rawHttpsGet({
    hostname:'query1.finance.yahoo.com',
    path:'/v1/test/getcrumb',
    headers:{ 'User-Agent': UA, Cookie: cookie }
  });
  const crumbRaw = crumbResp.body ? crumbResp.body.trim() : '';
  if(!crumbRaw) throw new Error('Yahoo crumb missing');
  yahooSession = { cookie, crumb: crumbRaw, ts: Date.now() };
  return yahooSession;
}

async function getYahooSession(){
  if(yahooSession && (Date.now() - yahooSession.ts) < SESSION_TTL_MS){
    return yahooSession;
  }
  return refreshYahooSession();
}

async function yahooRequest(path, params={}){
  const exec = async ()=>{
    const session = await getYahooSession();
    const url = `https://query2.finance.yahoo.com${path}`;
    const query = { ...params, crumb: session.crumb };
    return axios.get(url,{
      params: query,
      headers:{ 'User-Agent': UA, Cookie: session.cookie },
      timeout:15000
    });
  };
  try{
    return await exec();
  }catch(err){
    if(err?.response?.status === 401 || err?.response?.status === 403){
      yahooSession = null;
      return exec();
    }
    throw err;
  }
}

async function getCached(key, fetcher){
  const cached = await getCache(key, QUOTE_TTL_MS);
  if(cached) return cached;
  const fresh = await fetcher();
  if(fresh) await setCache(key, fresh);
  return fresh;
}

export async function getYahooQuote(symbol){
  if(!symbol) throw new Error('Missing symbol for Yahoo quote');
  const cacheKey = `rt_yahoo_${symbol}`;
  return getCached(cacheKey, async ()=>{
    const { data } = await yahooRequest('/v7/finance/quote',{ symbols: symbol });
    const row = data?.quoteResponse?.result?.[0];
    if(!row) throw new Error('Yahoo quote empty');
    const price = toNumber(row.regularMarketPrice ?? row.postMarketPrice ?? row.preMarketPrice);
    if(price==null) throw new Error('Yahoo quote missing price');
    const asOfTs = row.regularMarketTime || row.postMarketTime || row.preMarketTime;
    const asOf = asOfTs ? new Date(asOfTs * 1000).toISOString().slice(0,10) : undefined;
    return { price, asOf, source:'yahoo_quote' };
  });
}

export async function getYahooPriceTarget(symbol){
  if(!symbol) throw new Error('Missing symbol for Yahoo price target');
  const cacheKey = `yahoo_pt_${symbol}`;
  const cached = await getCache(cacheKey);
  if(cached) return cached;
  const encoded = encodeURIComponent(symbol);
  const { data } = await yahooRequest(`/v10/finance/quoteSummary/${encoded}`, { modules:'financialData' });
  const fd = data?.quoteSummary?.result?.[0]?.financialData;
  const out = {
    source:'yahoo',
    targetHigh: toNumber(fd?.targetHighPrice?.raw ?? fd?.targetHighPrice),
    targetLow: toNumber(fd?.targetLowPrice?.raw ?? fd?.targetLowPrice),
    targetMean: toNumber(fd?.targetMeanPrice?.raw ?? fd?.targetMeanPrice),
    targetMedian: null
  };
  if(out.targetHigh!=null || out.targetLow!=null || out.targetMean!=null){
    await setCache(cacheKey, out);
    return out;
  }
  throw new Error('Yahoo price target unavailable');
}
