import axios from 'axios';

const FMP_BASE_V3 = 'https://financialmodelingprep.com/api/v3';
const FMP_BASE_V4 = 'https://financialmodelingprep.com/api/v4';

function ensureKey(key){
  if(!key) throw new Error('[FMP] Missing API key');
}

export async function getFmpQuote(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/quote/${encodeURIComponent(symbol)}`;
  const { data } = await axios.get(url,{ params:{ apikey:key }, timeout:15000 });
  if(Array.isArray(data) && data.length){
    const row = data[0];
    const price = Number(row.price ?? row.c ?? row.currentPrice);
    if(Number.isFinite(price)){
      return {
        price,
        asOf: row.timestamp ? new Date(row.timestamp * 1000).toISOString().slice(0,10) : undefined,
        raw: row
      };
    }
  }
  throw new Error('[FMP] quote empty');
}

export async function getFmpHistorical(symbol, date, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/historical-price-full/${encodeURIComponent(symbol)}`;
  const params = { from: date, to: date, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  const row = data?.historical?.[0];
  const price = Number(row?.close ?? row?.adjClose ?? row?.open);
  if(Number.isFinite(price)){
    return { price, source:'fmp_historical', date: row?.date || date };
  }
  throw new Error('[FMP] historical empty');
}

export async function getFmpPriceTarget(symbol, key, limit=50){
  ensureKey(key);
  const url = `${FMP_BASE_V4}/price-target`;
  const params = { symbol, apikey:key, limit };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  const records = Array.isArray(data) ? data.filter(row=>Number.isFinite(Number(row?.priceTarget))) : [];
  if(!records.length) throw new Error('[FMP] price target empty');
  const values = records.map(row=>Number(row.priceTarget));
  values.sort((a,b)=>a-b);
  const sum = values.reduce((acc,val)=>acc+val,0);
  const median = values.length % 2 === 1
    ? values[(values.length-1)/2]
    : (values[values.length/2 -1] + values[values.length/2]) / 2;
  return {
    source:'fmp',
    targetHigh: values[values.length-1],
    targetLow: values[0],
    targetMean: sum / values.length,
    targetMedian: median
  };
}

export async function getFmpSplits(symbol, key, limit=5){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/stock_split/${encodeURIComponent(symbol)}`;
  const params = { limit, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpDailySeries(symbol, key, limit=400){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/historical-price-full/${encodeURIComponent(symbol)}`;
  const params = { timeseries: limit, apikey:key, serietype:'line' };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  if(Array.isArray(data?.historical) && data.historical.length){
    return data.historical.map(row=>({
      date: row.date,
      close: Number(row.close ?? row.adjClose ?? row.open) || 0,
      high: Number(row.high ?? row.close) || 0,
      low: Number(row.low ?? row.close) || 0,
      volume: Number(row.volume) || 0
    }));
  }
  throw new Error('[FMP] daily series empty');
}

export async function getFmpNews(symbol, key, limit=50){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/stock_news`;
  const params = { tickers: symbol, limit, apikey: key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpInstitutionalHolders(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/institutional-holder/${encodeURIComponent(symbol)}`;
  const { data } = await axios.get(url,{ params:{ apikey:key }, timeout:20000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpEarningsCallTranscript({ symbol, quarter, year, key }){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/earning_call_transcript/${encodeURIComponent(symbol)}`;
  const params = { quarter, year, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:30000 });
  return Array.isArray(data) ? data : [];
}
