import axios from 'axios';

const FMP_BASE_STABLE = 'https://financialmodelingprep.com/stable';
const HISTORICAL_EOD_FULL = `${FMP_BASE_STABLE}/historical-price-eod/full`;

function ensureKey(key){
  if(!key) throw new Error('[FMP] Missing API key');
}

function normalizeHistoricalRows(data){
  if(Array.isArray(data)) return data;
  if(Array.isArray(data?.historical)) return data.historical;
  return [];
}

export async function getFmpQuote(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/quote`;
  const params = { symbol, apikey: key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
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
  const params = { symbol, from: date, to: date, apikey:key };
  const { data } = await axios.get(HISTORICAL_EOD_FULL,{ params, timeout:20000 });
  const rows = normalizeHistoricalRows(data);
  if(rows.length){
    const row = rows.find(r=>r.date===date) || rows[0];
    const price = Number(row?.close ?? row?.price ?? row?.open);
    if(Number.isFinite(price)){
      return { price, source:'fmp_historical', date: row?.date || date };
    }
  }
  throw new Error('[FMP] historical empty');
}

export async function getFmpPriceTarget(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/price-target-consensus`;
  const params = { symbol, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  const row = Array.isArray(data) ? data[0] : null;
  if(!row) throw new Error('[FMP] price target empty');
  return {
    source:'fmp',
    targetHigh: Number(row.targetHigh) || null,
    targetLow: Number(row.targetLow) || null,
    targetMean: Number(row.targetConsensus ?? row.targetMean ?? row.targetAvg) || null,
    targetMedian: Number(row.targetMedian) || null
  };
}

export async function getFmpSplits(symbol, key, limit=5){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/splits`;
  const params = { symbol, limit, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpDailySeries(symbol, key, limit=400){
  ensureKey(key);
  const params = { symbol, apikey:key };
  const { data } = await axios.get(HISTORICAL_EOD_FULL,{ params, timeout:20000 });
  const rows = normalizeHistoricalRows(data);
  if(rows.length){
    return rows.slice(0, limit).map(row=>({
      date: row.date,
      close: Number(row.close ?? row.price ?? row.open) || 0,
      high: Number(row.high ?? row.close ?? row.price ?? 0) || 0,
      low: Number(row.low ?? row.close ?? row.price ?? 0) || 0,
      volume: Number(row.volume) || 0
    }));
  }
  throw new Error('[FMP] daily series empty');
}

export async function getFmpNews(symbol, key, limit=50){
  ensureKey(key);
  const params = { tickers: symbol, limit, apikey: key };
  const url = `${FMP_BASE_STABLE}/news/stock`;
  const { data } = await axios.get(url,{ params, timeout:15000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpInstitutionalHolders({ symbol, year, quarter, key, page=0, limit=100 }){
  ensureKey(key);
  const params = { symbol, year, quarter, page, limit, apikey:key };
  const summaryParams = { symbol, year, quarter, apikey:key };
  const [holdersRes, summaryRes] = await Promise.allSettled([
    axios.get(`${FMP_BASE_STABLE}/institutional-ownership/extract-analytics/holder`,{ params, timeout:30000 }),
    axios.get(`${FMP_BASE_STABLE}/institutional-ownership/symbol-positions-summary`,{ params: summaryParams, timeout:15000 })
  ]);
  if(holdersRes.status !== 'fulfilled'){
    const message = holdersRes.reason?.response?.data?.message
      || holdersRes.reason?.response?.data?.error
      || holdersRes.reason?.message
      || holdersRes.reason?.toString()
      || 'unknown error';
    throw new Error(`[FMP] Institutional holders failed: ${message}`);
  }
  const holders = holdersRes.status==='fulfilled' && Array.isArray(holdersRes.value.data)
    ? holdersRes.value.data
    : [];
  const summary = summaryRes.status==='fulfilled' && Array.isArray(summaryRes.value.data)
    ? summaryRes.value.data[0] || null
    : null;
  return { holders, summary };
}

export async function getFmpEarningsCallTranscript({ symbol, quarter, year, key }){
  ensureKey(key);
  const params = { symbol, quarter, year, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_STABLE}/earning-call-transcript`,{ params, timeout:30000 });
  return Array.isArray(data) ? data : [];
}
