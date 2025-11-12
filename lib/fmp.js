import axios from 'axios';

const FMP_BASE_STABLE = 'https://financialmodelingprep.com/stable';
const FMP_BASE_V3 = 'https://financialmodelingprep.com/api/v3';
const FMP_BASE_V4 = 'https://financialmodelingprep.com/api/v4';
const HISTORICAL_EOD_FULL = `${FMP_BASE_STABLE}/historical-price-eod/full`;
const BATCH_QUOTE_ENDPOINT = `${FMP_BASE_STABLE}/batch-quote`;
const BATCH_QUOTE_CHUNK = 50;
const DEFAULT_TIMEOUT = 20000;

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

export async function getFmpBatchQuote(symbols, key){
  ensureKey(key);
  if(!Array.isArray(symbols) || !symbols.length) return [];
  const unique = Array.from(new Set(symbols.map(sym=>String(sym || '').trim().toUpperCase()).filter(Boolean)));
  if(!unique.length) return [];
  const rows = [];
  for(let idx=0; idx<unique.length; idx+=BATCH_QUOTE_CHUNK){
    const chunk = unique.slice(idx, idx + BATCH_QUOTE_CHUNK);
    const params = { symbols: chunk.join(','), apikey:key };
    const { data } = await axios.get(BATCH_QUOTE_ENDPOINT, { params, timeout:15000 });
    if(Array.isArray(data)){
      rows.push(...data);
    }
  }
  return rows;
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

export async function getFmpPriceTargetSummary(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/price-target-summary`;
  const params = { symbol, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  if(Array.isArray(data) && data.length) return data[0];
  throw new Error('[FMP] price target summary empty');
}

export async function getFmpAnalystEstimates({ symbol, period='annual', page=0, limit=8 }, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/analyst-estimates`;
  const params = { symbol, period, page, limit, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpRatingsSnapshot(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/ratings-snapshot`;
  const params = { symbol, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  if(Array.isArray(data) && data.length) return data[0];
  return null;
}

export async function getFmpRatingsHistorical({ symbol, limit=10 }, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/ratings-historical`;
  const params = { symbol, limit, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpGrades(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/grades`;
  const params = { symbol, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpGradesHistorical({ symbol, limit=100 }, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/grades-historical`;
  const params = { symbol, limit, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:20000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpGradesConsensus(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_STABLE}/grades-consensus`;
  const params = { symbol, apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  if(Array.isArray(data) && data.length) return data[0];
  return null;
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

function firstRow(data){
  if(!data) return null;
  if(Array.isArray(data)) return data[0] || null;
  return data;
}

export async function getFmpAftermarketQuote(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V4}/aftermarket/quote/${encodeURIComponent(symbol)}`;
  const params = { apikey:key };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  if(Array.isArray(data) && data.length) return data[0];
  if(data && data.symbol) return data;
  return null;
}

export async function getFmpAftermarketTrades(symbol, key, { limit=5 }={}){
  ensureKey(key);
  const url = `${FMP_BASE_V4}/aftermarket/trade/${encodeURIComponent(symbol)}`;
  const params = { apikey:key, limit };
  const { data } = await axios.get(url,{ params, timeout:15000 });
  return Array.isArray(data) ? data : [];
}

export async function getFmpTechnicalIndicator({ symbol, interval='daily', type='sma', period=20, series_type='close', fast=12, slow=26, signal=9 }, key){
  ensureKey(key);
  const encSymbol = encodeURIComponent(symbol);
  const url = `${FMP_BASE_V3}/technical_indicator/${interval}/${encSymbol}`;
  const params = {
    type,
    period,
    series_type,
    fast,
    slow,
    signal,
    apikey:key
  };
  const { data } = await axios.get(url,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpStockEtfs(symbol, key){
  ensureKey(key);
  const params = { symbol, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V3}/stock/etf`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpEtfSectorWeights(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/etf-sector-weightings/${encodeURIComponent(symbol)}`;
  const params = { apikey:key };
  const { data } = await axios.get(url,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpEtfCountryWeights(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/etf-country-weightings/${encodeURIComponent(symbol)}`;
  const params = { apikey:key };
  const { data } = await axios.get(url,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpInsiderTrading({ symbol, limit=20, page=0, from, to }, key){
  ensureKey(key);
  const params = { symbol, limit, page, from, to, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V4}/insider-trading`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpInsiderStats({ symbol, period='monthly', limit=4 }, key){
  ensureKey(key);
  const params = { symbol, period, limit, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V4}/insider-transaction-statistics`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpAnalystActions({ symbol, limit=20, page=0 }, key){
  ensureKey(key);
  const params = { symbol, limit, page, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V4}/analyst-upgrade-downgrade`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpEconomicCalendar({ from, to }, key){
  ensureKey(key);
  const params = { from, to, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V3}/economic_calendar`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpTreasuryCurve({ from, to, maturity }, key){
  ensureKey(key);
  const params = { from, to, maturity, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V4}/treasury`,{ params, timeout:DEFAULT_TIMEOUT });
  return Array.isArray(data) ? data : [];
}

export async function getFmpMarketRiskPremium({ from, to }, key){
  ensureKey(key);
  const params = { from, to, apikey:key };
  const { data } = await axios.get(`${FMP_BASE_V4}/market_risk_premium`,{ params, timeout:DEFAULT_TIMEOUT });
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
