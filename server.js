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
import { getCachedAnalysis, saveAnalysisResult, deleteAnalysis } from './lib/analysisStore.js';
import { buildNewsBundle } from './lib/news.js';
import { computeMomentumMetrics } from './lib/momentum.js';
import { getFmpQuote } from './lib/fmp.js';
import { getYahooQuote } from './lib/yahoo.js';
import { clearCacheForTicker } from './lib/cache.js';
import { summarizeMda } from './lib/mdaSummarizer.js';

const app = express();
app.use(express.json());
app.use(morgan('dev'));
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const UA   = process.env.SEC_USER_AGENT || 'App/1.0 (email@example.com)';
const SEC_KEY = process.env.SEC_API_KEY || '';
const FH_KEY  = process.env.FINNHUB_KEY || '';
const AV_KEY  = process.env.ALPHAVANTAGE_KEY || '';
const FMP_KEY = process.env.FMP_API_KEY || process.env.FMP_KEY || '';
const OPENAI_KEY = process.env.OPENAI_API_KEY || '';
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-5';
const OPENAI_SECONDARY_MODEL = process.env.OPENAI_MODEL_SECONDARY || 'gpt-4o-mini';
const BATCH_CONCURRENCY = Math.max(1, Number(process.env.BATCH_CONCURRENCY || 3));
const upload = multer({ storage: multer.memoryStorage(), limits:{ fileSize: 10 * 1024 * 1024 } });
const REALTIME_TTL_MS = 6 * 60 * 60 * 1000;
const HISTORICAL_TTL_MS = 30 * 24 * 60 * 60 * 1000;

function resolveModelName(){
  return OPENAI_MODEL;
}

function resolveSecondaryModel(){
  return OPENAI_SECONDARY_MODEL;
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
  const parsedDate = dayjs(date);
  if(!parsedDate.isValid()) throw new Error('invalid date format');
  const baselineDate = parsedDate.format('YYYY-MM-DD');
  const upperTicker = ticker.toUpperCase();
  const isHistorical = parsedDate.isBefore(dayjs(), 'day');
  const analysisTtl = isHistorical ? HISTORICAL_TTL_MS : REALTIME_TTL_MS;
  const llmModel = resolveModelName(opts.model);
  const secondaryModel = resolveSecondaryModel();

  const cachedResult = getCachedAnalysis({ ticker: upperTicker, baselineDate, ttlMs: analysisTtl, model: llmModel });
  if(cachedResult){
    return cachedResult;
  }

  const cik = await getCIK(upperTicker, UA, SEC_KEY);
  const filings = await getRecentFilings(cik, baselineDate, UA, SEC_KEY);
  const perFiling = await mapWithConcurrency(filings, 3, async (f)=>{
    const mda = await fetchMDA(f.url, UA);
    let summary = mda.slice(0, 1200);
    try{
      summary = await summarizeMda({ text: mda, openKey: OPENAI_KEY, model: secondaryModel, meta:{ ticker: upperTicker, form: f.form } });
    }catch(err){
      console.warn('[MDA Summary]', err.message);
    }
    return {
      form:f.form,
      formLabel:f.formLabel,
      filingDate:f.filingDate,
      reportDate:f.reportDate,
      mda_summary: summary,
      mda_excerpt: mda.slice(0,1500)
    };
  });

  const cacheContext = baselineDate;
  const [recoRes, earnRes, quoteRes] = await Promise.allSettled([
    getRecommendations(upperTicker, FH_KEY, cacheContext),
    getEarnings(upperTicker, FH_KEY, cacheContext),
    getQuote(upperTicker, FH_KEY, cacheContext)
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
      const hist = await getHistoricalPrice(upperTicker, baselineDate, {
        fmpKey: FMP_KEY,
        finnhubKey: FH_KEY,
        alphaKey: AV_KEY,
      });
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
    if(FMP_KEY){
      try{
        const quote = await getFmpQuote(upperTicker, FMP_KEY);
        current = quote.price;
        priceMeta.source = 'fmp_quote';
        if(quote.asOf) priceMeta.as_of = quote.asOf;
      }catch(err){ console.warn('[FMP Quote]', err.message); }
    }
    if(current==null){
      try{
        const yahoo = await getYahooQuote(upperTicker);
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
      const quote = await getFmpQuote(upperTicker, FMP_KEY);
      current = quote.price;
      priceMeta.source = 'fmp_quote';
      priceMeta.as_of = quote.asOf || priceMeta.as_of;
      priceMeta.kind = 'real-time';
    }catch(err){ console.warn('[FMP Quote fallback]', err.message); }
  }
  if(current==null){
    try{
      const yahoo = await getYahooQuote(upperTicker);
      current = yahoo.price;
      priceMeta.source = yahoo.source;
      priceMeta.as_of = yahoo.asOf || priceMeta.as_of;
      priceMeta.kind = 'real-time';
    }catch(err){ console.warn('[Yahoo Quote fallback]', err.message); }
  }

  priceMeta.value = current;
  const quote = { ...(finnhub.quote || {}), c: current };
  finnhub.quote = quote;
  finnhub.price_meta = priceMeta;

  let ptAgg;
  try{
    ptAgg = await getAggregatedPriceTarget(upperTicker, FH_KEY, AV_KEY, current, FMP_KEY);
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

  const finnhubSnapshot = {
    recommendation: Array.isArray(finnhub.recommendation)
      ? compactRecommendation(finnhub.recommendation)
      : (finnhub.recommendation?.error ? { error: finnhub.recommendation.error } : null),
    earnings: Array.isArray(finnhub.earnings)
      ? compactEarningsRows(finnhub.earnings)
      : (finnhub.earnings?.error ? { error: finnhub.earnings.error } : []),
    quote: compactQuote(finnhub.quote),
    price_target: ptAgg,
    price_meta: priceMeta
  };

  const payload = {
    company: upperTicker,
    baseline_date: baselineDate,
    sec_filings: perFiling.map(x=>({
      form: x.form,
      form_label: x.formLabel || x.form,
      filingDate: x.filingDate,
      reportDate: x.reportDate,
      mda_summary: x.mda_summary,
      mda_excerpt: x.mda_excerpt
    })),
    finnhub: finnhubSnapshot
  };

  const newsRaw = await buildNewsBundle({ ticker: upperTicker, baselineDate, openKey: OPENAI_KEY, model: secondaryModel });
  const newsCompact = compactNewsBundle(newsRaw);
  payload.news = newsCompact;
  const momentumRaw = await computeMomentumMetrics(upperTicker, baselineDate);
  const momentum = compactMomentum(momentumRaw);
  payload.momentum = momentum;
  const llmTtlMs = analysisTtl;
  const llm = await analyzeWithLLM(OPENAI_KEY, llmModel, payload, { cacheTtlMs: llmTtlMs, promptVersion: 'profile_v2' });
  const llmUsage = llm?.__usage || null;

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
    momentum
  };
  saveAnalysisResult({ ticker: upperTicker, baselineDate, isHistorical, model: llmModel, result });
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
  const {ticker, date, model} = req.body||{};
  if(!ticker||!date) return res.status(400).json({error:'ticker and date required'});
  const resolvedModel = resolveModelName(model);
  try{
    const result = await performAnalysis(ticker, date, { model: resolvedModel });
    res.json(result);
  }catch(err){ return errRes(res, err); }
});

app.post('/api/reset-cache', async (req,res)=>{
  const { ticker, date, model } = req.body || {};
  if(!ticker || !date) return res.status(400).json({ error:'ticker and date required' });
  const normalizedDate = normalizeDate(date);
  if(!normalizedDate || !dayjs(normalizedDate).isValid()){
    return res.status(400).json({ error:'invalid date' });
  }
  const upperTicker = ticker.toUpperCase();
  const resolvedModel = resolveModelName(model);
  deleteAnalysis({ ticker: upperTicker, baselineDate: normalizedDate, model: resolvedModel });
  const clearedExact = clearCacheForTicker({ ticker: upperTicker, baselineDate: normalizedDate });
  const clearedAll = clearCacheForTicker({ ticker: upperTicker, includeAllDates: true });
  res.json({ ok:true, cleared_cache_files: clearedExact + clearedAll });
});

app.post('/api/batch', upload.single('file'), async (req,res)=>{
  try{
    const tasks = parseBatchFile(req.file);
    if(!tasks.length) return res.status(400).json({error:'檔案內沒有有效的 ticker/date 列'});
    const memo = new Map();
    const rows = await mapWithConcurrency(tasks, BATCH_CONCURRENCY, async (task)=>{
      const resolvedModel = resolveModelName(task.model);
      const key = `${task.ticker.toUpperCase()}__${task.date}__${resolvedModel}`;
      if(!memo.has(key)){
        memo.set(key, (async ()=>{
          try{
            const result = await performAnalysis(task.ticker, task.date, { model: resolvedModel });
            return { ok:true, result };
          }catch(error){
            return { ok:false, error };
          }
        })());
      }
      const outcome = await memo.get(key);
      if(!outcome.ok){
        return {
          ticker: task.ticker.toUpperCase(),
          date: task.date,
          model: resolvedModel,
          current_price: '',
          analyst_mean_target: '',
          llm_target_price: '',
          recommendation: `ERROR: ${outcome.error.message}`,
          segment: '',
          quality_score: '',
          news_sentiment: '',
          momentum_score: '',
          trend_flag: ''
        };
      }
      const result = outcome.result;
      const summary = result.fetched?.finnhub_summary || {};
      const profile = result.analysis?.profile;
      const newsSent = result.news?.sentiment;
      const momentum = result.momentum || {};
      return {
        ticker: result.input.ticker,
        date: task.date,
        model: resolvedModel,
        current_price: summary.quote?.c ?? '',
        analyst_mean_target: summary.price_target?.targetMean ?? summary.price_target?.targetMedian ?? '',
        llm_target_price: result.analysis?.action?.target_price ?? '',
        recommendation: result.analysis?.action?.rating ?? '',
        segment: profile?.segment_label || profile?.segment || '',
        quality_score: profile?.score ?? '',
        news_sentiment: newsSent?.sentiment_label || '',
        momentum_score: momentum.score ?? '',
        trend_flag: momentum.trend || ''
      };
    });
    const fields = ['ticker','date','model','current_price','analyst_mean_target','llm_target_price','recommendation','segment','quality_score','news_sentiment','momentum_score','trend_flag'];
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
