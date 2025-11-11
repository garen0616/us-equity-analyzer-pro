import dayjs from 'dayjs';
import fetch from 'node-fetch';
import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { callOpenAIChat } from './openaiClient.js';
import { getFmpNews } from './fmp.js';

const GDELT_ENDPOINT = 'https://api.gdeltproject.org/api/v2/doc/doc';
const NEWS_CACHE_TTL = 6 * 60 * 60 * 1000;
const KEYWORD_TTL = 7 * 24 * 60 * 60 * 1000;
const RELIABLE_SOURCES = new Set(['finance.yahoo.com','fool.com','fool.co.uk','reuters.com','bloomberg.com','wsj.com','marketwatch.com','seekingalpha.com','investing.com','cnbc.com','barrons.com','forbes.com','fortune.com']);
const FMP_NEWS_TTL = 30 * 60 * 1000;
const FINNHUB_NEWS_ENDPOINT = 'https://finnhub.io/api/v1/company-news';
const EVENT_KEYWORDS = [
  { label:'財報', terms:['earnings','results','guidance','outlook','quarter','財報','季度'] },
  { label:'監管', terms:['regulation','regulatory','antitrust','fta','compliance','監管','審查'] },
  { label:'併購/合作', terms:['merger','acquisition','deal','partnership','contract','agreement','收購','合作'] },
  { label:'供應鏈', terms:['supply','capacity','fab','foundry','shortage','供應','產能','封測'] },
  { label:'資金/股本', terms:['buyback','repurchase','dividend','equity offering','增資','回購'] }
];

function cacheKey(prefix, parts){
  return `${prefix}_${parts.filter(Boolean).join('_')}`;
}

function fetchWithTimeout(url, timeoutMs=20000, options={}){
  const controller = new AbortController();
  const timer = setTimeout(()=>controller.abort(), timeoutMs);
  const config = { ...options, signal: controller.signal };
  return fetch(url, config).finally(()=> clearTimeout(timer));
}

function fallbackKeywords(ticker){
  const upper = ticker?.toUpperCase() || '';
  return [upper, `${upper} earnings`, `${upper} outlook`, 'guidance', 'margin'].filter(Boolean);
}

export async function getNewsKeywords(ticker, openKey, model, { useLlm=true }={}){
  const baseKey = cacheKey('news_kw', [ticker, useLlm?'llm':'noai']);
  const cached = await getCache(baseKey, KEYWORD_TTL);
  if(cached) return cached;
  if(!useLlm || !openKey) return fallbackKeywords(ticker);
  const prompt = [
    { role:'system', content:'你是幫助投資研究的助理，請回傳 JSON 陣列，不要加入其他文字。' },
    { role:'user', content:`請列出 5 個和 ${ticker} 及其產業高度關聯的英文關鍵字，回應格式須為 ["keyword"]。` }
  ];
  try{
    const hash = crypto.createHash('sha256').update(JSON.stringify({ model, prompt })).digest('hex');
    const cacheId = cacheKey('news_kw_resp', [model, hash]);
    const memo = await getCache(cacheId, KEYWORD_TTL);
    if(memo) return memo;
    const { data } = await callOpenAIChat({ openKey, model, messages: prompt, timeoutMs:120000 });
    const text = data?.choices?.[0]?.message?.content?.trim();
    if(!text) return [ticker];
    const cleaned = text.replace(/```json|```/gi,'').trim();
    const arr = JSON.parse(cleaned);
    if(Array.isArray(arr) && arr.length){
      const picked = arr.map(x=>String(x||'').trim()).filter(Boolean).slice(0,5);
      if(picked.length){
        await setCache(baseKey, picked);
        await setCache(cacheId, picked);
        return picked;
      }
    }
    return fallbackKeywords(ticker);
  }catch(err){
    console.warn('[News] keyword generation failed', err.message);
    return fallbackKeywords(ticker);
  }
}

function buildBooleanQuery(ticker, keywords){
  const sanitized = (val)=>String(val||'').replace(/"/g,' ').trim();
  const companyTerms = [ticker, sanitized(ticker).toUpperCase(), sanitized(ticker).toLowerCase()].filter(Boolean).map(term=> term.includes(' ') ? `"${term}"` : term);
  const keywordTerms = keywords
    .map(sanitized)
    .filter(x=>x && x.length>2)
    .map(term=> term.includes(' ') ? `"${term}"` : term);
  const companyClause = companyTerms.length ? `(${companyTerms.join(' OR ')})` : ticker;
  if(!keywordTerms.length) return companyClause;
  const keywordClause = `(${keywordTerms.join(' OR ')})`;
  return `${companyClause} AND ${keywordClause}`;
}

function extractTags(text){
  if(!text) return [];
  const lower = text.toLowerCase();
  const tags = [];
  for (const item of EVENT_KEYWORDS){
    if(item.terms.some(term=> lower.includes(term.toLowerCase()))){
      tags.push(item.label);
    }
  }
  return tags;
}

export async function fetchGdeltArticles({ ticker, keywords=[], baselineDate, monthsBack=1, max=50 }){
  const end = dayjs(baselineDate).endOf('day');
  const start = end.subtract(monthsBack, 'month');
  const startStr = start.format('YYYYMMDD000000');
  const endStr = end.format('YYYYMMDD235959');
  const query = buildBooleanQuery(ticker, keywords);
  const params = new URLSearchParams({
    query: query || ticker,
    mode: 'ArtList',
    format: 'JSON',
    maxrecords: String(max),
    sort: 'DateDesc',
    startdatetime: startStr,
    enddatetime: endStr
  });
  const url = `${GDELT_ENDPOINT}?${params.toString()}`;
  const res = await fetchWithTimeout(url, 20000);
  if(!res.ok) throw new Error(`GDELT ${res.status}`);
  const text = await res.text();
  let data;
  try{
    data = JSON.parse(text);
  }catch(err){
    throw new Error(`GDELT parse fail: ${text.slice(0,80)}`);
  }
  const articles = (data?.articles || [])
    .map(a=>({
      title: a.title || '',
      summary: a.excerpt || '',
      url: a.url || a.articleurl || '',
      source: (a.domain || a.source || '').toLowerCase(),
      language: (a.language || '').toLowerCase(),
      published_at: a.seendate || a.published || '',
      tone: typeof a.tone==='number'? a.tone : null
    }))
    .filter(x=>x.title && x.url)
    .filter(x=> !x.language || x.language==='english')
    .map(x=>({ ...x, tags: extractTags(`${x.title} ${x.summary}`) }))
    .filter(x=> !RELIABLE_SOURCES.size || RELIABLE_SOURCES.has(x.source) || x.tags.length);
  return articles.slice(0,20);
}

async function fetchFmpArticles({ ticker, baselineDate, key, limit=40 }){
  if(!key) return [];
  const cacheId = cacheKey('news_fmp', [ticker, baselineDate]);
  const cached = await getCache(cacheId, FMP_NEWS_TTL);
  if(cached) return cached;
  const rows = await getFmpNews(ticker, key, limit);
  const end = dayjs(baselineDate).endOf('day');
  const start = end.subtract(45, 'day');
  const endPlus = end.add(1,'day');
  const articles = rows
    .map(item=>{
      const published = item.publishedDate || item.publishedAt || item.date || item.datetime || '';
      const publishedIso = published ? dayjs(published).toISOString() : '';
      return {
        title: item.title || '',
        summary: item.text || item.summary || '',
        url: item.url || '',
        source: (item.site || '').toLowerCase(),
        published_at: publishedIso,
        tags: extractTags(`${item.title || ''} ${item.text || ''}`),
        tone: null
      };
    })
    .filter(x=>x.title && x.url)
    .filter(x=> !x.published_at || (
      dayjs(x.published_at).isAfter(start) &&
      dayjs(x.published_at).isBefore(endPlus)
    ));
  await setCache(cacheId, articles);
  return articles;
}

function uniqArticles(list){
  const seen = new Set();
  const out = [];
  for(const item of list){
    const key = (item.url || item.title || '').toLowerCase();
    if(!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

export async function fetchFinnhubNews({ ticker, baselineDate, key }){
  if(!key) return [];
  const end = dayjs(baselineDate).endOf('day');
  const start = end.subtract(30, 'day');
  const params = new URLSearchParams({
    symbol: ticker,
    from: start.format('YYYY-MM-DD'),
    to: end.format('YYYY-MM-DD'),
    token: key
  });
  const url = `${FINNHUB_NEWS_ENDPOINT}?${params.toString()}`;
  const res = await fetchWithTimeout(url, 15000);
  if(!res.ok) throw new Error(`Finnhub news ${res.status}`);
  const data = await res.json();
  if(!Array.isArray(data)) return [];
  return data.slice(0,20).map(item=>({
    title: item.headline || item.title || '',
    summary: item.summary || '',
    url: item.url || '',
    source: (item.source || '').toLowerCase(),
    published_at: item.datetime ? dayjs.unix(item.datetime).toISOString() : item.datetime,
    tags: extractTags(`${item.headline || ''} ${item.summary || ''}`),
    tone: item.sentiment ? Number(item.sentiment) : null
  })).filter(x=>x.title && x.url);
}

export async function analyzeNewsSentiment({ ticker, baselineDate, articles, openKey, model }){
  if(!articles?.length) return {
    sentiment_label:'中性',
    summary:'近一個月無明顯新聞事件。',
    supporting_events:[]
  };
  if(!openKey) return {
    sentiment_label:'中性',
    summary:'缺少 LLM 金鑰，無法分析新聞情緒。',
    supporting_events: articles.slice(0,3).map(a=>({ title:a.title, url:a.url }))
  };
  const messages=[
    { role:'system', content:'你是財經新聞分析師。請根據輸入的新聞列表，輸出 JSON 物件 {"sentiment_label":"樂觀|中性|悲觀","summary":"100字說明","supporting_events":[{"title":string,"reason":string}]}。只輸出 JSON。' },
    { role:'user', content: JSON.stringify({ ticker, baseline_date: baselineDate, articles }) }
  ];
  try{
    const hash = crypto.createHash('sha256').update(JSON.stringify({ model, articles })).digest('hex');
    const cacheId = cacheKey('news_sentiment', [hash]);
    const cached = await getCache(cacheId, NEWS_CACHE_TTL);
    if(cached) return cached;
    const { data } = await callOpenAIChat({ openKey, model, messages, timeoutMs:120000 });
    const text = data?.choices?.[0]?.message?.content?.trim();
    if(!text) throw new Error('empty LLM response');
    const cleaned = text.replace(/```json|```/gi,'').trim();
    const parsed = JSON.parse(cleaned);
    await setCache(cacheId, parsed);
    return parsed;
  }catch(err){
    console.warn('[News] sentiment failed', err.message);
    return {
      sentiment_label:'中性',
      summary:'新聞情緒分析失敗，請稍後重試。',
      supporting_events:[]
    };
  }
}

export async function buildNewsBundle({ ticker, baselineDate, openKey, model, useLlm=true, articleLimit=5, finnhubKey, fmpKey }){
  const key = cacheKey('news_bundle', [ticker, baselineDate, useLlm?'llm':'noai', model || 'default']);
  const cached = await getCache(key, NEWS_CACHE_TTL);
  if(cached) return cached;
  try{
    const keywords = await getNewsKeywords(ticker, openKey, model, { useLlm });
    const fetchers = [];
    if(fmpKey){
      fetchers.push({ label:'FMP', promise: fetchFmpArticles({ ticker, baselineDate, key: fmpKey, limit: articleLimit * 4 }) });
    }
    fetchers.push({ label:'GDELT', promise: fetchGdeltArticles({ ticker, keywords, baselineDate }) });
    if(finnhubKey){
      fetchers.push({ label:'Finnhub', promise: fetchFinnhubNews({ ticker, baselineDate, key: finnhubKey }) });
    }
    const settled = await Promise.allSettled(fetchers.map(f=>f.promise));
    let articles = [];
    settled.forEach((result, idx)=>{
      const label = fetchers[idx].label;
      if(result.status === 'fulfilled'){
        if(Array.isArray(result.value)) articles = articles.concat(result.value);
      }else{
        console.warn(`[News] ${label} failed`, result.reason?.message || result.reason);
      }
    });
    articles = uniqArticles(articles || []).sort((a,b)=>{
      const aTs = a.published_at ? dayjs(a.published_at).valueOf() : 0;
      const bTs = b.published_at ? dayjs(b.published_at).valueOf() : 0;
      return bTs - aTs;
    });
    const trimmedArticles = articles.slice(0, articleLimit).map(article=>({
      ...article,
      summary: (article.summary || '').slice(0, 200)
    }));
    const sentiment = useLlm
      ? await analyzeNewsSentiment({ ticker, baselineDate, articles: trimmedArticles, openKey, model })
      : {
          sentiment_label:'中性',
          summary:'依公開資料產生的快速摘要（未呼叫 LLM）。',
          supporting_events: trimmedArticles.slice(0,3).map(a=>({ title:a.title, reason: a.tags?.[0] || '焦點事件' }))
        };
    const bundle = { keywords, articles: trimmedArticles, sentiment };
    await setCache(key, bundle);
    return bundle;
  }catch(err){
    console.warn('[News] bundle failed', err.message);
    const fallback = { keywords:[ticker], articles:[], sentiment:{ sentiment_label:'中性', summary:'無法取得新聞資料。', supporting_events:[] } };
    await setCache(key, fallback);
    return fallback;
  }
}
