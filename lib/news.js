import dayjs from 'dayjs';
import fetch from 'node-fetch';
import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { callOpenAIChat } from './openaiClient.js';

const GDELT_ENDPOINT = 'https://api.gdeltproject.org/api/v2/doc/doc';
const NEWS_CACHE_TTL = 6 * 60 * 60 * 1000;
const KEYWORD_TTL = 7 * 24 * 60 * 60 * 1000;
const RELIABLE_SOURCES = new Set(['finance.yahoo.com','fool.com','fool.co.uk','reuters.com','bloomberg.com','wsj.com','marketwatch.com','seekingalpha.com','investing.com','cnbc.com','barrons.com','forbes.com','fortune.com']);
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

export async function getNewsKeywords(ticker, openKey, model){
  const baseKey = cacheKey('news_kw', [ticker]);
  const cached = await getCache(baseKey, KEYWORD_TTL);
  if(cached) return cached;
  if(!openKey) return [ticker];
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
    return [ticker];
  }catch(err){
    console.warn('[News] keyword generation failed', err.message);
    return [ticker];
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

export async function buildNewsBundle({ ticker, baselineDate, openKey, model }){
  const key = cacheKey('news_bundle', [ticker, baselineDate, model]);
  const cached = await getCache(key, NEWS_CACHE_TTL);
  if(cached) return cached;
  try{
    const keywords = await getNewsKeywords(ticker, openKey, model);
    const articles = await fetchGdeltArticles({ ticker, keywords, baselineDate });
    const sentiment = await analyzeNewsSentiment({ ticker, baselineDate, articles, openKey, model });
    const bundle = { keywords, articles, sentiment };
    await setCache(key, bundle);
    return bundle;
  }catch(err){
    console.warn('[News] bundle failed', err.message);
    const fallback = { keywords:[ticker], articles:[], sentiment:{ sentiment_label:'中性', summary:'無法取得新聞資料。', supporting_events:[] } };
    await setCache(key, fallback);
    return fallback;
  }
}
