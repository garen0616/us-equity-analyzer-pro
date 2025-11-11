import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { callOpenAIChat } from './openaiClient.js';

const SUMMARY_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const MAX_INPUT_CHARS = Math.max(4000, Number(process.env.MDA_MAX_INPUT_CHARS || 9000));
const SECTION_KEYWORDS = ['overview','discussion','performance','outlook','guidance','liquidity','capital','risk','strategy','trend'];

function hashPayload(model, text){
  return crypto.createHash('sha256').update(`${model}__${text}`).digest('hex');
}

function sanitize(text){
  return String(text || '')
    .replace(/<table[\s\S]*?<\/table>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractKeySections(text, limit){
  if(!text) return '';
  const sections = text.split(/\n{2,}/);
  const picked = [];
  for(const section of sections){
    const lower = section.toLowerCase();
    if(SECTION_KEYWORDS.some(keyword=> lower.includes(keyword))){
      picked.push(section.trim());
    }
    if(picked.join('\n').length >= limit) break;
  }
  if(!picked.length) return text.slice(0, limit);
  return picked.join('\n').slice(0, limit);
}

function buildFallbackSummary(text, lines=5){
  const sentences = text
    .split(/(?<=[。！？.!?])/)
    .map(s=>s.trim())
    .filter(Boolean);
  if(!sentences.length) return text.slice(0, 800);
  const picked = [];
  for(const sentence of sentences){
    picked.push(sentence.slice(0, 160));
    if(picked.length >= lines) break;
  }
  return picked.join('\n');
}

function buildCacheValue(summary, kind){
  return { summary, kind };
}

function decodeCacheValue(val){
  if(!val) return null;
  if(typeof val === 'string') return { summary: val, kind:'llm' };
  if(typeof val === 'object' && val.summary){
    return { summary: val.summary, kind: val.kind || 'llm' };
  }
  return null;
}

export async function summarizeMda({ text, openKey, model, meta={}, useLlm=true }){
  if(!text) return { summary:'', kind:'fallback' };
  const cleaned = sanitize(text);
  const focused = extractKeySections(cleaned, MAX_INPUT_CHARS);
  const trimmed = focused.slice(0, MAX_INPUT_CHARS);
  if(!useLlm || !openKey || !model){
    return { summary: buildFallbackSummary(trimmed), kind:'fallback' };
  }
  const cacheKey = `mda_summary_${hashPayload(model, trimmed)}`;
  const cachedRaw = await getCache(cacheKey, SUMMARY_CACHE_TTL);
  const cached = decodeCacheValue(cachedRaw);
  if(cached) return cached;
  const { ticker='', form='' } = meta;
  const messages = [
    {
      role:'system',
      content:'你是財報分析師，請以繁體中文濃縮輸入的 MD&A，產出 5-6 行要點（每行 20-30 字），涵蓋成長動能、毛利/營益率變化、現金流與資本配置、風險與後續催化。僅輸出文字列，不要額外說明。'
    },
    {
      role:'user',
      content:`[${ticker || 'TICKER'} | ${form || 'FORM'}] 摘要以下內容：\n${trimmed}`
    }
  ];
  try{
    const { data } = await callOpenAIChat({ openKey, model, messages, timeoutMs:60000, temperature:0, maxCompletionTokens:600 });
    const summary = data?.choices?.[0]?.message?.content?.trim();
    if(summary){
      const value = buildCacheValue(summary, 'llm');
      await setCache(cacheKey, value);
      return value;
    }
  }catch(err){
    console.warn('[MDA Summary] failed', err.message);
  }
  const fallback = buildFallbackSummary(trimmed);
  const fallbackValue = buildCacheValue(fallback, 'fallback');
  await setCache(cacheKey, fallbackValue);
  return fallbackValue;
}
