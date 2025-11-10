import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { callOpenAIChat } from './openaiClient.js';

const SUMMARY_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const MAX_INPUT_CHARS = Math.max(4000, Number(process.env.MDA_MAX_INPUT_CHARS || 9000));

function hashPayload(model, text){
  return crypto.createHash('sha256').update(`${model}__${text}`).digest('hex');
}

export async function summarizeMda({ text, openKey, model, meta={} }){
  if(!text) return '';
  const trimmed = text.slice(0, MAX_INPUT_CHARS);
  if(!openKey || !model) return trimmed.slice(0, 2000);
  const cacheKey = `mda_summary_${hashPayload(model, trimmed)}`;
  const cached = await getCache(cacheKey, SUMMARY_CACHE_TTL);
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
    const { data } = await callOpenAIChat({ openKey, model, messages, timeoutMs:60000 });
    const summary = data?.choices?.[0]?.message?.content?.trim();
    if(summary){
      await setCache(cacheKey, summary);
      return summary;
    }
  }catch(err){
    console.warn('[MDA Summary] failed', err.message);
  }
  const fallback = trimmed.slice(0, 2000);
  await setCache(cacheKey, fallback);
  return fallback;
}
