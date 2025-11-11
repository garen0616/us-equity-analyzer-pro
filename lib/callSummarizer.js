import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { callOpenAIChat } from './openaiClient.js';

const CALL_SUMMARY_TTL = 30 * 24 * 60 * 60 * 1000;
const MAX_INPUT_CHARS = Number(process.env.CALL_TRANSCRIPT_MAX_CHARS || 6000);

function hashPayload(model, text){
  return crypto.createHash('sha256').update(`${model}__${text}`).digest('hex');
}

export async function summarizeCallTranscript({ text, openKey, model, meta={} }){
  if(!text) return { summary:'', bullets:[] };
  const trimmed = text.slice(0, MAX_INPUT_CHARS);
  if(!openKey || !model){
    return {
      summary: trimmed.slice(0, 400),
      bullets: []
    };
  }
  const hash = hashPayload(model, trimmed);
  const cacheKey = `call_summary_${hash}`;
  const cached = await getCache(cacheKey, CALL_SUMMARY_TTL);
  if(cached) return cached;
  const { ticker='TICKER', quarter='Q', year='YEAR' } = meta;
  const messages = [
    {
      role:'system',
      content:'你是財報會議記錄的分析師，請以繁體中文輸出 JSON：{"summary":"120字重點","bullets":[{"title":string,"detail":string}]}，涵蓋 demand、margin/cost、資本配置與指引。'
    },
    {
      role:'user',
      content:`[${ticker} | ${quarter} ${year} Earnings Call] 摘要以下內容：\n${trimmed}`
    }
  ];
  try{
    const { data } = await callOpenAIChat({
      openKey,
      model,
      messages,
      timeoutMs:120000,
      temperature:0,
      responseFormat:{ type:'json_object' },
      maxCompletionTokens:500
    });
    const textResp = data?.choices?.[0]?.message?.content?.trim();
    if(!textResp) throw new Error('empty response');
    const cleaned = textResp.replace(/```json|```/gi,'').trim();
    const parsed = JSON.parse(cleaned);
    await setCache(cacheKey, parsed);
    return parsed;
  }catch(err){
    console.warn('[CallSummarizer]', err.message);
    const fallback = {
      summary: trimmed.slice(0, 400),
      bullets: []
    };
    await setCache(cacheKey, fallback);
    return fallback;
  }
}
