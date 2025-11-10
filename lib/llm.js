import axios from 'axios';
import crypto from 'crypto';
import { getCache, setCache } from './cache.js';

const MODEL_PRICING = {
  'gpt-4o-mini': {
    input: 0.15 / 1_000_000,
    output: 0.60 / 1_000_000
  }
};

function getPricing(model){
  return MODEL_PRICING[model];
}

const SYSTEM_PROMPT = [
  '你是資深金融分析師，需依據輸入資料輸出 JSON，所有文字必須使用繁體中文。',
  '結構：{"per_filing":[{"form":"10-Q|10-K","filingDate":"YYYY-MM-DD","reportDate":"YYYY-MM-DD","five_indicators":{"alignment_score":number,"key_conflicts":[string],"valuation_rationale":string,"risk_factors":[string],"catalyst_timeline":[{"event":string,"window":string,"why":string}]},"explanation":"300-500字"}],"consensus_view":{"summary":string,"agreement_ratio":number},"action":{"rating":"BUY|HOLD|SELL","target_price":number,"stop_loss":number,"rationale":string},"profile":{"segment":"large_cap|small_cap","segment_label":string,"summary":string,"filters":{"total":8,"met":number,"items":[{"name":string,"met":boolean,"reason":string}]},"score":number,"score_detail":[{"category":string,"points":number,"reason":string}],"catalysts":[string]},"news_insight":{"summary":string,"impact":"正面|中性|負面","key_events":[{"title":string,"why":string}]}}',
  '請綜合 SEC MD&A 摘要、分析師/目標價資料、新聞情緒、動能與 ETF 參考，評估估值、成長與風險。需完成大型股或小型股的 8 項硬性過濾（至少 6 項通過）與 100 分制打分，並給出可執行的投資建議與關鍵催化、風險。'
].join('\n');

export async function analyzeWithLLM(openKey, model, payload, options={}){
  if(!openKey) throw new Error('[OPENAI] Missing API key');
  const { cacheTtlMs, promptVersion='v1' } = options;
  const hashInput = JSON.stringify({ payload, promptVersion, model });
  const payloadHash = crypto.createHash('sha256').update(hashInput).digest('hex');
  const cacheKey = `llm_${model}_${payloadHash}`;
  const ttl = Number.isFinite(cacheTtlMs) ? cacheTtlMs : undefined;
  const cached = await getCache(cacheKey, ttl);
  if(cached) return cached;
  try{
    const {data} = await axios.post('https://api.openai.com/v1/chat/completions',{
      model,
      messages:[
        { role:'system', content: SYSTEM_PROMPT },
        { role:'user', content: JSON.stringify(payload) }
      ]
    },{
      headers:{ 'Authorization':`Bearer ${openKey}`, 'Content-Type':'application/json' },
      timeout:240000
    });
    const usage = data?.usage;
    const pricing = getPricing(model);
    let usageInfo = null;
    if(usage){
      usageInfo = {
        model,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens ?? (usage.prompt_tokens + usage.completion_tokens)
      };
      if(pricing){
        const inputCost = usage.prompt_tokens * pricing.input;
        const outputCost = usage.completion_tokens * pricing.output;
        const totalCost = inputCost + outputCost;
        usageInfo.input_cost = Number(inputCost.toFixed(6));
        usageInfo.output_cost = Number(outputCost.toFixed(6));
        usageInfo.total_cost = Number(totalCost.toFixed(6));
        console.log(`[OpenAI] model=${model} prompt=${usage.prompt_tokens} completion=${usage.completion_tokens} approx_cost=$${totalCost.toFixed(4)}`);
      }else{
        console.log(`[OpenAI] model=${model} prompt=${usage.prompt_tokens} completion=${usage.completion_tokens} (cost unavailable)`);
      }
    }
    const text = data?.choices?.[0]?.message?.content || '{}';
    const cleaned = text.trim().replace(/^```json/i,'').replace(/```$/,'').trim();
    try{
      const parsed = JSON.parse(cleaned);
      if(usageInfo){
        parsed.__usage = usageInfo;
      }
      await setCache(cacheKey, parsed);
      return parsed;
    }catch{
      const rawFallback = { raw: text };
      if(usageInfo){
        rawFallback.__usage = usageInfo;
      }
      await setCache(cacheKey, rawFallback);
      return rawFallback;  // 保留原文避免 throw
    }
  }catch(err){
    throw new Error(`[OPENAI] ${err.response?.data?.error?.message || err.message}`);
  }
}
