import crypto from 'crypto';
import { getCache, setCache } from './cache.js';
import { getLlmCache, setLlmCache } from './analysisStore.js';
import { callOpenAIChat } from './openaiClient.js';

// Pricing per https://openai.com/pricing (retrieved 2024-11)
const MODEL_PRICING = {
  'gpt-4o': {
    input: 5 / 1_000_000,
    output: 15 / 1_000_000
  },
  'gpt-4o-mini': {
    input: 0.15 / 1_000_000,
    output: 0.60 / 1_000_000
  }
};

const DEFAULT_LLM_CACHE_TTL_MS = Number(process.env.LLM_CACHE_TTL_HOURS || 48) * 60 * 60 * 1000;

const GPT5_INPUT_COST = Number(process.env.OPENAI_GPT5_INPUT_COST_PER_TOKEN);
const GPT5_OUTPUT_COST = Number(process.env.OPENAI_GPT5_OUTPUT_COST_PER_TOKEN);
if(Number.isFinite(GPT5_INPUT_COST) && Number.isFinite(GPT5_OUTPUT_COST)){
  MODEL_PRICING['gpt-5'] = {
    input: GPT5_INPUT_COST,
    output: GPT5_OUTPUT_COST
  };
}

function getPricing(model){
  return MODEL_PRICING[model];
}

const SYSTEM_PROMPT = [
  '你是資深金融分析師，需依據輸入資料輸出 JSON，所有文字必須使用繁體中文。',
  '結構：{"per_filing":[{"form":"10-Q|10-K","filingDate":"YYYY-MM-DD","reportDate":"YYYY-MM-DD","five_indicators":{"alignment_score":number,"key_conflicts":[string],"valuation_rationale":string,"risk_factors":[string],"catalyst_timeline":[{"event":string,"window":string,"why":string}]},"explanation":"300-500字"}],"consensus_view":{"summary":string,"agreement_ratio":number},"action":{"rating":"BUY|HOLD|SELL","target_price":number,"stop_loss":number,"rationale":string},"profile":{"segment":"large_cap|small_cap","segment_label":string,"summary":string,"filters":{"total":8,"met":number,"items":[{"name":string,"met":boolean,"reason":string}]},"score":number,"score_detail":[{"category":string,"points":number,"reason":string}],"catalysts":[string]},"news_insight":{"summary":string,"impact":"正面|中性|負面","key_events":[{"title":string,"why":string}]}}',
  '請綜合 SEC MD&A 摘要、分析師/目標價資料、新聞情緒、動能與 ETF 參考，評估估值、成長與風險。需完成大型股或小型股的 8 項硬性過濾（至少 6 項通過）與 100 分制打分，並給出可執行的投資建議與關鍵催化、風險。'
].join('\n');

const COMPLETION_TOKEN_CEILING = Number(process.env.OPENAI_COMPLETION_MAX_TOKENS || 1500);
const REPAIR_MODEL = process.env.OPENAI_MODEL_REPAIR || process.env.OPENAI_MODEL_SECONDARY || 'gpt-4o-mini';
const JSON_FORMAT_MODELS = new Set([
  'gpt-4o',
  'gpt-4o-mini',
  'gpt-4o-mini-200k',
  'gpt-4.1',
  'gpt-4.1-mini'
]);

function supportsJsonFormat(model){
  if(!model) return false;
  return JSON_FORMAT_MODELS.has(String(model).toLowerCase());
}

function tryParseJson(text){
  if(!text) return null;
  try{
    return JSON.parse(text);
  }catch{}
  const first = text.indexOf('{');
  const last = text.lastIndexOf('}');
  if(first>=0 && last>first){
    const snippet = text.slice(first, last+1);
    try{
      return JSON.parse(snippet);
    }catch{}
  }
  return null;
}
export async function analyzeWithLLM(openKey, model, payload, options={}){
  if(!openKey) throw new Error('[OPENAI] Missing API key');
  const { cacheTtlMs, promptVersion='v1', fallbackModel } = options;
  const ttl = Number.isFinite(cacheTtlMs) ? cacheTtlMs : DEFAULT_LLM_CACHE_TTL_MS;
  const attemptModels = [model];
  if(fallbackModel && fallbackModel !== model){
    attemptModels.push(fallbackModel);
  }
  let lastErr;

  for(const targetModel of attemptModels){
    try{
      const result = await runWithModel(targetModel);
      if(result) return result;
    }catch(err){
      lastErr = err;
      console.warn(`[OpenAI] model ${targetModel} failed: ${err.message}`);
    }
  }
  throw lastErr || new Error('[OPENAI] all models failed');

  async function runWithModel(targetModel){
    const hashInput = JSON.stringify({ payload, promptVersion, model: targetModel });
    const payloadHash = crypto.createHash('sha256').update(hashInput).digest('hex');
    const cacheKey = `llm_${targetModel}_${payloadHash}`;
    const dbCached = getLlmCache(payloadHash, ttl);
    if(dbCached) return dbCached;
    const cached = await getCache(cacheKey, ttl);
    if(cached){
      setLlmCache({ hash: payloadHash, ttlMs: ttl, result: cached });
      return cached;
    }
    const deterministicSeed = parseInt(payloadHash.slice(0, 12), 16) % 1_000_000_000;
    const preferStructured = supportsJsonFormat(targetModel);
    return await invokeModel({ enforceJson: preferStructured, rawAttempt: false });

    async function invokeModel({ enforceJson, rawAttempt }){
      const { data } = await callOpenAIChat({
        openKey,
        model: targetModel,
        messages:[
          { role:'system', content: SYSTEM_PROMPT },
          { role:'user', content: JSON.stringify(payload) }
        ],
        timeoutMs:240000,
        temperature:0,
        responseFormat: enforceJson ? { type:'json_object' } : undefined,
        maxCompletionTokens: COMPLETION_TOKEN_CEILING,
        seed: deterministicSeed
      });
      const usage = data?.usage;
      const pricing = getPricing(targetModel);
      let usageInfo = null;
      if(usage){
        usageInfo = {
          model: targetModel,
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
          console.log(`[OpenAI] model=${targetModel} prompt=${usage.prompt_tokens} completion=${usage.completion_tokens} approx_cost=$${totalCost.toFixed(4)}`);
        }else{
          console.log(`[OpenAI] model=${targetModel} prompt=${usage.prompt_tokens} completion=${usage.completion_tokens} (cost unavailable)`);
        }
      }
      const text = data?.choices?.[0]?.message?.content || '{}';
      const cleaned = text.trim().replace(/^```json/i,'').replace(/```$/,'').trim();
      let parsed = tryParseJson(cleaned);
      if(!parsed){
        if(enforceJson){
          console.warn(`[OpenAI] structured response parse failed (model=${targetModel}), retrying without enforced JSON.`);
          return invokeModel({ enforceJson:false, rawAttempt });
        }
        const repaired = rawAttempt ? null : await repairJsonOutput(cleaned);
        if(repaired){
          parsed = repaired;
        }else{
          throw new Error('[OPENAI] invalid JSON output');
        }
      }
      if(!parsed?.action?.rating || parsed?.action?.rating === 'N/A'){
        throw new Error('[OPENAI] output missing key fields');
      }
      if(usageInfo){
        parsed.__usage = usageInfo;
      }
      await setCache(cacheKey, parsed);
      setLlmCache({ hash: payloadHash, ttlMs: ttl, result: parsed });
      return parsed;
    }

    async function repairJsonOutput(rawText){
      if(!rawText || !REPAIR_MODEL || !openKey) return null;
      try{
        const { data } = await callOpenAIChat({
          openKey,
          model: REPAIR_MODEL,
          messages:[
            { role:'system', content:'你是 JSON 修復器。請將輸入內容轉換為合法 JSON，結構需符合 {"per_filing":[],"consensus_view":{},"action":{},"profile":{},"news_insight":{}} 並只輸出 JSON。' },
            { role:'user', content:`請修復以下內容使其成為合法 JSON：\n${rawText}` }
          ],
          timeoutMs:120000,
          temperature:0,
          responseFormat:{ type:'json_object' },
          maxCompletionTokens:600
        });
        const text = data?.choices?.[0]?.message?.content || '';
        return tryParseJson(text.trim());
      }catch(err){
        console.warn('[OpenAI] repair failed', err.message);
        return null;
      }
    }
  }
}
