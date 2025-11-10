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
  return MODEL_PRICING[model] || MODEL_PRICING['gpt-4o-mini'];
}

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
        { role:'system', content:[
          '你是專業金融分析師與審核者。',
          '請根據「SEC 10-Q/10-K MD&A 摘要」與「分析師資料」，輸出有效 JSON：',
          '所有文字欄位（包含 explanation、valuation_rationale、risk_factors、key_conflicts、catalyst_timeline、consensus_view.summary、action.rationale）必須以繁體中文撰寫。',
          '若 payload.news 存在，須整合新聞摘要與情緒，說明其對估值、動能與風險的影響。',
          'payload.momentum 提供動能評分、趨勢、技術指標與對應 ETF 變化，請納入估值與風險評估。',
          '請同步完成「大型股 vs 小型股」體質檢核：',
          '  - 大型股硬性過濾（至少滿足 6/8）：市值≥$10B、3m ADV≥$50M、近四季 GAAP 淨利為正且近兩季毛利/營益率擴張、近 6-12M EPS 一致預期上修、FCF 正且回購殖利率≥1-2%並淨縮股本、自由流通股≥50%且機構持股上升、核心產能/資產稀缺、無重大監管/財務疑慮。',
          '  - 大型股打分（100 分，≥75 才納入，≥85 主推）：結構性主題與供需稀缺20、EPS/營收上修+利潤率擴張20、價格動能品質10、FCF與資本配置10、估值 vs 成長10、被動/指數動能10、護城河/產能10、風險控制10。',
          '  - 小型股硬性過濾（至少滿足 6/8）：股價≥$3、市值≥$300M、自由流通市值≥$150M、3m ADV≥$5M、TTM 營收≥$100M或YoY≥30%、毛利率≥30%（硬體20%）、TTM OCF 轉正或虧損收斂≥50%、現金 runway≥18m、近12M 淨發股率≤8%、負債到期牆可控無重大違約風險。',
          '  - 小型股打分（100 分，≥75 才操作）：生存力/財務安全20、成長與單位經濟20、可驗證催化15、機構與內部人支持15、價格動能10、估值 vs 成長10、治理與披露10。',
          '{',
          '"per_filing":[{',
          ' "form":"10-Q/10-K","filingDate":"YYYY-MM-DD","reportDate?":"YYYY-MM-DD",',
          ' "five_indicators":{',
          '   "alignment_score": number,',
          '   "key_conflicts": [string],',
          '   "valuation_rationale": string,',
          '   "risk_factors": [string],',
          '   "catalyst_timeline": [{"event":string,"window":string,"why":string}]',
          ' },',
          ' "explanation": "300-500字詳解"',
          '}]',
          '"consensus_view":{"summary":string,"agreement_ratio":number},',
          '"action":{"rating":"BUY|HOLD|SELL","target_price":number,"stop_loss":number,"rationale":string},',
          '"profile":{',
          '  "segment":"large_cap|small_cap",',
          '  "segment_label":"大型股或小型股",',
          '  "summary": "150 字內描述體質重點",',
          '  "filters":{"total":8,"met":number,"items":[{"name":string,"met":boolean,"reason":string}]},',
          '  "score":number,',
          '  "score_detail":[{"category":string,"points":number,"reason":string}],',
          '  "catalysts":[string]',
          ' },',
          ' "news_insight":{',
          '  "summary":string,',
          '  "impact":"正面|中性|負面",',
          '  "key_events":[{"title":string,"why":string}]',
          '}',
          '}'
        ].join('\n') },
        { role:'user', content: JSON.stringify(payload) }
      ],
      temperature:0.2
    },{
      headers:{ 'Authorization':`Bearer ${openKey}`, 'Content-Type':'application/json' },
      timeout:120000
    });
    const usage = data?.usage;
    const pricing = getPricing(model);
    let usageInfo = null;
    if(usage && pricing){
      const inputCost = usage.prompt_tokens * pricing.input;
      const outputCost = usage.completion_tokens * pricing.output;
      const totalCost = inputCost + outputCost;
      usageInfo = {
        model,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens ?? (usage.prompt_tokens + usage.completion_tokens),
        input_cost: Number(inputCost.toFixed(6)),
        output_cost: Number(outputCost.toFixed(6)),
        total_cost: Number(totalCost.toFixed(6))
      };
      console.log(`[OpenAI] model=${model} prompt=${usage.prompt_tokens} completion=${usage.completion_tokens} approx_cost=$${totalCost.toFixed(4)}`);
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
