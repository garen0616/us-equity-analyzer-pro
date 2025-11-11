import axios from 'axios';
import dayjs from 'dayjs';
import { getCache, setCache } from './cache.js';

const SUBMISSIONS = (cik)=>`https://data.sec.gov/submissions/CIK${cik}.json`;
const INDEX_URL = 'https://www.sec.gov/files/company_tickers.json';
const RETRY_ATTEMPTS = Number(process.env.SEC_RETRY_ATTEMPTS || 3);
const RETRY_DELAY_MS = Number(process.env.SEC_RETRY_DELAY_MS || 1500);
const SUPPORTED_FORMS = ['10-Q','10-K','20-F','6-K'];
const FORM_LABEL = {
  '10-Q':'Form 10-Q（美國季報）',
  '10-K':'Form 10-K（美國年報）',
  '20-F':'Form 20-F（外國發行人年報）',
  '6-K':'Form 6-K（外國發行人臨時報告）'
};

function sleep(ms){ return new Promise(resolve=>setTimeout(resolve, ms)); }

function buildTickerMap(data){
  const map = {};
  Object.values(data || {}).forEach(row=>{
    const t = row?.ticker?.toUpperCase();
    if(t) map[t] = row;
  });
  return map;
}

async function fetchWithRetry(url, options, attempts=RETRY_ATTEMPTS){
  let lastErr;
  for(let attempt=1; attempt<=attempts; attempt++){
    try{
      return await axios.get(url, options);
    }catch(err){
      lastErr = err;
      if(attempt === attempts) break;
      await sleep(RETRY_DELAY_MS * attempt);
    }
  }
  throw lastErr;
}

export async function getCIK(ticker, userAgent, apiKey){
  const t = ticker.toUpperCase().trim();
  const key = `sec_index_all`;
  let idx = await getCache(key);
  let shouldPersist = false;
  if(!idx){
    try{
      const {data} = await fetchWithRetry(INDEX_URL,{
        headers:{ 'User-Agent': userAgent, 'Authorization': apiKey?`Bearer ${apiKey}`:undefined },
        timeout:15000
      });
      idx = data;
      shouldPersist = true;
    }catch(err){ throw new Error(`[SEC] getCIK index failed: ${err.message}`); }
  }
  if(!idx.__byTicker){
    idx.__byTicker = buildTickerMap(idx);
    shouldPersist = true;
  }
  if(shouldPersist){
    await setCache(key, idx);
  }
  const row = idx.__byTicker?.[t];
  if(!row) throw new Error('[SEC] Ticker not found in SEC index');
  return String(row.cik_str).padStart(10,'0');
}

export async function getRecentFilings(cik, baselineDate, userAgent, apiKey){
  const url = SUBMISSIONS(cik);
  const cacheKey = `sec_submissions_${cik}`;
  let data = await getCache(cacheKey);
  if(!data){
    try{
      const resp = await fetchWithRetry(url,{
        headers:{ 'User-Agent': userAgent, 'Authorization': apiKey?`Bearer ${apiKey}`:undefined },
        timeout:20000
      });
      data = resp.data; await setCache(cacheKey, data);
    }catch(err){ throw new Error(`[SEC] submissions failed: ${err.message}`); }
  }
  const forms = data?.filings?.recent;
  if(!forms) throw new Error('[SEC] No recent filings');
  const rows = forms.form.map((f,i)=>({
    form: f,
    reportDate: forms.reportDate[i],
    filingDate: forms.filingDate[i],
    accession: forms.accessionNumber[i],
    primary: forms.primaryDocument[i]
  })).filter(r=> SUPPORTED_FORMS.includes(r.form));
  const base = dayjs(baselineDate);
  const filtered = rows
    .filter(r=> dayjs(r.filingDate).isBefore(base.add(1,'day')))
    .sort((a,b)=> dayjs(b.filingDate)-dayjs(a.filingDate))
    .slice(0,4);
  const withLinks = filtered.map(r=>{
    const accNo = r.accession.replace(/-/g,'');
    const file = `https://www.sec.gov/Archives/edgar/data/${parseInt(cik,10)}/${accNo}/${r.primary}`;
    return {...r, url:file, formLabel: FORM_LABEL[r.form] || r.form};
  });
  if(!withLinks.length) throw new Error('[SEC] No supported filings (10-Q/10-K/20-F/6-K) found before baseline');
  return withLinks;
}
