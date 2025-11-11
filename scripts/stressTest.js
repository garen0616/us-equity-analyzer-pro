import fetch from 'node-fetch';
import dayjs from 'dayjs';

const API_BASE = process.env.STRESS_TEST_URL || 'http://localhost:4010';
const TICKERS = ['NVDA','AAPL','TSM','TMDX','ONDS','MSFT','AMZN','META','GOOGL','NFLX'];
const RUNS_PER_TICKER = Number(process.env.STRESS_TEST_RUNS || 10);
const STRESS_TEST_MODE = process.env.STRESS_TEST_MODE || 'metrics-only';

const BASE_CHECKS = [
  { label: 'quote.c', test: data => Number.isFinite(Number(data.fetched?.finnhub_summary?.quote?.c)) },
  { label: 'price_meta.value', test: data => Number.isFinite(Number(data.fetched?.finnhub_summary?.price_meta?.value)) },
  { label: 'news.articles', test: data => Array.isArray(data.news?.articles) && data.news.articles.length > 0 },
  { label: 'momentum.score', test: data => Number.isFinite(Number(data.momentum?.score)) },
  { label: 'institutional.summary', test: data => Boolean(data.institutional?.summary) },
  { label: 'earnings_call.summary', test: data => Boolean(data.earnings_call?.summary) }
];
const LLM_CHECKS = [
  { label: 'analysis.action.rating', test: data => typeof data.analysis?.action?.rating === 'string' && data.analysis.action.rating !== 'N/A' },
  { label: 'analysis.action.target_price', test: data => Number.isFinite(Number(data.analysis?.action?.target_price)) },
  { label: 'consensus.summary', test: data => Boolean(data.analysis?.consensus_view?.summary) }
];
const REQUIRED_CHECKS = STRESS_TEST_MODE === 'full' ? [...BASE_CHECKS, ...LLM_CHECKS] : BASE_CHECKS;

function randomDateWithinThreeYears(){
  const end = dayjs();
  const start = end.subtract(3, 'year');
  const randMs = start.valueOf() + Math.random() * (end.valueOf() - start.valueOf());
  return dayjs(randMs).format('YYYY-MM-DD');
}

async function callApi(path, payload){
  const res = await fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers:{ 'Content-Type':'application/json' },
    body: JSON.stringify(payload)
  });
  if(!res.ok){
    const text = await res.text();
    throw new Error(`${path} HTTP ${res.status} ${text}`);
  }
  return res.json();
}

async function runSingle(ticker){
  const date = randomDateWithinThreeYears();
  await callApi('/api/reset-cache', { ticker, date });
  const result = await callApi('/api/analyze', { ticker, date, mode: STRESS_TEST_MODE });
  const failures = REQUIRED_CHECKS
    .filter(check => !check.test(result))
    .map(check => check.label);
  return { ticker, date, failures };
}

async function main(){
  const issues = [];
  for(const ticker of TICKERS){
    for(let i=0;i<RUNS_PER_TICKER;i++){
      try{
        const { date, failures } = await runSingle(ticker);
        if(failures.length){
          issues.push({ ticker, date, failures });
        }
      }catch(err){
        issues.push({ ticker, error: err.message });
      }
    }
  }
  if(!issues.length){
    console.log('Stress test completed: all checks passed');
    return;
  }
  console.log('Stress test completed with issues:');
  for(const issue of issues){
    console.log(JSON.stringify(issue));
  }
  process.exitCode = 1;
}

main();
