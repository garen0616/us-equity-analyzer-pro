import { getFmpPriceTarget } from './fmp.js';
import { getYahooPriceTarget } from './yahoo.js';

function toNum(x){
  if(x==null || x==='') return null;
  const n = Number(x);
  return Number.isFinite(n)? n : null;
}
function round2(x){ return x==null? null : Math.round(x*100)/100; }

function normalizeTargets(obj={}, current=null){
  let mean = toNum(obj.targetMean ?? obj.targetMedian);
  let hi   = toNum(obj.targetHigh);
  let lo   = toNum(obj.targetLow);

  // 補齊邏輯
  if(mean!=null){
    if(hi==null && lo==null){
      hi = mean * 1.15;
      lo = mean * 0.85;
    } else if(hi!=null && lo==null){
      lo = mean ? Math.min(mean * 0.9, hi / 1.1) : hi / 1.1;
    } else if(lo!=null && hi==null){
      hi = mean ? Math.max(mean * 1.1, lo * 1.1) : lo * 1.1;
    }
  }
  if(hi!=null && lo==null) lo = hi / 1.2;
  if(lo!=null && hi==null) hi = lo * 1.2;

  const cur = toNum(current);
  if(cur!=null){
    if(hi!=null && cur>hi) hi = cur * 1.05;
    if(lo!=null && cur<lo) lo = cur * 0.95;
  }

  return {
    source: obj.source || 'aggregated',
    targetHigh: round2(hi),
    targetLow:  round2(lo),
    targetMean: round2(mean),
    targetMedian: round2(toNum(obj.targetMedian))
  };
}

export async function getAggregatedPriceTarget(symbol, finnhubKey, alphaKey, current, fmpKey){
  const errors=[];
  if(fmpKey){
    try{
      return normalizeTargets(await getFmpPriceTarget(symbol,fmpKey), current);
    }catch(e){
      errors.push(e.message);
    }
  }
  try{
    return normalizeTargets(await getYahooPriceTarget(symbol), current);
  }catch(e){
    errors.push(e.message);
  }
  return {
    source:'unavailable',
    error: errors.join(' | ') || 'No analyst price target source succeeded',
    targetHigh:null,
    targetLow:null,
    targetMean:null,
    targetMedian:null
  };
}
