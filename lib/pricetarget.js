import { getFmpPriceTarget } from './fmp.js';
import { getYahooPriceTarget } from './yahoo.js';
import { getSplitRatio } from './splits.js';

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

function adjustTargetsForSplit(targets, ratio){
  if(!targets || !Number.isFinite(ratio) || ratio === 1) return targets;
  const adjust = (value)=> value==null? null : round2(value * ratio);
  return {
    ...targets,
    targetHigh: adjust(targets.targetHigh),
    targetLow: adjust(targets.targetLow),
    targetMean: adjust(targets.targetMean),
    targetMedian: adjust(targets.targetMedian)
  };
}

export async function getAggregatedPriceTarget(symbol, finnhubKey, alphaKey, current, fmpKey, baselineDate){
  const errors=[];
  let fmpTargets=null;
  if(fmpKey){
    try{
      fmpTargets = await getFmpPriceTarget(symbol,fmpKey);
    }catch(e){ errors.push(`[FMP] ${e.message}`); }
  }
  let yahooTargets=null;
  try{
    yahooTargets = await getYahooPriceTarget(symbol);
  }catch(e){ errors.push(`[Yahoo] ${e.message}`); }

  if(fmpTargets){
    const ratio = await getSplitRatio({ symbol, baselineDate, fmpKey });
    fmpTargets = adjustTargetsForSplit(fmpTargets, ratio);
  }

  if(fmpTargets && yahooTargets){
    const fmpMean = toNum(fmpTargets.targetMean);
    const yahooMean = toNum(yahooTargets.targetMean);
    if(fmpMean && yahooMean){
      const diffPct = Math.abs(fmpMean - yahooMean) / Math.max(1, ((fmpMean + yahooMean)/2));
      if(diffPct > 0.4){
        return fmpMean > yahooMean ? normalizeTargets(yahooTargets, current) : normalizeTargets(fmpTargets, current);
      }
      const blended = {
        source: 'fmp+yahoo',
        targetHigh: (toNum(fmpTargets.targetHigh) + toNum(yahooTargets.targetHigh)) / 2,
        targetLow: (toNum(fmpTargets.targetLow) + toNum(yahooTargets.targetLow)) / 2,
        targetMean: (fmpMean + yahooMean) / 2,
        targetMedian: (toNum(fmpTargets.targetMedian) + toNum(yahooTargets.targetMedian)) / 2
      };
      return normalizeTargets(blended, current);
    }
  }

  if(fmpTargets) return normalizeTargets(fmpTargets, current);
  if(yahooTargets) return normalizeTargets(yahooTargets, current);
  return {
    source:'unavailable',
    error: errors.join(' | ') || 'No analyst price target source succeeded',
    targetHigh:null,
    targetLow:null,
    targetMean:null,
    targetMedian:null
  };
}
