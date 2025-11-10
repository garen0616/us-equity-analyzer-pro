import dayjs from 'dayjs';
import { getCache, setCache } from './cache.js';
import { getFmpSplits } from './fmp.js';

const SPLIT_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 天

function cacheKey(symbol){
  return `split_ratio_${symbol}`;
}

export async function getSplitRatio({ symbol, baselineDate, fmpKey }){
  if(!symbol || !baselineDate || !fmpKey) return 1;
  const upper = symbol.toUpperCase();
  const key = cacheKey(`${upper}_${baselineDate}`);
  const cached = await getCache(key, SPLIT_CACHE_TTL);
  if(cached) return cached;
  try{
    const rows = await getFmpSplits(upper, fmpKey, 10);
    if(!Array.isArray(rows) || !rows.length){
      await setCache(key, 1);
      return 1;
    }
    const targetDate = dayjs(baselineDate);
    let ratio = 1;
    for(const row of rows){
      const splitDate = row?.date ? dayjs(row.date) : null;
      if(!splitDate || !splitDate.isValid()) continue;
      if(splitDate.isAfter(targetDate)) continue;
      const numerator = Number(row?.numerator || row?.splitRatio?.split('/')[0]);
      const denominator = Number(row?.denominator || row?.splitRatio?.split('/')[1]);
      if(Number.isFinite(numerator) && Number.isFinite(denominator) && denominator!==0){
        ratio *= (denominator / numerator);
      }
    }
    const finalRatio = ratio || 1;
    await setCache(key, finalRatio);
    return finalRatio;
  }catch(err){
    console.warn('[SplitRatio] fetch failed', err.message);
    await setCache(key, 1);
    return 1;
  }
}
