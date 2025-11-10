import fs from 'fs';
import path from 'path';
const CACHE_DIR = path.resolve('cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

export function cachePath(key){ return path.join(CACHE_DIR, encodeURIComponent(key)+'.json'); }

export async function getCache(key, maxAgeMs=24*60*60*1000){
  try{
    const p = cachePath(key);
    if(!fs.existsSync(p)) return null;
    const stat = fs.statSync(p);
    if(Date.now() - stat.mtimeMs > maxAgeMs) return null;
    const raw = fs.readFileSync(p,'utf8');
    return JSON.parse(raw);
  }catch{ return null; }
}

export async function setCache(key, data){
  try{
    const p = cachePath(key);
    fs.writeFileSync(p, JSON.stringify(data));
  }catch{}
}

function listCacheFiles(){
  try{
    return fs.readdirSync(CACHE_DIR);
  }catch{
    return [];
  }
}

export function clearCacheEntries(predicate){
  if(typeof predicate !== 'function') return 0;
  const files = listCacheFiles();
  let cleared = 0;
  for(const file of files){
    if(!file.endsWith('.json')) continue;
    const decoded = decodeURIComponent(file.slice(0,-5));
    try{
      if(predicate(decoded)){
        fs.unlinkSync(path.join(CACHE_DIR, file));
        cleared++;
      }
    }catch{}
  }
  return cleared;
}

export function clearCacheForTicker({ ticker, baselineDate, includeAllDates=false }){
  if(!ticker) return 0;
  const upper = ticker.toUpperCase();
  const dateStr = includeAllDates ? '' : (baselineDate || '');
  return clearCacheEntries((decoded)=>{
    if(!decoded.toUpperCase().includes(upper)) return false;
    if(dateStr && !decoded.includes(dateStr)) return false;
    return true;
  });
}
