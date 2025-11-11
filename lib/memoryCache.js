const store = new Map();

function cleanup(key){
  const entry = store.get(key);
  if(!entry) return null;
  if(entry.expireAt && Date.now() > entry.expireAt){
    store.delete(key);
    return null;
  }
  return entry.value;
}

export function getMemoryCache(key){
  return cleanup(key);
}

export function setMemoryCache(key, value, ttlMs){
  const expireAt = ttlMs ? Date.now() + ttlMs : null;
  store.set(key, { value, expireAt });
  return value;
}

export async function memoize(key, ttlMs, fetcher){
  const cached = getMemoryCache(key);
  if(cached !== null && cached !== undefined) return cached;
  const fresh = await fetcher();
  return setMemoryCache(key, fresh, ttlMs);
}
