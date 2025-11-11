import axios from 'axios';

const FMP_BASE_V3 = 'https://financialmodelingprep.com/api/v3';
const FMP_BASE_V4 = 'https://financialmodelingprep.com/api/v4';

function ensureKey(key){
  if(!key) throw new Error('[FMP] Missing API key');
}

export async function getFmpQuote(symbol, key){
  ensureKey(key);
  const url = `${FMP_BASE_V3}/quote/${encodeURIComponent(symbol)}`;
  const { data } = await axios.get(url,{ params:{ apikey:key }, timeout:15000 });
  if(Array.isArray(data) && data.length){
    const row = data[0];
    const price = Number(row.price ?? row.c ?? row.currentPrice);
    if(Number.isFinite(price)){
      return {
        price,
        asOf: row.timestamp ? new Date(row.timestamp * 1000).toISOString().slice(0,10) : undefined,
        raw: row
      };
    }
  }
  throw new Error('[FMP] quote empty');
}
