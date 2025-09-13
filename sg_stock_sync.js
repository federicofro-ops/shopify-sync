// Sync giacenze StoreGest -> Shopify (SKU-based)
// Uso:
//   node sg_stock_sync.js --since=1440 --verbose   (ultime 24h)
//   node sg_stock_sync.js --full --verbose        (tutte le giacenze)
//   node sg_stock_sync.js --since=15 --dry        (prova a secco)
//
// .env richiesto:
//   STOREGEST_DOMAIN=bonaccorsobrand.it
//   STOREGEST_APIKEY=xxxxxxxx
//   SHOPIFY_STORE=seedgroupsrl.myshopify.com
//   SHOPIFY_ADMIN_TOKEN=shpat_xxx
//   SHOPIFY_API_VERSION=2024-07
//   SHOPIFY_LOCATION_ID=106744807804

require('dotenv').config();
const axios = require('axios');

const DRY = process.argv.includes('--dry');
const VERBOSE = process.argv.includes('--verbose');
const FULL = process.argv.includes('--full');
const sinceArg = process.argv.find(a => a.startsWith('--since='));
const sinceMin = sinceArg ? parseInt(sinceArg.split('=')[1], 10) : null;

// ---- Client StoreGest
const SG = axios.create({
  baseURL: 'https://bonaccorsobrand.storegest.it/API/',
  headers: {
    domain: process.env.STOREGEST_DOMAIN,
    apikey: process.env.STOREGEST_APIKEY
  },
  timeout: 180000
});

// ---- Client Shopify
const API_V = process.env.SHOPIFY_API_VERSION || '2024-07';
const SHOP_URL = `https://${process.env.SHOPIFY_STORE || process.env.SHOPIFY_STORE_DOMAIN}/admin/api/${API_V}`;
const SH = axios.create({
  baseURL: SHOP_URL,
  headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
  timeout: 60000
});
const SHQL = axios.create({
  baseURL: `${SHOP_URL}/graphql.json`,
  headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
  timeout: 60000
});

const LOCATION_ID = process.env.SHOPIFY_LOCATION_ID;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function toNumber(x, d=0){ const n = Number(String(x ?? '').replace(',', '.')); return Number.isFinite(n) ? n : d; }
function gidToNum(gid){ return gid?.split('/')?.pop(); }

async function smokeShopify(){
  const { data } = await SH.get('/shop.json');
  if (VERBOSE) console.log('Shopify OK:', data?.shop?.name, '-', data?.shop?.domain);
}

async function sgQta({ time = null } = {}) {
  const body = new URLSearchParams({ act: 'qta' });
  if (time) body.append('time', String(time)); // se non c'è, StoreGest restituisce tutto
  const { data } = await SG.post('', body).catch(e => {
    console.error('❌ SG qta errore:', e?.response?.status, e?.response?.data || e.message);
    return { data: { data: [] } };
  });
  return data?.data || []; // [{SKU, Qta}]
}

async function findVariantBySKU(sku){
  const query = `
    query($q:String!){
      productVariants(first:1, query:$q){
        nodes{ id sku inventoryItem{ id } product{ id title } }
      }
    }`;
  const { data } = await SHQL.post('', { query, variables: { q: `sku:${JSON.stringify(sku)}` } });
  return data?.data?.productVariants?.nodes?.[0] || null;
}
async function getInventoryItemId(variantIdNum){
  const { data } = await SH.get(`/variants/${variantIdNum}.json`);
  return data?.variant?.inventory_item_id;
}
async function setInventory({ inventory_item_id, location_id, available }){
  if (DRY) return { _dry:true };
  const { data } = await SH.post('/inventory_levels/set.json', { location_id, inventory_item_id, available: Number(available) });
  return data;
}

(async () => {
  try{
    if (!LOCATION_ID) throw new Error('SHOPIFY_LOCATION_ID mancante nel .env');
    await smokeShopify();

    // calcolo finestra temporale
    let timeParam = null;
    if (!FULL) {
      const minutes = Number.isFinite(sinceMin) ? sinceMin : 15; // default 15 minuti
      timeParam = Math.floor(Date.now()/1000) - minutes*60;
      console.log(`Aggiorno giacenze (ultimi ${minutes} minuti)...`);
    } else {
      console.log('Aggiorno giacenze (FULL: tutte le SKU)...');
    }

    // carica giacenze da StoreGest
    const rows = await sgQta({ time: timeParam });
    console.log(`Da processare: ${rows.length} SKU`);

    let ok=0, miss=0, err=0;
    for (const r of rows){
      const sku = r.SKU || r.Sku || r.sku;
      const qty = toNumber(r.Qta ?? r.qta ?? r.qty, 0);
      if (!sku) continue;

      try{
        const v = await findVariantBySKU(sku);
        if (!v){
          miss++;
          if (VERBOSE) console.log('! variante non trovata', sku);
          continue;
        }
        const variantIdNum = gidToNum(v.id);
        const inventory_item_id = await getInventoryItemId(variantIdNum);
        if (!inventory_item_id){
          miss++;
          if (VERBOSE) console.log('! inventory_item_id mancante', sku);
          continue;
        }
        await setInventory({ inventory_item_id, location_id:Number(LOCATION_ID), available:qty });
        ok++;
      }catch(e){
        err++;
        console.error('Errore stock', sku, e?.response?.data || e.message);
      }
      await sleep(80); // anti rate-limit
    }

    console.log(`Giacenze → OK: ${ok}, Non trovati: ${miss}, Errori: ${err}${DRY ? ' (DRY RUN)' : ''}`);
  }catch(e){
    console.error('FALLITO:', e.message);
    process.exit(1);
  }
})();
