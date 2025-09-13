// Import Google Merchant XML -> Shopify con varianti (SKU-based) + sync giacenze StoreGest
// USO:
//   node gm_xml_import.js import <feed.xml|https://...> [--dry] [--verbose]
//         [--group=auto|item_group_id|mpn|parent|titlebrand|idprefix|regex]
//         [--idsep=- --idparts=2]         // per group=idprefix
//         [--idregex="^(.+?)-[A-Z]+$"]    // per group=regex (1° gruppo catturato = padre)
//         [--map=map.json]                // mappatura personalizzata
//   node gm_xml_import.js stock [--verbose]
//
// .env richiesto:
//   SHOPIFY_STORE=seedgroupsrl.myshopify.com
//   SHOPIFY_ADMIN_TOKEN=shpat_xxx
//   SHOPIFY_API_VERSION=2024-07
//   SHOPIFY_LOCATION_ID=106744807804
//   STOREGEST_DOMAIN=bonaccorsobrand.it
//   STOREGEST_APIKEY=xxxxxx

require('dotenv').config();
const fs = require('fs');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

const DRY = process.argv.includes('--dry');
const VERBOSE = process.argv.includes('--verbose');

// ---------- Raggruppamento (scegli come aggregare le varianti)
const groupArg   = (process.argv.find(a => a.startsWith('--group=')) || '').split('=')[1] || 'auto';
const idsepArg   = (process.argv.find(a => a.startsWith('--idsep=')) || '').split('=')[1] || '-';
const idpartsArg = parseInt((process.argv.find(a => a.startsWith('--idparts=')) || '').split('=')[1] || '2', 10);
const idregexArg = (process.argv.find(a => a.startsWith('--idregex=')) || '').split('=')[1] || '';

// ---- mapping personalizzato da file
const MAP_FILE = (process.argv.find(a => a.startsWith('--map=')) || '').split('=')[1] || null;
let MAP = {};
if (MAP_FILE) {
  MAP = JSON.parse(fs.readFileSync(MAP_FILE, 'utf8'));
  if (VERBOSE) console.log('Mapping file:', MAP_FILE);
}

// ---------- Shopify clients
const API_V = process.env.SHOPIFY_API_VERSION || '2024-07';
const SHOP_DOMAIN = process.env.SHOPIFY_STORE || process.env.SHOPIFY_STORE_DOMAIN;
const SHOP_URL = `https://${SHOP_DOMAIN}/admin/api/${API_V}`;
const SH = axios.create({
  baseURL: SHOP_URL,
  headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
  timeout: 60000,
});
const SHQL = axios.create({
  baseURL: `${SHOP_URL}/graphql.json`,
  headers: { 'X-Shopify-Access-Token': process.env.SHOPIFY_ADMIN_TOKEN, 'Content-Type': 'application/json' },
  timeout: 60000,
});

// ---------- StoreGest per sync stock (HOST FISSO corretto)
const SG = axios.create({
  baseURL: 'https://bonaccorsobrand.storegest.it/API/',
  headers: { domain: process.env.STOREGEST_DOMAIN, apikey: process.env.STOREGEST_APIKEY },
  timeout: 180000,
});
const LOCATION_ID = process.env.SHOPIFY_LOCATION_ID;

// ---------- Utils
const sleep = ms => new Promise(r => setTimeout(r, ms));
const pick  = (...vals) => vals.find(v => v !== undefined && v !== null && String(v).trim() !== '');
const toNumber = (x, d=0) => {
  const s = String(x ?? '').replace(',', '.').replace(/[^\d. -]/g,'');
  const n = Number(s);
  return Number.isFinite(n) ? n : d;
};
const gidToNum = gid => gid?.split('/')?.pop();
function slugify(str){
  return String(str||'')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/[^a-z0-9]+/g,'-')
    .replace(/(^-|-$)/g,'');
}
function normalizeSku(s){
  return String(s ?? '').trim().replace(/\s+/g,'').toUpperCase();
}

// ---------- Shopify helpers
async function findVariantBySKU(sku){
  const query = `query($q:String!){ productVariants(first:1, query:$q){ nodes{ id sku inventoryItem{ id } product{ id title } } } }`;
  const { data } = await SHQL.post('', { query, variables: { q: `sku:${JSON.stringify(sku)}` } });
  return data?.data?.productVariants?.nodes?.[0] || null;
}
// (fallback per SKU senza virgolette)
async function findVariantBySKU_loose(sku){
  const query = `query($q:String!){ productVariants(first:1, query:$q){ nodes{ id sku product{ id title } } } }`;
  const { data } = await SHQL.post('', { query, variables: { q: `sku:${sku}` } });
  return data?.data?.productVariants?.nodes?.[0] || null;
}
// Cerca prodotto per tag GMGroup:<groupId> (idempotenza)
async function findProductByGroupTag(groupId){
  const tag = `GMGroup:${groupId}`;
  const query = `query($q:String!){ products(first:1, query:$q){ nodes{ id title handle } } }`;
  const { data } = await SHQL.post('', { query, variables: { q: `tag:${JSON.stringify(tag)}` } });
  return data?.data?.products?.nodes?.[0] || null;
}
async function createProduct(product){ if (DRY) return { id:'0', title:product.title, _dry:true }; const { data } = await SH.post('/products.json',{product}); return data.product; }
async function updateProduct(id, patch){ if (DRY) return { id, _dry:true }; const { data } = await SH.put(`/products/${id}.json`,{ product:{ id, ...patch }}); return data.product; }
async function getProduct(id){ const { data } = await SH.get(`/products/${id}.json`); return data.product; }
async function createVariant(pid, variant){ if (DRY) return { id:'0', _dry:true }; const { data } = await SH.post('/variants.json', { variant: { ...variant, product_id: pid } }); return data.variant; }
async function updateVariant(id, patch){ if (DRY) return { id, _dry:true }; const { data } = await SH.put(`/variants/${id}.json`, { variant: { id, ...patch } }); return data.variant; }
async function getInventoryItemId(variantIdNum){ if (DRY) return '0'; const { data } = await SH.get(`/variants/${variantIdNum}.json`); return data?.variant?.inventory_item_id; }
async function setInventory({ inventory_item_id, location_id, available }){ if (DRY) return { _dry:true }; const { data } = await SH.post('/inventory_levels/set.json', { location_id, inventory_item_id, available:Number(available) }); return data; }

// ---------- XML parsing (Google Merchant)
const parser = new XMLParser({ ignoreAttributes:false, attributeNamePrefix:'@_', preserveOrder:false });
function g(it, key){ return it[`g:${key}`] ?? it[key] ?? it[`item_${key}`]; }  // robusto ai namespace
function arrayify(x){ return Array.isArray(x) ? x : (x === undefined || x === null ? [] : [x]); }
function splitImages(x){
  if (!x) return [];
  if (Array.isArray(x)) return x.filter(Boolean);
  const s = String(x);
  if (/^https?:\/\//i.test(s)) return [s];
  return s.split(/[|;,]+|\s+(?=https?:\/\/)/g).map(s=>s.trim()).filter(u=>/^https?:\/\//i.test(u));
}

// ---------- Raggruppamento (varianti -> prodotto)
function normalize(s){ return String(s||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^\w]+/g,' ').trim(); }
function baseFromId(id){
  const sid = String(id||'');
  if (idregexArg){
    try{
      const re = new RegExp(idregexArg);
      const m = sid.match(re);
      if (m && m[1]) return m[1];
    }catch(e){ /* regex non valida, ignora */ }
  }
  if (idpartsArg > 0 && idsepArg){
    const parts = sid.split(idsepArg);
    if (parts.length >= idpartsArg) return parts.slice(0, idpartsArg).join(idsepArg);
  }
  return sid;
}
function computeGroupKey(it){
  // priorità: mapping esplicito
  const mapped = mget(it, 'group', []);
  if (mapped) return mapped;

  const id    = g(it,'id') || '';
  const brand = g(it,'brand') || '';
  const title = g(it,'title') || '';
  const igid  = g(it,'item_group_id');
  const parent= g(it,'parent_sku') || g(it,'parent') || g(it,'item_group') || null;
  const mpn   = g(it,'mpn');

  switch (groupArg){
    case 'item_group_id': return igid || null;
    case 'mpn':          return mpn  || null;
    case 'parent':       return parent|| null;
    case 'titlebrand':   return `${normalize(title)}|${normalize(brand)}`;
    case 'idprefix':     return baseFromId(id);
    case 'regex':        return baseFromId(id);
    case 'auto':
    default:
      return igid || parent || mpn || baseFromId(id);
  }
}
function groupItems(items){
  const map = new Map();
  for (const it of items){
    const key = computeGroupKey(it) || g(it,'id'); // ultima spiaggia
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(it);
  }
  return [...map.entries()].map(([groupId, variants]) => ({ groupId, variants }));
}
function parsePrice(pstr){
  if (!pstr) return { value:null, currency:null };
  const s = String(pstr).replace(',', '.').trim();
  const m1 = s.match(/^([A-Z]{3})\s*([\d.]+)/i);
  const m2 = s.match(/^([\d.]+)\s*([A-Z]{3})/i);
  const value = m1 ? Number(m1[2]) : m2 ? Number(m2[1]) : toNumber(s, null);
  const currency = m1 ? m1[1] : m2 ? m2[2] : null;
  return { value, currency };
}

// ---- mapping helpers
function getByKey(it, k){
  if (!k) return null;
  if (k.startsWith('g:')) return g(it, k.slice(2));
  return it[k] ?? it[k?.toLowerCase?.()] ?? it[k?.toUpperCase?.()] ?? null;
}
function mget(it, fieldName, defaultKeys=[]){
  const keys = (MAP[fieldName] || defaultKeys);
  for (const k of keys){
    const v = getByKey(it, k);
    if (v !== undefined && v !== null && String(v).trim() !== '') return v;
  }
  return null;
}
function applyTagTemplates(templates, values){
  return (templates || []).map(t => t.replace(/\$\{(\w+)\}/g, (_,k)=> values[k] ?? '')).filter(Boolean);
}

// ---------- Mapping GM group -> Shopify product
function mapGMGroupToShopify({ groupId, variants }){
  const first = variants[0] || {};

  const title = mget(first, 'title', ['g:title']) || `${mget(first,'brand',['g:brand'])||''} ${mget(first,'mpn',['g:mpn'])||groupId}`.trim();
  const description = mget(first, 'description', ['g:description']) || '';
  const vendor   = mget(first, 'brand', ['g:brand']) || '';
const category = mget(first, 'category',  ['g:google_product_category']) || '';
const category2= mget(first, 'category2',[]) || '';
const category3= mget(first, 'category3',[]) || '';
const type     = mget(first, 'type', ['g:product_highlight','g:product_type']) || '';

// Product type = Tipologia, fallback categoria
const product_type = type || category;

// Tags: SOLO categorie (niente Brand/Tipologia/GoogleCat/GMGroup)
const tags = [category, category2, category3]
  .filter(Boolean)
  .map(c => `Categoria:${c}`)
  .join(', ');

  const mainImg = mget(first, 'image', ['g:image_link']);
  const extraImgsRaw = mget(first, 'additional_images', ['g:additional_image_link']);
  const extraImgs = arrayify(extraImgsRaw);
  const images = [...splitImages(mainImg), ...extraImgs.flatMap(splitImages)]
    .filter(Boolean).map(src => ({ src }));

  const hasColor = variants.some(v => mget(v,'color',['g:color']));
  const hasSize  = variants.some(v => mget(v,'size',['g:size']));
  const options = [];
  if (hasColor) options.push({ name: 'Colore' });
  if (hasSize)  options.push({ name: 'Taglia'  });

  const shopifyVariants = variants.map(v => {
    const sku = normalizeSku(mget(v, 'sku', ['g:mpn','g:id']));
    const { value:priceVal } = parsePrice(mget(v,'price',['g:sale_price','g:price']));
    const { value:cmpVal }   = parsePrice(mget(v,'compare_at_price',['g:price']));
    const price = String(priceVal ?? 0);
    const compare_at_price = (cmpVal && priceVal && priceVal < cmpVal) ? String(cmpVal) : (cmpVal && !priceVal ? String(cmpVal) : undefined);
    const barcode = mget(v,'barcode',['g:gtin','g:mpn']) || undefined;

    return {
      sku,
      price,
      compare_at_price,
      barcode,
      option1: mget(v,'color',['g:color']) || (hasColor ? 'Default' : undefined),
      option2: mget(v,'size',['g:size'])   || (hasSize  ? 'Default' : undefined),
      inventory_management: 'shopify',
      inventory_policy: 'deny',
    };
  }).filter(v => v.sku);

  const tagValues = { brand: vendor, category, type };
  const mappedTags = applyTagTemplates(MAP.tags, tagValues);
  const tags = [
    vendor && `Brand:${vendor}`,
    product_type && `GoogleCat:${product_type}`,
    `GMGroup:${groupId}`,       // idempotenza
    ...mappedTags
  ].filter(Boolean).join(', ');

  const handle = `gm-${slugify(String(groupId))}`;

  return {
    title,
    body_html: description,
    vendor,
    product_type,
    images,
    options: options.length?options:undefined,
    variants: shopifyVariants,
    tags,
    handle,
    status: 'active'
  };
}

// ---------- UPSERT (SKU-based con idempotenza handle/tag)
async function upsertProductFromGroup(group){
  const mapped = mapGMGroupToShopify(group);
  const skus = mapped.variants.map(v => v.sku).filter(Boolean);
  if (VERBOSE) console.log(`→ ${mapped.title} | group=${group.groupId} | SKU: ${skus.slice(0,6).join(', ')}${skus.length>6?'…':''}`);
  if (!skus.length) return { created:false, updated:0, productId:null };

  // 1) prova a trovare una variante esistente per SKU (precisa, poi "loose")
  let existingVariant = null;
  for (const s of skus){
    existingVariant = await findVariantBySKU(s);
    if (!existingVariant) existingVariant = await findVariantBySKU_loose(s);
    if (existingVariant) break;
    await sleep(60);
  }

  // 2) se non trovi nulla, cerca il prodotto per TAG GMGroup:<groupId>
  let productIdNum = null;
  if (!existingVariant){
    const byTag = await findProductByGroupTag(group.groupId);
    if (byTag) productIdNum = gidToNum(byTag.id);
  } else {
    productIdNum = gidToNum(existingVariant.product.id);
  }

  // 3) CREA o AGGIORNA
  if (!productIdNum){
    if (VERBOSE) console.log('   nuovo prodotto → createProduct() (handle & tag per idempotenza)');
    if (DRY) return { created:true, updated:0, productId:'0' };
    const created = await createProduct({
      title: mapped.title,
      body_html: mapped.body_html,
      vendor: mapped.vendor,
      product_type: mapped.product_type,
      images: mapped.images,
      options: mapped.options,
      variants: mapped.variants,
      tags: mapped.tags,
      handle: mapped.handle,
      status: mapped.status
    });
    return { created:true, updated:0, productId: created.id };
  }

  if (VERBOSE) console.log(`   presente → productId ${productIdNum}`);
  const product = await getProduct(productIdNum);

  // merge tags esistenti + nuovi (mantieni GMGroup)
  let newTags = product.tags || '';
  if (mapped.tags) {
    const set = new Set(
      (newTags ? newTags.split(',') : []).map(t=>t.trim()).filter(Boolean)
      .concat(mapped.tags.split(',').map(t=>t.trim()).filter(Boolean))
    );
    newTags = [...set].join(', ');
  }

  await updateProduct(productIdNum, {
    title: mapped.title || product.title,
    body_html: mapped.body_html ?? product.body_html,
    vendor: mapped.vendor || product.vendor,
    product_type: mapped.product_type || product.product_type,
    tags: newTags
  });

  // varianti: crea mancanti / aggiorna esistenti
  const existingBySKU = new Map((product.variants || []).map(v => [normalizeSku(v.sku), v]));
  let createdCount = 0, updatedCount = 0;

  for (const v of mapped.variants){
    const ex = existingBySKU.get(normalizeSku(v.sku));
    if (!ex){
      if (VERBOSE) console.log(`   + crea variante SKU ${v.sku}`);
      await createVariant(productIdNum, v);
      createdCount++;
    } else {
      const patch = {};
      if (String(ex.price) !== String(v.price)) patch.price = v.price;
      if (String(ex.compare_at_price||'') !== String(v.compare_at_price||'')) patch.compare_at_price = v.compare_at_price || null;
      if (ex.inventory_management !== 'shopify') patch.inventory_management = 'shopify';
      if (ex.sku !== v.sku) patch.sku = v.sku; // riallinea SKU se diverso

      if (Object.keys(patch).length){
        if (VERBOSE) console.log(`   ~ aggiorna variante SKU ${v.sku}`, patch);
        await updateVariant(ex.id, patch);
        updatedCount++;
      }
    }
    await sleep(120);
  }
  return { created:false, updated:(createdCount + updatedCount), productId: productIdNum };
}

// ---------- STOCK SYNC (StoreGest → Shopify per SKU)
async function sgQta({ time = null } = {}) {
  const body = new URLSearchParams({ act: 'qta' });
  if (time) body.append('time', String(time));
  const { data } = await SG.post('', body).catch(e => {
    console.error('❌ SG qta errore:', e?.response?.status, e?.response?.data || e.message);
    return { data: { data: [] } };
  });
  return data?.data || [];
}
async function syncStocks({ sinceSecondsAgo = 15*60 } = {}){
  if (!LOCATION_ID){ console.error('❌ SHOPIFY_LOCATION_ID mancante nel .env'); process.exit(1); }
  const time = Math.floor(Date.now()/1000) - sinceSecondsAgo;
  console.log(`Aggiorno giacenze (ultimi ${sinceSecondsAgo/60} min)...`);
  const rows = await sgQta({ time });
  console.log(`Da processare: ${rows.length} SKU`);

  let ok=0, miss=0, err=0;
  for (const r of rows){
    const sku = normalizeSku(pick(r.SKU, r.Sku, r.sku));
    const qty = toNumber(pick(r.Qta, r.qta, r.qty), 0);
    if (!sku) continue;
    try{
      let v = await findVariantBySKU(sku);
      if (!v) v = await findVariantBySKU_loose(sku);
      if (!v){ miss++; if (VERBOSE) console.log('! variante non trovata', sku); continue; }
      const variantIdNum = gidToNum(v.id);
      const inventory_item_id = await getInventoryItemId(variantIdNum);
      if (!inventory_item_id){ miss++; continue; }
      await setInventory({ inventory_item_id, location_id:Number(LOCATION_ID), available:qty });
      ok++;
    }catch(e){
      err++; console.error('Errore stock', sku, e?.response?.data || e.message);
    }
    await sleep(80);
  }
  console.log(`Giacenze → OK:${ok}, Non trovati:${miss}, Errori:${err}${DRY?' (DRY RUN)':''}`);
}

// ---------- MAIN
async function importFromXML(feedPath){
  if (!SHOP_DOMAIN || !process.env.SHOPIFY_ADMIN_TOKEN){
    console.error('❌ Variabili mancanti: SHOPIFY_STORE (o *_DOMAIN) o SHOPIFY_ADMIN_TOKEN nel .env'); process.exit(1);
  }

  // Legge sia file locale che URL
  let xml;
  if (/^https?:\/\//i.test(feedPath)) {
    const resp = await axios.get(feedPath, { timeout: 120000 });
    xml = resp.data;
  } else {
    xml = fs.readFileSync(feedPath, 'utf8');
  }
  const obj = parser.parse(xml);

  const channel = obj?.rss?.channel || obj?.channel || {};
  const items = arrayify(channel.item || obj?.rss?.channel?.item || obj?.item || []);
  if (!items.length){ console.error('❌ Nessun <item> trovato nel feed'); process.exit(1); }

  const groups = groupItems(items);
  console.log(`Feed: ${items.length} item → ${groups.length} gruppi (group="${groupArg}")`);

  let created=0, touched=0, errors=0;
  for (const g of groups){
    try{
      const res = await upsertProductFromGroup(g);
      if (res.created) created++; else touched++;
    }catch(e){
      errors++;
      console.error('Errore import gruppo', g.groupId, e?.response?.data || e.message);
    }
    await sleep(150);
  }
  console.log(`Fatto. Creati: ${created}, Aggiornati/Completati: ${touched}, Errori: ${errors}${DRY?' (DRY RUN)':''}`);
}

(async () => {
  const cmd = process.argv[2];
  if (cmd === 'import'){
    const feed = process.argv[3];
    if (!feed){ console.error('Uso: node gm_xml_import.js import <feed.xml|url> [--dry] [--verbose] [--group=...] [--idsep=- --idparts=2] [--idregex="^(.+?)-[A-Z]+$"] [--map=map.json]'); process.exit(1); }
    await importFromXML(feed);
  } else if (cmd === 'stock'){
    await syncStocks({});
  } else {
    console.log('Usa: node gm_xml_import.js import <feed.xml|url> [--dry] [--verbose] [--group=...] [--idsep=- --idparts=2] [--idregex="^(.+?)-[A-Z]+$"] [--map=map.json]  |  node gm_xml_import.js stock [--verbose]');
  }
})();

