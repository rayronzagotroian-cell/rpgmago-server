/* accounts.js — salvamento de contas/progresso do RPG do Mago (rode junto do seu servidor atual)
 * Uso no seu server.js (1 linha no começo do handler HTTP):
 *     const accounts = require('./accounts');
 *     http.createServer((req,res)=>{ if (accounts.handle(req,res)) return; ...resto do seu código... })
 * Armazenamento PERMANENTE (recomendado): crie um Redis grátis no Upstash e ponha no Render:
 *     UPSTASH_REDIS_REST_URL   e   UPSTASH_REDIS_REST_TOKEN
 * Sem isso, grava em arquivo (DATA_DIR) — no plano grátis do Render o disco APAGA ao reiniciar/atualizar.
 */
'use strict';
const fs = require('fs'), path = require('path'), crypto = require('crypto');
const URL_ = process.env.UPSTASH_REDIS_REST_URL, TOK_ = process.env.UPSTASH_REDIS_REST_TOKEN;
const DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const MAX = 250 * 1024;                       // máx. por conta
const NAME = /^[a-z0-9_\-. ]{3,20}$/;         // mesmo nome em minúsculas do jogo
const ALLOWED_KEYS = n => ['rpgmago1_' + n, 'rpgmago_casa_' + n];

async function up(cmd) {
  const r = await fetch(URL_, { method: 'POST', headers: { Authorization: 'Bearer ' + TOK_ }, body: JSON.stringify(cmd) });
  if (!r.ok) throw new Error('upstash ' + r.status);
  return (await r.json()).result;
}
const file = n => path.join(DIR, crypto.createHash('sha256').update(n).digest('hex').slice(0, 40) + '.json');
async function load(n) {
  if (URL_ && TOK_) { const v = await up(['GET', 'acc:' + n]); return v ? JSON.parse(v) : null; }
  try { return JSON.parse(fs.readFileSync(file(n), 'utf8')); } catch (e) { return null; }
}
async function store(n, rec) {
  const s = JSON.stringify(rec);
  if (URL_ && TOK_) { await up(['SET', 'acc:' + n, s]); return; }
  fs.mkdirSync(DIR, { recursive: true });
  const f = file(n), tmp = f + '.tmp'; fs.writeFileSync(tmp, s); fs.renameSync(tmp, f);   // escrita atômica
}
const same = (a, b) => { a = Buffer.from(String(a)); b = Buffer.from(String(b)); return a.length === b.length && crypto.timingSafeEqual(a, b); };

const hits = new Map();
const limited = ip => { const t = Date.now(), a = (hits.get(ip) || []).filter(x => t - x < 60000); a.push(t); hits.set(ip, a); return a.length > 120; };
setInterval(() => hits.clear(), 600000).unref();

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}
function body(req) {
  return new Promise((ok, no) => { let s = '', n = 0;
    req.on('data', c => { n += c.length; if (n > MAX + 4096) { no(new Error('big')); req.destroy(); } else s += c; });
    req.on('end', () => { try { ok(JSON.parse(s)); } catch (e) { no(e); } }); req.on('error', no); });
}

function handle(req, res) {
  const u = new URL(req.url, 'http://x');
  if (!u.pathname.startsWith('/acc/')) return false;
  if (req.method === 'OPTIONS') { send(res, 204, {}); return true; }
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (limited(ip)) { send(res, 429, { ok: false }); return true; }
  (async () => {
    try {
      if (u.pathname === '/acc/get' && req.method === 'GET') {
        const n = (u.searchParams.get('n') || '').toLowerCase(), tok = u.searchParams.get('tok') || '';
        if (!NAME.test(n) || !/^[a-f0-9]{64}$/.test(tok)) return send(res, 400, { ok: false });
        const rec = await load(n);
        if (!rec) return send(res, 404, { ok: false });
        if (!same(rec.tok, tok)) return send(res, 403, { ok: false });
        return send(res, 200, { ok: true, t: rec.t, data: rec.data });
      }
      if (u.pathname === '/acc/put' && req.method === 'POST') {
        const b = await body(req), n = String(b.n || '').toLowerCase(), tok = String(b.tok || '');
        if (!NAME.test(n) || !/^[a-f0-9]{64}$/.test(tok) || !b.data || typeof b.data !== 'object') return send(res, 400, { ok: false });
        const ok = ALLOWED_KEYS(n), data = {};
        for (const k of ok) if (typeof b.data[k] === 'string') data[k] = b.data[k];
        const len = JSON.stringify(data).length;
        if (len > MAX) return send(res, 413, { ok: false });
        const old = await load(n);
        if (old && !same(old.tok, tok)) return send(res, 403, { ok: false });
        const t = Math.min(Number(b.t) || Date.now(), Date.now() + 60000);
        const rec = { tok, t: Math.max(t, old ? old.t : 0), data: Object.assign({}, old && old.data, data), created: old ? old.created : Date.now() };
        await store(n, rec);
        const back = await load(n);                      // confirma que gravou de verdade
        const got = {}; if (back) for (const k of ok) if (k in data && back.data[k] !== undefined) got[k] = back.data[k];
        const good = !!back && JSON.stringify(got) === JSON.stringify(data);
        return send(res, good ? 200 : 500, { ok: good, t: rec.t, len: JSON.stringify(got).length });
      }
      send(res, 404, { ok: false });
    } catch (e) { send(res, 500, { ok: false }); }
  })();
  return true;
}
module.exports = { handle };
