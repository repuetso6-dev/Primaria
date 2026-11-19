'use strict';
// Servidor único de "6to Primaria": sirve el juego, guarda claims/monedas/códigos
// y reparte la posición de todos los jugadores en vivo (WebSocket).
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const WebSocket = require('ws');

const PORT = +process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUB = fs.existsSync(path.join(__dirname, 'public')) ? path.join(__dirname, 'public') : __dirname; // acepta carpeta public/ o todo suelto
const WEB = new Set(['index.html', 'cover.jpg', 'icon-192.png', 'icon-512.png', 'manifest.webmanifest', 'sw.js']); // solo estos se publican
const ROSTER = ['Aidalia', 'Arturo', 'Lucas', 'Hassiel', 'Rafael', 'Omar', 'Said', 'Abel', 'Bruno', 'Ezequiel',
  'Vianney', 'Adrian', 'Dañaría', 'Josue', 'Luana', 'Fernanda', 'Oriana', 'Jazmín', 'Leonel', 'Nayara'];
const KK = n => n.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');

// ---------- datos (se guardan en disco) ----------
fs.mkdirSync(DATA_DIR, { recursive: true });
const FILE = path.join(DATA_DIR, 'db.json');
let db = { docs: {}, adminKey: null, salt: null };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(FILE, 'utf8'))); } catch (e) { /* primera vez */ }
const docs = db.docs;
if (process.env.SALT) db.salt = process.env.SALT; else if (!db.salt) db.salt = crypto.randomBytes(16).toString('hex');
if (process.env.ADMIN_KEY) db.adminKey = process.env.ADMIN_KEY; else if (!db.adminKey) db.adminKey = crypto.randomBytes(4).toString('hex');

let saveTimer = null;
function flush() {
  try { const tmp = FILE + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(db)); fs.renameSync(tmp, FILE); }
  catch (e) { console.error('No se pudo guardar:', e.message); }
}
function save() { if (!saveTimer) saveTimer = setTimeout(() => { saveTimer = null; flush(); }, 1500); }

// Códigos de 3 números por personaje (uno distinto para cada uno).
function seedCodes() {
  const c = docs['admin/codes'] || {};
  if (process.env.CODES) {            // opcional: "Aidalia=123,Arturo=456,..." para que no cambien nunca
    for (const part of process.env.CODES.split(',')) {
      const [n, v] = part.split('=').map(s => (s || '').trim());
      const k = ROSTER.map(KK).find(x => x.toLowerCase() === (n || '').toLowerCase());
      if (k && /^\d{3}$/.test(v)) c[k] = v;
    }
  }
  const used = new Set(Object.values(c));
  for (const n of ROSTER) {
    const k = KK(n);
    if (!/^\d{3}$/.test(c[k] || '')) {
      let v; do { v = String(100 + crypto.randomInt(900)); } while (used.has(v) || v === '777' || v === '888');
      c[k] = v; used.add(v);
    }
  }
  docs['admin/codes'] = c;
}
const token = (k, code) => sha(db.salt + '|' + k + '|' + code).slice(0, 12);
// Calcula config/ch (lo que el juego compara) y saca a quien tenga un código viejo.
function rebuildCh() {
  const codes = docs['admin/codes'], ch = {};
  for (const n of ROSTER) { const k = KK(n); ch[k] = token(k, codes[k]); }
  docs['config/ch'] = ch;
  for (const p of Object.keys(docs)) {
    if (!p.startsWith('claims/')) continue;
    const c = docs[p];
    if (!c || !ROSTER.includes(c.c) || c.h !== ch[KK(c.c)]) delete docs[p];
  }
}
seedCodes(); rebuildCh(); flush();

const bans = () => docs['config/bans'] || {};
const cfg = () => Object.assign({ open: true, msg: '' }, docs['config/server'] || {});
function isAdmin(k) {
  if (typeof k !== 'string' || !k) return false;
  const a = crypto.createHash('sha256').update(k).digest(), b = crypto.createHash('sha256').update(String(db.adminKey)).digest();
  return crypto.timingSafeEqual(a, b);
}

// ---------- límite de intentos por IP (códigos y clave de admin) ----------
const fails = new Map();
const limited = ip => { const e = fails.get(ip); return e && e.until > Date.now() ? e.until - Date.now() : 0; };
function fail(ip) {
  const now = Date.now(); let e = fails.get(ip);
  if (!e || now - e.t > 60000) e = { n: 0, until: (e && e.until) || 0, t: now };
  if (++e.n >= 5) { e.until = now + 60000; e.n = 0; e.t = now; }
  fails.set(ip, e);
}
setInterval(() => { const now = Date.now(); for (const [k, e] of fails) if (e.until < now && now - e.t > 60000) fails.delete(k); }, 600000);

// ---------- conexiones ----------
const conns = new Map(), dirty = new Set();
let seq = 0;
const tx = (c, o) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(o)); };
const ack = (c, rid, ok, d, err) => tx(c, { t: 'ack', rid, ok, d: d === undefined ? null : d, err });
const dirtyAll = () => { for (const id of conns.keys()) dirty.add(id); };

const DOC = /^(claims|coins)\/[A-Za-z0-9_-]{1,40}$/;
const SYS = new Set(['admin/codes', 'config/ch', 'config/server', 'config/bans']);
const okPath = p => typeof p === 'string' && (DOC.test(p) || SYS.has(p));
const canRead = (p, c) => !p.startsWith('admin/') || c.adm;

function snapTo(c, id, s) {
  if (s.col) {
    if (s.col !== 'claims' && s.col !== 'coins') return tx(c, { t: 'snap', id, err: 'path' });
    const pre = s.col + '/', out = [];
    for (const p in docs) if (p.startsWith(pre)) out.push({ id: p.slice(pre.length), data: docs[p] });
    return tx(c, { t: 'snap', id, docs: out });
  }
  if (!okPath(s.path) || !canRead(s.path, c)) return tx(c, { t: 'snap', id, err: 'perm' });
  tx(c, { t: 'snap', id, data: docs[s.path] === undefined ? null : docs[s.path] });
}
function changed(p) {
  const col = p.split('/')[0];
  for (const c of conns.values()) for (const [id, s] of c.subs) if (s.col ? s.col === col : s.path === p) snapTo(c, id, s);
}

function setDoc(c, p, data) {
  if (!okPath(p)) throw new Error('ruta');
  if (typeof data !== 'object' || data === null || Array.isArray(data)) throw new Error('datos');
  if (JSON.stringify(data).length > (p === 'config/bans' ? 20000 : 2000)) throw new Error('muy grande');
  const [col, id] = p.split('/');
  if (col === 'claims') {
    if (id !== c.uid) throw new Error('perm');
    const n = data.c;
    if (!ROSTER.includes(n)) throw new Error('personaje');
    if (!c.adm && bans()[c.uid]) throw new Error('expulsado');
    if (!c.adm && cfg().open === false) throw new Error('cerrado');
    const tk = c.tickets.get(n);
    if (!c.adm && !(tk && tk > Date.now())) throw new Error('codigo');
    for (const q in docs) if (q.startsWith('claims/') && q !== p && docs[q].c === n) throw new Error('ocupado');
    docs[p] = { c: n, t: Date.now(), h: docs['config/ch'][KK(n)] };
    c.tickets.delete(n);
  } else if (col === 'coins') {
    if (id !== c.uid && !c.adm) throw new Error('perm');
    const n = data.n;
    if (!Number.isInteger(n) || n < 0 || n > 100000) throw new Error('monedas');
    const prev = (docs[p] && docs[p].n) || 0;
    if (!c.adm && n > prev + 200) throw new Error('monedas');
    docs[p] = { n, c: typeof data.c === 'string' ? data.c.slice(0, 24) : '' };
  } else {
    if (!c.adm) throw new Error('perm');
    if (p === 'admin/codes') {
      const m = Object.assign({}, docs[p]);
      for (const n of ROSTER) {
        const k = KK(n);
        if (data[k] !== undefined) { if (!/^\d{3}$/.test(String(data[k]))) throw new Error('codigo'); m[k] = String(data[k]); }
      }
      if (new Set(Object.values(m)).size !== ROSTER.length) throw new Error('codigo repetido');
      docs[p] = m; rebuildCh(); changed('config/ch'); changed('claims/x');
    } else if (p === 'config/server') {
      docs[p] = { open: data.open !== false, msg: typeof data.msg === 'string' ? data.msg.slice(0, 80) : '' };
    } else if (p === 'config/bans') {
      const b = {}; for (const k of Object.keys(data)) if (/^[A-Za-z0-9_-]{1,40}$/.test(k) && data[k]) b[k] = 1;
      docs[p] = b;
    } // config/ch lo calcula el servidor: se ignora lo que mande el cliente
  }
  changed(p); dirtyAll(); save();
}
function delDoc(c, p) {
  if (!okPath(p)) throw new Error('ruta');
  const [col, id] = p.split('/');
  if (col === 'claims') { if (!c.adm && id !== c.uid) throw new Error('perm'); }
  else if (!c.adm || !(col === 'coins' || p === 'config/server' || p === 'config/bans')) throw new Error('perm');
  delete docs[p]; changed(p); dirtyAll(); save();
}

// ---------- presencia (posiciones en vivo) ----------
const NUM = (v, a, b) => Number.isFinite(v) ? Math.max(a, Math.min(b, Math.round(v))) : 0;
function cleanPres(c, p) {
  const r = c.pres;
  if ('c' in p) r.c = typeof p.c === 'string' ? p.c.slice(0, 24) : null;
  if ('x' in p) r.x = NUM(p.x, -9999, 9999);
  if ('y' in p) r.y = NUM(p.y, -9999, 9999);
  if ('f' in p) r.f = NUM(p.f, 1, 2);
  if ('d' in p) r.d = NUM(p.d, 0, 3);
  if ('m' in p) r.m = NUM(p.m, 0, 1);
  if ('e' in p) r.e = NUM(p.e, 0, 3);
  if ('en' in p) r.en = NUM(p.en, 0, 1e9);
}
// Solo se ve a quien de verdad tiene ese personaje (claim válido) y no está expulsado.
function entry(c) {
  const r = c.pres, cl = docs['claims/' + c.uid];
  if (!r.c || !cl || cl.c !== r.c) return null;
  if (!c.adm && (bans()[c.uid] || cfg().open === false)) return null;
  return { p: r, by: c.uid };
}
setInterval(() => {
  if (!dirty.size) return;
  const ids = [...dirty]; dirty.clear();
  const all = {};
  for (const id of ids) { const c = conns.get(id); all[id] = c ? entry(c) : null; }
  for (const c of conns.values()) {
    const u = {}; let n = 0;
    for (const id in all) if (id !== c.id) { u[id] = all[id]; n++; }
    if (n) tx(c, { t: 'pr', u });
  }
}, 100);

// ---------- mensajes ----------
function verify(c, m) {
  const lk = limited(c.ip);
  if (lk) return ack(c, m.rid, true, { n: null, lock: lk });
  const code = String(m.code || ''); let name = null;
  if (/^\d{3}$/.test(code)) { const codes = docs['admin/codes']; for (const n of ROSTER) if (codes[KK(n)] === code) name = n; }
  if (!name) { fail(c.ip); return ack(c, m.rid, true, { n: null }); }
  c.tickets.set(name, Date.now() + 120000);
  ack(c, m.rid, true, { n: name });
}
function onMsg(c, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  try {
    switch (m.t) {
      case 'sub': {
        if (typeof m.id !== 'number' || c.subs.size > 20) return;
        const s = m.col ? { col: String(m.col) } : { path: String(m.path) };
        c.subs.set(m.id, s); snapTo(c, m.id, s); break;
      }
      case 'unsub': c.subs.delete(m.id); break;
      case 'set': setDoc(c, m.path, m.data); ack(c, m.rid, true); break;
      case 'del': delDoc(c, m.path); ack(c, m.rid, true); break;
      case 'get':
        if (!okPath(m.path) || !canRead(m.path, c)) throw new Error('perm');
        ack(c, m.rid, true, docs[m.path] === undefined ? null : docs[m.path]); break;
      case 'pres': if (m.p && typeof m.p === 'object') { cleanPres(c, m.p); dirty.add(c.id); } break;
      case 'verify': verify(c, m); break;
      case 'adm': {
        if (limited(c.ip)) return ack(c, m.rid, true, false);
        const ok = isAdmin(m.k); if (!ok) fail(c.ip);
        ack(c, m.rid, true, ok); break;
      }
    }
  } catch (e) { if (m.rid !== undefined) ack(c, m.rid, false, null, e.message); }
}

// ---------- HTTP (sirve el juego) + WebSocket ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.webmanifest': 'application/manifest+json', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  let u; try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (u === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (u === '/') u = '/index.html';
  if (!WEB.has(u.slice(1))) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('No encontrado'); }
  const f = path.normalize(path.join(PUB, u));
  if (!f.startsWith(PUB + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, b) => {
    if (e) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); return res.end('No encontrado'); }
    res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream',
      'cache-control': (u === '/index.html' || u === '/sw.js') ? 'no-cache' : 'public, max-age=3600' });
    res.end(b);
  });
});
const wss = new WebSocket.Server({ noServer: true, maxPayload: 16384 });
server.on('upgrade', (req, sock, head) => {
  let q; try { q = new URL(req.url, 'http://x'); } catch (e) { return sock.destroy(); }
  if (q.pathname !== '/ws') return sock.destroy();
  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req, q));
});
wss.on('connection', (ws, req, q) => {
  const s = q.searchParams.get('s') || '';
  if (!/^[0-9a-f]{32}$/.test(s)) { ws.close(1008, 'id'); return; }
  const ip = (String(req.headers['x-forwarded-for'] || '').split(',').pop() || '').trim() || req.socket.remoteAddress || '?';
  const c = { ws, ip, id: 'p' + (++seq), uid: sha('uid|' + s).slice(0, 16), adm: isAdmin(q.searchParams.get('k') || ''),
    subs: new Map(), pres: {}, tickets: new Map(), alive: true, n: 0 };
  conns.set(c.id, c);
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', d => { if (++c.n > 400) { ws.close(1008, 'rate'); return; } onMsg(c, d.toString()); });
  ws.on('close', () => { conns.delete(c.id); dirty.add(c.id); });
  ws.on('error', () => {});
  tx(c, { t: 'hi', uid: c.uid, adm: c.adm, peer: c.id });
  const u = {};
  for (const o of conns.values()) if (o !== c) { const e = entry(o); if (e) u[o.id] = e; }
  if (Object.keys(u).length) tx(c, { t: 'pr', u });
});
setInterval(() => { for (const c of conns.values()) c.n = 0; }, 5000);
setInterval(() => {
  for (const c of conns.values()) {
    if (!c.alive) { c.ws.terminate(); continue; }
    c.alive = false; try { c.ws.ping(); } catch (e) { /* nada */ }
  }
}, 25000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n=== 6to Primaria · servidor en el puerto ' + PORT + ' ===');
  console.log('Clave de admin: ' + db.adminKey);
  console.log('Códigos de los alumnos (guárdalos y pásale a cada uno SOLO el suyo):');
  for (const n of ROSTER) console.log('  ' + n.padEnd(10) + docs['admin/codes'][KK(n)]);
  console.log('');
});
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => { flush(); process.exit(0); });
