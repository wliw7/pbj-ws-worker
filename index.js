'use strict';
// ---------------------------------------------------------------------------
// Autumn — ES futures ORDERFLOW relay (founder-only).
//
// ONE process holds ONE Massive.com (ex-Polygon) futures WebSocket and fans a
// normalized, aggressor-classified tape out to the Edge desk over SSE. Keeps
// the licensed raw feed server-side and thin: the browser does the analytics
// (glass box, same philosophy as the rest of Autumn).
//
// Feed (Massive Futures Advanced plan — real-time):
//   trades: { ev:'T', sym, p, s, t, q }
//   quotes: { ev:'Q', sym, bp, bs, bt, ap, as, at, t }   (BBO)
//
// Aggressor classification (same convention as Autumn's orderflow engine):
//   fill >= ask  -> 'B' (buyer lifted the offer)
//   fill <= bid  -> 'S' (seller hit the bid)
//   between      -> lean by midpoint ('b' / 's' soft), exact mid -> 'M'
//
// State kept per session (rolls at 22:00 UTC ≈ Globex daily break):
//   ladder: tick-resolution price -> { bv, av, vol, delta, dark? } (no dark on CME)
//   cvd:    per-minute [t, cvdAll, cvdBig] (big = prints >= INST_SZ contracts)
//   prints: ring of the last PRINTS_KEEP normalized prints
//
// Endpoints (ticket = the SAME HMAC token ws-ticket.js mints; FOUNDER only —
// requires r:'admin' in the payload, or u:'admin' for legacy tickets):
//   GET /health           no auth; feed + session status
//   GET /snap?s=<t>       full snapshot { q, ladder, cvd, stats, prints(tail) }
//   GET /stream?s=<t>     SSE; snapshot then batched deltas every FLUSH_MS
//
// ENV:
//   MASSIVE_KEY      required (unless MOCK=1) — Massive.com API key
//   MASSIVE_WS       default wss://socket.massive.com/futures
//                    (legacy host wss://socket.polygon.io/futures also works)
//   ES_TICKER        required (unless MOCK=1) — exact front-month contract,
//                    e.g. ESU6. Comma list OK (first = primary). Roll manually
//                    or redeploy with the next contract; /health shows staleness.
//   SESSION_SECRET   required — same secret as gex.js / ws-ticket.js
//   ALLOWED_ORIGIN   default * — e.g. https://pipelinecapital.net
//   PORT             Railway injects
//   TICK             price tick (default 0.25)
//   INST_SZ          contracts per print to count as institutional (default 10)
//   PRINTS_KEEP      raw prints retained/replayed (default 4000)
//   FLUSH_MS         SSE batch cadence ms (default 250)
//   MOCK             '1' -> synthetic ES tape (no key needed; UI/dev mode)
// ---------------------------------------------------------------------------

const crypto  = require('crypto');
const express = require('express');
const cors    = require('cors');
const WebSocket = require('ws');

// ---- config -----------------------------------------------------------------
const MASSIVE_KEY  = (process.env.MASSIVE_KEY || '').trim();
const MASSIVE_WS   = (process.env.MASSIVE_WS || 'wss://socket.massive.com/futures').trim();
const ES_TICKER    = (process.env.ES_TICKER || '').trim().toUpperCase();   // e.g. ESU6
const SECRET       = process.env.SESSION_SECRET || '';
const ORIGIN       = process.env.ALLOWED_ORIGIN || '*';
const PORT         = parseInt(process.env.PORT || '8090', 10);
const TICK         = parseFloat(process.env.TICK || '0.25');
const INST_SZ      = parseInt(process.env.INST_SZ || '10', 10);
const PRINTS_KEEP  = parseInt(process.env.PRINTS_KEEP || '4000', 10);
const FLUSH_MS     = parseInt(process.env.FLUSH_MS || '250', 10);
const MOCK         = process.env.MOCK === '1';

if (!SECRET) console.warn('[warn] SESSION_SECRET unset — auth is OPEN (dev only).');
if (!MOCK && (!MASSIVE_KEY || !ES_TICKER)) {
  console.error('FATAL: MASSIVE_KEY and ES_TICKER are required (or set MOCK=1)');
  process.exit(1);
}
const PRIMARY = MOCK ? 'ES-MOCK' : ES_TICKER.split(',')[0].trim();

const num = (x) => { const n = parseFloat(x); return Number.isFinite(n) ? n : 0; };
const rTick = (p) => Math.round(p / TICK) * TICK;
const tkey  = (p) => rTick(p).toFixed(2);           // ladder key at tick resolution

// ---- founder auth (same HMAC ticket ws-ticket.js mints) ----------------------
const b64url = (b) => Buffer.from(b).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const hsign  = (p, s) => b64url(crypto.createHmac('sha256', s).update(p).digest());
function verifyTicket(tok) {
  if (!SECRET) return { u: 'dev', r: 'admin' };                 // dev: open
  if (!tok || tok.indexOf('.') < 0) return null;
  const [p, sig] = tok.split('.'); const ex = hsign(p, SECRET);
  if (!sig || sig.length !== ex.length) return null;
  try { if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(ex))) return null; } catch { return null; }
  let o; try { o = JSON.parse(Buffer.from(p.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString()); } catch { return null; }
  if (!(o.exp && o.exp >= Math.floor(Date.now() / 1000))) return null;
  return o;
}
function auth(req, res, next) {
  const tok = req.query.s || req.headers['x-pbj-session'] || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const sess = verifyTicket(tok);
  if (!sess) return res.status(401).json({ error: 'unauthorized' });
  // FOUNDER ONLY: raw CME tape must not reach members (display licence is individual).
  if (!(sess.r === 'admin' || sess.u === 'admin')) return res.status(403).json({ error: 'founder only' });
  req.sess = sess; next();
}

// ---- session state ------------------------------------------------------------
// Globex trade date rolls at the 21:00-22:00 UTC maintenance break; anchor at 22:00 UTC.
function sessionId(now) {
  const d = new Date(now);
  if (d.getUTCHours() >= 22) d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}
let S = null;
function freshSession(now) {
  return {
    id: sessionId(now), startedAt: now,
    q: { bp: 0, ap: 0, bs: 0, as: 0, t: 0 },        // latest BBO
    last: 0, lastT: 0,                               // latest trade
    ladder: new Map(),                               // tick -> {bv,av,vol,delta}
    cvd: [],                                         // [ [minuteTs, cvdAll, cvdBig] ]
    cvdAll: 0, cvdBig: 0,
    prints: [],                                      // ring of normalized prints
    vol: 0, buys: 0, sells: 0, mids: 0,
    lastMsg: 0, seq: 0,
  };
}
function rollIfNeeded(now) {
  const id = sessionId(now);
  if (!S || S.id !== id) { S = freshSession(now); console.log('[session]', S.id); }
}
rollIfNeeded(Date.now());

// pending SSE batch
let pend = { prints: [], rows: new Set(), q: false, cvd: false };

function ladderRow(k) {
  let r = S.ladder.get(k);
  if (!r) { r = { bv: 0, av: 0, vol: 0, delta: 0 }; S.ladder.set(k, r); }
  return r;
}

// classify + ingest ONE trade against the prevailing BBO
function onTrade(p, s, t) {
  rollIfNeeded(t);
  const q = S.q;
  let sd = 'M';
  if (q.ap > 0 && p >= q.ap) sd = 'B';
  else if (q.bp > 0 && p <= q.bp) sd = 'S';
  else if (q.ap > 0 && q.bp > 0) {
    const mid = (q.ap + q.bp) / 2;
    sd = p > mid ? 'b' : p < mid ? 's' : 'M';
  }
  const k = tkey(p);
  const row = ladderRow(k);
  row.vol += s;
  const signed = (sd === 'B' || sd === 'b') ? s : (sd === 'S' || sd === 's') ? -s : 0;
  if (signed > 0) { row.av += s; S.buys += s; }
  else if (signed < 0) { row.bv += s; S.sells += s; }
  else S.mids += s;
  row.delta += signed;
  S.vol += s;
  S.cvdAll += signed;
  if (s >= INST_SZ) S.cvdBig += signed;
  S.last = p; S.lastT = t; S.lastMsg = Date.now(); S.seq++;

  const pr = { t, p, s, sd, bp: q.bp, ap: q.ap };
  S.prints.push(pr);
  if (S.prints.length > PRINTS_KEEP) S.prints.shift();

  pend.prints.push(pr);
  pend.rows.add(k);

  // per-minute CVD point
  const m = Math.floor(t / 60000) * 60000;
  const cv = S.cvd;
  if (!cv.length || cv[cv.length - 1][0] !== m) { cv.push([m, S.cvdAll, S.cvdBig]); pend.cvd = true; }
  else { const lastPt = cv[cv.length - 1]; lastPt[1] = S.cvdAll; lastPt[2] = S.cvdBig; pend.cvd = true; }
  if (cv.length > 1500) cv.shift();                 // ~25h guard
}
function onQuote(m) {
  rollIfNeeded(m.t || Date.now());
  const q = S.q;
  if (num(m.bp) > 0) { q.bp = num(m.bp); q.bs = num(m.bs); }
  if (num(m.ap) > 0) { q.ap = num(m.ap); q.as = num(m.as); }
  q.t = m.t || Date.now();
  S.lastMsg = Date.now();
  pend.q = true;
}

// ---- snapshot / SSE ------------------------------------------------------------
function ladderArr() {
  const out = [];
  for (const [k, r] of S.ladder) out.push([+k, r.bv, r.av, r.vol, r.delta]);
  out.sort((a, b) => b[0] - a[0]);                   // high -> low, dashboard layout
  return out;
}
function snap(tailN) {
  return {
    type: 'snap', sym: PRIMARY, session: S.id, tick: TICK, instSz: INST_SZ,
    q: S.q, last: S.last, lastT: S.lastT,
    stats: { vol: S.vol, buys: S.buys, sells: S.sells, mids: S.mids, cvd: S.cvdAll, cvdBig: S.cvdBig },
    ladder: ladderArr(),
    cvd: S.cvd,
    prints: S.prints.slice(-(tailN || 400)),
  };
}

const clients = new Set();
setInterval(() => {
  if (!clients.size) { pend = { prints: [], rows: new Set(), q: false, cvd: false }; return; }
  if (!pend.prints.length && !pend.q && !pend.cvd && !pend.rows.size) return;
  const rows = [];
  for (const k of pend.rows) { const r = S.ladder.get(k); if (r) rows.push([+k, r.bv, r.av, r.vol, r.delta]); }
  const msg = {
    type: 'delta',
    q: pend.q ? S.q : undefined,
    last: S.last, lastT: S.lastT,
    stats: { vol: S.vol, buys: S.buys, sells: S.sells, cvd: S.cvdAll, cvdBig: S.cvdBig },
    prints: pend.prints.length ? pend.prints : undefined,
    rows: rows.length ? rows : undefined,
    cvdPt: pend.cvd && S.cvd.length ? S.cvd[S.cvd.length - 1] : undefined,
    session: S.id,
  };
  pend = { prints: [], rows: new Set(), q: false, cvd: false };
  const data = `data: ${JSON.stringify(msg)}\n\n`;
  for (const res of clients) { try { res.write(data); } catch {} }
}, FLUSH_MS);

// ---- HTTP ----------------------------------------------------------------------
const app = express();
app.use(cors({ origin: ORIGIN === '*' ? true : ORIGIN.split(',').map(s => s.trim()), credentials: true }));

app.get('/health', (_req, res) => {
  res.json({
    ok: true, mock: MOCK, sym: PRIMARY, session: S.id,
    feed: feedStatus, lastMsgAgeMs: S.lastMsg ? Date.now() - S.lastMsg : null,
    prints: S.prints.length, ladderRows: S.ladder.size, vol: S.vol,
    viewers: clients.size, uptimeSec: Math.round(process.uptime()),
  });
});
app.get('/snap', auth, (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json(snap(num(req.query.tail) || 400));
});
app.get('/stream', auth, (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders && res.flushHeaders();
  res.write('retry: 3000\n\n');
  res.write(`data: ${JSON.stringify(snap(400))}\n\n`);
  clients.add(res);
  const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
  req.on('close', () => { clearInterval(ping); clients.delete(res); });
});

// ---- Massive.com feed ------------------------------------------------------------
let feedStatus = MOCK ? 'mock' : 'connecting';
let ws = null, reconnectTimer = null, tries = 0;

function connect() {
  if (MOCK) return;
  feedStatus = 'connecting';
  console.log('[connect]', MASSIVE_WS, 'tickers:', ES_TICKER);
  ws = new WebSocket(MASSIVE_WS);
  ws.on('open', () => {
    // Massive/Polygon protocol: connect -> auth -> subscribe
    ws.send(JSON.stringify({ action: 'auth', params: MASSIVE_KEY }));
  });
  ws.on('message', (buf) => {
    let arr; try { arr = JSON.parse(buf.toString()); } catch { return; }
    if (!Array.isArray(arr)) arr = [arr];
    for (const m of arr) {
      if (!m || typeof m !== 'object') continue;
      if (m.ev === 'status') {
        console.log('[status]', m.status, m.message || '');
        if (m.status === 'auth_success') {
          feedStatus = 'open'; tries = 0;
          const subs = ES_TICKER.split(',').map(s => s.trim()).filter(Boolean)
            .flatMap(tk => [`T.${tk}`, `Q.${tk}`]).join(',');
          ws.send(JSON.stringify({ action: 'subscribe', params: subs }));
          console.log('[subscribe]', subs);
        }
        if (m.status === 'auth_failed') { feedStatus = 'auth_failed'; console.error('[auth failed]', m.message || ''); }
        continue;
      }
      if (m.ev === 'T') { const p = num(m.p), s = num(m.s); if (p > 0 && s > 0) onTrade(p, s, num(m.t) || Date.now()); }
      else if (m.ev === 'Q') onQuote(m);
    }
  });
  ws.on('error', (e) => { feedStatus = 'error'; console.error('[ws error]', (e && e.message) || e); });
  ws.on('close', (code) => {
    feedStatus = 'closed';
    console.warn('[ws closed]', code || '');
    if (!reconnectTimer) {
      const delay = [1000, 2000, 5000, 10000][tries++] || 15000;
      reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
    }
  });
}

// ---- mock tape (UI/dev without a key) ----------------------------------------------
function startMock() {
  let px = 6890, drift = 0;
  console.log('[mock] synthetic ES tape around', px);
  setInterval(() => {
    // BBO random walk
    drift += (Math.random() - 0.5) * 0.05;
    drift = Math.max(-0.6, Math.min(0.6, drift));
    px = Math.max(1000, px + drift * TICK + (Math.random() - 0.5) * TICK);
    const bp = rTick(px) - TICK / 2 - (Math.random() < 0.5 ? 0 : 0) ;
    const bid = rTick(px - TICK / 2), ask = bid + TICK;
    onQuote({ bp: bid, bs: 20 + (Math.random() * 200 | 0), ap: ask, as: 20 + (Math.random() * 200 | 0), t: Date.now() });
    // a burst of prints, biased with drift
    const n = 1 + (Math.random() * 4 | 0);
    for (let i = 0; i < n; i++) {
      const buyP = 0.5 + drift * 0.55;
      const isBuy = Math.random() < buyP;
      const size = Math.random() < 0.08 ? (10 + (Math.random() * 60 | 0)) : (1 + (Math.random() * 6 | 0));
      onTrade(isBuy ? ask : bid, size, Date.now());
    }
  }, 300);
}

if (MOCK) startMock(); else connect();

app.listen(PORT, () => console.log(`[http] autumn-es-worker on :${PORT} — ${MOCK ? 'MOCK tape' : ES_TICKER + ' via Massive'}`));
