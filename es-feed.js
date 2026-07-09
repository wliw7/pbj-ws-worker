'use strict';
// ---------------------------------------------------------------------------
// Autumn — ES futures tape feed (Massive.com) for the Edge "level radar".
//
// DROP-IN MODULE FOR THE RAILWAY WORKER (pbj-ws-worker). This file lives in the
// site repo under netlify/worker/ (never served — /netlify/* is 404'd by
// netlify.toml). Copy it into the worker repo next to index.js and wire it:
//
//   // index.js — after `const app = express()` and verifyToken are defined:
//   require('./es-feed').mount(app, { verifyToken });
//
//   ENV (Railway):
//     MASSIVE_KEY   required — Massive.com API key (Futures Advanced plan)
//     ES_PRODUCT    default "ES"    (product_code for front-month discovery)
//     ES_TICKER     optional        hard override, e.g. "ESU6" (skips discovery)
//     MASSIVE_WS    default "wss://socket.massive.com/futures"
//     ES_BIG_SIZE   default 20      contracts; prints >= this feed the "big" CVD
//
// WHY THE WORKER: one Massive socket serves every founder session, the key
// never reaches a browser, and the SSE fan-out + ticket auth mirror /stream.
//
// Protocol (confirmed against massive.com docs + official client):
//   connect  wss://socket.massive.com/futures
//   auth     {"action":"auth","params":"<KEY>"}
//   sub      {"action":"subscribe","params":"T.ESU6,Q.ESU6"}
//   trade    {ev:"T", sym, p, s, t(ms), q}      quote {ev:"Q", sym, bp, bs, ap, as, t}
//   Messages arrive as JSON ARRAYS. ev:"status" frames carry auth/sub acks.
//
// PRICE-SCALE GUARD: Massive's WS docs sample shows p=606450 for ESZ4 (i.e.
// price*100) while REST /futures/v1/trades returns 6052.00. We anchor against
// one REST last-trade at boot and derive a /1 or /100 divisor per field, so the
// feed is correct whichever encoding the socket actually emits.
//
// Endpoints (mounted on the existing express app):
//   GET /es/health           no auth — socket/tape liveness + ticker + scale
//   GET /es/state?s=<t>      founder ticket — engine snapshot (ladder/CVD/session)
//   GET /es/stream?s=<t>     founder ticket — SSE: init snapshot, then ~4/s batches
//
// FOUNDER-ONLY: tickets are minted by ws-ticket.js which now carries the role
// claim (r:"admin"). Members' tickets lack it -> 403. Licensed data must not
// fan out to members without a redistribution agreement.
// ---------------------------------------------------------------------------

const WebSocket = require('ws');

const API = 'https://api.massive.com';

function num(x) { const n = parseFloat(x); return Number.isFinite(n) ? n : 0; }

function mount(app, opts) {
  opts = opts || {};
  const verifyToken = opts.verifyToken;
  if (typeof verifyToken !== 'function') throw new Error('es-feed: pass { verifyToken } from index.js');

  const KEY        = (process.env.MASSIVE_KEY || '').trim();
  const PRODUCT    = (process.env.ES_PRODUCT || 'ES').trim().toUpperCase();
  const WS_URL     = (process.env.MASSIVE_WS || 'wss://socket.massive.com/futures').trim();
  const BIG_SIZE   = parseInt(process.env.ES_BIG_SIZE || '20', 10);
  const MIN_DTM    = 3;            // roll: skip contracts within 3 days of last trade
  // v3: ring big enough that a page refresh reconstructs the FULL 3-minute band
  // window instantly, even on fast tape (~3k prints ≈ several minutes of ES).
  const RECENT_MAX = parseInt(process.env.ES_RECENT || '3000', 10);
  const FLUSH_MS   = 250;          // SSE batch cadence
  const BAR_MS     = 1000;         // 1s bars ring (burst z-score source, client-side)
  const BARS_KEEP  = 900;          // 15 min of 1s bars
  const SAVE_MS    = 5000;         // session-state persistence cadence (Postgres)

  if (!KEY) { console.warn('[es-feed] MASSIVE_KEY not set — ES feed disabled'); }

  // ---- auth: founder-only (ticket must carry r:"admin"; see ws-ticket.js) ---
  function founder(req, res, next) {
    const tok = req.query.s
      || req.headers['x-pbj-session']
      || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    const sess = verifyToken(tok);
    if (!sess) return res.status(401).json({ error: 'unauthorized' });
    if (sess.r !== 'admin' && sess.u !== 'dev') return res.status(403).json({ error: 'founder only' });
    req.sess = sess; next();
  }

  // ---- live state -----------------------------------------------------------
  const S = {
    ticker: (process.env.ES_TICKER || '').trim().toUpperCase() || null,
    discovered: null,            // { ticker, last_trade_date, tick }
    sockStatus: 'idle',
    lastMsg: 0,
    scaleT: 0,                   // divisor for trade prices: 0 = not yet inferred
    scaleQ: 0,                   // divisor for quote prices
    anchor: 0,                   // REST last-trade price (decimal) used to infer scale
    // tape
    bid: 0, ask: 0, bs: 0, as: 0, qt: 0,
    last: 0, lastT: 0,
    cvd: 0, bigCvd: 0, vol: 0, buyVol: 0, sellVol: 0,
    sessionDate: null,           // ET trading date (18:00 ET boundary)
    ladder: new Map(),           // priceKey -> { b, s, v }  (0.25-pt native buckets)
    recent: [],                  // [{t,p,s,d,bp,ap}] ring, newest last
    bars: [],                    // 1s bars ring [{t,o,h,l,c,v,d}]
  };
  const clients = new Set();     // SSE responses
  let pending = [];              // trades since last flush

  // ---- session boundary: Globex day rolls at 18:00 ET -----------------------
  function etNow() {
    const p = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York', year: 'numeric', month: '2-digit',
      day: '2-digit', hour: '2-digit', hour12: false,
    }).formatToParts(new Date());
    const o = {}; for (const x of p) o[x.type] = x.value;
    return { d: `${o.year}-${o.month}-${o.day}`, h: +o.hour };
  }
  function sessionDate() { const t = etNow(); return t.h >= 18 ? t.d + '+1' : t.d; }
  function resetSession() {
    S.cvd = 0; S.bigCvd = 0; S.vol = 0; S.buyVol = 0; S.sellVol = 0;
    S.ladder.clear(); S.recent = []; S.bars = [];
    S.sessionDate = sessionDate();
    storeDirty = true;   // persist the fresh session row immediately
    console.log('[es-feed] session reset →', S.sessionDate);
  }

  // ---- REST helpers ----------------------------------------------------------
  async function mget(path) {
    const r = await fetch(`${API}${path}`, { headers: { Authorization: `Bearer ${KEY}`, Accept: 'application/json' } });
    const j = await r.json().catch(() => null);
    if (!r.ok) throw new Error(`HTTP ${r.status}${j && (j.error || j.message) ? ' — ' + (j.error || j.message) : ''}`);
    return j;
  }

  // Front month (v2): the v1 query used server-side sort/type filters the contracts
  // API rejected (REST /trades worked fine, so auth/plan were never the issue). Now we
  // pull the active board plainly and do ALL filtering client-side:
  //   single contracts only  = ticker matches  <PRODUCT><month code><year>  (e.g. ESU6)
  //     — this shape excludes spreads/combos like ESU6-ESZ6 by construction
  //   front month            = smallest days-to-maturity that is still ≥ MIN_DTM
  // ES_TICKER env still overrides everything (and disables auto-roll).
  const SINGLE_RE = new RegExp('^' + PRODUCT + '[FGHJKMNQUVXZ]\\d{1,2}$');
  async function resolveTicker() {
    if (process.env.ES_TICKER) { S.ticker = process.env.ES_TICKER.trim().toUpperCase(); return S.ticker; }
    let rows = [];
    try {
      const j = await mget(`/futures/v1/contracts?product_code=${encodeURIComponent(PRODUCT)}&active=true&limit=1000`);
      rows = (j && j.results) || [];
    } catch (e) {
      console.warn('[es-feed] contracts query (active=true) failed:', e.message, '— retrying plain');
      const j = await mget(`/futures/v1/contracts?product_code=${encodeURIComponent(PRODUCT)}&limit=1000`);
      rows = (j && j.results) || [];
    }
    const now = Date.now();
    const cands = rows
      .filter(c => c && c.ticker && SINGLE_RE.test(String(c.ticker).toUpperCase())
        && (c.type == null || c.type === 'single') && c.last_trade_date)
      .map(c => Object.assign({}, c, { _dtm: (new Date(c.last_trade_date + 'T21:00:00Z') - now) / 864e5 }))
      .filter(c => c._dtm >= MIN_DTM)
      .sort((a, b) => a._dtm - b._dtm);
    if (!cands.length) throw new Error(`no active ${PRODUCT} single contract found (${rows.length} rows returned)`);
    const c = cands[0];
    if (S.ticker !== c.ticker) console.log('[es-feed] front month →', c.ticker, '(last trade', c.last_trade_date + ')');
    S.discovered = { ticker: c.ticker, last_trade_date: c.last_trade_date, tick: num(c.trade_tick_size) || 0.25 };
    return (S.ticker = c.ticker);
  }

  // One REST last-trade → decimal price anchor for the WS scale inference.
  async function fetchAnchor() {
    try {
      const j = await mget(`/futures/v1/trades/${encodeURIComponent(S.ticker)}?limit=1&sort=timestamp.desc`);
      const p = num(j && j.results && j.results[0] && j.results[0].price);
      if (p > 0) { S.anchor = p; console.log('[es-feed] anchor', S.ticker, '=', p); }
    } catch (e) { console.warn('[es-feed] anchor fetch failed:', e.message); }
  }
  // Infer /1 vs /100 (vs /1000 just in case) against the anchor, once per field kind.
  function inferScale(raw) {
    if (!(raw > 0)) return 0;
    if (!(S.anchor > 0)) return 1;                       // no anchor — trust as-is
    let best = 1, bd = Infinity;
    for (const d of [1, 100, 1000]) {
      const diff = Math.abs(raw / d - S.anchor) / S.anchor;
      if (diff < bd) { bd = diff; best = d; }
    }
    return bd <= 0.2 ? best : 1;                          // within ±20% of anchor
  }
  const scaleTrade = (p) => { if (!S.scaleT) { S.scaleT = inferScale(p); if (S.scaleT !== 1) console.log('[es-feed] trade price divisor', S.scaleT); } return p / (S.scaleT || 1); };
  const scaleQuote = (p) => { if (!S.scaleQ) { S.scaleQ = inferScale(p); if (S.scaleQ !== 1) console.log('[es-feed] quote price divisor', S.scaleQ); } return p / (S.scaleQ || 1); };

  // ---- tape engine -----------------------------------------------------------
  function onQuote(m) {
    const bp = scaleQuote(num(m.bp)), ap = scaleQuote(num(m.ap));
    if (bp > 0) { S.bid = bp; S.bs = num(m.bs); }
    if (ap > 0) { S.ask = ap; S.as = num(m.as); }
    S.qt = num(m.t) || Date.now();
    S.lastMsg = Date.now();
  }
  function classify(p) {
    // aggressor vs prevailing BBO: at/above ask = buy (+1), at/below bid = sell (-1),
    // inside spread leans by midpoint, dead-center = 0.
    if (S.ask > 0 && p >= S.ask) return 1;
    if (S.bid > 0 && p <= S.bid) return -1;
    if (S.bid > 0 && S.ask > S.bid) {
      const mid = (S.bid + S.ask) / 2;
      if (p > mid) return 1;
      if (p < mid) return -1;
    }
    return 0;
  }
  // Core accumulation, shared by the live socket (d from BBO) and the REST gap
  // backfill (d from tick rule). `live` controls whether the print fans out to SSE.
  function applyTrade(p, s, t, d, live) {
    if (S.sessionDate !== sessionDate()) resetSession();
    S.last = p; S.lastT = t;
    S.vol += s;
    if (d > 0) { S.cvd += s; S.buyVol += s; if (s >= BIG_SIZE) S.bigCvd += s; }
    else if (d < 0) { S.cvd -= s; S.sellVol += s; if (s >= BIG_SIZE) S.bigCvd -= s; }
    // ladder at native tick buckets
    const key = p.toFixed(2);
    let L = S.ladder.get(key);
    if (!L) { L = { b: 0, s: 0, v: 0 }; S.ladder.set(key, L); }
    if (d > 0) L.b += s; else if (d < 0) L.s += s;
    L.v += s;
    // 1s bars
    const bt = Math.floor(t / BAR_MS) * BAR_MS;
    let bar = S.bars[S.bars.length - 1];
    if (!bar || bar.t !== bt) {
      bar = { t: bt, o: p, h: p, l: p, c: p, v: 0, d: 0 };
      S.bars.push(bar);
      while (S.bars.length > BARS_KEEP) S.bars.shift();
    }
    if (p > bar.h) bar.h = p; if (p < bar.l) bar.l = p;
    bar.c = p; bar.v += s; bar.d += d * s;
    // rings + SSE batch
    const row = { t, p, s, d, bp: S.bid, ap: S.ask, bs: S.bs, as: S.as };
    S.recent.push(row); while (S.recent.length > RECENT_MAX) S.recent.shift();
    if (live) pending.push(row);
    storeDirty = true;
  }
  function onTrade(m) {
    const p = scaleTrade(num(m.p)); if (!(p > 0)) return;
    const s = num(m.s) || 0; if (!(s > 0)) return;
    const t = num(m.t) || Date.now();
    S.lastMsg = Date.now();
    applyTrade(p, s, t, classify(p), true);
  }

  // ---- session persistence (Postgres, optional) + REST gap backfill ----------
  // Counters/ladder/ring survive worker restarts, so CVD really runs from the
  // Globex open. On boot with saved state from the SAME session, the downtime gap
  // is backfilled from Massive's REST trades (tick-rule classified — no BBO in
  // history; gaps are ~a minute, so the approximation is immaterial).
  let pgPool = null, storeDirty = false;
  function initStore() {
    const url = (process.env.DATABASE_URL || '').trim();
    if (!url) { console.log('[es-feed] DATABASE_URL not set — session persistence OFF'); return false; }
    let Pool; try { ({ Pool } = require('pg')); } catch { console.warn('[es-feed] "pg" missing — session persistence OFF'); return false; }
    pgPool = new Pool({ connectionString: url, max: 2 });
    pgPool.on('error', (e) => console.warn('[es-feed] pg pool error:', e.message));
    return true;
  }
  async function storeInit() {
    if (!initStore()) return;
    try {
      await pgPool.query(`CREATE TABLE IF NOT EXISTS es_session (
        ticker text NOT NULL, session text NOT NULL, data jsonb NOT NULL,
        updated timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (ticker, session))`);
      await pgPool.query(`DELETE FROM es_session WHERE updated < now() - interval '3 days'`);
    } catch (e) { console.warn('[es-feed] store init failed:', e.message); pgPool = null; }
  }
  async function storeLoad() {
    if (!pgPool || !S.ticker) return null;
    try {
      const r = await pgPool.query(`SELECT data FROM es_session WHERE ticker=$1 AND session=$2`, [S.ticker, S.sessionDate]);
      return (r.rows && r.rows[0] && r.rows[0].data) || null;
    } catch (e) { console.warn('[es-feed] store load failed:', e.message); return null; }
  }
  function restoreState(d) {
    try {
      S.cvd = num(d.cvd); S.bigCvd = num(d.bigCvd); S.vol = num(d.vol);
      S.buyVol = num(d.buyVol); S.sellVol = num(d.sellVol);
      S.last = num(d.last) || 0; S.lastT = num(d.lastT) || 0;
      S.ladder = new Map((d.ladder || []).map(r => [(+r[0]).toFixed(2), { b: num(r[1]), s: num(r[2]), v: num(r[3]) }]));
      S.recent = Array.isArray(d.recent) ? d.recent.slice(-RECENT_MAX) : [];
      console.log(`[es-feed] restored session ${S.sessionDate}: vol ${S.vol}, cvd ${S.cvd}, ladder ${S.ladder.size} px, ring ${S.recent.length}`);
      return true;
    } catch (e) { console.warn('[es-feed] restore failed:', e.message); return false; }
  }
  async function storeSave() {
    if (!pgPool || !S.ticker || !storeDirty) return;
    storeDirty = false;
    const data = {
      cvd: S.cvd, bigCvd: S.bigCvd, vol: S.vol, buyVol: S.buyVol, sellVol: S.sellVol,
      last: S.last, lastT: S.lastT,
      ladder: [...S.ladder].map(([k, v]) => [+k, v.b, v.s, v.v]),
      recent: S.recent.slice(-RECENT_MAX),
    };
    try {
      await pgPool.query(
        `INSERT INTO es_session (ticker, session, data, updated) VALUES ($1,$2,$3,now())
         ON CONFLICT (ticker, session) DO UPDATE SET data=$3, updated=now()`,
        [S.ticker, S.sessionDate, JSON.stringify(data)]);
    } catch (e) { console.warn('[es-feed] store save failed:', e.message); }
  }
  // Backfill the restart gap from REST history. Tick-rule side classification.
  async function backfillGap(sinceMs) {
    if (!(sinceMs > 0)) return;
    try {
      let path = `/futures/v1/trades/${encodeURIComponent(S.ticker)}?timestamp.gt=${Math.floor(sinceMs * 1e6)}&sort=timestamp.asc&limit=5000`;
      let pages = 0, prev = S.last || 0, n = 0;
      while (path && pages < 6) {
        const j = await mget(path);
        const rows = (j && j.results) || [];
        for (const r of rows) {
          const p = num(r.price), s = num(r.size), t = Math.round(num(r.timestamp) / 1e6);
          if (!(p > 0 && s > 0)) continue;
          const d = prev > 0 ? (p > prev ? 1 : (p < prev ? -1 : 0)) : 0;
          applyTrade(p, s, t, d, false);
          prev = p; n++;
        }
        path = (j && j.next_url) ? String(j.next_url).replace(API, '') : null;
        pages++;
        if (!rows.length) break;
      }
      if (n) console.log(`[es-feed] backfilled ${n} trades from REST (downtime gap)`);
    } catch (e) { console.warn('[es-feed] backfill failed:', e.message); }
  }
  setInterval(storeSave, SAVE_MS);

  // ---- Massive socket --------------------------------------------------------
  let ws = null, wsTries = 0, subbed = '';
  function subscribe() {
    if (!ws || ws.readyState !== 1 || !S.ticker) return;
    const want = `T.${S.ticker},Q.${S.ticker}`;
    if (subbed && subbed !== want) { try { ws.send(JSON.stringify({ action: 'unsubscribe', params: subbed })); } catch {} }
    try { ws.send(JSON.stringify({ action: 'subscribe', params: want })); subbed = want; console.log('[es-feed] subscribe', want); } catch {}
  }
  function connect() {
    if (!KEY) return;
    S.sockStatus = 'connecting';
    console.log('[es-feed] connect →', WS_URL);
    ws = new WebSocket(WS_URL);
    ws.on('open', () => { S.sockStatus = 'open'; wsTries = 0; try { ws.send(JSON.stringify({ action: 'auth', params: KEY })); } catch {} });
    ws.on('message', (buf) => {
      let arr; try { arr = JSON.parse(buf.toString()); } catch { return; }
      if (!Array.isArray(arr)) arr = [arr];
      for (const m of arr) {
        if (!m || typeof m !== 'object') continue;
        if (m.ev === 'status') {
          const st = String(m.status || '');
          if (st === 'auth_success') { console.log('[es-feed] auth ok'); subscribe(); }
          else if (st === 'auth_failed') { console.error('[es-feed] AUTH FAILED:', m.message); S.sockStatus = 'auth_failed'; }
          else if (m.message) console.log('[es-feed] status:', m.message);
          continue;
        }
        if (m.ev === 'Q') onQuote(m);
        else if (m.ev === 'T') onTrade(m);
      }
    });
    ws.on('error', (e) => { S.sockStatus = 'error'; console.error('[es-feed] ws error', (e && e.message) || e); });
    ws.on('close', () => {
      S.sockStatus = 'closed'; subbed = '';
      const delay = [1000, 2000, 5000, 10000][wsTries++] || 15000;
      setTimeout(connect, delay);
    });
  }

  // roll check: re-resolve the front month once an hour; resubscribe on change.
  setInterval(async () => {
    if (!KEY || process.env.ES_TICKER) return;
    const prev = S.ticker;
    try { await resolveTicker(); } catch { return; }
    if (prev !== S.ticker) { S.scaleT = 0; S.scaleQ = 0; await fetchAnchor(); resetSession(); subscribe(); }
  }, 3600_000);

  // ---- SSE fan-out -----------------------------------------------------------
  function snapshot() {
    const ladder = [];
    for (const [k, v] of S.ladder) ladder.push([+k, v.b, v.s]);
    ladder.sort((a, b) => b[0] - a[0]);
    return {
      type: 'init', ticker: S.ticker, session: S.sessionDate,
      last: S.last, bid: S.bid, ask: S.ask, bs: S.bs, as: S.as,
      cvd: S.cvd, bigCvd: S.bigCvd, vol: S.vol, buyVol: S.buyVol, sellVol: S.sellVol,
      bigSize: BIG_SIZE, ladder, recent: S.recent.slice(-RECENT_MAX),
    };
  }
  setInterval(() => {
    if (!pending.length || !clients.size) { pending = []; return; }
    const payload = `data: ${JSON.stringify({
      type: 'tape', trades: pending,
      cvd: S.cvd, bigCvd: S.bigCvd, vol: S.vol,
      bid: S.bid, ask: S.ask, bs: S.bs, as: S.as, last: S.last,
    })}\n\n`;
    pending = [];
    for (const res of clients) { try { res.write(payload); } catch {} }
  }, FLUSH_MS);

  // ---- routes -----------------------------------------------------------------
  app.get('/es/health', (_req, res) => {
    res.json({
      ok: true, enabled: !!KEY, socket: S.sockStatus, ticker: S.ticker,
      session: S.sessionDate, lastMsgAgeMs: S.lastMsg ? Date.now() - S.lastMsg : null,
      last: S.last, bid: S.bid, ask: S.ask,
      scale: { trade: S.scaleT || null, quote: S.scaleQ || null, anchor: S.anchor || null },
      vol: S.vol, cvd: S.cvd, bigCvd: S.bigCvd, viewers: clients.size,
    });
  });
  app.get('/es/state', founder, (_req, res) => { res.set('Cache-Control', 'no-store'); res.json(snapshot()); });
  app.get('/es/stream', founder, (req, res) => {
    res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.flushHeaders && res.flushHeaders();
    res.write('retry: 3000\n\n');
    res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
    clients.add(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
    req.on('close', () => { clearInterval(ping); clients.delete(res); });
  });

  // ---- boot -------------------------------------------------------------------
  (async () => {
    if (!KEY) return;
    S.sessionDate = sessionDate();
    try { await resolveTicker(); } catch (e) { console.error('[es-feed] ticker resolve failed:', e.message); S.ticker = S.ticker || 'ES?'; }
    await fetchAnchor();
    // restore this session's state (if any), then backfill the restart gap from REST
    await storeInit();
    const saved = await storeLoad();
    if (saved && restoreState(saved) && S.lastT > 0) await backfillGap(S.lastT);
    connect();
  })();

  console.log('[es-feed] mounted — /es/health /es/state /es/stream');
}

module.exports = { mount };
