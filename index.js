'use strict';
// ---------------------------------------------------------------------------
// PBJ Capital — GEX WebSocket worker
//
// ONE process holds ONE Unusual Whales WebSocket connection and fans the live
// dealer-gamma grid out to every dashboard user. UW API load becomes a function
// of how many SYMBOLS we track — NOT how many users connect. 50 users or 5,000,
// the upstream cost is identical.
//
// Source channel:  gex_strike_expiry:<TICKER>  (UW "Advanced" plan)
//   streams call/put gamma, vanna, charm by strike AND expiry, including the
//   bid/ask-classified (flow) gamma. Same "$ per 1% move" scaling as gex.js.
//
//   Flow GEX (cell)   = call_gamma_ask_vol + call_gamma_bid_vol + put_gamma_ask_vol + put_gamma_bid_vol
//   Standing (cell)   = call_gamma_oi + put_gamma_oi
//   Vanna (cell)      = call_vanna_ask_vol + call_vanna_bid_vol + put_vanna_ask_vol + put_vanna_bid_vol
//   spot              = payload.price (latest)
//
// NOT on this channel (kept on the existing REST/snapshot path, merged client-side):
//   Persist (settled ΔOI), the OI column, PURE (single-leg purity).
//
// Endpoints (all gated by the same pbj_session HMAC as gex.js, unless SESSION_SECRET unset):
//   GET /health                      — no auth; connection + per-symbol liveness
//   GET /gex?symbol=SPX&exps=a,b     — current assembled grid (same shape gex.js returns for live fields)
//   GET /seed?symbol=SPX             — { history } for the dashboard's momentum buffer (instant ROC%)
//   GET /stream?symbol=SPX           — SSE; pushes the assembled grid on change (real-time, no polling)
//
// ENV:
//   UW_KEY           (required)  UW API token (Advanced plan, WS-enabled)
//   SESSION_SECRET   (required for prod)  same secret gex.js uses to sign pbj_session
//   ALLOWED_ORIGIN   (default *) e.g. https://pbjcapital.net   — CORS origin for the dashboard
//   SYMBOLS          (default "SPX,SPY,QQQ,IWM,NDX,DIA,AAPL,NVDA,TSLA")  always-warm core,
//                    subscribed at boot. ANY other ticker is auto-subscribed on demand.
//   MAX_DYNAMIC      (default 60)      cap on concurrent on-demand subscriptions
//   IDLE_TTL_MS      (default 600000)  drop an on-demand sub after this long idle (10 min)
//   PORT             (default 8080)  Railway injects this
//   N_EXPIRIES       (default 5)     nearest expiries returned by default
// ---------------------------------------------------------------------------

const crypto  = require('crypto');
const express = require('express');
const cors    = require('cors');
const WebSocket = require('ws');
const { Socket } = require('phoenix');

// ---- config ----------------------------------------------------------------
const UW_KEY         = process.env.UW_KEY;
const PORT           = parseInt(process.env.PORT || '8080', 10);
const SESSION_SECRET = process.env.SESSION_SECRET || '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
// Always-warm CORE: subscribed at boot, never evicted (instant load for these).
const CORE           = (process.env.SYMBOLS || 'SPX,SPY,QQQ,IWM,NDX,DIA,AAPL,NVDA,TSLA')
  .split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
const N_EXPIRIES     = parseInt(process.env.N_EXPIRIES || '5', 10);
// On-demand: ANY other ticker a member opens is auto-subscribed, then dropped once
// idle. This makes the whole optionable universe available without holding thousands
// of always-on streams.
const MAX_DYNAMIC    = parseInt(process.env.MAX_DYNAMIC || '60', 10);     // cap on concurrent on-demand subs
const IDLE_TTL_MS    = parseInt(process.env.IDLE_TTL_MS || '600000', 10); // drop a dynamic sub after this idle (10 min)

const INDEX_LIKE  = new Set(['SPX', 'NDX', 'RUT', 'SPY', 'QQQ', 'IWM', 'DIA']);
const BAND_INDEX  = 0.03;   // ±3% strike window for index-like
const BAND_STOCK  = 0.35;   // ±35% for single names
const HIST_KEEP_MS = 45 * 60000;   // rolling momentum history retained
const HIST_STEP_MS = 60000;        // snapshot the flow grid once a minute
const SSE_FLUSH_MS = 1000;         // push to SSE clients at most once a second

if (!UW_KEY) { console.error('FATAL: UW_KEY not set'); process.exit(1); }
if (!SESSION_SECRET) console.warn('[warn] SESSION_SECRET unset — auth is OPEN (dev mode). Set it in production.');

const num = (x) => { const n = parseFloat(x); return Number.isFinite(n) ? n : 0; };

// ---- live state ------------------------------------------------------------
// state[sym] = { spot, ts, cells: Map("strike|expiry" -> {flow,standing,vanna}), dirty, lastMsg }
const state = {};
const S = (sym) => state[sym] || (state[sym] = { spot: 0, ts: 0, cells: new Map(), dirty: false, lastMsg: 0 });
const momHist = {};   // sym -> [{ t, gex: { "strike|expiry": flow } }]   (matches the dashboard buffer shape)

function ingest(sym, d) {
  if (!d || d.strike == null || !d.expiry) return;
  const st = S(sym);
  const price = num(d.price); if (price > 0) st.spot = price;
  st.ts = d.timestamp || Date.now();
  st.lastMsg = Date.now();
  const flow =
    num(d.call_gamma_ask_vol) + num(d.call_gamma_bid_vol) +
    num(d.put_gamma_ask_vol)  + num(d.put_gamma_bid_vol);
  const standing = num(d.call_gamma_oi) + num(d.put_gamma_oi);
  const vanna =
    num(d.call_vanna_ask_vol) + num(d.call_vanna_bid_vol) +
    num(d.put_vanna_ask_vol)  + num(d.put_vanna_bid_vol);
  st.cells.set(`${num(d.strike)}|${d.expiry}`, { flow, standing, vanna });
  st.dirty = true;
}

// ---- grid assembly (mirrors gex.js output for the LIVE fields) -------------
const bandFor = (sym) => (INDEX_LIKE.has(sym) ? BAND_INDEX : BAND_STOCK);
function labelFor(dateStr) {
  const d = new Date(dateStr + 'T00:00:00Z');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}
function dteFor(dateStr) {
  const now = new Date();
  const today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const d = new Date(dateStr + 'T00:00:00Z').getTime();
  return Math.round((d - today) / 86400000);
}

function assembleGrid(sym, expCsv) {
  const st = state[sym] || { spot: 0, ts: 0, cells: new Map() };
  const spot = st.spot;
  const keys = [...st.cells.keys()];

  // future expiries, sorted ascending; honor &exps= else nearest N
  let exps = [...new Set(keys.map(k => k.split('|')[1]))].filter(Boolean)
    .filter(e => dteFor(e) >= 0).sort();
  let chosen = null;
  if (expCsv) {
    const want = new Set(expCsv.split(',').map(s => s.trim()));
    chosen = exps.filter(e => want.has(e)).slice(0, 8);
  }
  if (!chosen || !chosen.length) chosen = exps.slice(0, N_EXPIRIES);
  const expRows = chosen.map(e => ({ date: e, label: labelFor(e), dte: dteFor(e) }));

  // strikes within band, high -> low (matches the dashboard layout)
  const band = bandFor(sym);
  const strikes = [...new Set(keys.map(k => num(k.split('|')[0])))]
    .filter(k => (spot > 0 ? Math.abs(k - spot) / spot <= band : true))
    .sort((a, b) => b - a);

  const cell = (k, e) => st.cells.get(`${k}|${e}`) || null;
  const gex   = strikes.map(k => expRows.map(er => { const c = cell(k, er.date); return c ? c.flow : 0; }));
  const naive = strikes.map(k => expRows.map(er => { const c = cell(k, er.date); return c ? c.standing : 0; }));
  const vanna = strikes.map(k => expRows.map(er => { const c = cell(k, er.date); return c ? c.vanna : 0; }));

  const net = gex.map(r => r.reduce((a, b) => a + b, 0));          // flow net per strike
  const netGex = net.reduce((a, b) => a + b, 0);
  let pk = 0; net.forEach((v, i) => { if (Math.abs(v) > Math.abs(net[pk])) pk = i; });
  const peakStrike = strikes.length ? strikes[pk] : null;

  // standing net per strike drives regime / flip / node
  const stdNet = naive.map(r => r.reduce((a, b) => a + b, 0));

  // regime: net standing gamma within ±1% of spot (stable, OI-based)
  let rs = 0, ra = 0;
  strikes.forEach((k, i) => {
    if (spot > 0 && Math.abs(k - spot) / spot > 0.01) return;
    rs += stdNet[i]; ra += Math.abs(stdNet[i]);
  });
  const regime = (ra > 0 && Math.abs(rs) / ra >= 0.15) ? (rs > 0 ? 'long' : 'short') : 'flat';

  // flip: cumulative standing gamma sign change, scanning high strike -> low.
  // (Simpler than gex.js's IV-repriced flip — the stream doesn't carry IV — but
  //  tracks the same zero-gamma boundary for the header.)
  let flip = null, cum = 0, prevCum = 0, prevK = null;
  for (let i = 0; i < strikes.length; i++) {
    prevCum = cum; cum += stdNet[i];
    if (prevK !== null && ((prevCum >= 0 && cum < 0) || (prevCum < 0 && cum >= 0))) {
      flip = Math.round(((strikes[i] + prevK) / 2) * 100) / 100; break;
    }
    prevK = strikes[i];
  }

  // node: standing-magnitude, spot-proximity-weighted centre of mass within ±2.5%
  let ws = 0, ks = 0;
  strikes.forEach((k, i) => {
    if (spot > 0 && Math.abs(k - spot) / spot > 0.025) return;
    const prox = Math.exp(-Math.pow((k - spot) / ((spot * 0.012) || 1), 2));
    const w = Math.abs(stdNet[i]) * prox;
    ws += w; ks += k * w;
  });
  const node = ws > 0 ? Math.round((ks / ws) * 10) / 10 : spot;

  return {
    symbol: sym, spot, ts: st.ts, source: 'ws',
    strikes, expiries: expRows,
    gex, naive, vanna,
    net, netGex, peakStrike, flip, regime, node,
    nodePull: Math.round((node - spot) * 10) / 10,
  };
}

// ---- auth (same HMAC scheme as gex.js verifySession) -----------------------
const b64url = (b) => Buffer.from(b).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const hsign  = (p, s) => b64url(crypto.createHmac('sha256', s).update(p).digest());
function verifyToken(tok) {
  if (!SESSION_SECRET) return { u: 'dev' };               // dev: open
  if (!tok || tok.indexOf('.') < 0) return null;
  const [p, sig] = tok.split('.'); const ex = hsign(p, SESSION_SECRET);
  if (!sig || sig.length !== ex.length) return null;
  try { if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(ex))) return null; } catch { return null; }
  let o; try { o = JSON.parse(Buffer.from(p.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString()); } catch { return null; }
  if (!(o.exp && o.exp >= Math.floor(Date.now() / 1000))) return null;
  return o;
}
function auth(req, res, next) {
  // EventSource can't set headers, so the dashboard passes the token as ?s= for SSE.
  const tok = req.query.s
    || req.headers['x-pbj-session']
    || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const sess = verifyToken(tok);
  if (!sess) return res.status(401).json({ error: 'unauthorized' });
  req.sess = sess; next();
}

// ---- HTTP / SSE ------------------------------------------------------------
const app = express();
app.use(cors({ origin: ALLOWED_ORIGIN === '*' ? true : ALLOWED_ORIGIN.split(',').map(s => s.trim()), credentials: true }));

const sse = {};   // sym -> Map(res -> expCsv)

app.get('/health', (_req, res) => {
  const now = Date.now();
  const subs = Object.keys(subMeta).sort();
  const symbols = {};
  for (const sym of subs) {
    const st = state[sym];
    symbols[sym] = {
      core: !!subMeta[sym].core,
      cells: st ? st.cells.size : 0,
      spot: st ? st.spot : 0,
      lastMsgAgeMs: (st && st.lastMsg) ? now - st.lastMsg : null,
      viewers: (sse[sym] && sse[sym].size) || 0,
    };
  }
  const dynamic = subs.filter(s => !subMeta[s].core).length;
  res.json({
    ok: true, socket: socketStatus, uptimeSec: Math.round(process.uptime()),
    subs: { total: subs.length, core: subs.length - dynamic, dynamic, maxDynamic: MAX_DYNAMIC },
    symbols,
  });
});

app.get('/gex', auth, (req, res) => {
  const sym = String(req.query.symbol || 'SPX').toUpperCase();
  ensureSub(sym);
  res.set('Cache-Control', 'no-store');
  res.json(assembleGrid(sym, req.query.exps));
});

app.get('/seed', auth, (req, res) => {
  const sym = String(req.query.symbol || 'SPX').toUpperCase();
  ensureSub(sym);
  res.set('Cache-Control', 'no-store');
  res.json({ symbol: sym, history: momHist[sym] || [] });
});

app.get('/stream', auth, (req, res) => {
  const sym  = String(req.query.symbol || 'SPX').toUpperCase();
  const exps = req.query.exps ? String(req.query.exps) : '';
  ensureSub(sym);
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-store',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders && res.flushHeaders();
  res.write(`retry: 3000\n\n`);
  res.write(`data: ${JSON.stringify(assembleGrid(sym, exps))}\n\n`);   // initial snapshot
  (sse[sym] || (sse[sym] = new Map())).set(res, exps);
  const ping = setInterval(() => { try { res.write(`: ping\n\n`); } catch {} }, 25000);
  req.on('close', () => { clearInterval(ping); const m = sse[sym]; if (m) m.delete(res); });
});

// flush dirty grids to SSE subscribers ~1/sec
setInterval(() => {
  for (const sym of Object.keys(sse)) {
    const subs = sse[sym]; if (!subs || !subs.size) continue;
    const st = state[sym]; if (!st || !st.dirty) continue;
    st.dirty = false;
    const byExps = new Map();   // assemble once per distinct expiry set this tick
    for (const [res, exps] of subs) {
      let payload = byExps.get(exps);
      if (!payload) { payload = `data: ${JSON.stringify(assembleGrid(sym, exps))}\n\n`; byExps.set(exps, payload); }
      try { res.write(payload); } catch {}
    }
  }
}, SSE_FLUSH_MS);

// snapshot the flow grid once a minute for the momentum seed (45-min rolling)
setInterval(() => {
  const t = Date.now();
  for (const sym of Object.keys(state)) {
    const st = state[sym]; if (!st.cells.size) continue;
    const gex = {}; for (const [k, v] of st.cells) gex[k] = v.flow;
    const arr = momHist[sym] || (momHist[sym] = []);
    arr.push({ t, gex });
    const cut = t - HIST_KEEP_MS;
    while (arr.length && arr[0].t < cut) arr.shift();
  }
}, HIST_STEP_MS);

// ---- UW WebSocket (Phoenix channels) ---------------------------------------
let socketStatus = 'connecting';
const socket = new Socket('wss://api.unusualwhales.com/socket', {
  params: { token: UW_KEY },
  transport: WebSocket,
  heartbeatIntervalMs: 30000,
  reconnectAfterMs: (tries) => [1000, 2000, 5000, 10000][tries - 1] || 10000,
});
socket.onOpen(()  => { socketStatus = 'open';   console.log('[socket] open'); });
socket.onError((e) => { socketStatus = 'error'; console.error('[socket] error', (e && e.message) || e); });
socket.onClose(() => { socketStatus = 'closed'; console.warn('[socket] closed'); });
socket.connect();

// ---- subscription manager (core + on-demand) -------------------------------
const channels = {};   // sym -> Phoenix channel
const subMeta  = {};   // sym -> { core, lastReq }

function joinSym(sym, core) {
  if (channels[sym]) { if (core && subMeta[sym]) subMeta[sym].core = true; return; }
  if (!core) {
    const dyn = Object.keys(subMeta).filter(s => !subMeta[s].core).length;
    if (dyn >= MAX_DYNAMIC) evictLRU();        // make room for the new one
  }
  const topic = `gex_strike_expiry:${sym}`;
  const ch = socket.channel(topic, {});
  // Catch EVERY event on the channel (robust to the exact push event name);
  // ingest anything that looks like a strike row.
  ch.onMessage = (event, payload) => { if (payload && payload.strike != null) ingest(sym, payload); return payload; };
  ch.join()
    .receive('ok',      () => console.log('[join ok]', topic, core ? '(core)' : '(on-demand)'))
    .receive('error',   (r) => console.error('[join error]', topic, r))
    .receive('timeout', () => console.error('[join timeout]', topic));
  channels[sym] = ch;
  subMeta[sym]  = { core: !!core, lastReq: Date.now() };
}

// Mark a symbol as wanted now; subscribe if we aren't already.
function ensureSub(sym) {
  if (!sym) return;
  if (subMeta[sym]) subMeta[sym].lastReq = Date.now();
  else joinSym(sym, false);
}

function dropSym(sym) {
  const ch = channels[sym];
  if (ch) { try { ch.leave(); } catch {} }
  delete channels[sym]; delete subMeta[sym]; delete state[sym]; delete momHist[sym];
}

// Evict the least-recently-requested on-demand sub that has no active SSE viewers.
function evictLRU() {
  let victim = null, oldest = Infinity;
  for (const s of Object.keys(subMeta)) {
    if (subMeta[s].core) continue;
    if (sse[s] && sse[s].size) continue;       // someone's streaming it — keep
    if (subMeta[s].lastReq < oldest) { oldest = subMeta[s].lastReq; victim = s; }
  }
  if (victim) { console.log('[evict]', victim); dropSym(victim); }
}

// Idle sweep: drop on-demand subs no one has touched in IDLE_TTL_MS (and no viewers).
setInterval(() => {
  const now = Date.now();
  for (const s of Object.keys(subMeta)) {
    if (subMeta[s].core) continue;
    if (sse[s] && sse[s].size) continue;
    if (now - subMeta[s].lastReq > IDLE_TTL_MS) { console.log('[idle drop]', s); dropSym(s); }
  }
}, 60000);

// boot: warm the core set
for (const sym of CORE) joinSym(sym, true);

app.listen(PORT, () => console.log(`[http] listening on :${PORT} — core: ${CORE.join(', ')} | on-demand: any other ticker (max ${MAX_DYNAMIC})`));
