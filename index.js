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

// ---- config ----------------------------------------------------------------
const UW_KEY         = (process.env.UW_KEY || '').trim();   // trim: kill any stray whitespace/newline from the env paste
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
  const price = num(d.price); if (price > 0) { st.spot = price; pushSpot(sym); }   // fast spot push — beat the 1s grid flush
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
      joined: !!(channels[sym] && channels[sym].joined),
      cells: st ? st.cells.size : 0,
      spot: st ? st.spot : 0,
      lastMsgAgeMs: (st && st.lastMsg) ? now - st.lastMsg : null,
      viewers: (sse[sym] && sse[sym].size) || 0,
    };
  }
  const dynamic = subs.filter(s => !subMeta[s].core).length;
  const joinedCount = subs.filter(s => channels[s] && channels[s].joined).length;
  res.json({
    ok: true, socket: socketStatus, uptimeSec: Math.round(process.uptime()),
    rev: 'uw-proto-2',
    joins: { ok: joinedCount, total: subs.length },
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

// ---- fast spot push --------------------------------------------------------
// The heavy grid flushes ~1/sec; that throttle + the gamma cadence is the "spot
// delay" users feel. So push a TINY spot-only SSE event (a named `spot` event)
// the moment the price changes — independent of the grid. Debounced so a busy
// symbol can't exceed ~SPOT_MIN_MS. The grid (and gex math) is unchanged.
const SPOT_MIN_MS = 150;        // ≤ ~6–7 spot ticks/sec per symbol
const spotPush = {};            // sym -> { last, timer }
function pushSpot(sym) {
  const subs = sse[sym]; if (!subs || !subs.size) return;
  const rec = spotPush[sym] || (spotPush[sym] = { last: 0, timer: null });
  const fire = () => {
    rec.last = Date.now(); rec.timer = null;
    const st = state[sym]; if (!st || !(st.spot > 0)) return;
    const data = `event: spot\ndata: ${JSON.stringify({ symbol: sym, spot: st.spot, t: st.ts })}\n\n`;
    for (const res of subs.keys()) { try { res.write(data); } catch {} }
  };
  const gap = Date.now() - rec.last;
  if (gap >= SPOT_MIN_MS) fire();
  else if (!rec.timer) rec.timer = setTimeout(fire, SPOT_MIN_MS - gap);
}

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

// ============================================================================
// EDGE — live option_trades (rich/cheap detector) + alert tape
// (unusual activity = flow-alerts ; dark pool = parameterized). Same UW socket,
// same pbj_session gate, fanned out over SSE. No extra REST, no polling.
//   GET /trades?symbol=SPX&s=<ticket>  — SSE; per-print stream for one symbol
//   GET /alerts?s=<ticket>             — SSE; global tape: flow-alerts (+ dark pool)
// The browser does the Black-Scholes / rich-cheap math; we just relay the fields.
// ============================================================================
const TRADES_KEEP = parseInt(process.env.TRADES_KEEP || '180', 10);   // prints retained per symbol
const ALERTS_KEEP = parseInt(process.env.ALERTS_KEEP || '150', 10);   // alerts retained (global ring)
// UW socket channels (confirmed via /api/socket): option_trades, option_trades:<T>,
// flow-alerts, price:<T>, news, lit_trades, off_lit_trades, gex, gex_strike,
// gex_strike_expiry, market_tide, net_flow, interval_flow, contract_screener,
// trading_halts, custom_alerts. Dark pool = off_lit_trades (off-lit/TRF prints).
const DARKPOOL_CHANNEL = (process.env.DARKPOOL_CHANNEL || 'off_lit_trades').trim();   // '' to disable
// off_lit_trades is the FULL dark-pool firehose (every print) — relay only notable
// blocks so the tape shows institutional size, not noise. notional = price × shares.
const DARKPOOL_MIN_NOTIONAL = parseFloat(process.env.DARKPOOL_MIN_NOTIONAL || '1000000');

const tradesBuf   = {};   // sym -> [print,…] newest last
const tradeSubs   = {};   // sym -> Set(res)
const tradeMeta   = {};   // sym -> { lastReq }
const tradeTopics = {};   // 'option_trades:SYM' -> SYM
const alertsBuf   = [];   // [{source,…alert}] global ring
const alertSubs   = new Set();
let   alertsJoined = false;   // flow-alerts + dark pool joined only while someone views /alerts

const tradeTopic = (sym) => `option_trades:${sym}`;
function ensureTrades(sym) {
  if (!sym) return; sym = sym.toUpperCase();
  if (tradeMeta[sym]) { tradeMeta[sym].lastReq = Date.now(); return; }
  tradeMeta[sym] = { lastReq: Date.now() };
  const topic = tradeTopic(sym); tradeTopics[topic] = sym;
  if (wsReady) wsSend({ channel: topic, msg_type: 'join' });
  console.log('[trades join \u2192]', topic);
}
function dropTrades(sym) {
  const topic = tradeTopic(sym);
  if (wsReady) wsSend({ channel: topic, msg_type: 'leave' });
  delete tradeTopics[topic]; delete tradeMeta[sym]; delete tradesBuf[sym];
}
// relay only the fields the tab needs; the rich/cheap + BS math runs client-side
function shapeTrade(sym, p) {
  const tags  = Array.isArray(p.tags) ? p.tags : [];
  const flags = Array.isArray(p.report_flags) ? p.report_flags : [];
  return {
    id: p.id, t: p.executed_at || p.created_at || Date.now(), sym,
    opt: p.option_symbol, type: String(p.option_type || '').charAt(0).toUpperCase(),
    strike: num(p.strike), expiry: p.expiry,
    price: num(p.price), size: num(p.size), premium: num(p.premium),
    spot: num(p.underlying_price), bid: num(p.nbbo_bid), ask: num(p.nbbo_ask),
    theo: num(p.theo), uwiv: num(p.implied_volatility),
    oi: num(p.open_interest), vol: num(p.volume), exch: p.exchange || '',
    side: tags.find(t => t === 'ask_side' || t === 'bid_side' || t === 'mid_side' || t === 'no_side') || '',
    dir:  tags.find(t => t === 'bullish' || t === 'bearish') || '',
    sweep: flags.includes('sweep') || tags.includes('sweep'),
  };
}
function onTrade(topic, p) {
  const sym = tradeTopics[topic] || (p.underlying_symbol ? String(p.underlying_symbol).toUpperCase() : null);
  if (!sym) return;
  if (tradeMeta[sym]) tradeMeta[sym].lastReq = Date.now();
  const row = shapeTrade(sym, p);
  const buf = tradesBuf[sym] || (tradesBuf[sym] = []);
  buf.push(row); if (buf.length > TRADES_KEEP) buf.shift();
  const subs = tradeSubs[sym];
  if (subs && subs.size) { const data = `data: ${JSON.stringify(row)}\n\n`; for (const res of subs) { try { res.write(data); } catch {} } }
}
function onAlert(source, p) {
  const row = Object.assign({ source, t: Date.now() }, p);
  alertsBuf.push(row); if (alertsBuf.length > ALERTS_KEEP) alertsBuf.shift();
  if (alertSubs.size) { const data = `data: ${JSON.stringify(row)}\n\n`; for (const res of alertSubs) { try { res.write(data); } catch {} } }
}
// off_lit_trades is the full dark-pool tape — keep only notable blocks, shape compact
function onDarkpool(p) {
  const price = num(p.price), size = num(p.size), notional = price * size;
  if (!(notional >= DARKPOOL_MIN_NOTIONAL)) return;
  onAlert('darkpool', {
    ticker: p.symbol, price, size, notional, volume: num(p.volume),
    bid: num(p.nbbo_bid), ask: num(p.nbbo_ask), sector: p.sector || '',
    executed_at: p.trf_executed_at || p.executed_at,
  });
}
// flow-alerts + dark pool are global firehoses — join only while someone views /alerts
function ensureAlerts() {
  if (alertsJoined) return; alertsJoined = true;
  if (wsReady) { wsSend({ channel: 'flow-alerts', msg_type: 'join' }); if (DARKPOOL_CHANNEL) wsSend({ channel: DARKPOOL_CHANNEL, msg_type: 'join' }); }
  console.log('[alerts join \u2192] flow-alerts' + (DARKPOOL_CHANNEL ? ' + ' + DARKPOOL_CHANNEL : ''));
}
function dropAlerts() {
  if (!alertsJoined || alertSubs.size) return; alertsJoined = false;
  if (wsReady) { wsSend({ channel: 'flow-alerts', msg_type: 'leave' }); if (DARKPOOL_CHANNEL) wsSend({ channel: DARKPOOL_CHANNEL, msg_type: 'leave' }); }
  console.log('[alerts leave] no viewers');
}

// SSE: live prints for one symbol (rich/cheap tab)
app.get('/trades', auth, (req, res) => {
  const sym = String(req.query.symbol || 'SPX').toUpperCase();
  ensureTrades(sym);
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders && res.flushHeaders();
  res.write(`retry: 3000\n\n`);
  for (const row of (tradesBuf[sym] || []).slice(-60)) res.write(`data: ${JSON.stringify(row)}\n\n`);   // backlog → instant paint
  (tradeSubs[sym] || (tradeSubs[sym] = new Set())).add(res);
  const ping = setInterval(() => { try { res.write(`: ping\n\n`); } catch {} }, 25000);
  req.on('close', () => { clearInterval(ping); const s = tradeSubs[sym]; if (s) s.delete(res); });
});

// SSE: global alert tape — unusual activity (flow-alerts) + dark pool (off_lit_trades)
app.get('/alerts', auth, (req, res) => {
  ensureAlerts();
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders && res.flushHeaders();
  res.write(`retry: 3000\n\n`);
  for (const row of alertsBuf.slice(-60)) res.write(`data: ${JSON.stringify(row)}\n\n`);
  alertSubs.add(res);
  const ping = setInterval(() => { try { res.write(`: ping\n\n`); } catch {} }, 25000);
  req.on('close', () => { clearInterval(ping); alertSubs.delete(res); setTimeout(dropAlerts, 5000); });
});

// idle sweep for on-demand trade subs (mirrors the gex idle policy)
setInterval(() => {
  const now = Date.now();
  for (const sym of Object.keys(tradeMeta)) {
    if (tradeSubs[sym] && tradeSubs[sym].size) continue;
    if (now - tradeMeta[sym].lastReq > IDLE_TTL_MS) { console.log('[trades idle drop]', sym); dropTrades(sym); }
  }
}, 60000);

// ---- UW WebSocket (raw ws; UW's own JSON protocol at /socket) ---------------
// UW is NOT standard Phoenix wire protocol. Per the official docs you connect to
// /socket?token=... then join a channel by sending a simple JSON object:
//   join   -> {"channel":"gex_strike_expiry:SPX","msg_type":"join"}
//   reply  -> ["gex_strike_expiry:SPX", {"response":{}, "status":"ok"}]
//   data   -> ["gex_strike_expiry:SPX", { ...strike row... }]   (during market hours)
// Keepalive is a plain WS ping; no app-level heartbeat frame is required.
const WS_URL = `wss://api.unusualwhales.com/socket?token=${UW_KEY}&vsn=2.0.0`;   // raw token, exactly like the wscat test that connected

let socketStatus = 'connecting';
let ws = null, wsReady = false, hbTimer = null, reconnectTimer = null, reconnectTries = 0, rawLogged = 0;
let refCtr = 0; const nextRef = () => String(++refCtr);

const channels   = {};   // sym -> { topic, joinRef }
const subMeta    = {};   // sym -> { core, lastReq }
const topicToSym = {};   // topic -> sym

function wsSend(arr) {
  if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(arr)); } catch (e) { console.error('[ws send]', e.message); } }
}

function joinChannel(sym) {
  const c = channels[sym]; if (!c) return;
  topicToSym[c.topic] = sym;
  wsSend({ channel: c.topic, msg_type: 'join' });
  if (c.priceTopic) {                              // dedicated price-tick feed (index symbols)
    topicToSym[c.priceTopic] = sym;
    wsSend({ channel: c.priceTopic, msg_type: 'join' });
  }
  console.log('[join \u2192]', c.topic, c.priceTopic ? '+ ' + c.priceTopic : '');
}

function joinSym(sym, core) {
  if (channels[sym]) { if (core) subMeta[sym].core = true; return; }
  if (!core) {
    const dyn = Object.keys(subMeta).filter(s => !subMeta[s].core).length;
    if (dyn >= MAX_DYNAMIC) evictLRU();
  }
  // Index symbols also subscribe the dedicated price-tick channel (lower latency than
  // the gamma-piggybacked price). If UW gates it, the join just errors and we fall back
  // to the gamma price — no regression. Stocks rely on their frequent gamma prints.
  channels[sym] = { topic: `gex_strike_expiry:${sym}`, priceTopic: INDEX_LIKE.has(sym) ? `price:${sym}` : null, joined: false };
  subMeta[sym]  = { core: !!core, lastReq: Date.now() };
  if (wsReady) joinChannel(sym);            // socket up -> join now; else joined on (re)connect
}

// Mark a symbol as wanted now; subscribe if we aren't already.
function ensureSub(sym) {
  if (!sym) return;
  if (subMeta[sym]) subMeta[sym].lastReq = Date.now();
  else joinSym(sym, false);
}

function dropSym(sym) {
  const c = channels[sym];
  if (c) {
    if (wsReady) wsSend({ channel: c.topic, msg_type: 'leave' });
    delete topicToSym[c.topic];
    if (c.priceTopic) { if (wsReady) wsSend({ channel: c.priceTopic, msg_type: 'leave' }); delete topicToSym[c.priceTopic]; }
  }
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

// Parse an incoming frame and ingest strike rows. Tolerant of the 5-element
// Phoenix array, a simplified [topic, payload], or an object form.
function handleFrame(m) {
  // UW frames are ["<channel>", <payload>]. A join/leave reply payload carries
  // {status,response}; a data row carries {strike, expiry, price, ...}.
  let topic, payload;
  if (Array.isArray(m) && m.length >= 2)            { topic = m[0]; payload = m[1]; }
  else if (m && typeof m === 'object' && m.channel) { topic = m.channel; payload = m.data || m.payload; }
  else return;
  if (!payload || typeof payload !== 'object') return;
  if (payload.status !== undefined && payload.strike === undefined) {   // join / leave ack
    const jsym = topicToSym[topic];
    if (payload.status !== 'ok') console.error('[join ERR]', topic, JSON.stringify(payload).slice(0, 160));
    else { if (jsym && channels[jsym]) channels[jsym].joined = true; console.log('[join ok]', topic); }
    return;
  }
  // edge tab: route non-GEX channels before the gex-cell ingest
  if (typeof topic === 'string') {
    if (topic.lastIndexOf('price:', 0) === 0) {                      // dedicated price tick → fast spot
      const psym = topicToSym[topic] || topic.slice(6).toUpperCase();
      const px = num(payload.price != null ? payload.price
               : payload.last  != null ? payload.last
               : payload.value != null ? payload.value
               : payload.p     != null ? payload.p : payload.close);
      if (psym && px > 0) { const st = S(psym); st.spot = px; st.lastMsg = Date.now(); pushSpot(psym); }
      return;
    }
    if (topic === 'flow-alerts' || topic.indexOf('flow-alerts') === 0) { onAlert('flow', payload); return; }
    if (DARKPOOL_CHANNEL && (topic === DARKPOOL_CHANNEL || topic.indexOf(DARKPOOL_CHANNEL) === 0)) { onDarkpool(payload); return; }
    if (topic.indexOf('option_trades') === 0) { onTrade(topic, payload); return; }
  }
  if (payload.strike == null) return;
  const sym = topicToSym[topic] || (payload.ticker ? String(payload.ticker).toUpperCase() : null);
  if (sym && channels[sym]) ingest(sym, payload);   // only ingest channels we still track
}

function connect() {
  socketStatus = 'connecting'; wsReady = false;
  console.log('[connect] \u2192', WS_URL.replace(/token=[^&]*/, 'token=***'), '| key len', UW_KEY.length);
  ws = new WebSocket(WS_URL);
  ws.on('open', () => {
    socketStatus = 'open'; wsReady = true; reconnectTries = 0;
    console.log('[socket] open');
    for (const sym of Object.keys(channels)) joinChannel(sym);   // (re)join everything we track
    for (const topic of Object.keys(tradeTopics)) wsSend({ channel: topic, msg_type: 'join' });   // (re)join trade subs
    if (alertsJoined) {                                                                            // re-join alert firehoses only if a viewer is connected
      wsSend({ channel: 'flow-alerts', msg_type: 'join' });
      if (DARKPOOL_CHANNEL) wsSend({ channel: DARKPOOL_CHANNEL, msg_type: 'join' });
    }
    if (hbTimer) clearInterval(hbTimer);
    hbTimer = setInterval(() => { try { ws.ping(); } catch {} }, 30000);   // WS-level keepalive
  });
  ws.on('message', (buf) => {
    let m; try { m = JSON.parse(buf.toString()); } catch { return; }
    if (rawLogged < 3) { rawLogged++; console.log('[raw msg]', JSON.stringify(m).slice(0, 300)); }
    handleFrame(m);
  });
  ws.on('error', (e) => { socketStatus = 'error'; console.error('[socket] error', (e && e.message) || e); });
  ws.on('close', (code, reason) => {
    socketStatus = 'closed'; wsReady = false;
    for (const s in channels) channels[s].joined = false;   // re-join + re-ack on reconnect
    if (hbTimer) { clearInterval(hbTimer); hbTimer = null; }
    console.warn('[socket] closed', code || '', reason ? reason.toString().slice(0, 120) : '');
    if (!reconnectTimer) {
      const delay = [1000, 2000, 5000, 10000][reconnectTries++] || 10000;
      reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
    }
  });
}

// boot: register the core set, then connect (channels join on 'open')
for (const sym of CORE) joinSym(sym, true);
connect();

app.listen(PORT, () => console.log(`[http] rev uw-proto-2 listening on :${PORT} — core: ${CORE.join(', ')} | on-demand: any other ticker (max ${MAX_DYNAMIC})`));
