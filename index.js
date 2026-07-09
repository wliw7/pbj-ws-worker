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
// M1: without SESSION_SECRET the worker now fails CLOSED (every request 401s), matching the
// Netlify functions. Open "dev mode" is available ONLY with an explicit opt-in, never by default.
const ALLOW_OPEN_AUTH = process.env.ALLOW_OPEN_AUTH === '1';
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
// M2: strip non-directional multi-leg (spread/combo) volume from the LIVE flow grid so live
// Flow GEX uses the SAME single-leg methodology as the EOD gex.js path (slFrac = 1 - multi_leg/vol).
// Per-cell purity comes from the option_trades tape (purAcc), the same source as the PURE column.
// Default ON; set STRIP_MULTILEG=0 on Railway to show UW's gross flow (pre-fix behavior) instantly.
const STRIP_MULTILEG = process.env.STRIP_MULTILEG !== '0';

const INDEX_LIKE  = new Set(['SPX', 'NDX', 'RUT', 'SPY', 'QQQ', 'IWM', 'DIA']);
const BAND_INDEX  = 0.03;   // ±3% strike window for index-like
const BAND_STOCK  = 0.35;   // ±35% for single names
const HIST_KEEP_MS = 45 * 60000;   // rolling momentum history retained
const HIST_STEP_MS = 60000;        // snapshot the flow grid once a minute
const SSE_FLUSH_MS = 1000;         // push to SSE clients at most once a second

if (!UW_KEY) { console.error('FATAL: UW_KEY not set'); process.exit(1); }
if (!SESSION_SECRET) {
  if (ALLOW_OPEN_AUTH) console.warn('[warn] SESSION_SECRET unset + ALLOW_OPEN_AUTH=1 — auth is OPEN. DEV ONLY; never run this in production.');
  else console.error('[auth] SESSION_SECRET unset — auth DISABLED: every /stream and /trades request will 401. Set SESSION_SECRET in production (or ALLOW_OPEN_AUTH=1 for local dev).');
}

const num = (x) => { const n = parseFloat(x); return Number.isFinite(n) ? n : 0; };

// ---- live state ------------------------------------------------------------
// state[sym] = { spot, ts, cells: Map("strike|expiry" -> {flow,standing,vanna}), dirty, lastMsg }
const state = {};
const S = (sym) => state[sym] || (state[sym] = { spot: 0, ts: 0, cells: new Map(), dirty: false, lastMsg: 0 });
const momHist = {};   // sym -> [{ t, gex: { "strike|expiry": flow } }]   (matches the dashboard buffer shape)
let _gexSamples = [];   // DIAGNOSTIC: raw SPX gamma-flow field values, to verify the dealer-sign convention

function ingest(sym, d) {
  if (!d || d.strike == null || !d.expiry) return;
  const st = S(sym);
  // DIAGNOSTIC: capture a few SPX gamma frames that have flow, to inspect the sign convention
  if (sym === 'SPX' && _gexSamples.length < 8) {
    const anyFlow = num(d.call_gamma_ask_vol) || num(d.call_gamma_bid_vol) || num(d.put_gamma_ask_vol) || num(d.put_gamma_bid_vol);
    if (anyFlow) _gexSamples.push({ k: d.strike, exp: d.expiry, ca: d.call_gamma_ask_vol, cb: d.call_gamma_bid_vol, pa: d.put_gamma_ask_vol, pb: d.put_gamma_bid_vol, coi: d.call_gamma_oi, poi: d.put_gamma_oi });
  }
  const price = num(d.price);
  if (price > 0) st.gammaSpot = price;   // always keep the raw gamma price (snapped-to-5 for indices) as a sanity anchor
  if (price > 0 && !(st.liveSpotTs && Date.now() - st.liveSpotTs < 15000)) { st.spot = price; pushSpot(sym); }   // gamma-piggybacked spot; don't clobber a fresher print/REST spot
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

// M2: single-leg fraction (1 - multi_leg/vol) for a cell key "strike|expiry", from the option_trades
// purity accumulator (purAcc) — the SAME source and math as the PURE column and gex.js's slFrac.
// Returns 1 (no haircut) when STRIP_MULTILEG is off or the cell has no recorded volume yet.
function singleLegFrac(sym, cellKey) {
  if (!STRIP_MULTILEG) return 1;
  const pm = purAcc[SPOT_ALIAS[sym] || sym]; if (!pm) return 1;
  const cc = pm.get(cellKey + '|C'), pp = pm.get(cellKey + '|P');
  const vv = (cc ? cc.vol : 0) + (pp ? pp.vol : 0);
  const mm = (cc ? cc.ml  : 0) + (pp ? pp.ml  : 0);
  return vv > 0 ? Math.max(0, Math.min(1, 1 - mm / vv)) : 1;
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
  // M2: haircut FLOW by the per-cell single-leg fraction (shared singleLegFrac, same source as the
  // PURE column). Only FLOW is stripped; standing/vanna are OI-based. Unknown cell => no haircut.
  const gex   = strikes.map(k => expRows.map(er => { const c = cell(k, er.date); return c ? c.flow * singleLegFrac(sym, `${k}|${er.date}`) : 0; }));
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
  if (!SESSION_SECRET) return ALLOW_OPEN_AUTH ? { u: 'dev' } : null;   // M1: prod fails CLOSED; open only with explicit ALLOW_OPEN_AUTH=1
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
require('./es-feed').mount(app, { verifyToken });   // ES tape for the Edge level radar (founder-only; needs MASSIVE_KEY)

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
      spotSrc: (st && st.liveSpotTs && now - st.liveSpotTs < 15000) ? 'print' : 'gamma',
      liveAgeMs: (st && st.liveSpotTs) ? now - st.liveSpotTs : null,
      viewers: (sse[sym] && sse[sym].size) || 0,
    };
  }
  const dynamic = subs.filter(s => !subMeta[s].core).length;
  const joinedCount = subs.filter(s => channels[s] && channels[s].joined).length;
  res.json({
    ok: true, socket: socketStatus, uptimeSec: Math.round(process.uptime()),
    rev: 'uw-gex-verify',
    joins: { ok: joinedCount, total: subs.length },
    subs: { total: subs.length, core: subs.length - dynamic, dynamic, maxDynamic: MAX_DYNAMIC },
    trades: Object.keys(tradeTopics),
    spxw: { prints: _spxwN, last: _spxwLast, ageMs: _spxwTs ? now - _spxwTs : null, mode: _spxMode, parityLegs: spxPar.legs.size },   // live SPX: mode = print | parity | gamma
    tradeFrames: tradeFrameN,   // frames received per trade channel (lightweight liveness check)
    gexSamples: _gexSamples,    // DIAGNOSTIC: raw SPX gamma-flow fields (inspect ask/bid + call/put signs)
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
// The heavy grid flushes ~1/sec; that throttle + the gamma cadence is the spot
// delay users feel. So push a TINY spot-only SSE event (named `spot`) the moment
// the price changes -- independent of the grid. Debounced so a busy symbol can't
// exceed ~SPOT_MIN_MS. The grid (and gex math) is unchanged.
const SPOT_MIN_MS = 150;        // up to ~6-7 spot ticks/sec per symbol
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
    const gex = {}; for (const [k, v] of st.cells) gex[k] = v.flow * singleLegFrac(sym, k);   // M2: same single-leg haircut as the live grid, so ROC%/momentum stays on one scale
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
const purAcc      = {};   // sym -> Map("strike|expiry|type" -> {vol, ml})  single-leg purity source for the recorder
let   _recDate    = '';   // ET date of the current purity accumulation (cleared on day rollover)
const tradeTopics = {};   // 'option_trades:SYM' -> SYM
const joinAcks    = {};   // topic -> 'ok' | 'err:...'   (diagnostic: did the channel join succeed?)
const tradeFrameN = {};   // 'option_trades:SYM' -> # data frames received  (diagnostic)
const alertsBuf   = [];   // [{source,…alert}] global ring
const alertSubs   = new Set();
let   alertsJoined = false;   // flow-alerts + dark pool joined only while someone views /alerts

// UW is contractually barred from serving the Cboe SPX *index* price, so on the socket the SPXW
// prints arrive with underlying_price BLANK. We map SPXW->SPX for the spot, and because that field
// is empty we reconstruct SPX from the SPXW 0DTE call/put prints via put-call parity (see below).
const SPOT_ALIAS = { SPXW: 'SPX' };
let _spxwN = 0, _spxwLast = 0, _spxwTs = 0;   // SPX live-spot updates (parity or, if ever populated, a real print)
// underlying spot can ride under a few field names on the trade frames; never use `price` (that's the option price)
const _UP_FIELDS = ['underlying_price', 'underlying', 'underlying_px', 'und_price', 'underlyingPrice', 'spot'];
function underPx(p) { for (const f of _UP_FIELDS) { const v = num(p[f]); if (v > 0) return v; } return 0; }

// ---- SPX live spot via 0DTE put-call parity (Cboe-safe: we DERIVE it, not read it) ----
// UW blanks the SPX *index* price on the realtime socket, but the SPXW prints still carry
// strike / option_type / price. For 0DTE, put-call parity gives  spot ≈ strike + call − put
// (rate & time ≈ 0). We keep the latest call & put trade price per 0DTE strike and average the
// few nearest-the-money strikes that have BOTH legs fresh → precise (±~0.2), sub-second.
function etDate() { return new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' }); }
let _realUpTs = 0, _spxMode = 'gamma';                  // _realUpTs: last real underlying_price; _spxMode: print | parity | gamma
const spxPar = { exp: '', legs: new Map() };            // 0DTE SPXW legs: strike -> { c, cT, p, pT }
function recordParityLeg(p) {
  const exp = p.expiry, k = num(p.strike), px = num(p.price);
  if (!exp || !(k > 0) || !(px > 0)) return;
  const today = etDate();
  if (exp !== today) return;                            // 0DTE only — parity is exact only when time-to-expiry ≈ 0
  if (spxPar.exp !== today) { spxPar.exp = today; spxPar.legs.clear(); }   // new day → fresh legs
  const isCall = String(p.option_type || '').toLowerCase().charAt(0) === 'c';
  let leg = spxPar.legs.get(k); if (!leg) { leg = { c: 0, cT: 0, p: 0, pT: 0 }; spxPar.legs.set(k, leg); }
  const t = Date.now();
  if (isCall) { leg.c = px; leg.cT = t; } else { leg.p = px; leg.pT = t; }
}
function spxParitySpot(est) {
  const t = Date.now(), cand = [];
  for (const [k, leg] of spxPar.legs) {
    if (leg.cT && leg.pT && (t - leg.cT) < 4000 && (t - leg.pT) < 4000)   // both legs traded within the last 4s
      cand.push({ s: k + leg.c - leg.p, d: Math.abs(k - est) });
  }
  if (!cand.length) return 0;
  cand.sort((a, b) => a.d - b.d);
  const near = cand.slice(0, 5);                         // nearest-ATM strikes → tightest spreads, least noise
  return near.reduce((a, x) => a + x.s, 0) / near.length;
}

const tradeTopic = (sym) => `option_trades:${sym}`;
function ensureTrades(sym, pin) {   // pin = always-warm spot stream (never idle-dropped)
  if (!sym) return; sym = sym.toUpperCase();
  if (tradeMeta[sym]) { tradeMeta[sym].lastReq = Date.now(); if (pin) tradeMeta[sym].pin = true; return; }
  tradeMeta[sym] = { lastReq: Date.now(), pin: !!pin };
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
  tradeFrameN[topic] = (tradeFrameN[topic] || 0) + 1;   // per-channel frame counter (kept: cheap, handy for /health)
  const sym = tradeTopics[topic] || (p.underlying_symbol ? String(p.underlying_symbol).toUpperCase() : null);
  if (!sym) return;
  if (tradeMeta[sym]) tradeMeta[sym].lastReq = Date.now();
  const _up = underPx(p);
  if (_up > 0) {                                          // precise live spot from the print tape — beats the snapped gamma price
    const spotSym = SPOT_ALIAS[sym] || sym;               // SPXW weekly prints carry the real SPX *index* value
    const _st = S(spotSym); _st.spot = _up; _st.liveSpotTs = Date.now(); _st.lastMsg = Date.now(); pushSpot(spotSym);
    if (spotSym === 'SPX') { _spxwN++; _spxwLast = _up; _spxwTs = Date.now(); _realUpTs = Date.now(); _spxMode = 'print'; }
  }
  if (sym === 'SPXW') recordParityLeg(p);                 // feed the 0DTE parity fallback (used when the index price is blanked)
  // single-leg purity for the replay recorder: keep the latest cumulative vol/ml per contract
  if (p.strike != null && p.expiry && num(p.volume) > 0) {
    const purSym = SPOT_ALIAS[sym] || sym;                // SPXW prints' purity belongs to SPX cells
    const pm = purAcc[purSym] || (purAcc[purSym] = new Map());
    const ty = String(p.option_type || '').charAt(0).toUpperCase();   // 'C' / 'P'
    pm.set(`${num(p.strike)}|${p.expiry}|${ty}`, { vol: num(p.volume), ml: num(p.multi_vol) });
  }
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
    if (tradeMeta[sym].pin) continue;   // always-warm spot streams (SPXW + watchlist equities/ETFs) — never idle-drop
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
  if (c.priceTopic) { topicToSym[c.priceTopic] = sym; wsSend({ channel: c.priceTopic, msg_type: 'join' }); }
  console.log('[join \u2192]', c.topic);
}

function joinSym(sym, core) {
  if (channels[sym]) { if (core) subMeta[sym].core = true; return; }
  if (!core) {
    const dyn = Object.keys(subMeta).filter(s => !subMeta[s].core).length;
    if (dyn >= MAX_DYNAMIC) evictLRU();
  }
  // SPX-only: also subscribe UW's dedicated price-tick channel (price:SPX) for a faster,
  // cleaner spot than the gamma-piggybacked price. If UW gates it, the join just errors and
  // we fall back to the gamma price -- no regression. Easy to widen later.
  channels[sym] = { topic: `gex_strike_expiry:${sym}`, priceTopic: (sym === 'SPX') ? `price:${sym}` : null, joined: false };
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
    joinAcks[topic] = payload.status === 'ok' ? 'ok' : ('err:' + JSON.stringify(payload.response || payload).slice(0, 80));
    if (payload.status !== 'ok') console.error('[join ERR]', topic, JSON.stringify(payload).slice(0, 160));
    else { if (jsym && channels[jsym]) channels[jsym].joined = true; console.log('[join ok]', topic); }
    return;
  }
  // edge tab: route non-GEX channels before the gex-cell ingest
  if (typeof topic === 'string') {
    if (topic.lastIndexOf('price:', 0) === 0) {
      const psym = topicToSym[topic] || topic.slice(6).toUpperCase();
      const px = num(payload.price != null ? payload.price : payload.last != null ? payload.last : payload.value != null ? payload.value : payload.p != null ? payload.p : payload.close);
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
// Always-warm live SPOT streams so every watchlist symbol ticks sub-second (vs the ~15s gamma cadence):
//   • equities / ETFs → option_trades:<SYM> carries a live, precise underlying_price (like AAPL)
//   • SPX             → option_trades:SPXW (0DTE weeklys) → spot via put-call parity (index price is blanked)
//   • NDX / RUT       → index price blanked AND no dense 0DTE flow → stays on the gamma feed
const INDEX_GATED = new Set(['SPX', 'NDX', 'RUT']);
for (const sym of CORE) {
  if (sym === 'SPX') { ensureTrades('SPXW', true); continue; }
  if (INDEX_GATED.has(sym)) continue;
  ensureTrades(sym, true);
}
connect();

// publish a derived SPX spot from 0DTE parity ~4x/sec — unless a real underlying_price is live
setInterval(() => {
  if (Date.now() - _realUpTs < 4000) return;             // a real index price is driving SPX — leave it alone
  if (!spxPar.legs.size) return;
  const st = S('SPX');
  const ref = st.gammaSpot || st.spot || 0;              // snapped gamma price = sanity anchor
  const ps  = spxParitySpot(ref > 0 ? ref : 7000);
  if (ps > 0 && (!(ref > 0) || Math.abs(ps - ref) / ref < 0.02)) {   // reject bad legs: stay within 2% of the gamma anchor
    st.spot = Math.round(ps * 100) / 100; st.liveSpotTs = Date.now(); st.ts = Date.now();
    pushSpot('SPX');
    _spxwN++; _spxwLast = st.spot; _spxwTs = Date.now(); _spxMode = 'parity';
  }
}, 250);

// NOTE: live SPX spot now comes from the option_trades:SPXW websocket stream (joined at
// boot above) — UW can't serve the Cboe index price directly, but SPXW prints carry it in
// underlying_price, routed onto SPX in onTrade(). No REST polling needed. The full-tape
// REST endpoint was an end-of-day archive (404 / NoSuchKey intraday), so it's gone.

// ───────────────────────────────────────────────────────────────────────────
// REPLAY RECORDER (optional) — persist the live flow-GEX grid to Postgres so a
// past session can be replayed with FULL fidelity and ZERO REST calls. It records
// only what's already streaming (the CORE symbols), gated to live frames (≈ market
// hours). Cleanly disabled if DATABASE_URL / pg are absent — never blocks the desk.
//   Storage: one row per (symbol, ~30s) holding the cumulative flow per cell.
//   Serve:   GET /replay?symbol=SPX&date=YYYY-MM-DD&s=<ticket>
// ───────────────────────────────────────────────────────────────────────────
const DATABASE_URL    = (process.env.DATABASE_URL || '').trim();
const REC_INTERVAL_MS = parseInt(process.env.REC_INTERVAL_MS || '30000', 10);
const REC_BAND        = parseFloat(process.env.REC_BAND || '0.06');   // ±6% of spot (wider than the ±3.5% display)
const REC_EXPIRIES    = parseInt(process.env.REC_EXPIRIES || '12', 10);   // nearest N future expiries to record (bounds storage)
const REC_RETAIN_DAYS = parseInt(process.env.REC_RETAIN_DAYS || '7', 10);   // keep this many days of full-grid replay history (older auto-pruned daily)
const REC_LEVELS_DAYS = parseInt(process.env.REC_LEVELS_DAYS || '95', 10);   // keep this many days of the lean levels history (drives the 3-month chart)
let pgPool = null;
(function initRecorder() {
  if (!DATABASE_URL) { console.log('[rec] DATABASE_URL not set — recorder OFF'); return; }
  let Pool;
  try { ({ Pool } = require('pg')); } catch { console.error('[rec] "pg" dependency missing — recorder OFF'); return; }
  const needSSL = !/\.railway\.internal/.test(DATABASE_URL);   // internal Railway network needs no SSL; public proxy does
  pgPool = new Pool({ connectionString: DATABASE_URL, ssl: needSSL ? { rejectUnauthorized: false } : false, max: 4 });
  pgPool.query(`CREATE TABLE IF NOT EXISTS replay_frames (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    spot DOUBLE PRECISION,
    cells JSONB NOT NULL
  )`)
    .then(() => pgPool.query(`CREATE INDEX IF NOT EXISTS replay_frames_lookup ON replay_frames (symbol, trade_date, ts)`))
    .then(() => pgPool.query(`CREATE TABLE IF NOT EXISTS replay_levels (
      id BIGSERIAL PRIMARY KEY, symbol TEXT NOT NULL, trade_date DATE NOT NULL, ts TIMESTAMPTZ NOT NULL,
      spot DOUBLE PRECISION, flip DOUBLE PRECISION, call_wall DOUBLE PRECISION, put_wall DOUBLE PRECISION,
      node DOUBLE PRECISION, pull DOUBLE PRECISION, peak DOUBLE PRECISION, net_gex DOUBLE PRECISION, net_vanna DOUBLE PRECISION)`))
    .then(() => pgPool.query(`CREATE INDEX IF NOT EXISTS replay_levels_lookup ON replay_levels (symbol, ts)`))
    .then(() => console.log('[rec] Postgres recorder ON (every ' + (REC_INTERVAL_MS / 1000) + 's)'))
    .catch(e => { console.error('[rec] schema init failed:', e.message); pgPool = null; });
})();

function etDateStr(d) {   // trading date as YYYY-MM-DD in America/New_York
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(d);
  const g = t => (parts.find(x => x.type === t) || {}).value;
  return `${g('year')}-${g('month')}-${g('day')}`;
}
async function recordFrame() {
  if (!pgPool) return;
  const now = Date.now();
  const today = etDateStr(new Date(now));
  if (today !== _recDate) { _recDate = today; for (const s in purAcc) purAcc[s].clear(); }   // new session → reset purity accumulation
  for (const sym of CORE) {
    const st = state[sym];
    if (!st || !st.cells || !st.cells.size) continue;
    if (!(st.lastMsg && now - st.lastMsg < 120000)) continue;   // live frames only → gates to RTH automatically
    const spot = st.spot || 0; if (!(spot > 0)) continue;
    const lo = spot * (1 - REC_BAND), hi = spot * (1 + REC_BAND);
    // keep only the nearest REC_EXPIRIES future expiries (bounds storage)
    const exps = new Set();
    for (const k of st.cells.keys()) { const e = k.slice(k.indexOf('|') + 1); if (e >= today) exps.add(e); }
    const keep = new Set([...exps].sort().slice(0, REC_EXPIRIES));
    const pm = purAcc[sym];
    const cells = {};
    for (const [k, c] of st.cells) {
      const strike = parseFloat(k); const exp = k.slice(k.indexOf('|') + 1);   // key = "strike|expiry"
      if (!(strike >= lo && strike <= hi) || !keep.has(exp) || !c) continue;
      const f = Math.round(c.flow || 0), s = Math.round(c.standing || 0), v = Math.round(c.vanna || 0);
      if (!(f || s || v)) continue;
      let pur = -1;                                             // single-leg purity %, -1 = unknown
      if (pm) {
        const cc = pm.get(k + '|C'), pp = pm.get(k + '|P');
        const vv = (cc ? cc.vol : 0) + (pp ? pp.vol : 0), mm = (cc ? cc.ml : 0) + (pp ? pp.ml : 0);
        if (vv > 0) pur = Math.max(0, Math.min(100, Math.round((1 - mm / vv) * 100)));
      }
      cells[k] = [f, s, v, pur];                               // [flow, standing, vanna, purity%]
    }
    if (!Object.keys(cells).length) continue;
    try {
      await pgPool.query(
        `INSERT INTO replay_frames (symbol, trade_date, ts, spot, cells) VALUES ($1,$2,to_timestamp($3),$4,$5)`,
        [sym, today, now / 1000, spot, JSON.stringify(cells)]
      );
    } catch (e) { /* recording must never break the worker */ }
    try {
      const lv = computeLevels(st, spot);
      if (lv) await pgPool.query(
        `INSERT INTO replay_levels (symbol, trade_date, ts, spot, flip, call_wall, put_wall, node, pull, peak, net_gex, net_vanna)
         VALUES ($1,$2,to_timestamp($3),$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        [sym, today, now / 1000, lv.spot, lv.flip, lv.call_wall, lv.put_wall, lv.node, lv.pull, lv.peak, lv.net_gex, lv.net_vanna]);
    } catch (e) { /* levels recording must never break the worker */ }
  }
}
// ticker-level levels (aggregate across expiries within ±band) for the history chart
function computeLevels(st, spot) {
  if (!(spot > 0) || !st.cells || !st.cells.size) return null;
  const lo = spot * (1 - REC_BAND), hi = spot * (1 + REC_BAND);
  const byK = new Map();
  for (const [key, c] of st.cells) {
    const k = parseFloat(key); if (!(k >= lo && k <= hi) || !c) continue;
    const a = byK.get(k) || { std: 0, flow: 0, vanna: 0 };
    a.std += (c.standing || 0); a.flow += (c.flow || 0); a.vanna += (c.vanna || 0); byK.set(k, a);
  }
  const strikes = [...byK.keys()].sort((a, b) => b - a);   // high -> low
  if (!strikes.length) return null;
  let flip = null, cum = 0, prev = 0, prevK = null;
  for (const k of strikes) { prev = cum; cum += byK.get(k).std; if (prevK !== null && ((prev >= 0 && cum < 0) || (prev < 0 && cum >= 0))) { flip = Math.round((k + prevK) / 2); break; } prevK = k; }
  let cw = null, cwv = -Infinity, pw = null, pwv = Infinity, peak = null, pkv = 0, netGex = 0, netVanna = 0;
  for (const k of strikes) { const a = byK.get(k);
    if (k > spot && a.std > cwv) { cwv = a.std; cw = k; }
    if (k < spot && a.std < pwv) { pwv = a.std; pw = k; }
    if (Math.abs(a.flow) > Math.abs(pkv)) { pkv = a.flow; peak = k; }
    netGex += a.flow; netVanna += a.vanna;
  }
  if (!(cwv > 0)) cw = null; if (!(pwv < 0)) pw = null;
  let ws = 0, ks = 0;
  for (const k of strikes) { if (Math.abs(k - spot) / spot > 0.025) continue; const prox = Math.exp(-Math.pow((k - spot) / (spot * 0.012), 2)); const w = Math.abs(byK.get(k).std) * prox; ws += w; ks += k * w; }
  const node = ws > 0 ? Math.round(ks / ws * 100) / 100 : null;
  const pull = node != null ? Math.round((node - spot) * 100) / 100 : null;
  return { spot: Math.round(spot * 100) / 100, flip, call_wall: cw, put_wall: pw, node, pull, peak, net_gex: Math.round(netGex), net_vanna: Math.round(netVanna) };
}
// retention: drop frames older than REC_RETAIN_DAYS so Railway storage stays flat
async function pruneOldFrames() {
  if (!pgPool || !(REC_RETAIN_DAYS > 0)) return;
  try {
    const r = await pgPool.query(`DELETE FROM replay_frames WHERE trade_date < (CURRENT_DATE - $1::int)`, [REC_RETAIN_DAYS]);
    if (r.rowCount) console.log(`[rec] pruned ${r.rowCount} frames older than ${REC_RETAIN_DAYS}d`);
    const rl = await pgPool.query(`DELETE FROM replay_levels WHERE trade_date < (CURRENT_DATE - $1::int)`, [REC_LEVELS_DAYS]);
    if (rl.rowCount) console.log(`[rec] pruned ${rl.rowCount} level rows older than ${REC_LEVELS_DAYS}d`);
  } catch (e) { console.error('[rec] prune failed:', e.message); }
}
if (DATABASE_URL) {
  setInterval(recordFrame, REC_INTERVAL_MS);
  setTimeout(pruneOldFrames, 60000);                 // first sweep ~1 min after boot
  setInterval(pruneOldFrames, 12 * 3600 * 1000);     // then twice a day
}

// recorded full-fidelity frames for one past session (full-grid frames; the page normalizes both sources)
app.get('/replay', auth, async (req, res) => {
  try {
    if (!pgPool) return res.status(503).json({ error: 'recorder not configured (no DATABASE_URL)' });
    const sym = String(req.query.symbol || 'SPX').toUpperCase();
    const date = String(req.query.date || '');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'date=YYYY-MM-DD required' });
    let rows;
    try {
      const q = await pgPool.query(
        `SELECT extract(epoch from ts) * 1000 AS t, spot, cells FROM replay_frames
         WHERE symbol = $1 AND trade_date = $2 ORDER BY ts ASC`, [sym, date]);
      rows = q.rows;
    } catch (e) { return res.status(502).json({ error: 'db: ' + e.message }); }
    if (!rows.length) return res.status(404).json({ error: `no recording for ${sym} on ${date}` });

    const quad = v => { const a = Array.isArray(v) ? v.slice() : [v || 0, 0, 0]; while (a.length < 4) a.push(-1); return a; };   // back-compat: number→flow-only, 3-len→no purity
    const spots = rows.map(r => +r.spot).filter(s => s > 0).sort((a, b) => a - b);
    const spotMid = spots[Math.floor(spots.length / 2)] || 0;
    const lo = spotMid * 0.965, hi = spotMid * 1.035;
    const kSet = new Set(), eSet = new Set();
    for (const r of rows) for (const key in r.cells) {
      const bar = key.split('|'); const k = parseFloat(bar[0]), exp = bar[1];
      if (k >= lo && k <= hi && exp >= date) { kSet.add(k); eSet.add(exp); }
    }
    const strikes = [...kSet].sort((a, b) => b - a);
    const expiries = [...eSet].sort().slice(0, 10);            // nearest 10 expiries
    const expOk = new Set(expiries);
    const sIdx = new Map(strikes.map((k, i) => [k, i]));
    const eIdx = new Map(expiries.map((e, i) => [e, i]));
    // cap payload: at most ~420 frames (keeps the JSON a few MB and plays smooth)
    let sel = rows; const MAXF = 420;
    if (rows.length > MAXF) { const step = Math.ceil(rows.length / MAXF); sel = rows.filter((_, i) => i % step === 0 || i === rows.length - 1); }
    const blank = () => strikes.map(() => expiries.map(() => 0));
    const blankP = () => strikes.map(() => expiries.map(() => -1));
    const frames = sel.map(r => {
      const flow = blank(), standing = blank(), vanna = blank(), pure = blankP();
      for (const key in r.cells) {
        const bar = key.split('|'); const k = parseFloat(bar[0]), exp = bar[1];
        if (!sIdx.has(k) || !expOk.has(exp)) continue;
        const t = quad(r.cells[key]); const si = sIdx.get(k), ei = eIdx.get(exp);
        flow[si][ei] = t[0]; standing[si][ei] = t[1]; vanna[si][ei] = t[2]; pure[si][ei] = t[3];
      }
      return { t: new Date(+r.t).toISOString().slice(0, 16), spot: Math.round(+r.spot * 100) / 100, flow, standing, vanna, pure };
    });
    res.json({
      symbol: sym, date, source: 'ws',
      strikes,
      expiries: expiries.map(e => ({ date: e, label: labelFor(e), dte: Math.round((Date.parse(e + 'T00:00:00Z') - Date.parse(date + 'T00:00:00Z')) / 864e5) })),
      frames, spot: Math.round(spotMid * 100) / 100, recorded: rows.length, served: frames.length,
    });
  } catch (e) { try { res.status(500).json({ error: 'replay: ' + e.message }); } catch {} }
});

// GET /levels?symbol=SPX&days=90&res=15m — ticker-level history (spot OHLC + flip/walls/node/peak), bucketed to the timeframe
app.get('/levels', auth, async (req, res) => {
  try {
    if (!pgPool) return res.status(503).json({ error: 'recorder not configured (no DATABASE_URL)' });
    const sym = String(req.query.symbol || 'SPX').toUpperCase();
    const days = Math.min(120, Math.max(1, parseInt(req.query.days || '90', 10)));
    const resStr = String(req.query.res || '15m');
    const bucket = { '1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 }[resStr] || 900;
    let rows;
    try {
      const q = await pgPool.query(
        `SELECT extract(epoch from ts) AS t, spot, flip, call_wall, put_wall, node, peak, net_gex, net_vanna
         FROM replay_levels WHERE symbol = $1 AND ts > now() - ($2 * interval '1 day') ORDER BY ts ASC`, [sym, days]);
      rows = q.rows;
    } catch (e) { return res.status(502).json({ error: 'db: ' + e.message }); }
    if (!rows.length) return res.status(404).json({ error: `no level history for ${sym} yet` });
    // bucket to the requested timeframe: OHLC on spot, last value for each level
    const out = []; let bk = -1, b = null;
    for (const r of rows) {
      const t = +r.t, key = Math.floor(t / bucket) * bucket, s = +r.spot;
      if (key !== bk) { if (b) out.push(b); bk = key; b = { t: key, o: s, h: s, l: s, c: s, flip: r.flip, cw: r.call_wall, pw: r.put_wall, node: r.node, peak: r.peak, gex: r.net_gex, vanna: r.net_vanna }; }
      else { if (s > b.h) b.h = s; if (s < b.l) b.l = s; b.c = s; b.flip = r.flip; b.cw = r.call_wall; b.pw = r.put_wall; b.node = r.node; b.peak = r.peak; b.gex = r.net_gex; b.vanna = r.net_vanna; }
    }
    if (b) out.push(b);
    res.json({ symbol: sym, res: resStr, days, points: out.length, series: out });
  } catch (e) { try { res.status(500).json({ error: 'levels: ' + e.message }); } catch {} }
});

app.listen(PORT, () => console.log(`[http] rev uw-spxw-1 listening on :${PORT} — core: ${CORE.join(', ')} | on-demand: any other ticker (max ${MAX_DYNAMIC})`));
