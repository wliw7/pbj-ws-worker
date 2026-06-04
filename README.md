# PBJ Capital — GEX WebSocket worker

One always-on process that holds **one** Unusual Whales WebSocket connection and fans the
live dealer-gamma grid out to every dashboard user. **UW API load depends on how many
symbols you track, not how many users connect** — 50 users or 5,000, the upstream cost is
identical. This is what removes the rate-limit risk at launch.

It consumes the `gex_strike_expiry:<TICKER>` channel (UW Advanced plan), which streams
call/put gamma + vanna + charm by strike **and** expiry, including the bid/ask-classified
(flow) gamma — so it serves your **GEX (Flow), Standing, Vanna, and Gravity** views in
real time, at the same `$ per 1% move` scaling `gex.js` already uses.

**Not** covered by the stream (keep these on your existing REST/snapshot path and merge
client-side): **Persist** (settled ΔOI), the **OI** column, and **PURE** (single-leg
purity). They're slow/daily, so no per-user polling is needed for them.

---

## Endpoints

All gated by the same `pbj_session` HMAC as `gex.js` (unless `SESSION_SECRET` is unset).

| Route | Auth | Returns |
|---|---|---|
| `GET /health` | no | socket status + per-symbol `{ cells, spot, lastMsgAgeMs }` |
| `GET /gex?symbol=SPX&exps=a,b` | yes | assembled grid (same shape `gex.js` returns for the live fields) |
| `GET /seed?symbol=SPX` | yes | `{ history }` for the dashboard's momentum buffer (instant ROC%) |
| `GET /stream?symbol=SPX` | yes | **SSE** — pushes the grid on change (real-time, no polling) |

The dashboard passes the member's `pbj_session` token to the worker. For `/gex` and `/seed`
use a header (`X-PBJ-Session: <token>`) or `?s=<token>`. For `/stream`, EventSource can't set
headers, so pass `?s=<token>`.

---

## Run locally

```bash
cd pbj-ws-worker
cp .env.example .env        # fill in UW_KEY (+ SESSION_SECRET for the gate)
npm install
npm start
# → [http] listening on :8080 — symbols: SPX, SPY, QQQ
curl localhost:8080/health
```

## Deploy on Railway

1. Push this folder to a GitHub repo (or `railway init` from here).
2. Railway → **New Project → Deploy from repo** (or `railway up`).
3. **Variables**: set `UW_KEY`, `SESSION_SECRET`, `ALLOWED_ORIGIN=https://pbjcapital.net`,
   `SYMBOLS=SPX,SPY,QQQ`. (Don't set `PORT` — Railway injects it.)
4. **Networking → Generate Domain**. You'll get e.g. `https://pbj-gex.up.railway.app`.
5. Confirm: open `https://<your-domain>/health` — you want `socket: "open"` and, during
   market hours, `cells > 0` for each symbol.

It's a tiny long-running Node process — the Hobby plan (~$5/mo) is plenty.

---

## Wire the dashboard to it (next step)

In `gex_dashboard.html`, point data fetching at the worker instead of `/.netlify/functions/gex`:

- **Drop-in (keep the 30s poll):** change `fetchOne()` to call `https://<worker>/gex?symbol=…`
  and `seedMom()` to call `https://<worker>/seed?symbol=…`, sending `X-PBJ-Session`/`?s=`.
  UW calls per user drop to **zero** (the worker is the only consumer).
- **Real-time (recommended):** replace the poll with an `EventSource(
  'https://<worker>/stream?symbol=SPX&s=<token>')` and repaint on each message.

Either way, keep calling your existing `gex.js` for the **slow** fields it still owns
(Persist ΔOI, OI column, PURE) — but you can now cache that hard / call it infrequently,
since it's no longer on the hot path. I can do this wiring for you on the next pass.

---

## First-run checklist (since I can't run it against your key)

The worker is built to be observable. After deploying, hit `/health` during market hours:

- `socket: "open"` but **every symbol shows `cells: 0`** → the channel didn't deliver. Check
  the logs for `[join error]`/`[join timeout]`, and confirm the channel ticker. The `gex.js`
  REST path uses the `SPXW` option root, but the GEX/exposure channels use the plain index
  ticker — i.e. `gex_strike_expiry:SPX` (not `SPXW`). If `SPX` doesn't deliver, try `SPXW` in
  `SYMBOLS`.
- `socket: "error"`/`"closed"` repeatedly → token/plan issue. Confirm `UW_KEY` is the
  WS-enabled Advanced key and the connect URL `wss://api.unusualwhales.com/socket?token=…`.
- The worker logs **every** channel message via a catch-all handler, so if UW pushes the data
  under an unexpected event name it still ingests (anything carrying a `strike`). If `cells`
  stays 0 but you see traffic, paste me one logged payload and I'll adjust the field mapping.

The connection/protocol details are handled by the official `phoenix` client (the same library
UW's own Node example uses), so the handshake, heartbeat, and reconnect are not hand-rolled.
