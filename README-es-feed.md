# Edge · ES level radar — worker wiring

The Edge tab's ES tape comes from **Massive.com** (Futures Advanced, $199/mo,
real-time CME). One socket lives on the Railway worker; founders stream it via
SSE with the same ticket auth as `/stream`.

## 1. Massive account
- Sign up at massive.com → subscribe **Futures Advanced** ($199/mo, real-time).
- Copy the API key.

## 2. Worker repo (pbj-ws-worker)
- Copy `es-feed.js` (this folder) into the worker repo next to `index.js`.
- In `index.js`, after `const app = express()` and after `verifyToken` is
  defined, add:

```js
require('./es-feed').mount(app, { verifyToken });
```

- Railway → Variables:
  - `MASSIVE_KEY` = your Massive API key  (required)
  - `ES_TICKER`   = optional override (e.g. `ESU6`); leave unset for
    automatic front-month discovery + hourly roll check
  - `ES_BIG_SIZE` = optional, default 20 (contracts ≥ this feed the "big" CVD)

- Deploy, then confirm: `https://<worker>/es/health` →
  `socket: "open"`, a real `ticker` (e.g. `ESU6`), and during Globex hours
  `lastMsgAgeMs` under a few seconds. The `scale` block shows the price-divisor
  guard (trade/quote divisor 1 or 100 — both are handled automatically).

## 3. This repo (already done)
- `netlify/functions/ws-ticket.js` now carries the `r:"admin"` claim so the
  worker can enforce founder-only on `/es/*`. Redeploy the site.
- `autumn.html` Edge tab renders the level radar for admins; members still see
  the work-in-progress notice.

## Endpoints
| Route | Auth | Purpose |
|---|---|---|
| `GET /es/health` | none | liveness, ticker, scale guard, CVD |
| `GET /es/state?s=<ticket>` | founder | one-shot engine snapshot |
| `GET /es/stream?s=<ticket>` | founder | SSE: `init` snapshot then ~4/s `tape` batches |

## Notes
- Licensing: individual plan = display use for you. Do NOT relay `/es/*` to
  members without a redistribution agreement (the founder gate enforces this).
- The feed self-heals: auto-reconnect, hourly front-month re-resolve (resets
  session + resubscribes on roll), and Globex session reset at 18:00 ET.
