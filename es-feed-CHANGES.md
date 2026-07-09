# es-feed.js v2 — what changed and how to apply

**One file, one purpose:** fixes front-month auto-discovery so the feed can roll
contracts by itself (ESU6 → ESZ6 in September) without the `ES_TICKER` override.
Nothing else in the module changed — tape engine, SSE, auth, scale guard are
byte-identical to what's running now.

## Exactly what changed (2 spots)

1. **`mget()` error messages** now include the HTTP status code, so if Massive
   rejects a request the Railway log says `HTTP 400 — <their message>` instead
   of a vague error.

2. **`resolveTicker()` rewritten.** v1 asked Massive's contracts API to
   sort/filter server-side (`sort=last_trade_date.asc&type=single`) and the API
   rejected the request — that's why `/es/health` showed `"ticker":"ES?"`. v2:
   - queries plainly: `product_code=ES&active=true&limit=1000` (with a fallback
     retry without `active` if that ever fails),
   - filters client-side to single contracts via ticker shape
     `ES + month code + year` (`ESU6` ✓, spread `ESU6-ESZ6` ✗ — tested),
   - picks the nearest contract with ≥3 days to last trade (roll safety),
   - sorts by days-to-maturity itself.

## How to apply

1. Upload this `es-feed.js` to the **worker repo** on GitHub (replaces the
   existing one). Railway auto-deploys. Nothing else to touch.
2. Verify `/es/health` still shows `ESU6` + flowing tape (ES_TICKER is still
   set, so behavior is unchanged — this proves the deploy is clean).
3. **To enable auto-roll:** Railway → Variables → delete `ES_TICKER` → restart.
   `/es/health` should come back with `"ticker":"ESU6"` discovered on its own.
   If it ever shows `ES?` again, the log line `[es-feed] contracts query...`
   now contains the exact server error — paste it to me.
4. If you'd rather not test now: keep `ES_TICKER=ESU6` and just remember to
   change it to `ESZ6` around **Sept 10, 2026** (roll week). The v2 code is
   still worth deploying so the fix is in place when you do remove it.

Verified before delivery: `node --check` clean, module mounts, ticker regex
tested against singles/spreads/junk.
