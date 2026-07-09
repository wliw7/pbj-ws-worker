# Update pack — session persistence + full-session refresh + delta/volume ladder

Two files. Nothing in your folders was touched — review, then place them yourself.

---

## 1. `es-feed.js` v3 → worker repo (replaces the one on GitHub)

Includes the v2 discovery fix, plus:

**Session persistence (your #1).** State (CVD, big Δ, volume, buy/sell, the full
per-price ladder, the print ring) saves to your existing Railway Postgres every
5s, keyed to ticker + Globex session date. On boot, same-session state is
restored, then the downtime gap is **backfilled from Massive REST history**
(tick-rule side classification for the gap only). Result: counters truly run
from the 18:00 ET open and survive deploys/restarts. Uses `DATABASE_URL` and
`pg` — both already on your worker. If either is absent it logs
`persistence OFF` and behaves like today. Old session rows purge after 3 days.

**Bigger replay ring (your #2).** `RECENT_MAX` 400 → **3000** prints
(`ES_RECENT` env to change). A page refresh now hands the browser several
minutes of tape, so scores re-print in ~1s with the full 3-minute band window —
no "building…" wait after refresh.

**Verify after upload:** Railway log shows
`[es-feed] restored session … vol … cvd …` on the next restart, and
`/es/health` volume no longer resets to ~0 after a deploy.

---

## 2. `autumn.html` → site folder (replaces yours after review)

Your current file + **92 changed/added lines**, all inside the Edge (`ed2`)
section:

**Delta/volume ladder beside the price axis** (your screenshot, adapted):
- Grey bars anchored against the price axis growing left = total session
  volume at each price, with the contract count.
- Colored bars with the signed delta number: **blue = net buy**, **red = net
  sell** at that price.
- Rows auto-bucket with zoom (0.5 → 10 pts) so they stay readable at any
  timeframe; redraws live with the tape and on pan/zoom.
- **Ladder** toggle button next to the timeframe pills (state remembered).
- Data = the worker's full-session ladder (restored + persisted per #1) plus
  live prints — so it's the true Globex-open volume profile, not
  since-page-load.

Also: client print ring cap raised to match the bigger server ring, and the
tab-note explains the ladder colors.

**Verified:** inline JS `node --check` clean on the full patched file.

**Apply:** diff it against your copy if you want (`diff autumn.html <yours>`),
drop it into the site folder, `netlify deploy --prod`, hard-refresh.

---

Order doesn't matter, but the ladder shows richest data once the worker v3 is
live (full-session profile instead of since-boot).
