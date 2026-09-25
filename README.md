# Dhan PWA Compatible Backend — GammaXBinery Style

Frontend-agnostic Dhan market-data gateway for the RSI + DEMA + Volume PWA.

## Root diagnostic page
Opening the Render URL directly shows a GammaXBinery-style live backend dashboard with feed status, tick/packet counters, subscriptions, clients, instruments and push-device counts, plus clickable health/status/bootstrap/instrument/push endpoints.

## PWA contract
- `GET /api/v1/instruments/search`
- `GET /api/v1/bootstrap`
- `GET /api/v1/snapshot`
- `GET /api/v1/ticks`
- `GET /api/v1/candles`
- `POST /api/v1/history`
- `POST /api/v1/quote`
- `GET /api/v1/vapid-public-key`
- `POST /api/v1/push/subscribe`
- `POST /api/v1/push/test`
- `POST /api/v1/push/event`
- `WS /ws`

The strategy logic remains in the PWA. The backend supplies Dhan history/ticks and the alarm push transport; it does not decide CALL/PUT entries.

## Volume fix (NIFTY / SENSEX)
NIFTY and SENSEX are index *values* — nothing is ever bought or sold "at the index", so the exchange
never publishes a real traded volume for them. Dhan's own `IDX_I` feed and `/charts/intraday` history
both return 0 / meaningless numbers in the `volume` field for these two instruments. Any chart (including
Dhan's own app) that shows a NIFTY/SENSEX volume bar is computing it another way — usually a weighted
sum of the 50 constituent stocks' volume — which is heavy to replicate exactly and not something you get
"as is" from any single API call.

This backend instead mirrors volume from each index's **current-month futures contract** (NIFTY-FUT /
SENSEX-FUT), which does carry a real, exchange-reported traded volume:
- On startup (and every 6h, alongside the instrument-master refresh) it resolves the nearest-expiry
  futures contract for NIFTY and SENSEX from Dhan's scrip master — so it survives monthly expiry rollover
  automatically.
- Whenever a client subscribes to NIFTY/SENSEX over `/ws`, the backend silently also subscribes to that
  index's linked futures contract (quote mode) purely to read its volume. The client only ever sees ticks
  for the index it asked for — the futures subscription and its raw ticks are never forwarded.
- Live: the futures contract's tick-by-tick *cumulative session volume* is converted to a per-minute delta
  and accumulated into the current forming 1-minute bucket, then mirrored into the index's `quote.volume`
  and its outgoing WS ticks (`volumeSource: "LINKED_FUTURES"`).
- History (`POST /api/v1/history`): when called for NIFTY/SENSEX, the backend transparently fetches the
  same interval/date range for the linked futures contract and swaps its (already correct, per-candle)
  volume array in by matching timestamps, before replying. The PWA calls this endpoint exactly as before —
  no frontend change needed.

If Dhan's scrip master ever changes its instrument-type/symbol naming for futures, check the console log
line `Linked futures (volume source) resolved: {...}` on boot — if it shows `null` for NIFTY or SENSEX,
the match in `resolveFutures()`/`futMatch` needs adjusting to the current CSV values.

## Strategy expected by the PWA
CALL: RSI oversold -> recover above lower -> RSI/EMA bullish cross -> RSI above middle 50 -> price closes above DEMA -> volume above Volume EMA -> closed candle -> entry.

PUT: RSI overbought -> recover below upper -> RSI/EMA bearish cross -> RSI below middle 50 -> price closes below DEMA -> volume above Volume EMA -> closed candle -> entry.

All values are configurable in the PWA Settings tab.
