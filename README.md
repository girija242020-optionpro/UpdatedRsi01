# RSI + DEMA + Volume PWA — Dynamic Confluence Engine

The UI is unchanged. The strategy engine now follows the exact sequential closed-candle logic:

## CALL
1. RSI enters Oversold: RSI < Lower threshold (default 30)
2. RSI recovers above Lower threshold
3. RSI crosses above its smoothing EMA
4. RSI crosses above Middle threshold (default 50)
5. Price closes above DEMA
6. Volume is above Volume EMA
7. The entry candle is CLOSED
8. Only when all conditions are satisfied: CALL ENTRY + alarm + entry log

## PUT mirror
1. RSI enters Overbought: RSI > Upper threshold (default 70)
2. RSI recovers below Upper threshold
3. RSI crosses below its smoothing EMA
4. RSI crosses below Middle threshold (default 50)
5. Price closes below DEMA
6. Volume is above Volume EMA
7. The entry candle is CLOSED
8. Only when all conditions are satisfied: PUT ENTRY + alarm + entry log

## Custom inputs
The live engine reads the current Settings values. Saving a new RSI length, RSI smoothing length, lower/middle/upper levels, DEMA length, or Volume EMA length recalculates the indicator series and resets the sequence state using the new values.

## Closed-candle rule
A signal is never fired from an unfinished candle. The live candle is display-only; when a new candle starts, the previous candle is processed as the closed entry candle.

## Entry archive
Entries are stored in browser localStorage with the exact settings snapshot used at the time of the entry. The Log tab can filter by any saved Indian-calendar date or show ALL.

## Backend
The PWA uses the GammaXBinery-style Dhan PWA-compatible backend through:
- REST history/instrument APIs
- `/ws` live tick WebSocket
- Web Push subscription/event endpoints
