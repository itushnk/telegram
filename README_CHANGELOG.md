# Bot Patch v3 — Strict Search, Broadcast Toggle, Price Logic (ILS-first)

**Build:** 2025-12-21T20:37:22.151905Z

This bundle includes:
- **main.py** — updated entry with:
  - 📡 **Broadcast toggle** (default: OFF after deploy)
  - ⏱ **Single "Set interval (minutes)"** control instead of multiple 5/10/20 buttons
  - 🎯 **Strict manual search** filter (title must contain the query tokens)
  - 💰 **Price pipeline: ILS-first** (no conversion), with optional USD→ILS conversion + dynamic rate
  - 🧭 Status shows broadcast state & interval
- **presets_topics.json** — a large set of topic presets for "Browse by Topic"
- **config_example.env** — minimal ENV you can paste to Railway to avoid conflicts

## ENV — minimal recommended
```
# core
BOT_TOKEN=xxx
PUBLIC_CHANNEL=@your_channel
ADMIN_USER_IDS=123456789

# AliExpress core
AE_APP_KEY=xxx
AE_APP_SECRET=xxx
AE_TRACKING_ID=xxx
AE_SHIP_TO_COUNTRY=IL
AE_TARGET_LANGUAGE=HE

# search quality
AE_MIN_ORDERS=300
AE_MIN_RATING=88

# pricing (ILS-first)
AE_PRICE_INPUT_CURRENCY=ILS
AE_PRICE_CONVERT_USD_TO_ILS=0
AE_PRICE_INT_IS_CENTS=0
USD_TO_ILS_RATE=3.70
PRICE_DECIMALS=0

# manual search
AE_MANUAL_SEARCH_PAGE_SIZE=24
AE_MANUAL_SEARCH_TARGET_LANGUAGE=EN
```

## Notes
- If the supplier already returns ILS → leave conversion OFF.
- Switch to USD-origin + conversion only when you *know* source is USD.
- If you see inflated prices like ×8.2, check `USD_TO_ILS_RATE` & ensure conversion is OFF when source is ILS.
- Topics are split into pages by the bot UI (expected by main.py).

