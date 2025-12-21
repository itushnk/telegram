# -*- coding: utf-8 -*-
"""
main.py — Telegram Post Bot + AliExpress Affiliate refill

Version: 2025-12-16h
Changes vs previous:
- Fix TOP timestamp to GMT+8 (per TOP gateway requirement)
- Raise on TOP error_response (so you finally see the real error instead of '0 products' and None)
- Better refill diagnostics when 0 products returned
"""

import html
import os, sys
os.environ.setdefault("PYTHONUNBUFFERED", "1")
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

import logging
import hashlib
import random
import math
from logging.handlers import RotatingFileHandler

# ========= LOGGING / VERSION =========
CODE_VERSION = os.environ.get("CODE_VERSION", "v2025-12-20currencyfix")
def _code_fingerprint() -> str:
    try:
        p = os.path.abspath(__file__)
        with open(p, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:12]
    except Exception:
        return "unknown"

LOG_DIR = os.environ.get("BOT_DATA_DIR", "./data")
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception:
    pass

LOG_PATH = os.path.join(LOG_DIR, "bot.log")

STATE_PATH = os.path.join(LOG_DIR, "bot_state.json")

def _load_state():
    try:
        if not os.path.exists(STATE_PATH):
            return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_state(state: dict):
    try:
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state or {}, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_PATH)
    except Exception:
        pass

BOT_STATE = _load_state()

def _get_state_str(key: str, default: str = "") -> str:
    v = BOT_STATE.get(key)
    if v is None:
        v = default
    return str(v or "").strip()

def _set_state_str(key: str, value: str):
    BOT_STATE[key] = (value or "").strip()
    _save_state(BOT_STATE)


def _set_state_bool(key: str, value: bool):
    _set_state_str(key, "1" if value else "0")


def _get_state_int(key: str, default: int = 0) -> int:
    try:
        s = _get_state_str(key, str(default))
        if s == "":
            return int(default)
        return int(float(s))
    except Exception:
        return int(default)

def _get_state_float(key: str, default: float = 0.0) -> float:
    try:
        s = _get_state_str(key, str(default))
        if s == "":
            return float(default)
        return float(s)
    except Exception:
        return float(default)

def _get_state_bool(key: str, default: bool = False) -> bool:
    s = _get_state_str(key, "1" if default else "0").lower()
    return s in ("1", "true", "yes", "y", "on", "t")

def _get_state_csv_set(key: str, default_raw: str = "") -> set[str]:
    raw = _get_state_str(key, default_raw)
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    return set(parts)


def _parse_price_buckets(raw: str):
    """Parse bucket spec like: '1-5,5-10,10-20,20-50,50+' into [(1,5),(5,10),(10,20),(20,50),(50,None)].
    Safe to call early during module import (no dependencies on other helpers).
    """
    raw = (raw or "").strip()
    if not raw:
        return []
    def _num(s: str):
        s = (s or "").strip().replace(",", "")
        m = re.search(r"[-+]?(?:\d+\.?\d*|\.\d+)", s)
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    buckets = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if part.endswith("+"):
            mn = _num(part[:-1])
            if mn is not None:
                buckets.append((mn, None))
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            mn = _num(a)
            mx = _num(b)
            if mn is None or mx is None:
                continue
            if mx < mn:
                mn, mx = mx, mn
            buckets.append((mn, mx))
            continue
        # single number => treat as '>= number'
        mn = _num(part)
        if mn is not None:
            buckets.append((mn, None))
    return buckets

def _set_state_csv_set(key: str, values: set[str]):
    raw = ",".join(sorted({(v or "").strip() for v in (values or set()) if (v or "").strip()}))
    _set_state_str(key, raw)




_logger = logging.getLogger("bot")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setLevel(logging.INFO)
    _fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    _sh.setFormatter(_fmt)
    _logger.addHandler(_sh)
    try:
        _fh = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        _fh.setLevel(logging.INFO)
        _fh.setFormatter(_fmt)
        _logger.addHandler(_fh)
    except Exception:
        pass

def log_info(msg: str):
    try:
        _logger.info(msg)
    except Exception:
        print(msg, flush=True)


def log_error(msg: str):
    """Backwards-compat alias: some places call log_error()."""
    try:
        log_exc(msg)
    except Exception:
        try:
            print(f"[ERROR] {msg}", flush=True)
        except Exception:
            pass

def log_exc(msg: str):
    try:
        _logger.exception(msg)
    except Exception:
        print(msg, flush=True)

import csv
import time
import re
import json
import socket
import threading
import hashlib
import requests
from datetime import datetime, timedelta, time as dtime, timezone
from zoneinfo import ZoneInfo

import telebot
from telebot import types

# ========= PERSISTENT DATA DIR =========
BASE_DIR = os.environ.get("BOT_DATA_DIR", "./data")
os.makedirs(BASE_DIR, exist_ok=True)

# ========= CONFIG (Telegram) =========
BOT_TOKEN = (os.environ.get("BOT_TOKEN", "") or "").strip()  # חובה ב-ENV
CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL", "@nisayon121")  # יעד ציבורי ברירת מחדל

# Join link for the channel (used in captions/buttons). Prefer explicit env var; fallback to https://t.me/<channel>
JOIN_URL = (
    os.environ.get("JOIN_URL")
    or os.environ.get("CHANNEL_JOIN_URL")
    or os.environ.get("PUBLIC_JOIN_URL")
    or ""
).strip()
if not JOIN_URL:
    if CHANNEL_ID.startswith("@"):
        JOIN_URL = f"https://t.me/{CHANNEL_ID[1:]}"
    elif CHANNEL_ID.startswith("http"):
        JOIN_URL = CHANNEL_ID
    else:
        JOIN_URL = f"https://t.me/{CHANNEL_ID}"
ADMIN_USER_IDS_RAW = (os.environ.get("ADMIN_USER_IDS", "") or "").strip()  # "123,456"
ADMIN_USER_IDS = set(int(x) for x in ADMIN_USER_IDS_RAW.split(",") if x.strip().isdigit()) if ADMIN_USER_IDS_RAW else set()

# קבצים (בתיקיית DATA המתמשכת)
DATA_CSV    = os.path.join(BASE_DIR, "workfile.csv")        # קובץ המקור האחרון שהועלה
PENDING_CSV = os.path.join(BASE_DIR, "pending.csv")         # תור הפוסטים
DELAY_FILE  = os.path.join(BASE_DIR, "post_delay.txt")      # מרווח שידור
PUBLIC_PRESET_FILE  = os.path.join(BASE_DIR, "public_target.preset")
PRIVATE_PRESET_FILE = os.path.join(BASE_DIR, "private_target.preset")

SCHEDULE_FLAG_FILE      = os.path.join(BASE_DIR, "schedule_enforced.flag")
CONVERT_NEXT_FLAG_FILE  = os.path.join(BASE_DIR, "convert_next_usd_to_ils.flag")
AUTO_FLAG_FILE          = os.path.join(BASE_DIR, "auto_delay.flag")
ADMIN_CHAT_ID_FILE      = os.path.join(BASE_DIR, "admin_chat_id.txt")  # לשידורי סטטוס/מילוי

USD_TO_ILS_RATE_DEFAULT = float(os.environ.get("USD_TO_ILS_RATE", "3.55") or "3.55")

# ========= PRICE CURRENCY MODE =========
# AE affiliate API usually returns prices in the requested target_currency (default USD),
# but sometimes the returned fields (especially app_* fields) may already be in ILS.
# We support a runtime switch to tell the bot what currency the incoming prices are in,
# and whether to convert USD→ILS for display.
AE_PRICE_INPUT_CURRENCY_DEFAULT = (os.environ.get("AE_PRICE_INPUT_CURRENCY", "USD") or "USD").strip().upper()
AE_PRICE_INPUT_CURRENCY = (_get_state_str("price_input_currency", AE_PRICE_INPUT_CURRENCY_DEFAULT) or AE_PRICE_INPUT_CURRENCY_DEFAULT).strip().upper()
if AE_PRICE_INPUT_CURRENCY not in ("USD", "ILS"):
    AE_PRICE_INPUT_CURRENCY = "USD"

AE_PRICE_CONVERT_USD_TO_ILS_DEFAULT = (os.environ.get("AE_PRICE_CONVERT_USD_TO_ILS", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
AE_PRICE_CONVERT_USD_TO_ILS = _get_state_bool("convert_usd_to_ils", AE_PRICE_CONVERT_USD_TO_ILS_DEFAULT)

def _display_currency_code() -> str:
    # If input is already ILS, never convert again.
    if AE_PRICE_INPUT_CURRENCY == "ILS":
        return "ILS"
    # Input is USD: convert only when enabled
    return "ILS" if AE_PRICE_CONVERT_USD_TO_ILS else "USD"

def _display_currency_suffix_he() -> str:
    return 'ש"ח' if _display_currency_code() == "ILS" else "$"

def _display_currency_symbol() -> str:
    return "₪" if _display_currency_code() == "ILS" else "$"

PRICE_DECIMALS = int(os.environ.get("PRICE_DECIMALS", "2") or "2")
AE_USE_APP_PRICE = (os.environ.get("AE_USE_APP_PRICE", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
# If TOP returns integer-like prices that are actually cents (e.g. 3690 instead of 36.90), enable this.
AE_PRICE_INT_IS_CENTS = (os.environ.get("AE_PRICE_INT_IS_CENTS", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
# When price is a range like "1.23-4.56": choose "min" or "max" or "mid"
AE_PRICE_PICK_MODE = (os.environ.get("AE_PRICE_PICK_MODE", "min") or "min").strip().lower()

AE_KEYWORDS = (os.environ.get("AE_KEYWORDS", "") or "").strip()
LOCK_PATH = os.environ.get("BOT_LOCK_PATH", os.path.join(BASE_DIR, "bot.lock"))

# ========= CONFIG (AliExpress Affiliate / TOP) =========
# TOP gateway: לפי הדוקומנטציה שער ברירת המחדל ל-Overseas הוא https://api.taobao.com/router/rest
# בפועל, יש משתמשים שמקבלים "isv.appkey-not-exists" על שער מסוים אבל עובדים על שער אחר.
# לכן אנחנו מגדירים *רשימת* שערים וננסה אחד-אחד עד הצלחה.
_env_top_url  = (os.environ.get("AE_TOP_URL", "") or "").strip()      # שער מועדף (אם הוגדר)
_env_top_urls = (os.environ.get("AE_TOP_URLS", "") or "").strip()    # רשימה מופרדת בפסיקים (אם הוגדרה)

_default_candidates = [
    "https://api-sg.aliexpress.com/sync",       # Newer AliExpress gateway (/sync)
    "https://api.taobao.com/router/rest",        # Overseas (US)
    "https://gw.api.taobao.com/router/rest",     # Legacy gateway
    "https://eco.taobao.com/router/rest",        # Alt/legacy
    # "https://de-api.aliexpress.com/router/rest", # EU gateway (often returns isv.appkey-not-exists)
]

AE_TOP_URL_CANDIDATES = []
if _env_top_url:
    AE_TOP_URL_CANDIDATES.append(_env_top_url)
if _env_top_urls:
    for u in _env_top_urls.split(","):
        u = (u or "").strip()
        if u:
            AE_TOP_URL_CANDIDATES.append(u)
for u in _default_candidates:
    if u not in AE_TOP_URL_CANDIDATES:
        AE_TOP_URL_CANDIDATES.append(u)

AE_TOP_URL = AE_TOP_URL_CANDIDATES[0]
AE_APP_KEY = (os.environ.get("AE_APP_KEY", "") or "").strip()
AE_APP_SECRET = (os.environ.get("AE_APP_SECRET", "") or "").strip()
AE_TRACKING_ID = (os.environ.get("AE_TRACKING_ID", "") or "").strip()

# מומלץ לישראל: IL (שני תווים). אפשר לשנות ב-ENV.
AE_SHIP_TO_COUNTRY = (os.environ.get("AE_SHIP_TO_COUNTRY", "IL") or "IL").strip().upper()
AE_TARGET_LANGUAGE = (os.environ.get("AE_TARGET_LANGUAGE", "HE") or "HE").strip().upper()

# target_currency של API לא כולל ILS, לכן עובדים עם USD וממירים לש"ח.
AE_TARGET_CURRENCY = "USD"


# =================== AI (OpenAI) — “לתת חיים למוצרים” ===================
# הפעלה/כיבוי:
GPT_ENABLED = (os.environ.get("GPT_ENABLED", "0") or "0").strip().lower() in ("1","true","yes","on")
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY", "") or "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL", "gpt-4o-mini") or "gpt-4o-mini").strip()

# מודל אפקטיבי (יכול להשתנות אוטומטית לפי הרשאות הפרויקט)
OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL

# הדפס מודלים זמינים ובדוק הרשאות בתחילת ריצה (1 מומלץ עד שהכל עובד)
GPT_DIAG_ON_STARTUP = (os.environ.get("GPT_DIAG_ON_STARTUP", "1") or "1").strip().lower() in ("1","true","yes","on")

# כמה מוצרים לשלוח בכל Batch (אתה ביקשת 10)
GPT_BATCH_SIZE = int(os.environ.get("GPT_BATCH_SIZE", "10") or "10")

# האם לדרוס טקסט קיים (1=כן, 0=לא. אם 0 – משלים רק שדות ריקים)
GPT_OVERWRITE = (os.environ.get("GPT_OVERWRITE", "1") or "1").strip().lower() in ("1","true","yes","on")

# מתי להריץ AI
GPT_ON_REFILL = (os.environ.get("GPT_ON_REFILL", "1") or "1").strip().lower() in ("1","true","yes","on")
GPT_ON_UPLOAD = (os.environ.get("GPT_ON_UPLOAD", "1") or "1").strip().lower() in ("1","true","yes","on")

GPT_ON_SEND_FALLBACK = (os.environ.get("GPT_ON_SEND_FALLBACK", "0") or "0").strip().lower() in ("1","true","yes","on")

# ========= AI APPROVAL WORKFLOW =========
# Default behavior requested: do NOT send any products to OpenAI automatically on startup/refill/upload.
# Admin must explicitly approve items and trigger AI.
AI_AUTO_DEFAULT = (os.environ.get("AI_AUTO_MODE", os.environ.get("AI_AUTO_DEFAULT", "0")) or "0").strip().lower() in ("1","true","yes","on")
AI_AUTO_MODE = _get_state_bool("ai_auto_mode", AI_AUTO_DEFAULT)

def ai_auto_mode() -> bool:
    return bool(AI_AUTO_MODE)

def set_ai_auto_mode(flag: bool):
    global AI_AUTO_MODE
    AI_AUTO_MODE = bool(flag)
    _set_state_bool("ai_auto_mode", AI_AUTO_MODE)

# יציבות/ביצועים
GPT_TIMEOUT_SECONDS = int(os.environ.get("GPT_TIMEOUT_SECONDS", "45") or "45")
GPT_MAX_RETRIES = int(os.environ.get("GPT_MAX_RETRIES", "2") or "2")

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

_openai_client = None

def _ai_enabled() -> bool:
    return bool(GPT_ENABLED and OPENAI_API_KEY and OpenAI is not None)

def _get_openai_client():
    global _openai_client, OPENAI_MODEL_EFFECTIVE
    if _openai_client is None:
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
        # Resolve an actually-available model once (prevents 403 model_not_found loops).
        try:
            _resolve_model_effective(_openai_client)
        except Exception as e:
            logging.warning(f"[AI] model resolve failed: {e}")
            OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL
    return _openai_client


def _resolve_model_effective(client):
    """בחר מודל שעובד בפועל בפרויקט (לפי models.list), כדי למנוע 403 model_not_found."""
    global OPENAI_MODEL_EFFECTIVE

    if not _ai_enabled():
        OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL
        return OPENAI_MODEL_EFFECTIVE, []

    available = []
    try:
        ms = client.models.list()
        available = [m.id for m in getattr(ms, "data", [])]
    except Exception as e:
        logging.warning(f"[AI] models.list failed: {e}")
        OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL
        return OPENAI_MODEL_EFFECTIVE, []

    if OPENAI_MODEL in available:
        OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL
        return OPENAI_MODEL_EFFECTIVE, available

    # נסה לבחור מודל מועדף שקיים
    preferred = [
        "gpt-4o-mini",
        "gpt-4o",
        "gpt-4.1-mini",
        "gpt-4.1",
        "gpt-4-turbo",
        "gpt-4",
        "gpt-3.5-turbo",
    ]
    for cand in preferred:
        if cand in available:
            logging.warning(f"[AI] OPENAI_MODEL='{OPENAI_MODEL}' not available. Auto-selected '{cand}'.")
            OPENAI_MODEL_EFFECTIVE = cand
            return OPENAI_MODEL_EFFECTIVE, available

    logging.warning(f"[AI] OPENAI_MODEL='{OPENAI_MODEL}' not available and no preferred model found. Leaving as-is.")
    OPENAI_MODEL_EFFECTIVE = OPENAI_MODEL
    return OPENAI_MODEL_EFFECTIVE, available


def ai_diagnostics_startup():
    """בדיקות AI בתחילת ריצה: KEY, models.list, בחירת מודל אפקטיבי."""
    if not GPT_DIAG_ON_STARTUP:
        return
    if not GPT_ENABLED:
        logging.info("[AI] diagnostics: GPT_ENABLED=0 (skipping)")
        return
    if not OPENAI_API_KEY:
        logging.warning("[AI] diagnostics: OPENAI_API_KEY missing")
        return
    if OpenAI is None:
        logging.warning("[AI] diagnostics: openai package missing (pip install openai)")
        return

    try:
        client = _get_openai_client()
        effective, available = _resolve_model_effective(client)
        if available:
            # אל תציף לוגים – רק דגימה
            sample = [m for m in available if m.startswith("gpt-")][:60]
            logging.info(f"[AI] available models (gpt-* sample): {sample}")
        logging.info(f"[AI] model effective: {effective}")
    except Exception as e:
        logging.warning(f"[AI] diagnostics failed: {e}")


AE_REFILL_ENABLED = (os.environ.get("AE_REFILL_ENABLED", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
AE_REFILL_INTERVAL_SECONDS = int(os.environ.get("AE_REFILL_INTERVAL_SECONDS", "900") or "900")  # 15 דקות
AE_REFILL_MIN_QUEUE = int(os.environ.get("AE_REFILL_MIN_QUEUE", "30") or "30")
AE_REFILL_MAX_PAGES = int(os.environ.get("AE_REFILL_MAX_PAGES", "3") or "3")
AE_REFILL_PAGE_SIZE = int(os.environ.get("AE_REFILL_PAGE_SIZE", "50") or "50")
AE_REFILL_SORT = (os.environ.get("AE_REFILL_SORT", "LAST_VOLUME_DESC") or "LAST_VOLUME_DESC").strip().upper()

# Optional price filtering (ILS buckets) for refill results.
# Example: AE_PRICE_BUCKETS=1-5,5-10,10-20,20-50,50+
AE_PRICE_BUCKETS_RAW_DEFAULT = (os.environ.get("AE_PRICE_BUCKETS", "") or os.environ.get("AE_PRICE_FILTER", "") or "").strip()
# Allow runtime override via inline buttons (persisted in BOT_STATE)
AE_PRICE_BUCKETS_RAW = _get_state_str("price_buckets_raw", AE_PRICE_BUCKETS_RAW_DEFAULT)
AE_PRICE_BUCKETS = _parse_price_buckets(AE_PRICE_BUCKETS_RAW)

# Optional other filters (persisted)
AE_MIN_ORDERS_DEFAULT = int(float(os.environ.get("AE_MIN_ORDERS", "0") or "0"))
AE_MIN_RATING_DEFAULT = float(os.environ.get("AE_MIN_RATING", "0") or "0")  # percent (0-100)
AE_FREE_SHIP_ONLY_DEFAULT = (os.environ.get("AE_FREE_SHIP_ONLY", "0") or "0").strip().lower() in ("1","true","yes","on")
AE_FREE_SHIP_THRESHOLD_ILS = float(os.environ.get("AE_FREE_SHIP_THRESHOLD_ILS", "38") or "38")  # heuristic
AE_CATEGORY_IDS_DEFAULT = (os.environ.get("AE_CATEGORY_IDS", "") or "").strip()

FREE_SHIP_THRESHOLD_ILS = float(os.environ.get("FREE_SHIP_THRESHOLD_ILS", str(AE_FREE_SHIP_THRESHOLD_ILS)) or str(AE_FREE_SHIP_THRESHOLD_ILS))  # alias/backward-compat
MIN_ORDERS = _get_state_int("min_orders", AE_MIN_ORDERS_DEFAULT)
MIN_RATING = _get_state_float("min_rating", AE_MIN_RATING_DEFAULT)
FREE_SHIP_ONLY = _get_state_bool("free_ship_only", AE_FREE_SHIP_ONLY_DEFAULT)
CATEGORY_IDS_RAW = _get_state_str("category_ids_raw", AE_CATEGORY_IDS_DEFAULT)

def set_min_orders(n: int):
    global MIN_ORDERS
    try:
        n = int(n)
    except Exception:
        n = 0
    MIN_ORDERS = max(0, n)
    _set_state_str("min_orders", str(MIN_ORDERS))

def set_min_rating(v: float):
    global MIN_RATING
    try:
        v = float(v)
    except Exception:
        v = 0.0
    MIN_RATING = max(0.0, v)
    _set_state_str("min_rating", str(MIN_RATING))

def set_free_ship_only(flag: bool):
    global FREE_SHIP_ONLY
    FREE_SHIP_ONLY = bool(flag)
    _set_state_str("free_ship_only", "1" if FREE_SHIP_ONLY else "0")

def _parse_category_ids(raw: str) -> list[str]:
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    # keep order, unique
    seen = set()
    out = []
    for p in parts:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out

def get_selected_category_ids() -> list[str]:
    return _parse_category_ids(CATEGORY_IDS_RAW)

def set_category_ids(ids: list[str]):
    global CATEGORY_IDS_RAW
    raw = ",".join([str(i).strip() for i in ids if str(i).strip()])
    CATEGORY_IDS_RAW = raw
    _set_state_str("category_ids_raw", CATEGORY_IDS_RAW)


# Keep a mutable set in memory for category selection UI (persisted via CATEGORY_IDS_RAW)
CATEGORY_IDS = set(get_selected_category_ids())

def save_user_state():
    """Persist current filters that are maintained in-memory (currently: category ids)."""
    global CATEGORY_IDS
    set_category_ids(sorted(list(CATEGORY_IDS), key=str))


def set_price_buckets_raw(raw: str):
    global AE_PRICE_BUCKETS_RAW, AE_PRICE_BUCKETS
    raw = (raw or "").strip()
    _set_state_str("price_buckets_raw", raw)
    AE_PRICE_BUCKETS_RAW = raw
    AE_PRICE_BUCKETS = _parse_price_buckets(AE_PRICE_BUCKETS_RAW)

# Keep last refill stats for debugging
LAST_REFILL_STATS = {"added": 0, "dup": 0, "skipped_no_link": 0, "price_filtered": 0, "last_error": None, "last_page": 0}

# ========= INIT =========
if not BOT_TOKEN:
    print("[WARN] BOT_TOKEN חסר – הבוט ירוץ אבל לא יתחבר לטלגרם עד שתגדיר ENV.", flush=True)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})
IL_TZ = ZoneInfo("Asia/Jerusalem")

CURRENT_TARGET = CHANNEL_ID
DELAY_EVENT = threading.Event()
EXPECTING_TARGET = {}      # dict[user_id] = "public"|"private"
EXPECTING_UPLOAD = set()   # user_ids שמצפים ל-CSV
FILE_LOCK = threading.Lock()

# ========= SINGLE INSTANCE LOCK =========
def acquire_single_instance_lock(lock_path: str):
    """מונע שתי ריצות *באותה מכונה/קונטיינר*. לא מונע שני קונטיינרים שונים בענן."""
    try:
        if os.name == "nt":
            import msvcrt
            f = open(lock_path, "w")
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                print("Another instance is running. Exiting.", flush=True)
                sys.exit(1)
            return f
        else:
            import fcntl
            f = open(lock_path, "w")
            try:
                fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                print("Another instance is running. Exiting.", flush=True)
                sys.exit(1)
            return f
    except Exception as e:
        print(f"[WARN] Could not acquire single-instance lock: {e}", flush=True)
        return None

# ========= WEBHOOK DIAGNOSTICS =========
def print_webhook_info():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo"
        r = requests.get(url, timeout=10)
        print("getWebhookInfo:", r.json(), flush=True)
    except Exception as e:
        print(f"[WARN] getWebhookInfo failed: {e}", flush=True)

def force_delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": True}, timeout=10)
        print("deleteWebhook:", r.json(), flush=True)
    except Exception as e:
        print(f"[WARN] deleteWebhook failed: {e}", flush=True)

# ========= HELPERS =========
def _now_il():
    return datetime.now(tz=IL_TZ)

def _mask(s: str, keep: int = 4) -> str:
    s = s or ""
    if len(s) <= keep:
        return "*" * len(s)
    return ("*" * (len(s) - keep)) + s[-keep:]

def _save_admin_chat_id(chat_id: int):
    try:
        with open(ADMIN_CHAT_ID_FILE, "w", encoding="utf-8") as f:
            f.write(str(chat_id))
    except Exception:
        pass

def _load_admin_chat_id():
    try:
        if not os.path.exists(ADMIN_CHAT_ID_FILE):
            return None
        with open(ADMIN_CHAT_ID_FILE, "r", encoding="utf-8") as f:
            s = (f.read() or "").strip()
            return int(s) if s.lstrip("-").isdigit() else None
    except Exception:
        return None

def notify_admin(text: str):
    chat_id = _load_admin_chat_id()
    if not chat_id:
        return
    try:
        bot.send_message(chat_id, text)
    except Exception as e:
        print(f"[WARN] notify_admin failed: {e}", flush=True)

def safe_int(value, default=0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default

def _to_str(x) -> str:
    """Safe string cast used in logs/UI."""
    return "" if x is None else str(x)


def norm_percent(value, decimals=1, empty_fallback=""):
    s = str(value).strip() if value is not None else ""
    if not s:
        return empty_fallback
    s = s.replace("%", "")
    try:
        f = float(s)
        return f"{round(f, decimals)}%"
    except Exception:
        return empty_fallback

def clean_price_text(s):
    if s is None:
        return ""
    s = str(s)
    for junk in ["ILS", "₪", "NIS"]:
        s = s.replace(junk, "")
    out = "".join(ch for ch in s if ch.isdigit() or ch == "." or ch == ",")
    return out.strip().replace(",", ".")

def _extract_float(s: str):
    if s is None:
        return None
    m = re.search(r"([-+]?\d+(?:[.,]\d+)?)", str(s))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))

def _format_money(num: float, decimals: int) -> str:
    """Format number with fixed decimals (Excel/Telegram friendly)."""
    try:
        decimals = int(decimals)
    except Exception:
        decimals = 2
    if decimals <= 0:
        return str(int(round(num)))
    return f"{num:.{decimals}f}"

def usd_to_ils(price_text: str, rate: float) -> str:
    """Convert a USD price string to ILS string, preserving decimals.

    Notes:
    - If the raw string already looks like ILS (₪/ILS/NIS), we DO NOT convert again.
    - If the API returns cents as an integer string (e.g. '1290' meaning $12.90), we normalize.
    """
    if price_text is None:
        return ""
    raw_original = str(price_text)
    raw_clean = clean_price_text(raw_original)
    num = _extract_float(raw_clean)
    if num is None:
        return ""

    # Heuristic: cents-as-integer (common in some affiliate fields)
    if AE_PRICE_INT_IS_CENTS and raw_clean and raw_clean.isdigit():
        try:
            ival = int(raw_clean)
            if ival >= 1000 and ival <= 10000000:
                num = ival / 100.0
        except Exception:
            pass

    # If already ILS -> don't convert again
    up = raw_original.upper()
    if ("₪" in raw_original) or ("ILS" in up) or ("NIS" in up):
        ils = float(num)
    else:
        ils = float(num) * float(rate)

    # Apply decimals
    dec = PRICE_DECIMALS
    try:
        dec = int(dec)
    except Exception:
        dec = 2
    return _format_money(round(ils, dec), dec)



def price_text_to_display_amount(price_text: str, usd_to_ils_rate: float) -> str:
    """Normalize incoming price text to what we display in the post.

    Rules:
    - If AE_PRICE_INPUT_CURRENCY=ILS → treat input as ILS and NEVER convert.
    - If input is USD:
        - If AE_PRICE_CONVERT_USD_TO_ILS is ON → convert USD→ILS using usd_to_ils_rate.
        - If OFF → keep USD as-is (no conversion).
    - Cents-as-integer normalization (AE_PRICE_INT_IS_CENTS) is applied in both modes.
    """
    if price_text is None:
        return ""
    raw = str(price_text)
    raw_clean = clean_price_text(raw)
    num = _extract_float(raw_clean)
    if num is None:
        return ""

    # Normalize integer-cents when configured
    if AE_PRICE_INT_IS_CENTS and raw_clean and raw_clean.isdigit():
        try:
            ival = int(raw_clean)
            if ival >= 1000 and ival <= 10000000:
                num = ival / 100.0
        except Exception:
            pass

    # If input currency is ILS, do not convert
    if AE_PRICE_INPUT_CURRENCY == "ILS":
        return _format_money(float(num), PRICE_DECIMALS)

    # Input is USD
    if not AE_PRICE_CONVERT_USD_TO_ILS:
        return _format_money(float(num), PRICE_DECIMALS)

    try:
        num = float(num) * float(usd_to_ils_rate)
    except Exception:
        pass
    return _format_money(float(num), PRICE_DECIMALS)


def _parse_price_buckets(raw: str):
    """Parse price bucket filters like: '1-5,5-10,10-20,20-50,50+'.
    Returns list of (min_inclusive, max_exclusive_or_None). Prices are assumed to be in ILS
    (after USD->ILS conversion in the mapped rows).
    """
    raw = (raw or "").strip()
    if not raw:
        return []
    buckets = []
    for part in raw.split(","):
        part = (part or "").strip()
        if not part:
            continue
        part = part.replace("–", "-").replace("—", "-")  # common dashes
        if part.endswith("+"):
            mn = _extract_float(part[:-1])
            if mn is not None:
                buckets.append((float(mn), None))
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            mn = _extract_float(a)
            mx = _extract_float(b)
            if mn is None or mx is None:
                continue
            # normalize if reversed
            if mx < mn:
                mn, mx = mx, mn
            buckets.append((float(mn), float(mx)))
            continue
        # single number -> treat as exact bucket [n, n+1)
        n = _extract_float(part)
        if n is not None:
            buckets.append((float(n), float(n) + 1.0))
    return buckets

def _price_in_buckets(price_ils: float, buckets) -> bool:
    if not buckets:
        return True
    if price_ils is None:
        return False
    for mn, mx in buckets:
        if mx is None:
            if price_ils >= mn:
                return True
        else:
            if price_ils >= mn and price_ils < mx:
                return True
    return False


def normalize_row_keys(row):
    out = dict(row)

    if "ImageURL" not in out:
        out["ImageURL"] = out.get("Image Url", "") or out.get("ImageURL", "")
    if "Video Url" not in out:
        out["Video Url"] = out.get("Video Url", "") or out.get("VideoURL", "") or ""
    if "BuyLink" not in out:
        out["BuyLink"] = out.get("Promotion Url", "") or out.get("BuyLink", "")

    out["OriginalPrice"] = clean_price_text(out.get("OriginalPrice", "") or out.get("Origin Price", ""))
    out["SalePrice"]     = clean_price_text(out.get("SalePrice", "") or out.get("Discount Price", ""))

    disc = f"{out.get('Discount', '')}".strip()
    if disc and not disc.endswith("%"):
        try:
            disc = f"{int(round(float(disc)))}%"
        except Exception:
            pass
    out["Discount"] = disc

    out["Rating"] = norm_percent(out.get("Rating", "") or out.get("Positive Feedback", "") or out.get("evaluate_rate",""), decimals=1, empty_fallback="")
    if not str(out.get("Orders", "")).strip():
        out["Orders"] = str(out.get("Sales180Day", "") or out.get("lastest_volume","")).strip()

    if "CouponCode" not in out:
        out["CouponCode"] = out.get("Code Name", "") or out.get("CouponCode", "")

    if "ItemId" not in out:
        out["ItemId"] = out.get("ProductId", "") or out.get("product_id","") or out.get("ItemId", "") or "ללא מספר"

    out["Opening"] = out.get("Opening", "") or ""
    out["Title"] = out.get("Title", "") or out.get("Product Desc", "") or out.get("product_title","") or ""
    out["Strengths"] = out.get("Strengths", "") or ""

    # AI workflow state: raw / approved / rejected / done
    st = str(out.get("AIState", "") or out.get("AiState", "") or out.get("ai_state", "") or "").strip().lower()
    if st not in ("raw", "approved", "rejected", "done"):
        st = ""
    if not st:
        if str(out.get("Opening", "")).strip() and str(out.get("Title", "")).strip() and str(out.get("Strengths", "")).strip():
            st = "done"
        else:
            st = "raw"
    out["AIState"] = st

    return out

# =================== AI helpers ===================

_AI_SCHEMA = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "string"},
                    "opening": {"type": "string"},
                    "title": {"type": "string"},
                    "strengths": {
                        "type": "array",
                        "minItems": 3,
                        "maxItems": 3,
                        "items": {"type": "string"}
                    }
                },
                "required": ["item_id", "opening", "title", "strengths"],
                "additionalProperties": False
            }
        }
    },
    "required": ["items"],
    "additionalProperties": False
}

def _openai_structured_items(client, prompt: str) -> dict:
    """Return parsed JSON dict in the schema {items:[...]}.

    - Tries the newer Responses API first (client.responses.create)
    - Falls back to Chat Completions (client.chat.completions.create)
    """
    # Prefer Responses API if available
    # NOTE: In the Responses API, Structured Outputs uses text.format (not response_format).
    # See OpenAI migration docs: "Instead of response_format, use text.format in Responses".
    if hasattr(client, "responses") and hasattr(client.responses, "create"):
        try:
            resp = client.responses.create(
                model=OPENAI_MODEL_EFFECTIVE,
                input=prompt,
                timeout=GPT_TIMEOUT_SECONDS,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "product_copy",
                        "schema": _AI_SCHEMA,
                        "strict": True,
                    }
                },
            )
            text_out = getattr(resp, "output_text", None) or ""
            return json.loads(text_out)
        except TypeError as e:
            # SDK/model mismatch; fall back to Chat Completions.
            logging.warning(f"[AI] Responses.create() TypeError -> fallback to chat.completions: {e}")

    # Fallback: Chat Completions
    system = (
        "אתה מחזיר אך ורק JSON תקין (ללא טקסט מסביב). "
        "ה-JSON חייב להתאים בדיוק לסכימה: {'items':[{'item_id':str,'opening':str,'title':str,'strengths':[str,str,str]}]}."
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL_EFFECTIVE,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        timeout=GPT_TIMEOUT_SECONDS,
    )
    content = (resp.choices[0].message.content or "").strip()
    return json.loads(content)

def _needs_ai(row: dict) -> bool:
    if GPT_OVERWRITE:
        return True
    # אם לא דורסים – משלימים רק אם חסר משהו
    return not (str(row.get("Opening","")).strip() and str(row.get("Title","")).strip() and str(row.get("Strengths","")).strip())

def ai_enrich_rows(rows: list[dict], reason: str = "") -> tuple[int, str | None]:
    """ממלא Opening/Title/Strengths בעברית שיווקית. עובד בבאצ'ים של GPT_BATCH_SIZE.
    מחזיר (כמה עודכנו, שגיאה אחרונה או None).
    """
    if not rows:
        return 0, None
    if not _ai_enabled():
        # לא עוצרים את הבוט אם אין AI – פשוט מדלגים
        if GPT_ENABLED and OpenAI is None:
            return 0, "GPT_ENABLED=1 אבל חסר dependency: openai (pip install openai)"
        if GPT_ENABLED and not OPENAI_API_KEY:
            return 0, "GPT_ENABLED=1 אבל OPENAI_API_KEY חסר"
        return 0, None

    client = _get_openai_client()
    updated = 0
    last_err = None

    # עובדים רק על אלה שבאמת צריכים AI
    todo = [r for r in rows if _needs_ai(r)]
    if not todo:
        return 0, None

    # Batch
    for i in range(0, len(todo), max(1, GPT_BATCH_SIZE)):
        batch = todo[i:i+max(1, GPT_BATCH_SIZE)]
        payload_items = []
        for r in batch:
            item_id = str(r.get("ItemId","") or "").strip()
            raw_title = str(r.get("Title","") or r.get("product_title","") or "").strip()
            # שומרים את המקור כדי לא לאבד מידע
            if raw_title and not str(r.get("OrigTitle","")).strip():
                r["OrigTitle"] = raw_title
            payload_items.append({
                "item_id": item_id,
                "raw_title": raw_title
            })

        # Prompt: בלי המצאות, בלי מחירים, בלי משלוח – רק שכתוב שיווקי על בסיס הכותרת.
        prompt = (
            "אתה קופירייטר שיווקי בעברית לטלגרם. "
            "לכל מוצר תחזיר: opening (משפט פתיחה שנון ורלוונטי, לא כשאלה, עם אימוג׳י אחד), "
            "title (תיאור קצר עד ~100 תווים, עם אימוג׳י מתאים), "
            "strengths (בדיוק 3 שורות, כל שורה מתחילה באימוג׳י מתאים ומדגישה יתרון/חומר/שימוש). "
            "כל מה שאתה כותב חייב להתבסס רק על raw_title. "
            "אל תמציא מפרטים שלא מופיעים. אל תכתוב מחירים/משלוח/קופונים. "
            "שמור על עברית טבעית בלי סימני שאלה מיותרים.\n\n"
            f"סיבה: {reason}\n"
            f"items: {json.dumps(payload_items, ensure_ascii=False)}"
        )

        try:
            # Prefer Responses API, fallback to Chat Completions automatically
            data = _openai_structured_items(client, prompt)
            items = data.get("items", []) if isinstance(data, dict) else []
            by_id = {str(it.get("item_id","")).strip(): it for it in items if isinstance(it, dict)}

            for r in batch:
                iid = str(r.get("ItemId","") or "").strip()
                it = by_id.get(iid)
                if not it:
                    continue
                opening = str(it.get("opening","")).strip()
                title = str(it.get("title","")).strip()
                strengths = it.get("strengths", [])
                if not (opening and title and isinstance(strengths, list) and len(strengths)==3):
                    continue
                r["Opening"] = opening
                r["Title"] = title
                r["Strengths"] = "\n".join([str(s).strip() for s in strengths])
                updated += 1

        except Exception as e:
            last_err = str(e)

    return updated, last_err


def read_products(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [normalize_row_keys(r) for r in reader]

def write_products(path, rows):
    base_headers = [
        "ItemId","ImageURL","Title","OriginalPrice","SalePrice","Discount",
        "Rating","Orders","BuyLink","CouponCode","Opening","Video Url","Strengths","AIState"
    ]
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=base_headers)
            w.writeheader()
        return
    headers = list(dict.fromkeys(base_headers + [k for r in rows for k in r.keys()]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def init_pending():
    if not os.path.exists(PENDING_CSV):
        src = read_products(DATA_CSV)
        write_products(PENDING_CSV, src)

# ---- PRESET HELPERS ----
def _save_preset(path: str, value):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(value))
    except Exception as e:
        print(f"[WARN] Failed to save preset {path}: {e}", flush=True)

def _load_preset(path: str):
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"[WARN] Failed to load preset {path}: {e}", flush=True)
        return None

def resolve_target(value):
    """Resolve chat target:
    - int -> returned
    - negative string -> int
    - '@channel' -> returned
    - special env var names like 'PUBLIC_CHANNEL' -> substituted from environment
    """
    try:
        if isinstance(value, int):
            return value
        s = str(value).strip()
        if not s:
            return s
        # allow selecting env var by name (avoids unsafe eval)
        if s in ("PUBLIC_CHANNEL", "CHANNEL_ID"):
            s2 = (os.environ.get(s) or "").strip()
            if s2:
                s = s2
        if s.startswith("-") and s[1:].isdigit():
            return int(s)
        return s
    except Exception:
        return value

def check_and_probe_target(target):
    try:
        t = resolve_target(target)
        chat = bot.get_chat(t)
        try:
            me = bot.get_me()
            member = bot.get_chat_member(chat.id, me.id)
            status = getattr(member, "status", "")
            if status not in ("administrator", "creator"):
                return False, f"⚠️ הבוט אינו אדמין ביעד {chat.id}."
        except Exception as e_mem:
            print("[WARN] get_chat_member failed:", e_mem, flush=True)

        try:
            m = bot.send_message(chat.id, "🟢 בדיקת הרשאה (תימחק מיד).", disable_notification=True)
            try:
                bot.delete_message(chat.id, m.message_id)
            except Exception:
                pass
            return True, f"✅ יעד תקין: {chat.title or chat.id}"
        except Exception as e_send:
            return False, f"❌ לא הצלחתי לפרסם ביעד: {e_send}"
    except Exception as e:
        return False, f"❌ יעד לא תקין: {e}"

# ========= BROADCAST WINDOW =========
def should_broadcast(now: datetime | None = None) -> bool:
    if now is None:
        now = _now_il()
    else:
        now = now.astimezone(IL_TZ)
    wd = now.weekday()  # Mon=0 ... Sun=6
    t = now.time()
    if wd in (6, 0, 1, 2, 3):
        return dtime(6, 0) <= t <= dtime(23, 59)
    if wd == 4:
        return dtime(6, 0) <= t <= dtime(17, 59)
    if wd == 5:
        return dtime(20, 15) <= t <= dtime(23, 59)
    return False

def is_schedule_enforced() -> bool:
    return os.path.exists(SCHEDULE_FLAG_FILE)

def set_schedule_enforced(enabled: bool) -> None:
    try:
        if enabled:
            with open(SCHEDULE_FLAG_FILE, "w", encoding="utf-8") as f:
                f.write("schedule=on")
        else:
            if os.path.exists(SCHEDULE_FLAG_FILE):
                os.remove(SCHEDULE_FLAG_FILE)
    except Exception as e:
        print(f"[WARN] Failed to set schedule mode: {e}", flush=True)

def is_quiet_now(now: datetime | None = None) -> bool:
    return not should_broadcast(now) if is_schedule_enforced() else False

# ========= SAFE EDIT =========
def safe_edit_message(bot, *, chat_id: int, message, new_text: str, reply_markup=None, parse_mode=None, cb_id=None, cb_info=None):
    """Safely edit an existing message and (optionally) answer callback queries.

    Telegram can return:
    - 400: query is too old / message not modified
    - 409: conflicts (handled elsewhere)
    This helper should NEVER crash the bot.
    """
    try:
        curr_text = (getattr(message, "text", None) or getattr(message, "caption", None) or "")
        # If text unchanged, just update markup (if provided) and answer callback
        if curr_text == (new_text or ""):
            if reply_markup is not None:
                try:
                    bot.edit_message_reply_markup(chat_id, message.message_id, reply_markup=reply_markup)
                except Exception:
                    pass
            if cb_id:
                try:
                    bot.answer_callback_query(cb_id)
                except Exception:
                    pass
            return

        try:
            bot.edit_message_text(new_text, chat_id, message.message_id, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            # Try caption edit (for media messages)
            try:
                bot.edit_message_caption(chat_id=chat_id, message_id=message.message_id, caption=new_text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception:
                pass

        if cb_id:
            try:
                bot.answer_callback_query(cb_id)
            except Exception:
                pass
    except Exception as e:
        try:
            log_error(f"safe_edit_message failed: {e} | {cb_info or ''}")
        except Exception:
            pass
        if cb_id:
            try:
                bot.answer_callback_query(cb_id)
            except Exception:
                pass

def format_post(product):
    item_id = product.get('ItemId', 'ללא מספר')
    image_url = product.get('ImageURL', '')
    title = product.get('Title', '')
    original_price = product.get('OriginalPrice', '')
    sale_price = product.get('SalePrice', '')
    discount = product.get('Discount', '')
    rating = product.get('Rating', '')
    orders = product.get('Orders', '')
    buy_link = product.get('BuyLink', '')
    # Use a shortened buy link for HTML anchors to avoid huge URLs in captions
    buy_link_short = ''
    try:
        buy_link_short = _maybe_shorten_buy_link(item_id, buy_link) if buy_link else ''
    except Exception:
        buy_link_short = buy_link
    coupon = product.get('CouponCode', '')

    opening = (product.get('Opening') or '').strip()
    strengths_src = (product.get("Strengths") or "").strip()

    rating_percent = rating if rating else "אין דירוג"
    orders_num = safe_int(orders, default=0)
    orders_text = f"{orders_num} הזמנות" if orders_num >= 50 else "פריט חדש לחברי הערוץ"
    discount_text = f"💸 חיסכון של {discount}!" if discount and discount != "0%" else ""
    coupon_text = f"🎁 קופון לחברי הערוץ בלבד: {coupon}" if str(coupon).strip() else ""

    lines = []
    if opening:
        lines.append(opening)
        lines.append("")
    if title:
        lines.append(title)
        lines.append("")

    if strengths_src:
        for part in [p.strip() for p in strengths_src.replace("|", "\n").replace(";", "\n").split("\n")]:
            if part:
                lines.append(part)
        lines.append("")
    price_label = "מחיר החל מ" if (product.get("PriceIsFrom") or "").strip() else "מחיר מבצע"
    cur_code = _display_currency_code()
    if cur_code == "ILS":
        price_line = f'💰 {price_label}: {sale_price} ש"ח (מחיר מקורי: {original_price} ש"ח)'
        ship_line = '🚚 משלוח חינם מעל 38 ש"ח או 7.49 ש"ח'
    else:
        price_line = f'💰 {price_label}: ${sale_price} (מחיר מקורי: ${original_price})'
        ship_line = '🚚 משלוח/מחירון לפי תנאי המוכר'
    lines += [
        price_line,
        discount_text,
        f"⭐ דירוג: {rating_percent}",
        f"📦 {orders_text}",
        ship_line,
    ]

    if coupon_text:
        lines += ["", coupon_text]

    lines += [
        "",
        f'להזמנה מהירה👈 <a href="{buy_link_short}">לחצו כאן</a>',
        "",
        f"מספר פריט: {item_id}",
        f'להצטרפות לערוץ לחצו כאן👈 <a href="{JOIN_URL}">קליק והצטרפתם</a>',
        "",
        "👇🛍הזמינו עכשיו🛍👇",
        f'<a href="{buy_link_short}">לחיצה וזה בדרך </a>',
    ]

    # לא מסננים שורות ריקות לגמרי, כדי לשמור על ריווח נעים
    post = "\n".join([l if l is not None else "" for l in lines])
    return post, image_url

def _strip_html(s: str) -> str:
    try:
        return re.sub(r"<[^>]+>", "", s or "")
    except Exception:
        return s or ""

def _count_buttons(markup) -> int:
    try:
        if not markup or not getattr(markup, 'keyboard', None):
            return 0
        return sum(len(row) for row in markup.keyboard if row)
    except Exception:
        return 0



def _canonical_item_url(item_id: str) -> str:
    item_id = str(item_id or "").strip()
    if not item_id:
        return ""
    return f"https://www.aliexpress.com/item/{item_id}.html"

def _find_first_url(obj):
    """Best-effort search for a URL inside nested dict/list responses."""
    if isinstance(obj, str):
        if obj.startswith("http://") or obj.startswith("https://"):
            return obj
        return None
    if isinstance(obj, dict):
        for k in ("promotion_link", "promotionUrl", "promotion_url", "url", "short_url"):
            got = _find_first_url(obj.get(k))
            if got:
                return got
        for v in obj.values():
            got = _find_first_url(v)
            if got:
                return got
    if isinstance(obj, list):
        for it in obj:
            got = _find_first_url(it)
            if got:
                return got
    return None

def _maybe_shorten_buy_link(item_id: str, buy_link: str) -> str:
    """If link is very long, regenerate a clean affiliate link via link.generate.
    Fallback to canonical item URL.
    """
    buy_link = (buy_link or "").strip()
    if buy_link and len(buy_link) <= 512 and "/s/" not in buy_link:
        return buy_link

    canonical = _canonical_item_url(item_id)
    fallback = canonical or (buy_link[:512] if buy_link else "")

    try:
        if not AE_APP_KEY or not AE_APP_SECRET or not AE_TRACKING_ID:
            return fallback
        payload = _top_call("aliexpress.affiliate.link.generate", {
            "tracking_id": AE_TRACKING_ID,
            "promotion_link_type": "0",
            "source_values": canonical or buy_link,
        })
        rr = _extract_resp_result(payload)
        short = _find_first_url(rr)
        if short and len(short) <= 512:
            return short
    except Exception as e:
        logging.warning("link.generate failed (fallback to canonical): %s", e)

    return fallback

def _build_post_buttons(item_id: str, buy_link: str):
    try:
        url_buy = _maybe_shorten_buy_link(item_id, buy_link)
        url_join = (JOIN_URL or "").strip()
        mk = types.InlineKeyboardMarkup(row_width=1)
        if url_buy:
            mk.add(types.InlineKeyboardButton("👇🛍 הזמינו עכשיו 🛍👇", url=url_buy))
        if url_join:
            mk.add(types.InlineKeyboardButton("👈 להצטרפות לערוץ", url=url_join))
        return mk
    except Exception as e:
        logging.warning("failed to build buttons: %s", e)
        return None

def post_to_channel(product) -> bool:
    """Send a single media message (photo/video) with HTML caption when possible.
    Returns True on success, False on failure (so queue won't advance on failures).
    """
    try:
        post_text, image_url = format_post(product)
        video_url = (product.get('Video Url') or product.get('VideoURL') or product.get('VideoURL'.lower()) or "").strip()
        target = resolve_target(CURRENT_TARGET)

        item_id = str(product.get("ProductId") or product.get("ItemId") or product.get("item_id") or "")
        buy_link_btn = str(product.get("BuyLink") or "")
        buttons = _build_post_buttons(item_id, buy_link_btn)

        # Caption safety: Telegram captions are 0-1024 characters AFTER entities parsing
        # We'll estimate using visible text (strip HTML tags).
        raw_lines = (post_text or "").splitlines()

        # Drop the bottom CTA block if needed (it is repetitive and tends to be long)
        trimmed_lines = []
        for ln in raw_lines:
            if ln.strip().startswith("👇🛍"):
                break
            trimmed_lines.append(ln)

        # Build caption without exceeding ~1000 visible chars
        caption_lines = []
        visible_total = 0
        for ln in trimmed_lines:
            vis = len(_strip_html(ln))
            if visible_total + vis + 1 > 1000:
                break
            caption_lines.append(ln)
            visible_total += vis + 1

        caption = "\n".join(caption_lines).strip()
        # Telegram caption hard limit is 1024 chars (raw, including hidden URLs in HTML).
        # If the caption is still too long, truncate safely by dropping lines from the bottom.
        while len(caption) > 1024 and len(caption_lines) > 1:
            caption_lines.pop()
            caption = "\n".join(caption_lines)
        if len(caption) > 1024:
            caption = caption[:1020] + "…"

        log_info(f"POST start item={product.get('ItemId','')} media={'video' if video_url.startswith('http') else 'photo'} raw_len={len(caption)} vis_len={len(_strip_html(caption))} buttons={_count_buttons(buttons)} target={target}")

        if video_url.startswith("http"):
            try:
                resp = SESSION.get(video_url, timeout=30)
                resp.raise_for_status()
                bot.send_video(target, resp.content, caption=caption, parse_mode="HTML")
                log_info(f"POST ok item={product.get('ItemId','')} (video)")
                return True
            except Exception as ve:
                log_info(f"Video fetch/send failed, fallback to photo. item={product.get('ItemId','')} err={ve}")

        resp = SESSION.get(image_url, timeout=30)
        resp.raise_for_status()
        bot.send_photo(target, resp.content, caption=caption, parse_mode="HTML")

        log_info(f"POST ok item={product.get('ItemId','')}")
        return True

    except Exception as e:
        log_exc(f"POST failed item={product.get('ItemId','')} err={e}")
        return False

# ========= ATOMIC SEND =========
# ========= ATOMIC SEND =========
def send_next_locked(source: str = "loop") -> bool:
    with FILE_LOCK:
        pending = read_products(PENDING_CSV)
        if not pending:
            log_info(f"{source}: no pending")
            return False

        item = pending[0]
        item_id = (item.get("ItemId") or "").strip()
        title = (item.get("Title") or "").strip()[:120]
        log_info(f"{source}: sending ItemId={item_id} | Title={title}")

        ok = post_to_channel(item)
        if not ok:
            # IMPORTANT: do NOT advance queue on failures
            log_info(f"{source}: send FAILED, queue NOT advanced (ItemId={item_id})")
            return False

        try:
            write_products(PENDING_CSV, pending[1:])
        except Exception as e:
            log_info(f"{source}: write FAILED, retry once: {e}")
            time.sleep(0.2)
            try:
                write_products(PENDING_CSV, pending[1:])
            except Exception as e2:
                log_exc(f"{source}: write FAILED permanently: {e2}")
                return False

        log_info(f"{source}: sent & advanced queue (ItemId={item_id})")
        return True


# ========= DELAY =========
AUTO_SCHEDULE = [
    (dtime(6, 0),  dtime(9, 0),  1200),
    (dtime(9, 0),  dtime(15, 0), 1500),
    (dtime(15, 0), dtime(22, 0), 1200),
    (dtime(22, 0), dtime(23, 59),1500),
]

def read_auto_flag():
    try:
        with open(AUTO_FLAG_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or "on"
    except Exception:
        return "on"

def write_auto_flag(value):
    with open(AUTO_FLAG_FILE, "w", encoding="utf-8") as f:
        f.write(value)

def get_auto_delay():
    now = _now_il().time()
    for start, end, delay in AUTO_SCHEDULE:
        if start <= now <= end:
            return delay
    return None

def load_delay_seconds(default_seconds: int = 1500) -> int:
    try:
        if os.path.exists(DELAY_FILE):
            with open(DELAY_FILE, "r", encoding="utf-8") as f:
                val = int((f.read() or "").strip())
                if val > 0:
                    return val
    except Exception:
        pass
    return default_seconds

def save_delay_seconds(seconds: int) -> None:
    try:
        with open(DELAY_FILE, "w", encoding="utf-8") as f:
            f.write(str(seconds))
    except Exception as e:
        print(f"[WARN] Failed to save delay: {e}", flush=True)

POST_DELAY_SECONDS = load_delay_seconds(1500)  # 25 דקות

# ========= ADMIN =========
def _is_admin(obj) -> bool:
    """Return True if the sender is allowed to use admin-only actions.

    Supports both:
    - Message (msg)
    - CallbackQuery (c)
    """
    # If no admins configured -> allow everyone (useful for first setup)
    if not ADMIN_USER_IDS:
        return True

    uid = None
    # CallbackQuery has from_user; Message has from_user; some objects have .message.from_user
    try:
        if getattr(obj, "from_user", None) is not None:
            uid = obj.from_user.id
        elif getattr(obj, "message", None) is not None and getattr(obj.message, "from_user", None) is not None:
            uid = obj.message.from_user.id
    except Exception:
        uid = None

    return (uid is not None) and (uid in ADMIN_USER_IDS)

# Backwards-compatible alias: some handlers were written with is_admin(...)
# and expect it to exist.
def is_admin(obj) -> bool:
    return _is_admin(obj)



@bot.message_handler(commands=["myid", "whoami"])
def cmd_myid(msg):
    uid = msg.from_user.id if msg.from_user else None
    uname = ("@" + msg.from_user.username) if (msg.from_user and msg.from_user.username) else "(no username)"
    bot.reply_to(msg, f"🆔 מזהה משתמש (User ID): {uid}\n👤 משתמש: {uname}")

@bot.message_handler(commands=["ai"])
def cmd_ai(msg):
    # Keep this mostly admin-only (but let anyone see basic instructions)
    if not _is_admin(msg):
        bot.reply_to(
            msg,
            "אין לך הרשאה לפקודות ניהול.\n"
            "כדי להגדיר הרשאה: הפעל /myid ואז הוסף את המספר שקיבלת ל-ADMIN_USER_IDS ב-ENV (ב-Railway), למשל: 123456789"
        )
        return

    key_ok = bool(OPENAI_API_KEY and OPENAI_API_KEY.strip())
    bot.reply_to(
        msg,
        "🤖 סטטוס AI\n"
        f"GPT_ENABLED={GPT_ENABLED}\n"
        f"OPENAI_MODEL={OPENAI_MODEL}\n"
        f"OPENAI_MODEL_EFFECTIVE={OPENAI_MODEL_EFFECTIVE}\n"
        f"OPENAI_API_KEY={'OK' if key_ok else 'MISSING'}\n"
        f"GPT_ON_REFILL={GPT_ON_REFILL} | GPT_ON_UPLOAD={GPT_ON_UPLOAD} | GPT_ON_SEND_FALLBACK={GPT_ON_SEND_FALLBACK}\n"

        f"GPT_BATCH_SIZE={GPT_BATCH_SIZE} | GPT_TIMEOUT_SECONDS={GPT_TIMEOUT_SECONDS} | GPT_MAX_RETRIES={GPT_MAX_RETRIES}"
    )

@bot.message_handler(commands=["ai_test"])
def cmd_ai_test(msg):
    if not _is_admin(msg):
        bot.reply_to(msg, "אין לך הרשאה.")
        return
    if not _ai_enabled():
        bot.reply_to(msg, "AI כבוי או OPENAI_API_KEY חסר. בדוק GPT_ENABLED ו-OPENAI_API_KEY.")
        return
    try:
        client = _get_openai_client()
        # Force a fresh model resolution and show what was chosen
        chosen, available = _resolve_model_effective(client)
        bot.reply_to(msg, f"✅ models.list עובד.\nנבחר מודל: {chosen}\nדגימה זמינה: {', '.join(available[:10]) or '(none)'}")
    except Exception as e:
        bot.reply_to(msg, f"❌ בדיקת AI נכשלה: {e}")

# ========= MERGE =========
def _key_of_row(r: dict):
    item_id = (r.get("ItemId") or "").strip()
    title   = (r.get("Title") or "").strip()
    buy     = (r.get("BuyLink") or "").strip()
    return (item_id if item_id else None, title if not item_id else None, buy)

def merge_from_data_into_pending():
    """Merge rows from DATA_CSV into PENDING_CSV.

    Returns: (added, already, total_after)
    """
    with FILE_LOCK:
        data_rows = read_products(DATA_CSV)
        pending_rows = read_products(PENDING_CSV)
        existing_keys = {_key_of_row(r) for r in pending_rows}

    # Only new candidates (so we don't waste AI calls)
    new_candidates = [r for r in data_rows if _key_of_row(r) not in existing_keys]

    # AI enrichment (optional) — run only on new candidates
    if ai_auto_mode() and GPT_ON_UPLOAD and new_candidates:
        try:
            upd, err = ai_enrich_rows(new_candidates, reason="csv_upload")
            if err:
                logging.warning(f"[AI] enrich warning: {err}")
            elif upd:
                logging.info(f"[AI] enriched {upd} items on upload")
        except Exception as _e:
            logging.warning(f"[AI] enrich failed: {_e}")

    added = 0
    already = 0
    for r in data_rows:
        k = _key_of_row(r)
        if k in existing_keys:
            already += 1
            continue
        pending_rows.append(r)
        existing_keys.add(k)
        added += 1

    with FILE_LOCK:
        write_products(PENDING_CSV, pending_rows)
        total_after = len(pending_rows)

    return added, already, total_after

def delete_source_csv_file():
    with FILE_LOCK:
        write_products(DATA_CSV, [])
    return True

def delete_source_rows_from_pending():
    with FILE_LOCK:
        src_rows = read_products(DATA_CSV)
        if not src_rows:
            return 0, 0

        src_keys = {_key_of_row(r) for r in src_rows}
        pending_rows = read_products(PENDING_CSV)
        if not pending_rows:
            write_products(PENDING_CSV, [])
            return 0, 0

        before = len(pending_rows)
        filtered = [r for r in pending_rows if _key_of_row(r) not in src_keys]
        removed = before - len(filtered)
        write_products(PENDING_CSV, filtered)
        return removed, len(filtered)

# ========= USD→ILS HELPERS (CSV upload option) =========
def _decode_csv_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1255", "iso-8859-8"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="ignore")

def _is_usd_price(raw_value: str) -> bool:
    s = (raw_value or "")
    if not isinstance(s, str):
        s = str(s)
    s_low = s.lower()
    return ("$" in s) or ("usd" in s_low)

def _rows_with_optional_usd_to_ils(rows_raw: list[dict], rate: float | None):
    out = []
    for r in rows_raw:
        rr = dict(r)
        if rate:
            orig_src = rr.get("OriginalPrice", rr.get("Origin Price", ""))
            sale_src = rr.get("SalePrice", rr.get("Discount Price", ""))

            if _is_usd_price(str(orig_src)):
                rr["OriginalPrice"] = usd_to_ils(orig_src, rate)
            if _is_usd_price(str(sale_src)):
                rr["SalePrice"] = usd_to_ils(sale_src, rate)
        out.append(normalize_row_keys(rr))
    return out

# ========= AliExpress Affiliate (TOP) =========
def _top_sign_md5(params: dict, secret: str) -> str:
    # Taobao TOP MD5 sign: md5(secret + concat(k+v sorted) + secret).upper()
    items = [(k, params[k]) for k in sorted(params.keys()) if params[k] is not None and params[k] != ""]
    base = secret + "".join(f"{k}{v}" for k, v in items) + secret
    return hashlib.md5(base.encode("utf-8")).hexdigest().upper()

def _top_timestamp_gmt8() -> str:
    # TOP requires timestamp in GMT+8
    ts = datetime.now(timezone.utc) + timedelta(hours=8)
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def _top_call(method_name: str, biz_params: dict) -> dict:
    if not AE_APP_KEY or not AE_APP_SECRET:
        raise RuntimeError("חסרים AE_APP_KEY / AE_APP_SECRET ב-ENV")

    params = {
        "method": method_name,
        "app_key": AE_APP_KEY,
        "format": "json",
        "v": "2.0",
        "sign_method": "md5",
        "timestamp": _top_timestamp_gmt8(),
        **{k: v for k, v in biz_params.items() if v is not None and v != ""},
    }
    params["sign"] = _top_sign_md5(params, AE_APP_SECRET)

    last_err = None

    for top_url in AE_TOP_URL_CANDIDATES:
        try:
            if "/sync" in (top_url or "").lower().rstrip("/"):
                r = SESSION.get(top_url, params=params, timeout=30)
                if r.status_code in (405, 414):
                    r = SESSION.post(top_url, data=params, timeout=30)
            else:
                r = SESSION.post(top_url, data=params, timeout=30)
            r.raise_for_status()
            payload = r.json()

            # אם יש error_response — נחליט האם לנסות URL נוסף או לזרוק חריגה
            if isinstance(payload, dict) and payload.get("error_response"):
                er = payload.get("error_response") or {}
                code = er.get("code")
                sub_code = er.get("sub_code")
                msg = er.get("msg")
                sub_msg = er.get("sub_msg")

                last_err = f"TOP error {code}: {msg} | sub_code={sub_code} | sub_msg={sub_msg} | url={top_url}"

                # appkey-not-exists בדרך כלל אומר שנפלנו על gateway שלא מכיר את ה-AppKey.
                # ננסה URL נוסף (גם אם הוגדר AE_TOP_URL ב-ENV), כדי לחסוך הסתבכויות בהגדרה.
                if sub_code == "isv.appkey-not-exists" or code == 29:
                    continue

                raise RuntimeError(last_err)

            # הצלחה: נשמור את ה-URL שעבד (כדי שכל הקריאות הבאות ישתמשו בו)
            global AE_TOP_URL
            AE_TOP_URL = top_url
            return payload

        except Exception as e:
            # אם זה לא היה error_response אלא בעיית רשת/HTTP — נשמור וננסה URL הבא
            last_err = f"TOP request failed via {top_url}: {type(e).__name__}: {e}"
            continue

    raise RuntimeError(last_err or "TOP call failed")
def _extract_resp_result(payload: dict) -> dict:
    # response wrapper key usually ends with "_response"
    if not isinstance(payload, dict):
        return {}
    wrapper_key = None
    for k in payload.keys():
        if k.endswith("_response"):
            wrapper_key = k
            break
    root = payload.get(wrapper_key, payload) if wrapper_key else payload
    return root.get("resp_result") or root.get("result") or root

def affiliate_hotproduct_query(page_no: int, page_size: int) -> tuple[list[dict], int | None, str | None]:
    if not AE_TRACKING_ID:
        raise RuntimeError("AE_TRACKING_ID חסר (בלי tracking_id לרוב לא תקבל promotion_link)")

    biz = {
        "page_no": page_no,
        "page_size": page_size,
        "sort": AE_REFILL_SORT,
        "target_currency": AE_TARGET_CURRENCY,
        "target_language": AE_TARGET_LANGUAGE,
        "target_currency": AE_TARGET_CURRENCY,
        "tracking_id": AE_TRACKING_ID,
        "ship_to_country": AE_SHIP_TO_COUNTRY,
        "fields": "product_id,product_title,product_main_image_url,promotion_link,sale_price,original_price,discount,evaluate_rate,lastest_volume,product_video_url,product_detail_url",
        "platform_product_type": "ALL",
    }
    payload = _top_call("aliexpress.affiliate.hotproduct.query", biz)
    resp = _extract_resp_result(payload)
    resp_code = resp.get("resp_code")
    resp_msg = resp.get("resp_msg")

    result = resp.get("result") or {}
    products = result.get("products") or []

    if isinstance(products, dict) and "product" in products:
        products = products.get("product") or []
    if products is None:
        products = []
    if not isinstance(products, list):
        products = [products]

    return products, resp_code, resp_msg


def affiliate_product_query(page_no: int, page_size: int, category_id: str | None = None, keywords: str | None = None) -> tuple[list[dict], int | None, str | None]:
    """Affiliate product query with optional category filter.

    Notes:
    - If `keywords` is provided, it is sent as-is to TOP.
    - Otherwise, if AE_KEYWORDS exists, it rotates keywords to avoid repetitive results.
    """
    fields = "product_id,product_title,product_main_image_url,promotion_link,promotion_url,sale_price,app_sale_price,original_price,discount,evaluate_rate,lastest_volume,product_video_url,product_detail_url"
    biz = {
        "tracking_id": AE_TRACKING_ID,
        "page_no": str(page_no),
        "page_size": str(page_size),
        "sort": AE_REFILL_SORT,
        "ship_to_country": AE_SHIP_TO_COUNTRY,
        "target_currency": AE_TARGET_CURRENCY,
        "target_language": AE_TARGET_LANGUAGE,
        "fields": fields,
        "platform_product_type": "ALL",
    }
    if category_id:
        biz["category_ids"] = str(category_id).strip()

    # Manual keywords override (e.g., admin "manual search")
    if keywords:
        biz["keywords"] = str(keywords).strip()
    # Otherwise rotate env keywords (if provided)
    elif AE_KEYWORDS:
        kws = [k.strip() for k in re.split(r"[\n,|]+", AE_KEYWORDS) if k.strip()]
        if kws:
            biz["keywords"] = kws[(page_no - 1) % len(kws)]
    payload = _top_call("aliexpress.affiliate.product.query", biz)
    resp = _extract_resp_result(payload)
    resp_code = resp.get("resp_code")
    resp_msg = resp.get("resp_msg")

    result = resp.get("result") or {}
    products = result.get("products") or result.get("product_list") or []
    if isinstance(products, dict) and "product" in products:
        products = products.get("product") or []
    if products is None:
        products = []
    if not isinstance(products, list):
        products = [products]
    return products, resp_code, resp_msg

def _map_affiliate_product_to_row(p: dict) -> dict:
    # מחיר מבצע / מקורי - טיפול בטווחים ("1.23-4.56") + מניעת המרה כפולה אם המחיר כבר בש"ח
    sale_raw = (
        p.get("app_sale_price")
        if AE_USE_APP_PRICE
        else (p.get("sale_price") or p.get("app_sale_price"))
    ) or p.get("target_app_sale_price") or p.get("target_sale_price") or ""
    orig_raw = p.get("original_price") or p.get("target_original_price") or ""

    def _pick_value(raw_val):
        s = str(raw_val or "").strip()
        if "-" in s:
            parts = [x.strip() for x in re.split(r"\s*-\s*", s) if x.strip()]
            if len(parts) >= 2:
                # Range price: choose min/max/mid by AE_PRICE_PICK_MODE, and mark as "from" for labeling
                a = _extract_float(clean_price_text(parts[0]))
                b = _extract_float(clean_price_text(parts[1]))
                if a is None and b is None:
                    return parts[0], True
                if a is None:
                    return parts[1], True
                if b is None:
                    return parts[0], True
                lo = min(float(a), float(b))
                hi = max(float(a), float(b))
                mode = (AE_PRICE_PICK_MODE or "min").lower()
                if mode == "max":
                    chosen = hi
                elif mode in ("mid", "avg", "mean"):
                    chosen = (lo + hi) / 2.0
                else:
                    chosen = lo
                return str(chosen), True
        return s, False

    sale_text, sale_is_from = _pick_value(sale_raw)
    orig_text, orig_is_from = _pick_value(orig_raw)

    sale_disp = price_text_to_display_amount(sale_text, USD_TO_ILS_RATE_DEFAULT)
    orig_disp = price_text_to_display_amount(orig_text, USD_TO_ILS_RATE_DEFAULT)

    product_id = str(p.get("product_id", "")).strip()

    # לפעמים TOP מחזיר promotion_link ריק אם tracking_id לא תקין/לא משויך.
    detail_url = (p.get("product_detail_url") or p.get("product_url") or "").strip()
    if not detail_url and product_id:
        detail_url = f"https://www.aliexpress.com/item/{product_id}.html"

    buy_link = (p.get("promotion_link") or p.get("promotion_url") or "").strip()
    if not buy_link:
        buy_link = detail_url

    return normalize_row_keys({
        "ItemId": product_id,
        "ImageURL": (p.get("product_main_image_url") or "").strip(),
        "Title": (p.get("product_title") or "").strip(),
        "OriginalPrice": orig_disp,
        "OriginalIsFrom": ("1" if orig_is_from else ""),
        "SalePrice": sale_disp,
        "PriceIsFrom": ("1" if sale_is_from else ""),
        "Discount": (p.get("discount") or "").strip(),
        "Rating": (p.get("evaluate_rate") or "").strip(),
        "Orders": str(p.get("lastest_volume") or "").strip(),
        "BuyLink": buy_link,
        "CouponCode": "",
        "Opening": "",
        "Strengths": "",
        "Video Url": (p.get("product_video_url") or "").strip(),
        "AIState": "raw",
    })



def refill_from_affiliate(max_needed: int, keywords: str | None = None) -> tuple[int, int, int, int, str | None]:
    """
    מחזיר: (added, duplicates, total_after, last_page_checked, last_error)
    """
    if not AE_APP_KEY or not AE_APP_SECRET or not AE_TRACKING_ID:
        return 0, 0, 0, 0, "חסרים AE_APP_KEY/AE_APP_SECRET/AE_TRACKING_ID"

    with FILE_LOCK:
        pending_rows = read_products(PENDING_CSV)
        existing_keys = {_key_of_row(r) for r in pending_rows}

    added = 0
    dup = 0
    skipped_no_link = 0
    skipped_price = 0
    last_error = None
    last_page = 0
    last_resp = None

    
    # Build active filters snapshot
    selected_cats = get_selected_category_ids()
    # Distribute evenly if categories selected
    if selected_cats:
        n = len(selected_cats)
        base = max_needed // n
        rem = max_needed % n
        per_cat = []
        for i, cid in enumerate(selected_cats):
            need = base + (1 if i < rem else 0)
            if need > 0:
                per_cat.append((cid, need))

        max_pages_per_cat = max(1, AE_REFILL_MAX_PAGES // max(1, len(per_cat)))

        for (cat_id, need_cat) in per_cat:
            got_cat = 0
            last_page = 0
            for page_no in range(1, max_pages_per_cat + 1):
                last_page = page_no
                try:
                    products, resp_code, resp_msg = affiliate_product_query(page_no, AE_REFILL_PAGE_SIZE, category_id=cat_id, keywords=keywords)
                    last_resp = (resp_code, resp_msg, len(products))

                    if resp_code is not None and str(resp_code).isdigit() and int(resp_code) != 200:
                        last_error = f"resp_code={resp_code} resp_msg={resp_msg}"
                        break

                    if not products:
                        break

                    new_rows = []
                    for p in products:
                        row = _map_affiliate_product_to_row(p)

                        # Filters
                        if AE_PRICE_BUCKETS:
                            sale_num = _extract_float(row.get("SalePrice") or "")
                            if sale_num is None or not _price_in_buckets(float(sale_num), AE_PRICE_BUCKETS):
                                skipped_price += 1
                                continue

                        if MIN_ORDERS:
                            o = safe_int(row.get("Orders") or "0", 0)
                            if o < int(MIN_ORDERS):
                                continue

                        if MIN_RATING:
                            r = _extract_float(row.get("Rating") or "")
                            if r is None or float(r) < float(MIN_RATING):
                                continue

                        if FREE_SHIP_ONLY:
                            sale_num = _extract_float(row.get("SalePrice") or "")
                            if sale_num is None or float(sale_num) < float(AE_FREE_SHIP_THRESHOLD_ILS):
                                continue

                        if not row.get("BuyLink"):
                            skipped_no_link += 1
                            continue

                        k = _key_of_row(row)
                        if k in existing_keys:
                            dup += 1
                            continue
                        existing_keys.add(k)
                        new_rows.append(row)
                        got_cat += 1
                        if got_cat >= need_cat:
                            break

                    # AI enrichment (optional)
                    if ai_auto_mode() and GPT_ON_REFILL and new_rows:
                        try:
                            upd, err = ai_enrich_rows(new_rows, reason="refill_from_affiliate")
                            if err:
                                logging.warning(f"[AI] enrich warning: {err}")
                            elif upd:
                                logging.info(f"[AI] enriched {upd} items on refill")
                        except Exception as _e:
                            logging.warning(f"[AI] enrich failed: {_e}")
                    if new_rows:
                        with FILE_LOCK:
                            pending_rows = read_products(PENDING_CSV)
                            pending_rows.extend(new_rows)
                            write_products(PENDING_CSV, pending_rows)
                        added += len(new_rows)

                    if got_cat >= need_cat or added >= max_needed:
                        break

                except Exception as e:
                    last_error = str(e)
                    break

            if added >= max_needed:
                break

    else:
        # No categories selected:
        # - If admin provided keywords: use affiliate.product.query with those keywords.
        # - Otherwise: use HotProduct feed (most stable) + optional AE_KEYWORDS fallback.
        for page_no in range(1, AE_REFILL_MAX_PAGES + 1):
            last_page = page_no
            try:
                if keywords:
                    products, resp_code, resp_msg = affiliate_product_query(page_no, AE_REFILL_PAGE_SIZE, category_id=None, keywords=keywords)
                else:
                    products, resp_code, resp_msg = affiliate_hotproduct_query(page_no, AE_REFILL_PAGE_SIZE)
                    # Fallback: some accounts/params return empty from hotproduct query.
                    # If AE_KEYWORDS exists, try affiliate.product.query with rotating keywords.
                    if (not products) and AE_KEYWORDS:
                        try:
                            products2, rc2, rm2 = affiliate_product_query(page_no, AE_REFILL_PAGE_SIZE, category_id=None)
                            if products2:
                                products, resp_code, resp_msg = products2, rc2, rm2
                        except Exception:
                            pass
                last_resp = (resp_code, resp_msg, len(products))

                if resp_code is not None and str(resp_code).isdigit() and int(resp_code) != 200:
                    last_error = f"resp_code={resp_code} resp_msg={resp_msg}"
                    break

                if not products:
                    break

                new_rows = []
                for p in products:
                    row = _map_affiliate_product_to_row(p)

                    if AE_PRICE_BUCKETS:
                        sale_num = _extract_float(row.get("SalePrice") or "")
                        if sale_num is None or not _price_in_buckets(float(sale_num), AE_PRICE_BUCKETS):
                            skipped_price += 1
                            continue

                    if MIN_ORDERS:
                        o = safe_int(row.get("Orders") or "0", 0)
                        if o < int(MIN_ORDERS):
                            continue

                    if MIN_RATING:
                        r = _extract_float(row.get("Rating") or "")
                        if r is None or float(r) < float(MIN_RATING):
                            continue

                    if FREE_SHIP_ONLY:
                        sale_num = _extract_float(row.get("SalePrice") or "")
                        if sale_num is None or float(sale_num) < float(AE_FREE_SHIP_THRESHOLD_ILS):
                            continue

                    if not row.get("BuyLink"):
                        skipped_no_link += 1
                        continue

                    k = _key_of_row(row)
                    if k in existing_keys:
                        dup += 1
                        continue
                    existing_keys.add(k)
                    new_rows.append(row)

                # AI enrichment (optional)
                if ai_auto_mode() and GPT_ON_REFILL and new_rows:
                    try:
                        upd, err = ai_enrich_rows(new_rows, reason="refill_from_affiliate")
                        if err:
                            logging.warning(f"[AI] enrich warning: {err}")
                        elif upd:
                            logging.info(f"[AI] enriched {upd} items on refill")
                    except Exception as _e:
                        logging.warning(f"[AI] enrich failed: {_e}")
                if new_rows:
                    with FILE_LOCK:
                        pending_rows = read_products(PENDING_CSV)
                        pending_rows.extend(new_rows)
                        write_products(PENDING_CSV, pending_rows)
                    added += len(new_rows)

                if added >= max_needed:
                    break

            except Exception as e:
                last_error = str(e)
                break

    with FILE_LOCK:
        total_after = len(read_products(PENDING_CSV))

    if added == 0 and last_error is None:
        if skipped_no_link > 0:
            last_error = (
                "⚠️ התקבלו מוצרים אבל כולם בלי promotion_link. "
                "בדרך כלל זה אומר ש-AE_TRACKING_ID לא תקין/לא משויך לחשבון האפילייט שלך. "
                f"(skipped_no_link={skipped_no_link}, last_resp={last_resp})"
            )
        elif last_resp is not None:
            rc, rm, n = last_resp
            last_error = f"0 מוצרים (resp_code={rc}, resp_msg={rm}, ship_to={AE_SHIP_TO_COUNTRY}, sort={AE_REFILL_SORT})"
    # update last stats snapshot
    try:
        LAST_REFILL_STATS.update({
            'added': added,
            'dup': dup,
            'skipped_no_link': skipped_no_link,
            'price_filtered': skipped_price,
            'last_error': last_error,
            'last_page': last_page,
        })
    except Exception:
        pass
    return added, dup, total_after, last_page, last_error

# ========= INLINE MENU =========
PRICE_BUCKET_PRESETS = [
    ("1-5", "1-5"),
    ("5-10", "5-10"),
    ("10-20", "10-20"),
    ("20-50", "20-50"),
    ("50+", "50+"),
]

def _active_price_bucket_ids():
    raw = (AE_PRICE_BUCKETS_RAW or "").strip()
    if not raw:
        return set()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return set(parts)

def _price_filter_menu_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    active = _active_price_bucket_ids()

    btns = []
    for label, bid in PRICE_BUCKET_PRESETS:
        mark = "✅ " if bid in active else ""
        suffix = bid.replace("-", "_").replace("+", "p")  # 50+ -> 50p
        btns.append(types.InlineKeyboardButton(f"{mark}₪ {label}", callback_data="pf_" + suffix))
    kb.add(*btns)

    kb.add(
        types.InlineKeyboardButton("🧹 נקה סינון", callback_data="pf_clear"),
        types.InlineKeyboardButton("⬅️ חזרה", callback_data="pf_back"),
    )
    kb.add(types.InlineKeyboardButton(f"מצב נוכחי: {AE_PRICE_BUCKETS_RAW or 'ללא'}", callback_data="noop_info"))
    return kb


# ========= Additional Filters UI & Category list =========
CATEGORIES_CACHE_PATH = os.path.join(LOG_DIR, "categories_cache.json")
_CATEGORIES_CACHE = None  # list of dicts: {"id": "...", "name": "..."}

def _load_categories_cache():
    global _CATEGORIES_CACHE
    try:
        if _CATEGORIES_CACHE is not None:
            return _CATEGORIES_CACHE
        if not os.path.exists(CATEGORIES_CACHE_PATH):
            _CATEGORIES_CACHE = []
            return _CATEGORIES_CACHE
        with open(CATEGORIES_CACHE_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f) or {}
        ts = payload.get("ts") or 0
        # refresh every 7 days
        if time.time() - float(ts) > 7 * 24 * 3600:
            _CATEGORIES_CACHE = []
            return _CATEGORIES_CACHE
        _CATEGORIES_CACHE = payload.get("cats") or []
        if not isinstance(_CATEGORIES_CACHE, list):
            _CATEGORIES_CACHE = []
        return _CATEGORIES_CACHE
    except Exception:
        _CATEGORIES_CACHE = []
        return _CATEGORIES_CACHE

def _save_categories_cache(cats: list[dict]):
    global _CATEGORIES_CACHE
    try:
        _CATEGORIES_CACHE = cats or []
        tmp = CATEGORIES_CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "cats": _CATEGORIES_CACHE}, f, ensure_ascii=False)
        os.replace(tmp, CATEGORIES_CACHE_PATH)
    except Exception:
        pass

def affiliate_category_get() -> tuple[list[dict], str | None]:
    """Returns (cats, err). Each cat dict has keys: id, name"""
    try:
        # Some gateways require fields parameter
        biz = {"fields": "category_id,category_name", "language": AE_TARGET_LANGUAGE or "EN"}
        payload = _top_call("aliexpress.affiliate.category.get", biz)
        resp = _extract_resp_result(payload)
        result = resp.get("result") or {}
        cats = result.get("categories") or result.get("category_list") or result.get("category") or []
        if isinstance(cats, dict) and "category" in cats:
            cats = cats.get("category") or []
        if cats is None:
            cats = []
        if not isinstance(cats, list):
            cats = [cats]
        out = []
        for c in cats:
            cid = str(c.get("category_id") or c.get("id") or "").strip()
            name = str(c.get("category_name") or c.get("name") or "").strip()
            if cid:
                out.append({"id": cid, "name": name or cid})
        if out:
            _save_categories_cache(out)
        return out, None
    except Exception as e:
        return [], str(e)

def get_categories() -> list[dict]:
    cats = _load_categories_cache()
    return cats or []

# ---------- Filter menus ----------
ORDERS_PRESETS = [0, 10, 50, 100, 300, 500, 1000, 3000, 5000]
RATING_PRESETS = [0, 80, 85, 90, 92, 94, 95, 97]

def _filters_home_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    price_label = AE_PRICE_BUCKETS_RAW or "ללא"
    kb.add(types.InlineKeyboardButton(f"💸 מחיר: {price_label}", callback_data="pf_menu"))

    kb.add(
        types.InlineKeyboardButton(f"📦 מינ' הזמנות: {MIN_ORDERS or 0}", callback_data="fo_menu"),
        types.InlineKeyboardButton(f"⭐ מינ' דירוג: {MIN_RATING or 0:g}%", callback_data="fr_menu"),
    )
    ship_lbl = "✅" if FREE_SHIP_ONLY else "❌"
    kb.add(types.InlineKeyboardButton(f"🚚 משלוח חינם לישראל: {ship_lbl}", callback_data="fs_toggle"))

    cats = get_selected_category_ids()
    cats_lbl = f"{len(cats)} נבחרו" if cats else "ללא"
    kb.add(types.InlineKeyboardButton(f"🧩 קטגוריות: {cats_lbl}", callback_data="fc_menu_0"))

    kb.add(
        types.InlineKeyboardButton("🧹 נקה כל הסינונים", callback_data="flt_clear_all"),
        types.InlineKeyboardButton("⬅️ חזרה", callback_data="flt_back"),
    )
    return kb

def _orders_filter_menu_kb():
    kb = types.InlineKeyboardMarkup(row_width=3)
    btns = []
    for v in ORDERS_PRESETS:
        mark = "✅ " if int(MIN_ORDERS or 0) == int(v) else ""
        btns.append(types.InlineKeyboardButton(f"{mark}{v}", callback_data=f"fo_set_{v}"))
    kb.add(*btns)
    kb.add(types.InlineKeyboardButton("⬅️ חזרה", callback_data="flt_menu"))
    return kb

def _rating_filter_menu_kb():
    kb = types.InlineKeyboardMarkup(row_width=4)
    btns = []
    for v in RATING_PRESETS:
        mark = "✅ " if float(MIN_RATING or 0) == float(v) else ""
        btns.append(types.InlineKeyboardButton(f"{mark}{v}%", callback_data=f"fr_set_{v}"))
    kb.add(*btns)
    kb.add(types.InlineKeyboardButton("⬅️ חזרה", callback_data="flt_menu"))
    return kb


# --- Category UI state (per admin user) ---
CAT_VIEW_MODE: dict[int, str] = {}      # uid -> "top" | "all" | "search"
CAT_LAST_QUERY: dict[int, str] = {}     # uid -> last search query
CAT_SEARCH_WAIT: dict[int, bool] = {}   # uid -> waiting for text query?
CAT_SEARCH_CTX: dict[int, tuple[int,int]] = {}  # uid -> (chat_id, message_id)

# --- Manual PRODUCT search UI state (per admin user) ---
# This is separate from category search. It lets admins type a keyword
# and the bot will fetch products from AliExpress Affiliate API and add
# them to the pending queue.
PROD_SEARCH_WAIT: dict[int, bool] = {}        # uid -> waiting for keyword text?
PROD_SEARCH_CTX: dict[int, tuple[int, int]] = {}  # uid -> (chat_id, menu_message_id)
PROD_SEARCH_PROMPT: dict[int, tuple[int, int]] = {}  # uid -> (chat_id, prompt_message_id)

# Keywords used to shrink the category list in "top" mode (Hebrew+English)
CATEGORY_TOP_KEYWORDS = [
    "טלפון", "אייפון", "iphone", "סמסונג", "samsung", "מובייל", "mobile",
    "שעון", "watch", "apple watch", "גאדג", "gadget",
    "אוזניות", "headphone", "earbud",
    "מחשב", "computer", "laptop",
    "אלקטרוניקה", "electronics",
    "בית", "home", "מטבח", "kitchen",
    "כלי", "tool", "עבודה", "work",
    "רכב", "auto", "car",
    "ספורט", "sport", "running", "fitness",
    "אופנה", "fashion", "בגד", "clothing",
    "יופי", "beauty", "טיפוח", "care",
    "תינוק", "baby", "ילד", "kids", "צעצוע", "toy",
]

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _filter_categories(cats: list[dict], mode: str, uid: int | None = None, query: str | None = None) -> list[dict]:
    if not cats:
        return []
    mode = (mode or "top").lower()

    if mode == "all":
        return cats

    if mode == "search":
        q = _norm(query) or _norm(CAT_LAST_QUERY.get(uid or 0, ""))  # type: ignore
        if not q:
            return []
        tokens = [t for t in re.split(r"\s+", q) if t]
        def match_all(name: str) -> bool:
            n = _norm(name)
            return all(t in n for t in tokens)
        def match_any(name: str) -> bool:
            n = _norm(name)
            return any(t in n for t in tokens)
        strict = [c for c in cats if match_all(c.get("name",""))]
        if strict:
            return strict
        loose = [c for c in cats if match_any(c.get("name",""))]
        return loose

    # mode == "top" (default): shrink big tree using keywords.
    kws = [k for k in CATEGORY_TOP_KEYWORDS if k]
    out = []
    for c in cats:
        n = _norm(c.get("name",""))
        if any(_norm(k) in n for k in kws):
            out.append(c)

    # If too small (e.g., language mismatch), fall back to first 80 categories so menu won't be empty.
    if len(out) < 25:
        return cats[:80]
    # Keep it reasonably short
    return out[:160]

def _categories_menu_kb(page: int = 0, per_page: int = 10, mode: str = "top", uid: int | None = None, query: str | None = None):
    kb = types.InlineKeyboardMarkup(row_width=2)
    cats = get_categories()
    view = _filter_categories(cats, mode=mode, uid=uid, query=query)

    selected = set(get_selected_category_ids())
    total = len(view)
    pages = max(1, math.ceil(total / per_page))
    page = max(0, min(page, pages - 1))

    start = page * per_page
    end = start + per_page
    for c in view[start:end]:
        cid = str(c.get("id"))
        name = c.get("name") or cid
        mark = "✅ " if cid in selected else ""
        kb.add(types.InlineKeyboardButton(f"{mark}{name}", callback_data=f"fc_t_{cid}_{mode}_{page}"))

    # Navigation
    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️ קודם", callback_data=f"fc_{'s' if mode=='search' else ('all' if mode=='all' else 'menu')}_{page-1}"))
    nav.append(types.InlineKeyboardButton(f"עמוד {page+1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(types.InlineKeyboardButton("הבא ➡️", callback_data=f"fc_{'s' if mode=='search' else ('all' if mode=='all' else 'menu')}_{page+1}"))
    kb.row(*nav)

    # Mode switching + search
    switch_row = []
    if mode != "top":
        switch_row.append(types.InlineKeyboardButton("⭐ פופולריות", callback_data="fc_menu_0"))
    if mode != "all":
        switch_row.append(types.InlineKeyboardButton("📚 כל הקטגוריות", callback_data="fc_all_0"))
    switch_row.append(types.InlineKeyboardButton("🔎 חיפוש", callback_data="fc_search"))
    kb.row(*switch_row[:2]) if len(switch_row) == 2 else kb.row(*switch_row)

    # Actions
    kb.row(
        types.InlineKeyboardButton("🧹 נקה הכל", callback_data="fc_clear"),
        types.InlineKeyboardButton("🎲 רנדומלי", callback_data="fc_random"),
    )
    kb.row(
        types.InlineKeyboardButton("🔄 סנכרן קטגוריות", callback_data="fc_sync"),
        types.InlineKeyboardButton("⬅️ חזרה לסינונים", callback_data="filters"),
    )
    return kb
def handle_filters_callback(c, data: str, chat_id: int) -> bool:
    """Return True if handled."""
    try:
        uid = getattr(getattr(c, 'from_user', None), 'id', None) or getattr(getattr(getattr(c, 'message', None), 'from_user', None), 'id', 0)
        # home
        if data == "flt_menu":
            txt = "🧰 סינונים\nבחר מה לשנות:"
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=txt, reply_markup=_filters_home_kb(), cb_id=c.id)
            return True
        if data == "flt_back":
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="✅ תפריט ראשי", reply_markup=inline_menu(), cb_id=c.id)
            return True
        if data == "flt_clear_all":
            with FILE_LOCK:
                set_price_buckets_raw("")
                set_min_orders(0)
                set_min_rating(0.0)
                set_free_ship_only(False)
                set_category_ids([])
            bot.answer_callback_query(c.id, "כל הסינונים אופסו.")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧰 סינונים\nבחר מה לשנות:", reply_markup=_filters_home_kb(), cb_id=None)
            return True

        # orders
        if data == "fo_menu":
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"📦 מינימום הזמנות (כרגע: {MIN_ORDERS})", reply_markup=_orders_filter_menu_kb(), cb_id=c.id)
            return True
        if data.startswith("fo_set_"):
            val = int(data.split("_")[-1])
            with FILE_LOCK:
                set_min_orders(val)
            bot.answer_callback_query(c.id, f"עודכן מינ' הזמנות ל-{val}")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"📦 מינימום הזמנות (כרגע: {MIN_ORDERS})", reply_markup=_orders_filter_menu_kb(), cb_id=None)
            return True

        # rating
        if data == "fr_menu":
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"⭐ מינימום דירוג באחוזים (כרגע: {MIN_RATING:g}%)", reply_markup=_rating_filter_menu_kb(), cb_id=c.id)
            return True
        if data.startswith("fr_set_"):
            val = float(data.split("_")[-1])
            with FILE_LOCK:
                set_min_rating(val)
            bot.answer_callback_query(c.id, f"עודכן מינ' דירוג ל-{val:g}%")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"⭐ מינימום דירוג באחוזים (כרגע: {MIN_RATING:g}%)", reply_markup=_rating_filter_menu_kb(), cb_id=None)
            return True

        # shipping toggle
        if data == "fs_toggle":
            with FILE_LOCK:
                set_free_ship_only(not FREE_SHIP_ONLY)
            bot.answer_callback_query(c.id, "עודכן.")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧰 סינונים\nבחר מה לשנות:", reply_markup=_filters_home_kb(), cb_id=None)
            return True


        # categories menu
        if data.startswith("fc_menu_"):
            page = int(data.split("_")[-1])
            CAT_VIEW_MODE[uid] = "top"
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 בחר קטגוריות (פופולריות):", reply_markup=_categories_menu_kb(page, mode="top", uid=uid), cb_id=None)
            return True

        if data.startswith("fc_all_"):
            page = int(data.split("_")[-1])
            CAT_VIEW_MODE[uid] = "all"
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 בחר קטגוריות (כל הקטגוריות):", reply_markup=_categories_menu_kb(page, mode="all", uid=uid), cb_id=None)
            return True

        if data.startswith("fc_s_"):
            page = int(data.split("_")[-1])
            CAT_VIEW_MODE[uid] = "search"
            q = (CAT_LAST_QUERY.get(uid) or "").strip()
            if not q:
                CAT_SEARCH_WAIT[uid] = True
                CAT_SEARCH_CTX[uid] = (chat_id, c.message.message_id)
                kb = types.InlineKeyboardMarkup(row_width=1)
                kb.add(types.InlineKeyboardButton("⬅️ חזרה לקטגוריות", callback_data="fc_menu_0"))
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🔎 שלח עכשיו מילת חיפוש לקטגוריה (לדוגמה: iPhone / שעון / בית / כלי עבודה)", reply_markup=kb, cb_id=None)
            else:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"🔎 תוצאות חיפוש לקטגוריה: {q}", reply_markup=_categories_menu_kb(page, mode="search", uid=uid, query=q), cb_id=None)
            return True

        if data == "fc_search":
            CAT_VIEW_MODE[uid] = "search"
            CAT_SEARCH_WAIT[uid] = True
            CAT_SEARCH_CTX[uid] = (chat_id, c.message.message_id)
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton("⬅️ חזרה לקטגוריות", callback_data="fc_menu_0"))
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🔎 שלח עכשיו מילת חיפוש לקטגוריה (לדוגמה: iPhone / שעון / בית / כלי עבודה)", reply_markup=kb, cb_id=None)
            return True

        # toggle category selection
        if data.startswith("fc_t_"):
            parts = data.split("_")
            # formats:
            #   fc_t_<cid>_<mode>_<page>
            # legacy:
            #   fc_t_<cid>_<page>
            cid = parts[2]
            if len(parts) >= 5:
                mode = parts[3]
                page = int(parts[4])
            else:
                mode = "top"
                page = int(parts[3])

            if cid in CATEGORY_IDS:
                CATEGORY_IDS.remove(cid)
            else:
                CATEGORY_IDS.add(cid)
            save_user_state()

            if mode == "all":
                CAT_VIEW_MODE[uid] = "all"
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 בחר קטגוריות (כל הקטגוריות):", reply_markup=_categories_menu_kb(page, mode="all", uid=uid), cb_id=None)
            elif mode == "search":
                CAT_VIEW_MODE[uid] = "search"
                q = (CAT_LAST_QUERY.get(uid) or "").strip()
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"🔎 תוצאות חיפוש לקטגוריה: {q}", reply_markup=_categories_menu_kb(page, mode="search", uid=uid, query=q), cb_id=None)
            else:
                CAT_VIEW_MODE[uid] = "top"
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 בחר קטגוריות (פופולריות):", reply_markup=_categories_menu_kb(page, mode="top", uid=uid), cb_id=None)
            return True

        if data == "fc_clear":
            CATEGORY_IDS.clear()
            save_user_state()
            mode = CAT_VIEW_MODE.get(uid, "top")
            if mode == "all":
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="✅ נוקה! עכשיו בחר קטגוריות:", reply_markup=_categories_menu_kb(0, mode="all", uid=uid), cb_id=None)
            elif mode == "search":
                q = (CAT_LAST_QUERY.get(uid) or "").strip()
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"✅ נוקה! תוצאות חיפוש: {q}", reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q), cb_id=None)
            else:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="✅ נוקה! עכשיו בחר קטגוריות:", reply_markup=_categories_menu_kb(0, mode="top", uid=uid), cb_id=None)
            return True

        if data == "fc_random":
            cats = get_categories()
            if cats:
                pick = random.choice(cats)["id"]
                CATEGORY_IDS.clear()
                CATEGORY_IDS.add(str(pick))
                save_user_state()
            mode = CAT_VIEW_MODE.get(uid, "top")
            if mode == "all":
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🎲 נבחרה קטגוריה רנדומלית:", reply_markup=_categories_menu_kb(0, mode="all", uid=uid), cb_id=None)
            elif mode == "search":
                q = (CAT_LAST_QUERY.get(uid) or "").strip()
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🎲 נבחרה קטגוריה רנדומלית:", reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q), cb_id=None)
            else:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🎲 נבחרה קטגוריה רנדומלית:", reply_markup=_categories_menu_kb(0, mode="top", uid=uid), cb_id=None)
            return True

        if data == "fc_sync":
            _ = get_categories(force=True)
            mode = CAT_VIEW_MODE.get(uid, "top")
            if mode == "all":
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🔄 סונכרן! בחר קטגוריות:", reply_markup=_categories_menu_kb(0, mode="all", uid=uid), cb_id=None)
            elif mode == "search":
                q = (CAT_LAST_QUERY.get(uid) or "").strip()
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"🔄 סונכרן! תוצאות חיפוש: {q}", reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q), cb_id=None)
            else:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🔄 סונכרן! בחר קטגוריות:", reply_markup=_categories_menu_kb(0, mode="top", uid=uid), cb_id=None)
            return True


    except Exception as e:
        try:
            bot.answer_callback_query(c.id, f"שגיאה: {e}", show_alert=True)
        except Exception:
            pass
        return True
    return False

# ========= AI REVIEW / APPROVAL UI =========
AI_REVIEW_CTX: dict[int, tuple[int,int]] = {}  # uid -> (chat_id, message_id) of last review photo/message

def _ai_candidates(pending_rows: list[dict]) -> list[int]:
    # We review only items that are not already "done" or "rejected".
    out = []
    for i, r in enumerate(pending_rows):
        st = str(r.get("AIState","") or "").strip().lower()
        if st not in ("raw", "approved", "rejected", "done"):
            st = "raw"
            r["AIState"] = st
        if st in ("raw", "approved"):
            out.append(i)
    return out

def _ai_get_pos(uid: int) -> int:
    return _get_state_int(f"ai_review_pos_{uid}", 0)

def _ai_set_pos(uid: int, pos: int):
    _set_state_str(f"ai_review_pos_{uid}", str(max(0, int(pos))))

def _safe_delete(chat_id: int, message_id: int):
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass

def _ai_caption_for_row(r: dict, pos: int, total: int) -> str:
    item_id = str(r.get("ItemId","") or "").strip()
    title_raw = str(r.get("OrigTitle","") or r.get("Title","") or "").strip()
    st = str(r.get("AIState","") or "").strip().lower()
    st_he = {"raw":"ממתין", "approved":"מאושר", "rejected":"לא", "done":"בוצע"}.get(st, st)
    price = str(r.get("SalePrice","") or "").strip()
    discount = str(r.get("Discount","") or "").strip()
    rating = str(r.get("Rating","") or "").strip()
    orders = str(r.get("Orders","") or "").strip()
    lines = [
        f"🖼️ אישור AI ({pos+1}/{max(1,total)})",
        f"🧾 מספר: <b>{html.escape(item_id) if item_id else '—'}</b>",
        f"🧠 סטטוס AI: <b>{html.escape(st_he)}</b>",
    ]
    if title_raw:
        # Keep it short
        t = title_raw
        if len(t) > 160:
            t = t[:157] + "..."
        lines.append(f"📝 כותרת: {html.escape(t)}")
    meta = []
    if price:
        meta.append(f"מחיר: {html.escape(price)}")
    if discount:
        meta.append(f"הנחה: {html.escape(discount)}")
    if rating:
        meta.append(f"דירוג: {html.escape(rating)}")
    if orders:
        meta.append(f"הזמנות: {html.escape(orders)}")
    if meta:
        lines.append(" • ".join(meta))
    lines.append("")
    lines.append("בחר מה לעשות:")
    return "\n".join(lines)

def _ai_review_kb(r: dict) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=3)
    st = str(r.get("AIState","") or "").strip().lower()
    approve_label = "✅ לאישור" if st != "approved" else "↩️ בטל אישור"
    kb.row(
        types.InlineKeyboardButton(approve_label, callback_data="ai_rev_toggle"),
        types.InlineKeyboardButton("⛔ לא לשליחה ל-AI", callback_data="ai_rev_reject"),
        types.InlineKeyboardButton("🚀 הרץ AI (מאושרים)", callback_data="ai_run_approved"),
    )
    kb.row(
        types.InlineKeyboardButton("⬅️ הקודם", callback_data="ai_rev_prev"),
        types.InlineKeyboardButton("הבא ➡️", callback_data="ai_rev_next"),
        types.InlineKeyboardButton("✅ אישור 5", callback_data="ai_rev_approve5"),
    )
    kb.row(
        types.InlineKeyboardButton("⬅️ חזרה לתפריט", callback_data="ai_rev_back"),
    )
    return kb

def _ai_review_show(chat_id: int, uid: int, prefer_delete: bool = True):
    with FILE_LOCK:
        pending_rows = read_products(PENDING_CSV)
    # ensure AIState exists
    for rr in pending_rows:
        _ = normalize_row_keys(rr)

    candidates = _ai_candidates(pending_rows)
    if not candidates:
        # cleanup previous review msg if any
        ctx = AI_REVIEW_CTX.get(uid)
        if ctx and prefer_delete:
            _safe_delete(ctx[0], ctx[1])
            AI_REVIEW_CTX.pop(uid, None)
        bot.send_message(chat_id, "אין פריטים שממתינים לאישור AI כרגע ✅", reply_markup=inline_menu())
        return

    pos = _ai_get_pos(uid)
    pos = max(0, min(pos, len(candidates)-1))
    _ai_set_pos(uid, pos)
    idx = candidates[pos]
    r = pending_rows[idx]
    caption = _ai_caption_for_row(r, pos, len(candidates))
    kb = _ai_review_kb(r)

    # delete previous review message (keeps chat clean)
    if prefer_delete:
        ctx = AI_REVIEW_CTX.get(uid)
        if ctx and ctx[0] == chat_id:
            _safe_delete(ctx[0], ctx[1])

    img = str(r.get("ImageURL","") or "").strip()
    try:
        if img:
            m = bot.send_photo(chat_id, photo=img, caption=caption, reply_markup=kb, parse_mode="HTML")
        else:
            m = bot.send_message(chat_id, caption, reply_markup=kb, parse_mode="HTML")
        AI_REVIEW_CTX[uid] = (chat_id, m.message_id)
    except Exception as e:
        # If URL photo failed, fall back to text
        log_warn(f"[AI-REVIEW] send photo failed: {e}")
        m = bot.send_message(chat_id, caption, reply_markup=kb, parse_mode="HTML")
        AI_REVIEW_CTX[uid] = (chat_id, m.message_id)


def inline_menu():
    kb = types.InlineKeyboardMarkup(row_width=3)

    kb.add(
        types.InlineKeyboardButton("📢 פרסם עכשיו", callback_data="publish_now"),
        types.InlineKeyboardButton("📊 סטטוס שידור", callback_data="pending_status"),
        types.InlineKeyboardButton("🔄 טען/מזג מהקובץ", callback_data="reload_merge"),
    )

    kb.add(
        types.InlineKeyboardButton("🧰 סינונים", callback_data="flt_menu"),
    )

    # AI approval / review controls
    ai_auto_txt = "פעיל" if ai_auto_mode() else "כבוי"
    kb.add(
        types.InlineKeyboardButton(f"🧠 AI אוטומטי: {ai_auto_txt}", callback_data="ai_auto_toggle"),
        types.InlineKeyboardButton("🖼️ אישור AI (תצוגה)", callback_data="ai_review"),
        types.InlineKeyboardButton("🚀 הרץ AI (מאושרים)", callback_data="ai_run_approved"),
    )


    # Currency / conversion controls (affiliate prices)
    conv_state = "פעיל" if (AE_PRICE_INPUT_CURRENCY == "USD" and AE_PRICE_CONVERT_USD_TO_ILS) else "כבוי"
    kb.add(
        types.InlineKeyboardButton(f"💱 מטבע מקור: {AE_PRICE_INPUT_CURRENCY}", callback_data="toggle_price_input_currency"),
        types.InlineKeyboardButton(f"🔁 המרת $→₪: {conv_state}", callback_data="toggle_usd2ils_convert"),
    )


    kb.add(
        types.InlineKeyboardButton("⏱️ דקה", callback_data="delay_60"),
        types.InlineKeyboardButton("⏱️ 20ד", callback_data="delay_1200"),
        types.InlineKeyboardButton("⏱️ 25ד", callback_data="delay_1500"),
        types.InlineKeyboardButton("⏱️ 30ד", callback_data="delay_1800"),
    )

    kb.add(
        types.InlineKeyboardButton("⚙️ מצב אוטומטי (קצב) החלפה", callback_data="toggle_auto_mode"),
        types.InlineKeyboardButton("🕒 מצב שינה (החלפה)", callback_data="toggle_schedule"),
        types.InlineKeyboardButton("📥 העלה CSV", callback_data="upload_source"),
    )

    kb.add(
        types.InlineKeyboardButton("🔥 מלא מהאפילייט עכשיו", callback_data="refill_now"),
        types.InlineKeyboardButton("🔎 חיפוש ידני", callback_data="prod_search"),
        types.InlineKeyboardButton("₪ המרת $→₪ (לקובץ הבא)", callback_data="convert_next"),
        types.InlineKeyboardButton("🔁 חזור להתחלה מהקובץ", callback_data="reset_from_data"),
    )

    kb.add(
        types.InlineKeyboardButton("🗑️ מחק פריטי התור מהקובץ", callback_data="delete_source_from_pending"),
        types.InlineKeyboardButton("🧹 מחק את workfile.csv", callback_data="delete_source_file"),
    )

    kb.add(
        types.InlineKeyboardButton("🎯 ציבורי (השתמש)", callback_data="target_public"),
        types.InlineKeyboardButton("🔒 פרטי (השתמש)", callback_data="target_private"),
    )
    kb.add(
        types.InlineKeyboardButton("🆕 בחר ערוץ ציבורי", callback_data="choose_public"),
        types.InlineKeyboardButton("🆕 בחר ערוץ פרטי", callback_data="choose_private"),
        types.InlineKeyboardButton("❌ בטל בחירת יעד", callback_data="choose_cancel"),
    )

    kb.add(types.InlineKeyboardButton(
        f"מרווח: ~{POST_DELAY_SECONDS//60} דק׳ | יעד: {CURRENT_TARGET}", callback_data="noop_info"
    ))
    return kb

# ========= INLINE CALLBACKS =========
@bot.callback_query_handler(func=lambda c: True)
def on_inline_click(c):
    global POST_DELAY_SECONDS, CURRENT_TARGET, AE_PRICE_BUCKETS_RAW, AE_PRICE_BUCKETS, AE_PRICE_INPUT_CURRENCY, AE_PRICE_CONVERT_USD_TO_ILS

    if not _is_admin(c):
        bot.answer_callback_query(c.id, "אין הרשאה.", show_alert=True)
        return

    data = c.data or ""
    chat_id = c.message.chat.id

    # Handle filter menus / callbacks
    if handle_filters_callback(c, data, chat_id):
        return

    # --- Manual product keyword search ---
    if data == "prod_search":
        uid = c.from_user.id
        PROD_SEARCH_WAIT[uid] = True
        PROD_SEARCH_CTX[uid] = (chat_id, c.message.message_id)
        try:
            prompt = bot.send_message(
                chat_id,
                "🔎 שלח עכשיו מילת חיפוש למוצרים (לדוגמה: iPhone / מקדחה / שעון / מטבח).\n"
                "טיפ: אם אתה בתוך קבוצה – *תענה/י* להודעה הזאת (Reply) כדי שהבוט יקבל את הטקסט.",
                parse_mode='Markdown',
                reply_markup=types.ForceReply(selective=True)
            )
            PROD_SEARCH_PROMPT[uid] = (chat_id, prompt.message_id)
        except Exception:
            # Fallback without ForceReply
            bot.send_message(chat_id, "🔎 שלח עכשיו מילת חיפוש למוצרים (לדוגמה: iPhone / מקדחה / שעון / מטבח)")
        bot.answer_callback_query(c.id)
        return


    # --- AI approval workflow ---
    if data == "ai_auto_toggle":
        set_ai_auto_mode(not ai_auto_mode())
        bot.answer_callback_query(c.id, "עודכן.")
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🧠 מצב AI אוטומטי כעת: {'פעיל' if ai_auto_mode() else 'כבוי'}",
                          reply_markup=inline_menu(), cb_id=None)
        return

    if data == "ai_review":
        bot.answer_callback_query(c.id)
        _ai_review_show(chat_id=chat_id, uid=c.from_user.id)
        return

    if data == "ai_rev_back":
        bot.answer_callback_query(c.id)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="✅ תפריט ראשי", reply_markup=inline_menu(), cb_id=None)
        return

    if data in ("ai_rev_next", "ai_rev_prev", "ai_rev_toggle", "ai_rev_reject", "ai_rev_approve5"):
        uid = c.from_user.id
        with FILE_LOCK:
            pending_rows = read_products(PENDING_CSV)

        # ensure AIState exists
        for rr in pending_rows:
            _ = normalize_row_keys(rr)

        candidates = _ai_candidates(pending_rows)
        if not candidates:
            bot.answer_callback_query(c.id, "אין פריטים לאישור.")
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

        pos = _ai_get_pos(uid)
        pos = max(0, min(pos, len(candidates)-1))

        def write_back():
            with FILE_LOCK:
                write_products(PENDING_CSV, pending_rows)

        if data == "ai_rev_next":
            pos = min(pos + 1, len(candidates)-1)
            _ai_set_pos(uid, pos)
            bot.answer_callback_query(c.id)
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

        if data == "ai_rev_prev":
            pos = max(pos - 1, 0)
            _ai_set_pos(uid, pos)
            bot.answer_callback_query(c.id)
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

        if data == "ai_rev_toggle":
            idx = candidates[pos]
            r = pending_rows[idx]
            st = str(r.get("AIState","") or "").strip().lower()
            if st == "approved":
                r["AIState"] = "raw"
                bot.answer_callback_query(c.id, "בוטל אישור.")
            else:
                r["AIState"] = "approved"
                bot.answer_callback_query(c.id, "אושר לשליחה ל-AI.")
            write_back()
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

        if data == "ai_rev_reject":
            idx = candidates[pos]
            r = pending_rows[idx]
            r["AIState"] = "rejected"
            write_back()
            bot.answer_callback_query(c.id, "סומן: לא לשליחה ל-AI.")
            # stay at same pos, but list might shrink; clamp
            _ai_set_pos(uid, min(pos, max(0, len(_ai_candidates(pending_rows))-1)))
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

        if data == "ai_rev_approve5":
            # approve current + next 4
            changed = 0
            for j in range(pos, min(pos + 5, len(candidates))):
                idx = candidates[j]
                r = pending_rows[idx]
                if str(r.get("AIState","") or "").strip().lower() != "approved":
                    r["AIState"] = "approved"
                    changed += 1
            write_back()
            bot.answer_callback_query(c.id, f"אושר: {changed} פריטים.")
            # move to next after the block
            new_pos = min(pos + 5, len(candidates)-1)
            _ai_set_pos(uid, new_pos)
            _ai_review_show(chat_id=chat_id, uid=uid)
            return

    if data == "ai_run_approved":
        uid = c.from_user.id
        bot.answer_callback_query(c.id)
        if not _ai_enabled():
            bot.send_message(chat_id, "❌ AI כבוי או OPENAI_API_KEY חסר. בדוק GPT_ENABLED ו-OPENAI_API_KEY.")
            return
        with FILE_LOCK:
            pending_rows = read_products(PENDING_CSV)
        for rr in pending_rows:
            _ = normalize_row_keys(rr)
        approved = [r for r in pending_rows if str(r.get("AIState","") or "").strip().lower() == "approved"]
        if not approved:
            bot.send_message(chat_id, "אין פריטים מאושרים לשליחה ל-AI כרגע ✅")
            return
        bot.send_message(chat_id, f"⏳ מריץ AI על {len(approved)} פריטים מאושרים…")
        try:
            upd, err = ai_enrich_rows(approved, reason="manual_approval")
            # mark done where filled
            done_count = 0
            for r in approved:
                if str(r.get("Opening","")).strip() and str(r.get("Title","")).strip() and str(r.get("Strengths","")).strip():
                    r["AIState"] = "done"
                    done_count += 1
            with FILE_LOCK:
                write_products(PENDING_CSV, pending_rows)
            if err:
                bot.send_message(chat_id, f"⚠️ AI הסתיים עם אזהרה: {err}\n✅ עודכנו: {upd}\n🟢 סומנו כ'בוצע': {done_count}")
            else:
                bot.send_message(chat_id, f"✅ AI הסתיים.\nעודכנו: {upd}\n🟢 סומנו כ'בוצע': {done_count}")
        except Exception as e:
            bot.send_message(chat_id, f"❌ שגיאה בהרצת AI: {e}")
        # refresh review view if user is in it
        _ai_review_show(chat_id=chat_id, uid=uid)
        return

    if data == "publish_now":
        ok = send_next_locked("manual")
        if not ok:
            bot.answer_callback_query(c.id, "אין פוסטים ממתינים או שגיאה בשליחה.", show_alert=True)
            return
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="✅ נשלח הפריט הבא בתור.", reply_markup=inline_menu(), cb_id=c.id)

    elif data == "pending_status":
        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
        count = len(pending)
        now_il = _now_il()
        schedule_line = "🕰️ מצב: מתוזמן (שינה פעיל)" if is_schedule_enforced() else "🟢 מצב: תמיד-פעיל"
        delay_line = f"⏳ מרווח נוכחי: {POST_DELAY_SECONDS//60} דק׳ ({POST_DELAY_SECONDS} שניות)"
        target_line = f"🎯 יעד נוכחי: {CURRENT_TARGET}"
        conv_state = "פעיל" if (AE_PRICE_INPUT_CURRENCY == "USD" and AE_PRICE_CONVERT_USD_TO_ILS) else "כבוי"
        currency_line = f"💱 מטבע מקור: {AE_PRICE_INPUT_CURRENCY} | המרה $→₪: {conv_state} | מציג: {_display_currency_code()}"
        if count == 0:
            text = f"{schedule_line}\n{delay_line}\n{target_line}\n{currency_line}\nאין פוסטים ממתינים ✅"
        else:
            total_seconds = (count - 1) * POST_DELAY_SECONDS
            eta = now_il + timedelta(seconds=total_seconds)
            eta_str = eta.strftime("%Y-%m-%d %H:%M:%S %Z")
            next_eta = now_il.strftime("%Y-%m-%d %H:%M:%S %Z")
            status_line = "🎙️ שידור אפשרי עכשיו" if not is_quiet_now(now_il) else "⏸️ כרגע מחוץ לחלון השידור"
            text = (
                f"{schedule_line}\n"
                f"{status_line}\n"
                f"{delay_line}\n"
                f"{target_line}\n"
                f"{currency_line}\n"
                f"יש כרגע <b>{count}</b> פוסטים ממתינים.\n"
                f"⏱️ השידור הבא (תיאוריה לפי מרווח): <b>{next_eta}</b>\n"
                f"🕒 שעת השידור המשוערת של האחרון: <b>{eta_str}</b>\n"
                f"(מרווח בין פוסטים: {POST_DELAY_SECONDS} שניות)"
            )
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=text, reply_markup=inline_menu(), parse_mode='HTML', cb_id=c.id)

    elif data == "pf_menu":
        cur_name = "ש\"ח" if _display_currency_code() == "ILS" else "$"
        txt = f'💸 סינון מחיר ({cur_name})\nמצב נוכחי: {AE_PRICE_BUCKETS_RAW or "ללא"}\nבחר טווחים:'
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=txt, reply_markup=_price_filter_menu_kb(), cb_id=c.id)

    elif data == "pf_back":
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="✅ תפריט ראשי", reply_markup=inline_menu(), cb_id=c.id)

    elif data == "pf_clear":
        with FILE_LOCK:
            set_price_buckets_raw("")
        bot.answer_callback_query(c.id, "סינון מחיר בוטל.")
        cur_name = "ש\"ח" if _display_currency_code() == "ILS" else "$"
        txt = f'💸 סינון מחיר ({cur_name})\nמצב נוכחי: ללא\nבחר טווחים:'
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=txt, reply_markup=_price_filter_menu_kb(), cb_id=None)

    elif data.startswith("pf_"):
        suffix = data[3:]  # 1_5 or 50p
        bid = "50+" if suffix == "50p" else suffix.replace("_", "-")
        allowed = {b for _, b in PRICE_BUCKET_PRESETS}
        if bid not in allowed:
            bot.answer_callback_query(c.id, "טווח לא מוכר.", show_alert=True)
            return
        active = _active_price_bucket_ids()
        if bid in active:
            active.remove(bid)
        else:
            active.add(bid)
        order = [b for _, b in PRICE_BUCKET_PRESETS]
        raw = ",".join([b for b in order if b in active])
        with FILE_LOCK:
            set_price_buckets_raw(raw)
        bot.answer_callback_query(c.id, f"עודכן: {raw or 'ללא'}")
        cur_name = "ש\"ח" if _display_currency_code() == "ILS" else "$"
        txt = f'💸 סינון מחיר ({cur_name})\nמצב נוכחי: {AE_PRICE_BUCKETS_RAW or "ללא"}\nבחר טווחים:'
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=txt, reply_markup=_price_filter_menu_kb(), cb_id=None)

    elif data == "reload_merge":
        added, already, total_after = merge_from_data_into_pending()
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🔄 מיזוג הושלם.\nנוספו: {added}\nכבר היו בתור: {already}\nסה\"כ בתור כעת: {total_after}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "upload_source":
        EXPECTING_UPLOAD.add(getattr(c.from_user, "id", None))
        safe_edit_message(
            bot, chat_id=chat_id, message=c.message,
            new_text="שלח/י עכשיו קובץ CSV (כמסמך). הבוט ימפה עמודות, יעדכן workfile.csv וימזג אל התור.",
            reply_markup=inline_menu(), cb_id=c.id
        )

    elif data == "toggle_schedule":
        set_schedule_enforced(not is_schedule_enforced())
        state = "🕰️ מתוזמן (שינה פעיל)" if is_schedule_enforced() else "🟢 תמיד-פעיל"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"החלפתי מצב לשידור: {state}",
                          reply_markup=inline_menu(), cb_id=c.id)


    elif data == "toggle_price_input_currency":
        # Toggle how we interpret incoming affiliate prices (USD vs ILS)
        AE_PRICE_INPUT_CURRENCY = "ILS" if AE_PRICE_INPUT_CURRENCY == "USD" else "USD"
        _set_state_str("price_input_currency", AE_PRICE_INPUT_CURRENCY)
        # If switched away from USD, conversion is irrelevant (but we keep the stored flag)
        bot.answer_callback_query(c.id, f"עודכן מטבע מקור למחירים: {AE_PRICE_INPUT_CURRENCY}")
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"✅ עודכן מטבע מקור למחירים: {AE_PRICE_INPUT_CURRENCY}",
                          reply_markup=inline_menu(), cb_id=None)

    elif data == "toggle_usd2ils_convert":
        if AE_PRICE_INPUT_CURRENCY != "USD":
            bot.answer_callback_query(c.id, "כדי להפעיל המרה צריך שמטבע המקור יהיה USD.", show_alert=True)
            return
        AE_PRICE_CONVERT_USD_TO_ILS = not bool(AE_PRICE_CONVERT_USD_TO_ILS)
        _set_state_bool("convert_usd_to_ils", AE_PRICE_CONVERT_USD_TO_ILS)
        state_txt = "פעיל" if AE_PRICE_CONVERT_USD_TO_ILS else "כבוי"
        bot.answer_callback_query(c.id, f"המרה $→₪: {state_txt}")
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"✅ המרת $→₪ כעת: {state_txt}",
                          reply_markup=inline_menu(), cb_id=None)

    elif data.startswith("delay_"):
        try:
            seconds = int(data.split("_", 1)[1])
            if seconds <= 0:
                raise ValueError("מרווח חייב להיות חיובי")
            POST_DELAY_SECONDS = seconds
            save_delay_seconds(seconds)
            write_auto_flag("off")
            DELAY_EVENT.set()
            mins = seconds // 60
            safe_edit_message(bot, chat_id=chat_id, message=c.message,
                              new_text=f"⏱️ עודכן מרווח: ~{mins} דק׳ ({seconds} שניות). (מצב ידני)",
                              reply_markup=inline_menu(), cb_id=c.id)
        except Exception as e:
            bot.answer_callback_query(c.id, f"שגיאה בעדכון מרווח: {e}", show_alert=True)

    elif data == "toggle_auto_mode":
        current = read_auto_flag()
        new_mode = "off" if current == "on" else "on"
        write_auto_flag(new_mode)
        new_label = "🟢 מצב אוטומטי פעיל" if new_mode == "on" else "🔴 מצב ידני בלבד"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"החלפתי מצב שידור: {new_label}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "target_public":
        v = _load_preset(PUBLIC_PRESET_FILE)
        if v is None:
            bot.answer_callback_query(c.id, "לא הוגדר יעד ציבורי. בחר דרך '🆕 בחר ערוץ ציבורי'.", show_alert=True)
            return
        CURRENT_TARGET = resolve_target(v)
        ok, details = check_and_probe_target(CURRENT_TARGET)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🎯 עברתי לשדר ליעד הציבורי: {v}\n{details}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "target_private":
        v = _load_preset(PRIVATE_PRESET_FILE)
        if v is None:
            bot.answer_callback_query(c.id, "לא הוגדר יעד פרטי. בחר דרך '🆕 בחר ערוץ פרטי'.", show_alert=True)
            return
        CURRENT_TARGET = resolve_target(v)
        ok, details = check_and_probe_target(CURRENT_TARGET)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🔒 עברתי לשדר ליעד הפרטי: {v}\n{details}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "choose_public":
        EXPECTING_TARGET[c.from_user.id] = "public"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=("שלח/י *Forward* של הודעה מאותו ערוץ **ציבורי** כדי לשמור אותו כיעד.\n\n"
                                    "טיפ: פוסט בערוץ → ••• → Forward → בחר/י את הבוט."),
                          reply_markup=inline_menu(), parse_mode='Markdown', cb_id=c.id)

    elif data == "choose_private":
        EXPECTING_TARGET[c.from_user.id] = "private"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=("שלח/י *Forward* של הודעה מאותו ערוץ **פרטי** כדי לשמור אותו כיעד.\n\n"
                                    "חשוב: הוסף/י את הבוט כמנהל בערוץ הפרטי."),
                          reply_markup=inline_menu(), parse_mode='Markdown', cb_id=c.id)

    elif data == "choose_cancel":
        EXPECTING_TARGET.pop(getattr(c.from_user, "id", None), None)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="ביטלתי את מצב בחירת היעד. אפשר להמשיך כרגיל.",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "convert_next":
        try:
            with open(CONVERT_NEXT_FLAG_FILE, "w", encoding="utf-8") as f:
                f.write(str(USD_TO_ILS_RATE_DEFAULT))
            safe_edit_message(
                bot, chat_id=chat_id, message=c.message,
                new_text=f"✅ הופעל: המרת מחירים מדולר לש\"ח בקובץ ה-CSV הבא בלבד (שער {USD_TO_ILS_RATE_DEFAULT}).",
                reply_markup=inline_menu(), cb_id=c.id
            )
        except Exception as e:
            bot.answer_callback_query(c.id, f"שגיאה בהפעלת המרה: {e}", show_alert=True)

    elif data == "reset_from_data":
        src = read_products(DATA_CSV)
        with FILE_LOCK:
            write_products(PENDING_CSV, src)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🔁 התור אופס ומתחיל מחדש ({len(src)} פריטים) מהקובץ הראשי.",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "delete_source_from_pending":
        removed, left = delete_source_rows_from_pending()
        safe_edit_message(
            bot, chat_id=chat_id, message=c.message,
            new_text=f"🗑️ הוסר מהתור: {removed} פריטים שנמצאו ב-workfile.csv\nנשארו בתור: {left}",
            reply_markup=inline_menu(), cb_id=c.id
        )

    elif data == "delete_source_file":
        ok = delete_source_csv_file()
        msg_txt = "🧹 workfile.csv אופס לריק (נשמרו רק כותרות). התור לא שונה." if ok else "שגיאה במחיקת workfile.csv"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=msg_txt, reply_markup=inline_menu(), cb_id=c.id)

    elif data == "refill_now":
        max_needed = 80
        added, dup, total_after, last_page, last_error = refill_from_affiliate(max_needed=max_needed)
        text = (
            "🔥 מילוי מהאפילייט הושלם.\n"
            f"נוספו לתור: {added}\n"
            f"כפולים: {dup}\n"
            f"סה\"כ בתור: {total_after}\n"
            f"דף אחרון שנבדק: {last_page}\n"
            f"שגיאה/מידע: {last_error}"
        )
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=text, reply_markup=inline_menu(), cb_id=c.id)

    else:
        bot.answer_callback_query(c.id)

# ========= FORWARD HANDLER =========
@bot.message_handler(
    func=lambda m: EXPECTING_TARGET.get(getattr(m.from_user, "id", None)) is not None,
    content_types=['text', 'photo', 'video', 'document', 'animation', 'audio', 'voice']
)
def handle_forward_for_target(msg):
    mode = EXPECTING_TARGET.get(getattr(msg.from_user, "id", None))
    fwd = getattr(msg, "forward_from_chat", None)
    if not fwd:
        bot.reply_to(msg, "לא זיהיתי *הודעה מועברת מערוץ*. נסה/י שוב: העבר/י פוסט מהערוץ הרצוי.", parse_mode='Markdown')
        return

    chat_id = fwd.id
    username = fwd.username or ""
    target_value = f"@{username}" if username else chat_id

    if mode == "public":
        _save_preset(PUBLIC_PRESET_FILE, target_value)
        label = "ציבורי"
    else:
        _save_preset(PRIVATE_PRESET_FILE, target_value)
        label = "פרטי"

    global CURRENT_TARGET
    CURRENT_TARGET = resolve_target(target_value)
    ok, details = check_and_probe_target(CURRENT_TARGET)

    EXPECTING_TARGET.pop(msg.from_user.id, None)

    bot.reply_to(msg,
        f"✅ נשמר יעד {label}: {target_value}\n"
        f"{details}\n\nאפשר לעבור בין יעדים מהתפריט: 🎯/🔒"
    )

# ========= CATEGORY SEARCH (text input) =========
@bot.message_handler(func=lambda m: bool(CAT_SEARCH_WAIT.get(m.from_user.id, False)) and _is_admin(m), content_types=["text"])
def handle_category_search_text(m):
    uid = m.from_user.id
    chat_id = m.chat.id
    q = (m.text or "").strip()
    # stop waiting even if query is empty
    CAT_SEARCH_WAIT[uid] = False
    if not q:
        bot.send_message(chat_id, "❗️לא קיבלתי מילת חיפוש. נסה שוב דרך 🔎 חיפוש בקטגוריות.")
        return
    CAT_LAST_QUERY[uid] = q
    ctx = CAT_SEARCH_CTX.get(uid)
    try:
        if ctx and ctx[0] == chat_id:
            bot.edit_message_text(
                f"🔎 תוצאות חיפוש לקטגוריה: {q}",
                chat_id=ctx[0],
                message_id=ctx[1],
                reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q),
            )
        else:
            bot.send_message(chat_id, f"🔎 תוצאות חיפוש לקטגוריה: {q}", reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q))
    except Exception as e:
        log_warn(f"[CAT] edit/search menu failed: {e}")
        bot.send_message(chat_id, f"🔎 תוצאות חיפוש לקטגוריה: {q}", reply_markup=_categories_menu_kb(0, mode="search", uid=uid, query=q))


# ========= MANUAL PRODUCT SEARCH (text input) =========
@bot.message_handler(func=lambda m: bool(PROD_SEARCH_WAIT.get(m.from_user.id, False)) and _is_admin(m), content_types=["text"])
def handle_manual_product_search_text(m):
    """Handle admin keyword search that fetches affiliate products and adds them to queue.

    Important: In group chats, bots often don't receive regular text due to privacy mode.
    We therefore send a ForceReply prompt and require the user to reply to that message.
    """
    uid = m.from_user.id
    chat_id = m.chat.id
    q = (m.text or "").strip()

    # If in group/supergroup, require reply to the prompt message so bot will receive it.
    try:
        chat_type = getattr(m.chat, "type", "") or ""
    except Exception:
        chat_type = ""

    prompt_ctx = PROD_SEARCH_PROMPT.get(uid)
    if chat_type in ("group", "supergroup"):
        if not (getattr(m, "reply_to_message", None) and prompt_ctx and prompt_ctx[0] == chat_id and m.reply_to_message.message_id == prompt_ctx[1]):
            bot.reply_to(m, "כדי שהחיפוש יעבוד בקבוצה: לחץ Reply על הודעת החיפוש של הבוט ואז כתוב את מילת החיפוש.")
            return

    # Clear wait state
    PROD_SEARCH_WAIT[uid] = False
    if prompt_ctx:
        try:
            _safe_delete(prompt_ctx[0], prompt_ctx[1])
        except Exception:
            pass
        PROD_SEARCH_PROMPT.pop(uid, None)

    if not q:
        bot.send_message(chat_id, "❗️לא קיבלתי מילת חיפוש. נסה שוב דרך 🔎 חיפוש ידני.")
        return

    # Immediate feedback
    try:
        status = bot.send_message(chat_id, f"⏳ מחפש מוצרים עבור: {q} ...")
    except Exception:
        status = None

    try:
        max_needed = int(os.environ.get("AE_MANUAL_SEARCH_MAX", "80") or 80)
        added, dup, total_after, last_page, last_error = refill_from_affiliate(max_needed=max_needed, keywords=q)

        note = ""
        if not added:
            note = (
                "\n\nטיפים אם אין תוצאות:\n"
                "• נסה מילה באנגלית (AliExpress מחזיר טוב יותר באנגלית)\n"
                "• בדוק סינונים (מחיר/דירוג/הזמנות/משלוח חינם)\n"
                "• נסה מילה כללית יותר"
            )

        text = (
            "🔎 חיפוש ידני הושלם\n"
            f"מילת חיפוש: {q}\n"
            f"נוספו לתור: {added}\n"
            f"כפולים שנדחו: {dup}\n"
            f"סה\"כ בתור: {total_after}\n"
            f"דף אחרון שנבדק: {last_page}\n"
            f"שגיאה/מידע: {last_error or 'אין'}"
            f"{note}"
        )

        if status:
            safe_edit_message(bot, chat_id=chat_id, message=status, new_text=text, reply_markup=inline_menu(), cb_id=None)
        else:
            bot.send_message(chat_id, text, reply_markup=inline_menu())
    except Exception as e:
        err = f"❌ שגיאה בחיפוש: {type(e).__name__}: {e}"
        log_warn(err)
        if status:
            safe_edit_message(bot, chat_id=chat_id, message=status, new_text=err, reply_markup=inline_menu(), cb_id=None)
        else:
            bot.send_message(chat_id, err, reply_markup=inline_menu())


# ========= UPLOAD CSV =========
@bot.message_handler(commands=['upload_source'])
def cmd_upload_source(msg):
    if not _is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    uid = getattr(msg.from_user, "id", None)
    if uid is None:
        bot.reply_to(msg, "שגיאה בזיהוי משתמש.")
        return
    EXPECTING_UPLOAD.add(uid)
    bot.reply_to(msg,
        "שלח/י עכשיו קובץ CSV (כמסמך). הבוט ימפה את העמודות אוטומטית, יעדכן את workfile.csv וימזג אל התור.\n"
        "לא נוגעים בתזמונים, ולא מאפסים את התור."
    )

@bot.message_handler(content_types=['document'])
def on_document(msg):
    uid = getattr(msg.from_user, "id", None)
    if uid not in EXPECTING_UPLOAD:
        return

    try:
        doc = msg.document
        filename = (doc.file_name or "").lower()
        if not filename.endswith(".csv"):
            bot.reply_to(msg, "זה לא נראה כמו CSV. נסה/י שוב עם קובץ .csv")
            return

        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)
        csv_text = _decode_csv_bytes(file_bytes)

        from io import StringIO
        raw_reader = csv.DictReader(StringIO(csv_text))
        rows_raw = [dict(r) for r in raw_reader]

        convert_rate = None
        if os.path.exists(CONVERT_NEXT_FLAG_FILE):
            try:
                with open(CONVERT_NEXT_FLAG_FILE, "r", encoding="utf-8") as f:
                    convert_rate = float((f.read() or "").strip() or USD_TO_ILS_RATE_DEFAULT)
            except Exception:
                convert_rate = USD_TO_ILS_RATE_DEFAULT
            try:
                os.remove(CONVERT_NEXT_FLAG_FILE)
            except Exception:
                pass

        rows = _rows_with_optional_usd_to_ils(rows_raw, convert_rate)

        with FILE_LOCK:
            write_products(DATA_CSV, rows)

            pending_rows = read_products(PENDING_CSV)
            existing_keys = {_key_of_row(r) for r in pending_rows}
            added = 0
            already = 0
            for r in rows:
                k = _key_of_row(r)
                if k in existing_keys:
                    already += 1
                    continue
                pending_rows.append(r)
                existing_keys.add(k)
                added += 1
            write_products(PENDING_CSV, pending_rows)
            total_after = len(pending_rows)

        extra_line = f"\n💱 בוצעה המרה לש\"ח בשער {convert_rate} לכל מחירי הדולר בקובץ זה." if convert_rate else ""
        bot.reply_to(msg,
            "✅ הקובץ נקלט בהצלחה.\n"
            f"נוספו לתור: {added}\nכבר היו בתור/כפולים: {already}\nסה\"כ בתור כעת: {total_after}"
            + extra_line +
            "\n\nהשידור ממשיך בקצב שנקבע. אפשר לבדוק '📊 סטטוס שידור' בתפריט."
        )

    except Exception as e:
        bot.reply_to(msg, f"שגיאה בעיבוד הקובץ: {e}")
    finally:
        EXPECTING_UPLOAD.discard(uid)

# ========= TEXT COMMANDS =========
@bot.message_handler(commands=['cancel'])
def cmd_cancel(msg):
    uid = getattr(msg.from_user, "id", None)
    if uid is not None:
        EXPECTING_TARGET.pop(uid, None)
        EXPECTING_UPLOAD.discard(uid)
    bot.reply_to(msg, "בוטל מצב בחירת יעד/העלאה. שלח /start לתפריט.")

@bot.message_handler(commands=['start', 'help', 'menu'])
def cmd_start(msg):
    try:
        uid = getattr(msg.from_user, "id", None)
        if uid is not None:
            EXPECTING_TARGET.pop(uid, None)
            EXPECTING_UPLOAD.discard(uid)
    except Exception:
        pass
    _save_admin_chat_id(msg.chat.id)
    bot.send_message(msg.chat.id, "בחר פעולה:", reply_markup=inline_menu())

@bot.message_handler(commands=['pending_status','queue'])
def pending_status_cmd(msg):
    with FILE_LOCK:
        pending = read_products(PENDING_CSV)
    count = len(pending)
    now_il = _now_il()
    schedule_line = "🕰️ מצב: מתוזמן (שינה פעיל)" if is_schedule_enforced() else "🟢 מצב: תמיד-פעיל"
    delay_line = f"⏳ מרווח נוכחי: {POST_DELAY_SECONDS//60} דק׳ ({POST_DELAY_SECONDS} שניות)"
    target_line = f"🎯 יעד נוכחי: {CURRENT_TARGET}"
    if count == 0:
        bot.reply_to(msg, f"{schedule_line}\n{delay_line}\n{target_line}\nאין פוסטים ממתינים ✅")
        return
    total_seconds = (count - 1) * POST_DELAY_SECONDS
    eta = now_il + timedelta(seconds=total_seconds)
    eta_str = eta.strftime("%Y-%m-%d %H:%M:%S %Z")
    status_line = "🎙️ שידור אפשרי עכשיו" if not is_quiet_now(now_il) else "⏸️ כרגע מחוץ לחלון השידור"
    bot.reply_to(msg,
        f"{schedule_line}\n{status_line}\n{delay_line}\n{target_line}\n"
        f"יש כרגע <b>{count}</b> פוסטים ממתינים.\n"
        f"🕒 שעת השידור המשוערת של האחרון: <b>{eta_str}</b>",
        parse_mode="HTML"
    )

@bot.message_handler(commands=['queue'])
def queue_cmd(msg):
    # Alias for /pending_status
    return pending_status_cmd(msg)



@bot.message_handler(commands=['version'])
def cmd_version(msg):
    if not _is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    commit = os.environ.get("RAILWAY_GIT_COMMIT_SHA") or os.environ.get("RAILWAY_COMMIT_SHA") or os.environ.get("GIT_COMMIT") or "n/a"
    fp = _code_fingerprint()
    bot.reply_to(
        msg,
        f"<b>Version</b>: {CODE_VERSION}\n<b>Fingerprint</b>: {fp}\n<b>Commit</b>: {commit}\n<b>Instance</b>: {socket.gethostname()}\n<b>Target</b>: {CURRENT_TARGET}\n<b>PriceFilter</b>: {AE_PRICE_BUCKETS_RAW or 'none'}",
        parse_mode="HTML",
    )

@bot.message_handler(commands=['tail', 'logs'])
def cmd_tail(msg):
    if not _is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    try:
        if not os.path.exists(LOG_PATH):
            bot.reply_to(msg, f"לא נמצא קובץ לוג: {LOG_PATH}")
            return
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            data = f.read().splitlines()[-80:]
        text = "\n".join(data).strip()
        if not text:
            text = "(ריק)"
        # Telegram message limit ~4096
        if len(text) > 3800:
            text = text[-3800:]
        bot.reply_to(msg, f"<pre>{text}</pre>", parse_mode="HTML")
    except Exception as e:
        log_exc(f"tail logs failed: {e}")
        bot.reply_to(msg, f"שגיאה בקריאת לוג: {e}")

@bot.message_handler(commands=['refill_now'])
def cmd_refill_now(msg):
    if not _is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    max_needed = 80
    added, dup, total_after, last_page, last_error = refill_from_affiliate(max_needed=max_needed)
    bot.reply_to(msg,
        "🔥 מילוי מהאפילייט הושלם.\n"
        f"נוספו לתור: {added}\n"
        f"כפולים: {dup}\n"
        f"סה\"כ בתור: {total_after}\n"
        f"דף אחרון שנבדק: {last_page}\n"
        f"שגיאה/מידע: {last_error}"
    )

# ========= SENDER LOOP =========
def auto_post_loop():
    if not os.path.exists(SCHEDULE_FLAG_FILE):
        set_schedule_enforced(True)
    init_pending()

    while True:
        if read_auto_flag() == "on":
            delay = get_auto_delay()
            if delay is None or is_quiet_now():
                DELAY_EVENT.wait(timeout=60)
                DELAY_EVENT.clear()
                continue

            with FILE_LOCK:
                pending = read_products(PENDING_CSV)
            if not pending:
                DELAY_EVENT.wait(timeout=15)
                DELAY_EVENT.clear()
                continue

            send_next_locked("auto")
            DELAY_EVENT.wait(timeout=delay)
            DELAY_EVENT.clear()
            continue

        if is_quiet_now():
            DELAY_EVENT.wait(timeout=30)
            DELAY_EVENT.clear()
            continue

        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
        if not pending:
            DELAY_EVENT.wait(timeout=30)
            DELAY_EVENT.clear()
            continue

        send_next_locked("loop")
        DELAY_EVENT.wait(timeout=POST_DELAY_SECONDS)
        DELAY_EVENT.clear()

# ========= REFILL DAEMON =========
def refill_daemon():
    if not AE_REFILL_ENABLED:
        print("[INFO] Affiliate refill disabled.", flush=True)
        return
    print("[INFO] Refill daemon started", flush=True)

    while True:
        try:
            with FILE_LOCK:
                qlen = len(read_products(PENDING_CSV))

            if qlen < AE_REFILL_MIN_QUEUE:
                need = max(AE_REFILL_MIN_QUEUE - qlen, 30)
                added, dup, total_after, last_page, last_error = refill_from_affiliate(max_needed=need)

                msg = (
                    "🔥 מילוי מהאפילייט הושלם.\n"
                    f"נוספו לתור: {added}\n"
                    f"כפולים: {dup}\n"
                    f"סה\"כ בתור: {total_after}\n"
                    f"מידע/שגיאה: {last_error}"
                )
                notify_admin(msg)
                print(msg.replace("\n", " | "), flush=True)

        except Exception as e:
            print(f"[WARN] refill_daemon error: {e}", flush=True)

        time.sleep(AE_REFILL_INTERVAL_SECONDS)

# ========= MAIN =========
if __name__ == "__main__":
    log_info(f"[BOOT] main.py {CODE_VERSION} fp={_code_fingerprint()} commit={os.environ.get('RAILWAY_GIT_COMMIT_SHA') or os.environ.get('RAILWAY_COMMIT_SHA') or os.environ.get('GIT_COMMIT') or 'n/a'}")
    log_info(f"Instance: {socket.gethostname()}")

    # הדפסה קצרה של קונפיג (מסכות)
    print(f"[CFG] AE_TOP_URL={AE_TOP_URL} | CANDIDATES={' | '.join(AE_TOP_URL_CANDIDATES)}", flush=True)
    print(f"[CFG] AE_APP_KEY={_mask(AE_APP_KEY)} | AE_APP_SECRET={_mask(AE_APP_SECRET)} | AE_TRACKING_ID={_mask(AE_TRACKING_ID)}", flush=True)
    print(f"[CFG] AE_SHIP_TO_COUNTRY={AE_SHIP_TO_COUNTRY} | AE_TARGET_LANGUAGE={AE_TARGET_LANGUAGE} | SORT={AE_REFILL_SORT}", flush=True)

    try:
        me = bot.get_me()
        print(f"Bot: @{me.username} ({me.id})", flush=True)
    except Exception as e:
        print("getMe failed:", e, flush=True)


    # Extra runtime diagnostics (safe)
    log_info(f"[CFG] PUBLIC_CHANNEL={os.environ.get('PUBLIC_CHANNEL', '')} | CURRENT_TARGET={CURRENT_TARGET}")
    log_info(f"[CFG] JOIN_URL={JOIN_URL}")
    log_info(f"[CFG] AE_PRICE_BUCKETS={AE_PRICE_BUCKETS_RAW or '(none)'} | parsed={AE_PRICE_BUCKETS}")
    log_info(f"[CFG] PRICE_INPUT_CURRENCY={AE_PRICE_INPUT_CURRENCY} | CONVERT_USD_TO_ILS={AE_PRICE_CONVERT_USD_TO_ILS} | DISPLAY={_display_currency_code()}")
    log_info(f"[CFG] MIN_ORDERS={MIN_ORDERS} | MIN_RATING={MIN_RATING:g}% | FREE_SHIP_ONLY={FREE_SHIP_ONLY} (threshold>=₪{AE_FREE_SHIP_THRESHOLD_ILS:g}) | CATEGORIES={CATEGORY_IDS_RAW or '(none)'}")
    log_info(f"[CFG] PYTHONUNBUFFERED={os.environ.get('PYTHONUNBUFFERED', '')} | PID={os.getpid()}")


# AI diagnostics
try:
    ai_state = "ON" if _ai_enabled() else "OFF"
    ai_diagnostics_startup()
    ai_note = ""
    if GPT_ENABLED and not OPENAI_API_KEY:
        ai_note = " (missing OPENAI_API_KEY)"
    elif GPT_ENABLED and OpenAI is None:
        ai_note = " (missing 'openai' package)"
    log_info(f"[CFG] AI={ai_state}{ai_note} | MODEL={OPENAI_MODEL} (effective={OPENAI_MODEL_EFFECTIVE}) | BATCH={GPT_BATCH_SIZE} | OVERWRITE={GPT_OVERWRITE} | ON_REFILL={GPT_ON_REFILL} | ON_UPLOAD={GPT_ON_UPLOAD}")
except Exception:
    pass
    _lock_handle = acquire_single_instance_lock(LOCK_PATH)
    if _lock_handle is None:
        print("Another instance is running (lock failed). Exiting.", flush=True)
        sys.exit(1)

    print_webhook_info()
    try:
        force_delete_webhook()
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        try:
            bot.remove_webhook()
        except Exception as e2:
            print(f"[WARN] remove_webhook failed: {e2}", flush=True)
    print_webhook_info()

    if not os.path.exists(AUTO_FLAG_FILE):
        write_auto_flag("on")

    t1 = threading.Thread(target=auto_post_loop, daemon=True)
    t1.start()
# AI diagnostics (show clearly if AI is really enabled)
try:
    try:
        import openai as _openai_pkg
        log_info(f"[CFG] OPENAI_SDK_VERSION={getattr(_openai_pkg, '__version__', 'unknown')}")
    except Exception:
        pass

    ai_state = "ON" if _ai_enabled() else "OFF"
    ai_note = ""
    if GPT_ENABLED and not OPENAI_API_KEY:
        ai_note = " (missing OPENAI_API_KEY)"
    elif GPT_ENABLED and OpenAI is None:
        ai_note = " (missing 'openai' package)"
    log_info(f"[CFG] AI={ai_state}{ai_note} | MODEL={OPENAI_MODEL} (effective={OPENAI_MODEL_EFFECTIVE}) | BATCH={GPT_BATCH_SIZE} | OVERWRITE={GPT_OVERWRITE} | ON_REFILL={GPT_ON_REFILL} | ON_UPLOAD={GPT_ON_UPLOAD}")
except Exception as e:
    log_error(f"[CFG] AI diagnostics failed: {e}")

_lock_handle = acquire_single_instance_lock(LOCK_PATH)
if _lock_handle is None:
    print("Another instance is running (lock failed). Exiting.", flush=True)
    sys.exit(1)

print_webhook_info()
try:
    force_delete_webhook()
    bot.delete_webhook(drop_pending_updates=True)
except Exception:
    try:
        bot.remove_webhook()
    except Exception as e2:
        print(f"[WARN] remove_webhook failed: {e2}", flush=True)
print_webhook_info()

if not os.path.exists(AUTO_FLAG_FILE):
    write_auto_flag("on")

t1 = threading.Thread(target=auto_post_loop, daemon=True)
t1.start()

t2 = threading.Thread(target=refill_daemon, daemon=True)
t2.start()

# Polling loop with automatic recovery (network hiccups, Telegram timeouts, etc.)
while True:
    try:
        bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
    except Exception as e:
        msg = str(e)
        wait = 30 if "Conflict: terminated by other getUpdates request" in msg else 5
        log_error(f"Polling error: {e}. Retrying in {wait}s...")
        time.sleep(wait)
