# -*- coding: utf-8 -*-
"""
main.py — Telegram Post Bot + AliExpress Affiliate refill

Version: 2025-12-16h
Changes vs previous:
- Fix TOP timestamp to GMT+8 (per TOP gateway requirement)
- Raise on TOP error_response (so you finally see the real error instead of '0 products' and None)
- Better refill diagnostics when 0 products returned
"""

import os, sys
os.environ.setdefault("PYTHONUNBUFFERED", "1")
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

import logging
import hashlib
import random
from logging.handlers import RotatingFileHandler

# ========= LOGGING / VERSION =========
CODE_VERSION = os.environ.get("CODE_VERSION", "v2025-12-17n")
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

def usd_to_ils(price_text: str, rate: float) -> str:
    num = _extract_float(price_text)
    if num is None:
        return ""
    ils = round(num * rate)
    return str(int(ils))

def _normalize_top_price_raw(raw) -> str:
    """TOP/AliExpress affiliate sometimes returns prices as integer cents (e.g. '362' == $3.62).
    This normalizes to a USD string for usd_to_ils()."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    # Already looks like a normal decimal/currency string
    if any(ch in s for ch in (".", ",")) or ("$" in s) or ("usd" in s.lower()) or ("us $" in s.lower()):
        return s
    # Pure digits: if length>=3 assume cents -> divide by 100
    if re.fullmatch(r"\d+", s) and len(s) >= 3:
        try:
            return f"{int(s) / 100:.2f}"
        except Exception:
            return s
    return s


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

    return out

def read_products(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [normalize_row_keys(r) for r in reader]

def write_products(path, rows):
    base_headers = [
        "ItemId","ImageURL","Title","OriginalPrice","SalePrice","Discount",
        "Rating","Orders","BuyLink","CouponCode","Opening","Video Url","Strengths"
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
    try:
        curr_text = (message.text or message.caption or "")
        if curr_text == (new_text or ""):
            try:
                if reply_markup is not None:
                    bot.edit_message_reply_markup(chat_id, message.message_id, reply_markup=reply_markup)
                    if cb_id:
                        bot.answer_callback_query(cb_id)
                    return
                if cb_id:
                    bot.answer_callback_query(cb_id)
                return
            except Exception as e_rm:
                if "message is not modified" in str(e_rm):
                    if cb_id:
                        bot.answer_callback_query(cb_id)
                    return
        bot.edit_message_text(new_text, chat_id, message.message_id, reply_markup=reply_markup, parse_mode=parse_mode)
        if cb_id:
            bot.answer_callback_query(cb_id)
    except Exception as e:
        if "message is not modified" in str(e):
            if cb_id:
                bot.answer_callback_query(cb_id)
            return
        if cb_id and cb_info:
            bot.answer_callback_query(cb_id, cb_info + f" (שגיאה: {e})", show_alert=True)
        else:
            raise

# ========= POSTING =========
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

    price_line = f'💰 מחיר מבצע: {sale_price} ש"ח (מחיר מקורי: {original_price} ש"ח)'
    lines += [
        price_line,
        discount_text,
        f"⭐ דירוג: {rating_percent}",
        f"📦 {orders_text}",
        "🚚 משלוח חינם מעל 38 ש\"ח או 7.49 ש\"ח",
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
def _is_admin(msg) -> bool:
    if not ADMIN_USER_IDS:
        return True
    return msg.from_user and (msg.from_user.id in ADMIN_USER_IDS)

# ========= MERGE =========
def _key_of_row(r: dict):
    item_id = (r.get("ItemId") or "").strip()
    title   = (r.get("Title") or "").strip()
    buy     = (r.get("BuyLink") or "").strip()
    return (item_id if item_id else None, title if not item_id else None, buy)

def merge_from_data_into_pending():
    data_rows = read_products(DATA_CSV)
    pending_rows = read_products(PENDING_CSV)

    existing_keys = {_key_of_row(r) for r in pending_rows}
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

    write_products(PENDING_CSV, pending_rows)
    return added, already, len(pending_rows)

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


def affiliate_product_query(page_no: int, page_size: int, category_id: str | None = None) -> tuple[list[dict], int | None, str | None]:
    """Affiliate product query with optional category filter."""
    fields = "product_id,product_title,product_main_image_url,promotion_link,promotion_url,sale_price,app_sale_price,original_price,discount,evaluate_rate,lastest_volume,product_video_url,product_detail_url"
    biz = {
        "tracking_id": AE_TRACKING_ID,
        "page_no": str(page_no),
        "page_size": str(page_size),
        "sort": AE_REFILL_SORT,
        "ship_to_country": AE_SHIP_TO_COUNTRY,
        "target_language": AE_TARGET_LANGUAGE,
        "fields": fields,
        "platform_product_type": "ALL",
    }
    if category_id:
        biz["category_ids"] = str(category_id).strip()
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
    # בחירה חכמה של מחיר מבצע: app_sale_price אם קיים, אחרת sale_price
    sale_raw = p.get("app_sale_price") or p.get("sale_price") or p.get("target_app_sale_price") or p.get("target_sale_price") or ""
    orig_raw = p.get("original_price") or p.get("target_original_price") or ""

    sale_ils = usd_to_ils(_normalize_top_price_raw(sale_raw), USD_TO_ILS_RATE_DEFAULT)
    orig_ils = usd_to_ils(_normalize_top_price_raw(orig_raw), USD_TO_ILS_RATE_DEFAULT)

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
        "OriginalPrice": orig_ils,
        "SalePrice": sale_ils,
        "Discount": (p.get("discount") or "").strip(),
        "Rating": (p.get("evaluate_rate") or "").strip(),
        "Orders": str(p.get("lastest_volume") or "").strip(),
        "BuyLink": buy_link,
        "CouponCode": "",
        "Opening": "",
        "Strengths": "",
        "Video Url": (p.get("product_video_url") or "").strip(),
    })



def refill_from_affiliate(max_needed: int) -> tuple[int, int, int, int, str | None]:
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
                    products, resp_code, resp_msg = affiliate_product_query(page_no, AE_REFILL_PAGE_SIZE, category_id=cat_id)
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
        # No categories selected -> use HotProduct feed (most stable) + apply filters
        for page_no in range(1, AE_REFILL_MAX_PAGES + 1):
            last_page = page_no
            try:
                products, resp_code, resp_msg = affiliate_hotproduct_query(page_no, AE_REFILL_PAGE_SIZE)
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

def _categories_menu_kb(page: int = 0, per_page: int = 10):
    kb = types.InlineKeyboardMarkup(row_width=2)
    cats = get_categories()
    selected = set(get_selected_category_ids())
    total = len(cats)

    if total == 0:
        kb.add(types.InlineKeyboardButton("🔄 סנכרן קטגוריות מאליאקספרס", callback_data="fc_sync"))
        kb.add(types.InlineKeyboardButton("⬅️ חזרה", callback_data="flt_menu"))
        return kb

    page = max(0, int(page))
    start = page * per_page
    end = min(total, start + per_page)
    slice_cats = cats[start:end]

    btns = []
    for c in slice_cats:
        cid = str(c.get("id") or "")
        name = str(c.get("name") or cid)
        mark = "✅ " if cid in selected else ""
        # keep name short in button
        short = name
        if len(short) > 22:
            short = short[:21] + "…"
        btns.append(types.InlineKeyboardButton(f"{mark}{short}", callback_data=f"fc_t_{cid}_{page}"))
    if btns:
        kb.add(*btns)

    # controls
    nav = []
    if start > 0:
        nav.append(types.InlineKeyboardButton("⬅️ הקודם", callback_data=f"fc_menu_{page-1}"))
    if end < total:
        nav.append(types.InlineKeyboardButton("הבא ➡️", callback_data=f"fc_menu_{page+1}"))
    if nav:
        kb.add(*nav)

    kb.add(
        types.InlineKeyboardButton("🎲 בחר קטגוריה רנדומלית", callback_data="fc_random"),
        types.InlineKeyboardButton("🧹 נקה קטגוריות", callback_data="fc_clear"),
    )
    kb.add(types.InlineKeyboardButton("🔄 סנכרן קטגוריות", callback_data="fc_sync"))
    kb.add(types.InlineKeyboardButton("⬅️ חזרה", callback_data="flt_menu"))
    kb.add(types.InlineKeyboardButton(f"נבחרו: {len(selected)} | עמוד {page+1}/{max(1, (total + per_page - 1)//per_page)}", callback_data="noop_info"))
    return kb

def handle_filters_callback(c, data: str, chat_id: int) -> bool:
    """Return True if handled."""
    try:
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
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 קטגוריות (בחירה מרובה):", reply_markup=_categories_menu_kb(page), cb_id=c.id)
            return True

        if data == "fc_clear":
            with FILE_LOCK:
                set_category_ids([])
            bot.answer_callback_query(c.id, "קטגוריות נוקו.")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 קטגוריות (בחירה מרובה):", reply_markup=_categories_menu_kb(0), cb_id=None)
            return True

        if data == "fc_sync":
            bot.answer_callback_query(c.id, "מסנכרן…")
            cats, err = affiliate_category_get()
            if err:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"❌ סנכרון נכשל: {err}", reply_markup=_categories_menu_kb(0), cb_id=None)
            else:
                safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=f"✅ סונכרנו {len(cats)} קטגוריות.", reply_markup=_categories_menu_kb(0), cb_id=None)
            return True

        if data == "fc_random":
            cats = get_categories()
            if not cats:
                bot.answer_callback_query(c.id, "אין קטגוריות במטמון. לחץ סנכרון.", show_alert=True)
                return True
            choice = random.choice(cats)
            cid = str(choice.get("id"))
            with FILE_LOCK:
                set_category_ids([cid])
            bot.answer_callback_query(c.id, f"נבחרה: {choice.get('name')}")
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 קטגוריות (בחירה מרובה):", reply_markup=_categories_menu_kb(0), cb_id=None)
            return True

        if data.startswith("fc_t_"):
            # fc_t_<cid>_<page>
            parts = data.split("_")
            if len(parts) >= 4:
                cid = parts[2]
                page = int(parts[3])
            else:
                return False
            selected = get_selected_category_ids()
            sset = set(selected)
            if cid in sset:
                sset.remove(cid)
            else:
                sset.add(cid)
            # preserve original order as much as possible
            new_order = [x for x in selected if x in sset] + [x for x in sset if x not in selected]
            with FILE_LOCK:
                set_category_ids(new_order)
            safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="🧩 קטגוריות (בחירה מרובה):", reply_markup=_categories_menu_kb(page), cb_id=c.id)
            return True

    except Exception as e:
        try:
            bot.answer_callback_query(c.id, f"שגיאה: {e}", show_alert=True)
        except Exception:
            pass
        return True
    return False

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
    global POST_DELAY_SECONDS, CURRENT_TARGET, AE_PRICE_BUCKETS_RAW, AE_PRICE_BUCKETS

    if not _is_admin(c.message):
        bot.answer_callback_query(c.id, "אין הרשאה.", show_alert=True)
        return

    data = c.data or ""
    chat_id = c.message.chat.id

    # Handle filter menus / callbacks
    if handle_filters_callback(c, data, chat_id):
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
        if count == 0:
            text = f"{schedule_line}\n{delay_line}\n{target_line}\nאין פוסטים ממתינים ✅"
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
                f"יש כרגע <b>{count}</b> פוסטים ממתינים.\n"
                f"⏱️ השידור הבא (תיאוריה לפי מרווח): <b>{next_eta}</b>\n"
                f"🕒 שעת השידור המשוערת של האחרון: <b>{eta_str}</b>\n"
                f"(מרווח בין פוסטים: {POST_DELAY_SECONDS} שניות)"
            )
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=text, reply_markup=inline_menu(), parse_mode='HTML', cb_id=c.id)

    elif data == "pf_menu":
        txt = f'💸 סינון מחיר (בש"ח)\nמצב נוכחי: {AE_PRICE_BUCKETS_RAW or "ללא"}\nבחר טווחים:'
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text=txt, reply_markup=_price_filter_menu_kb(), cb_id=c.id)

    elif data == "pf_back":
        safe_edit_message(bot, chat_id=chat_id, message=c.message, new_text="✅ תפריט ראשי", reply_markup=inline_menu(), cb_id=c.id)

    elif data == "pf_clear":
        with FILE_LOCK:
            set_price_buckets_raw("")
        bot.answer_callback_query(c.id, "סינון מחיר בוטל.")
        txt = '💸 סינון מחיר (בש"ח)\nמצב נוכחי: ללא\nבחר טווחים:'
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
        txt = f'💸 סינון מחיר (בש"ח)\nמצב נוכחי: {AE_PRICE_BUCKETS_RAW or "ללא"}\nבחר טווחים:'
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
    log_info(f"[CFG] MIN_ORDERS={MIN_ORDERS} | MIN_RATING={MIN_RATING:g}% | FREE_SHIP_ONLY={FREE_SHIP_ONLY} (threshold>=₪{AE_FREE_SHIP_THRESHOLD_ILS:g}) | CATEGORIES={CATEGORY_IDS_RAW or '(none)'}")
    log_info(f"[CFG] PYTHONUNBUFFERED={os.environ.get('PYTHONUNBUFFERED', '')} | PID={os.getpid()}")
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