# -*- coding: utf-8 -*-
import csv
import requests
import time
import telebot
from telebot import types
import threading
import os
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

# ========= CONFIG =========
BOT_TOKEN = os.getenv("8371104768:AAHi2lv7CFNFAWycjWeUSJiOn9YR0Qvep_4")  # ← לקבל מהסביבה (לא בקוד)
CHANNEL_ID = "@nisayon121"         # ← עדכן כאן (למשל: "@my_channel")
ADMIN_USER_IDS = {535299257}       # ← עדכן ל-user id שלך

# קבצים
DATA_CSV = "workfile.csv"          # קובץ המקור שאתה מכין
PENDING_CSV = "pending.csv"        # תור הפוסטים הממתינים

# מצב עבודה: 'מתוזמן' או 'תמיד-פעיל' באמצעות דגל קובץ
SCHEDULE_FLAG_FILE = "schedule_enforced.flag"  # קיים => מתוזמן; לא קיים => תמיד-פעיל

# מרווח בין פוסטים בשניות
POST_DELAY_SECONDS = 60

# ========= INIT =========
if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.1"})

# אזור זמן ישראל
IL_TZ = ZoneInfo("Asia/Jerusalem")


# ========= SINGLE INSTANCE LOCK =========
def acquire_single_instance_lock(lock_path: str = "bot.lock"):
    try:
        import sys
        if os.name == "nt":
            import msvcrt
            f = open(lock_path, "w")
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                print("Another instance is running. Exiting.")
                sys.exit(1)
            return f
        else:
            import fcntl
            f = open(lock_path, "w")
            try:
                fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                print("Another instance is running. Exiting.")
                sys.exit(1)
            return f
    except Exception as e:
        print(f"[WARN] Could not acquire single-instance lock: {e}")
        return None


# ========= WEBHOOK DIAGNOSTICS =========
def print_webhook_info():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo"
        r = requests.get(url, timeout=10)
        print("getWebhookInfo:", r.json())
    except Exception as e:
        print(f"[WARN] getWebhookInfo failed: {e}")

def force_delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": True}, timeout=10)
        print("deleteWebhook:", r.json())
    except Exception as e:
        print(f"[WARN] deleteWebhook failed: {e}")


# ========= UTILITIES =========
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
    for junk in ["ILS", "₪"]:
        s = s.replace(junk, "")
    out = "".join(ch for ch in s if ch.isdigit() or ch == ".")
    return out.strip()

def normalize_row_keys(row):
    out = dict(row)

    # normalize keys + strip
    out["ImageURL"] = (out.get("ImageURL") or out.get("Image Url") or "").strip()
    out["Video Url"] = (out.get("Video Url") or "").strip()
    out["BuyLink"] = (out.get("BuyLink") or out.get("Promotion Url") or "").strip()

    out["OriginalPrice"] = clean_price_text(out.get("OriginalPrice") or out.get("Origin Price") or "")
    out["SalePrice"]     = clean_price_text(out.get("SalePrice")
