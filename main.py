# -*- coding: utf-8 -*-
import os, sys
os.environ.setdefault("PYTHONUNBUFFERED", "1")
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

import csv
import requests
import time
import telebot
from telebot import types
import threading
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import socket
import re

# ========= PERSISTENT DATA DIR =========
BASE_DIR = os.environ.get("BOT_DATA_DIR", "./data")
os.makedirs(BASE_DIR, exist_ok=True)

# ========= CONFIG =========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL", "@your_channel")
ADMIN_USER_IDS = set()

# FILES
DATA_CSV = os.path.join(BASE_DIR, "workfile.csv")
PENDING_CSV = os.path.join(BASE_DIR, "pending.csv")
DELAY_FILE = os.path.join(BASE_DIR, "post_delay.txt")
PUBLIC_PRESET_FILE  = os.path.join(BASE_DIR, "public_target.preset")
PRIVATE_PRESET_FILE = os.path.join(BASE_DIR, "private_target.preset")
SCHEDULE_FLAG_FILE = os.path.join(BASE_DIR, "schedule_enforced.flag")

# Conversion flags/files
CONVERT_NEXT_FLAG_FILE = os.path.join(BASE_DIR, "convert_next_usd_to_ils.flag")
RATE_FILE = os.path.join(BASE_DIR, "usd_ils_rate.txt")
USD_TO_ILS_RATE_DEFAULT = 3.55

LOCK_PATH = os.path.join(BASE_DIR, "bot.lock")

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})
IL_TZ = ZoneInfo("Asia/Jerusalem")
CURRENT_TARGET = CHANNEL_ID
DELAY_EVENT = threading.Event()
EXPECTING_TARGET = {}
EXPECTING_UPLOAD = set()
FILE_LOCK = threading.Lock()

# ========= HELPERS =========
def _read_rate() -> float:
    try:
        if os.path.exists(RATE_FILE):
            with open(RATE_FILE, "r", encoding="utf-8") as f:
                v = float((f.read() or "").strip())
                if v > 0:
                    return v
    except Exception:
        pass
    return USD_TO_ILS_RATE_DEFAULT

def _write_rate(v: float):
    with open(RATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(v))

def _convert_enabled() -> bool:
    return os.path.exists(CONVERT_NEXT_FLAG_FILE)

def _set_convert_enabled(enabled: bool, rate: float | None = None):
    if enabled:
        with open(CONVERT_NEXT_FLAG_FILE, "w", encoding="utf-8") as f:
            f.write(str(rate if rate else _read_rate()))
    else:
        try:
            if os.path.exists(CONVERT_NEXT_FLAG_FILE):
                os.remove(CONVERT_NEXT_FLAG_FILE)
        except Exception:
            pass

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
    if "ImageURL" not in out:
        out["ImageURL"] = out.get("Image Url", "") or out.get("ImageURL", "")
    if "Video Url" not in out:
        out["Video Url"] = out.get("Video Url", "")
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
    out["Rating"] = norm_percent(out.get("Rating", "") or out.get("Positive Feedback", ""), decimals=1, empty_fallback="")
    if not str(out.get("Orders", "")).strip():
        out["Orders"] = str(out.get("Sales180Day", "")).strip()
    if "CouponCode" not in out:
        out["CouponCode"] = out.get("Code Name", "") or out.get("CouponCode", "")
    if "ItemId" not in out:
        out["ItemId"] = out.get("ProductId", "") or out.get("ItemId", "") or "ללא מספר"
    if "Opening" not in out:
        out["Opening"] = out.get("Opening", "") or ""
    if "Title" not in out:
        out["Title"] = out.get("Title", "") or out.get("Product Desc", "") or ""
    out["Strengths"] = out.get("Strengths", "")
    return out

def read_products(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [normalize_row_keys(r) for r in reader]
        return rows

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

# ========= USD→ILS =========
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

def _extract_number(s: str) -> float | None:
    if s is None:
        return None
    s = str(s)
    import re
    m = re.search(r"([-+]?\d+(?:[.,]\d+)?)", s)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))

def _convert_price_text(raw_value: str, rate: float) -> str:
    num = _extract_number(raw_value)
    if num is None:
        return ""
    ils = round(num * rate)
    return str(int(ils))

def _rows_with_optional_usd_to_ils(rows_raw: list[dict], rate: float | None):
    out = []
    for r in rows_raw:
        rr = dict(r)
        if rate:
            orig_src = rr.get("OriginalPrice", rr.get("Origin Price", ""))
            sale_src = rr.get("SalePrice", rr.get("Discount Price", ""))

            if _is_usd_price(str(orig_src)):
                rr["OriginalPrice"] = _convert_price_text(orig_src, rate)
            if _is_usd_price(str(sale_src)):
                rr["SalePrice"] = _convert_price_text(sale_src, rate)
        out.append(normalize_row_keys(rr))
    return out

# ========= MENU =========
def inline_menu():
    kb = types.InlineKeyboardMarkup(row_width=3)

    # פעולות בסיס
    kb.add(
        types.InlineKeyboardButton("📢 פרסם עכשיו", callback_data="publish_now"),
        types.InlineKeyboardButton("⏭ דלג פריט", callback_data="skip_one"),
        types.InlineKeyboardButton("📝 רשימת ממתינים", callback_data="list_pending"),
    )
    kb.add(
        types.InlineKeyboardButton("📊 סטטוס שידור", callback_data="pending_status"),
        types.InlineKeyboardButton("🔄 טען/מזג מהקובץ", callback_data="reload_merge"),
        types.InlineKeyboardButton("🕒 מצב שינה (החלפה)", callback_data="toggle_schedule"),
    )

    # מרווחים
    kb.add(
        types.InlineKeyboardButton("⏱️ דקה", callback_data="delay_60"),
        types.InlineKeyboardButton("⏱️ 15ד", callback_data="delay_900"),
        types.InlineKeyboardButton("⏱️ 20ד", callback_data="delay_1200"),
        types.InlineKeyboardButton("⏱️ 25ד", callback_data="delay_1500"),
        types.InlineKeyboardButton("⏱️ 30ד", callback_data="delay_1800"),
    )

    # העלאת CSV
    kb.add(types.InlineKeyboardButton("📥 העלה CSV", callback_data="upload_source"))

    # המרת $→₪ לקובץ הבא בלבד (חיווי ON/OFF)
    rate = _read_rate()
    status = "✅" if _convert_enabled() else "❌"
    kb.add(types.InlineKeyboardButton(f"₪ המרת $→₪ ({rate}) לקובץ הבא: {status}", callback_data="convert_toggle"))

    # שינוי שער המרה
    kb.add(
        types.InlineKeyboardButton("⚙️ קבע שער המרה", callback_data="show_set_rate")
    )

    # איפוס יזום מהקובץ הראשי
    kb.add(types.InlineKeyboardButton("🔁 חזור להתחלה מהקובץ", callback_data="reset_from_data"))

    # מחיקות
    kb.add(
        types.InlineKeyboardButton("🗑️ מחק פריטי התור מהקובץ", callback_data="delete_source_from_pending"),
        types.InlineKeyboardButton("🧹 מחק את workfile.csv", callback_data="delete_source_file"),
    )

    kb.add(types.InlineKeyboardButton(
        f"מרווח: ~{POST_DELAY_SECONDS//60} דק׳ | יעד: {CURRENT_TARGET}", callback_data="noop_info"
    ))
    return kb

# ========= BROADCAST =========
def should_broadcast(now: datetime | None = None) -> bool:
    if now is None:
        now = datetime.now(tz=IL_TZ)
    else:
        now = now.astimezone(IL_TZ)
    wd = now.weekday()
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

# ========= SEND =========
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

    price_line = f'💰 מחיר מבצע: <a href="{buy_link}">{sale_price} ש"ח</a> (מחיר מקורי: {original_price} ש"ח)'
    lines += [
        price_line,
        discount_text,
        f"⭐ דירוג: {rating_percent}",
        f"📦 {orders_text}",
        "🚚 משלוח חינם מעל 38 ש\"ח או 7.49 ש\"ח",
        "",
        coupon_text if coupon_text else "",
        "",
        f'להזמנה מהירה👈 <a href="{buy_link}">לחצו כאן</a>',
        "",
        f"מספר פריט: {item_id}",
        'להצטרפות לערוץ לחצו כאן👈 <a href="https://t.me/+LlMY8B9soOdhNmZk">קליק והצטרפתם</a>',
        "",
        "👇🛍הזמינו עכשיו🛍👇",
        f'<a href="{buy_link}">לחיצה וזה בדרך </a>',
    ]

    post = "\n".join([l for l in lines if l is not None and str(l).strip() != ""])
    return post, image_url

def post_to_channel(product):
    try:
        post_text, image_url = format_post(product)
        video_url = (product.get('Video Url') or "").strip()
        target = CURRENT_TARGET
        if video_url.endswith('.mp4') and video_url.startswith("http"):
            resp = SESSION.get(video_url, timeout=20)
            resp.raise_for_status()
            bot.send_video(target, resp.content, caption=post_text)
        else:
            resp = SESSION.get(image_url, timeout=20)
            resp.raise_for_status()
            bot.send_photo(target, resp.content, caption=post_text)
    except Exception as e:
        print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] Failed to post: {e}", flush=True)

def send_next_locked(source: str = "loop") -> bool:
    with FILE_LOCK:
        pending = read_products(PENDING_CSV)
        if not pending:
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: no pending", flush=True)
            return False

        item = pending[0]
        item_id = (item.get("ItemId") or "").strip()
        title = (item.get("Title") or "").strip()[:120]
        print(f"[{datetime.now(tz=IL_TZ)}] {source}: sending ItemId={item_id} | Title={title}", flush=True)

        try:
            post_to_channel(item)
        except Exception as e:
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: send FAILED: {e}", flush=True)
            return False

        try:
            write_products(PENDING_CSV, pending[1:])
        except Exception as e:
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: write FAILED, retry once: {e}", flush=True)
            time.sleep(0.2)
            try:
                write_products(PENDING_CSV, pending[1:])
            except Exception as e2:
                print(f"[{datetime.now(tz=IL_TZ)}] {source}: write FAILED permanently: {e2}", flush=True)
                return True

        print(f"[{datetime.now(tz=IL_TZ)}] {source}: sent & advanced queue", flush=True)
        return True

# ========= SCHEDULE =========
def load_delay_seconds(default_seconds: int = 1500) -> int:
    try:
        if os.path.exists(DELAY_FILE):
            with open(DELAY_FILE, "r", encoding="utf-8") as f:
                val = int(f.read().strip())
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

POST_DELAY_SECONDS = load_delay_seconds(1500)

# ========= ADMIN =========
def _is_admin(msg) -> bool:
    if not ADMIN_USER_IDS:
        return True
    return msg.from_user and (msg.from_user.id in ADMIN_USER_IDS)

# ========= MERGE =========
def merge_from_data_into_pending():
    data_rows = read_products(DATA_CSV)
    pending_rows = read_products(PENDING_CSV)

    def key_of(r):
        item_id = (r.get("ItemId") or "").strip()
        title = (r.get("Title") or "").strip()
        buy = (r.get("BuyLink") or "").strip()
        return (item_id if item_id else None, title if not item_id else None, buy)

    existing_keys = {key_of(r) for r in pending_rows}
    added = 0
    already = 0

    for r in data_rows:
        k = key_of(r)
        if k in existing_keys:
            already += 1
            continue
        pending_rows.append(r)
        existing_keys.add(k)
        added += 1

    write_products(PENDING_CSV, pending_rows)
    return added, already, len(pending_rows)

# ========= DELETE HELPERS =========
def _key_of_row(r: dict):
    item_id = (r.get("ItemId") or "").strip()
    title   = (r.get("Title") or "").strip()
    buy     = (r.get("BuyLink") or "").strip()
    return (item_id if item_id else None, title if not item_id else None, buy)

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

# ========= CALLBACKS =========
@bot.callback_query_handler(func=lambda c: True)
def on_inline_click(c):
    global POST_DELAY_SECONDS, CURRENT_TARGET
    if not _is_admin(c.message):
        bot.answer_callback_query(c.id, "אין הרשאה.", show_alert=True)
        return

    data = c.data or ""
    chat_id = c.message.chat.id

    if data == "publish_now":
        ok = send_next_locked("manual")
        if not ok:
            bot.answer_callback_query(c.id, "אין פוסטים ממתינים או שגיאה בשליחה.", show_alert=True)
            return
        bot.edit_message_text("✅ נשלח הפריט הבא בתור.", chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "skip_one":
        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
            if not pending:
                bot.answer_callback_query(c.id, "אין מה לדלג – התור ריק.", show_alert=True)
                return
            write_products(PENDING_CSV, pending[1:])
        bot.edit_message_text("⏭ דילגתי על הפריט הבא בתור.", chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "list_pending":
        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
        if not pending:
            bot.answer_callback_query(c.id, "אין פוסטים ממתינים ✅", show_alert=True)
            return
        preview = pending[:10]
        lines = []
        for i, p in enumerate(preview, start=1):
            title = str(p.get('Title',''))[:80]
            sale = p.get('SalePrice','')
            disc = p.get('Discount','')
            rating = p.get('Rating','')
            lines.append(f"{i}. {title}\n   מחיר מבצע: {sale} | הנחה: {disc} | דירוג: {rating}")
        more = len(pending) - len(preview)
        if more > 0:
            lines.append(f"...ועוד {more} בהמתנה")
        bot.edit_message_text("📝 פוסטים ממתינים:\n\n" + "\n".join(lines), chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "pending_status":
        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
        count = len(pending)
        now_il = datetime.now(tz=IL_TZ)
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
        bot.edit_message_text(text, chat_id, c.message.message_id, reply_markup=inline_menu(), parse_mode='HTML')

    elif data == "reload_merge":
        added, already, total_after = merge_from_data_into_pending()
        bot.edit_message_text(f"🔄 מיזוג הושלם.\nנוספו: {added}\nבעבר בתור: {already}\nסה\"כ בתור כעת: {total_after}",
                              chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "upload_source":
        EXPECTING_UPLOAD.add(getattr(c.from_user, "id", None))
        bot.edit_message_text("שלח/י עכשיו קובץ CSV (כמסמך). הבוט ימפה עמודות, יעדכן workfile.csv וימזג אל התור.",
                              chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "toggle_schedule":
        set_schedule_enforced(not is_schedule_enforced())
        state = "🕰️ מתוזמן (שינה פעיל)" if is_schedule_enforced() else "🟢 תמיד-פעיל"
        bot.edit_message_text(f"החלפתי מצב לשידור: {state}", chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data.startswith("delay_"):
        try:
            seconds = int(data.split("_", 1)[1])
            if seconds <= 0:
                raise ValueError("מרווח חייב להיות חיובי")
            global POST_DELAY_SECONDS
            POST_DELAY_SECONDS = seconds
            save_delay_seconds(seconds)
            DELAY_EVENT.set()
            mins = seconds // 60
            bot.edit_message_text(f"⏱️ עודכן מרווח: ~{mins} דק׳ ({seconds} שניות).",
                                  chat_id, c.message.message_id, reply_markup=inline_menu())
        except Exception as e:
            bot.answer_callback_query(c.id, f"שגיאה בעדכון מרווח: {e}", show_alert=True)

    elif data == "convert_toggle":
        if _convert_enabled():
            _set_convert_enabled(False)
            bot.edit_message_text("❌ המרת $→₪ בוטלה לקובץ הבא.", chat_id, c.message.message_id, reply_markup=inline_menu())
        else:
            rate = _read_rate()
            _set_convert_enabled(True, rate)
            bot.edit_message_text(f"✅ הופעל: המרת $→₪ לקובץ הבא (שער {rate}).", chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "show_set_rate":
        rate = _read_rate()
        bot.edit_message_text(
            f"שער נוכחי: {rate}\n\nהגדר שער חדש באמצעות הפקודה:\n/set_rate 3.60",
            chat_id, c.message.message_id, reply_markup=inline_menu()
        )

    elif data == "reset_from_data":
        src = read_products(DATA_CSV)
        with FILE_LOCK:
            write_products(PENDING_CSV, src)
        bot.edit_message_text(f"🔁 התור אופס ומתחיל מחדש ({len(src)} פריטים) מהקובץ הראשי.",
                              chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "delete_source_from_pending":
        removed, left = delete_source_rows_from_pending()
        bot.edit_message_text(f"🗑️ הוסר מהתור: {removed} פריטים שנמצאו ב-workfile.csv\nנשארו בתור: {left}",
                              chat_id, c.message.message_id, reply_markup=inline_menu())

    elif data == "delete_source_file":
        ok = delete_source_csv_file()
        msg_txt = "🧹 workfile.csv אופס לריק (נשמרו רק כותרות). התור לא שונה." if ok else "שגיאה במחיקת workfile.csv"
        bot.edit_message_text(msg_txt, chat_id, c.message.message_id, reply_markup=inline_menu())

    else:
        bot.answer_callback_query(c.id)

# ========= COMMANDS =========
@bot.message_handler(commands=['set_rate'])
def cmd_set_rate(msg):
    parts = (msg.text or "").split()
    if len(parts) < 2:
        bot.reply_to(msg, "שימוש: /set_rate 3.60")
        return
    try:
        v = float(parts[1])
        if v <= 0:
            raise ValueError()
        _write_rate(v)
        # אם ההמרה פעילה, נעדכן את הדגל כדי שישמור את השער החדש
        if _convert_enabled():
            _set_convert_enabled(True, v)
        bot.reply_to(msg, f"✅ עודכן שער ההמרה ל-{v}.")
    except Exception:
        bot.reply_to(msg, "ערך לא תקין. דוגמה: /set_rate 3.60")

@bot.message_handler(commands=['convert_status'])
def cmd_convert_status(msg):
    rate = _read_rate()
    status = "פעילה ✅" if _convert_enabled() else "כבויה ❌"
    bot.reply_to(msg, f"המרת $→₪ לקובץ הבא: {status}\nשער נוכחי: {rate}\nלהפעלה/ביטול: כפתור בתפריט")

@bot.message_handler(commands=['start', 'help', 'menu'])
def cmd_start(msg):
    bot.send_message(msg.chat.id, "בחר פעולה:", reply_markup=inline_menu())

# ========= UPLOAD =========
@bot.message_handler(content_types=['document'])
def on_document(msg):
    uid = getattr(msg.from_user, "id", None)

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
        if _convert_enabled():
            try:
                with open(CONVERT_NEXT_FLAG_FILE, "r", encoding="utf-8") as f:
                    convert_rate = float((f.read() or "").strip() or _read_rate())
            except Exception:
                convert_rate = _read_rate()
            # כיבוי אוטומטי לאחר קובץ זה
            _set_convert_enabled(False)

        rows = _rows_with_optional_usd_to_ils(rows_raw, convert_rate)

        with FILE_LOCK:
            write_products(DATA_CSV, rows)
            pending_rows = read_products(PENDING_CSV)

            def key_of(r):
                item_id = (r.get("ItemId") or "").strip()
                title = (r.get("Title") or "").strip()
                buy = (r.get("BuyLink") or "").strip()
                return (item_id if item_id else None, title if not item_id else None, buy)

            existing_keys = {key_of(r) for r in pending_rows}
            added = 0
            already = 0
            for r in rows:
                k = key_of(r)
                if k in existing_keys:
                    already += 1
                    continue
                pending_rows.append(r)
                existing_keys.add(k)
                added += 1
            write_products(PENDING_CSV, pending_rows)
            total_after = len(pending_rows)

        extra_line = ""
        if convert_rate:
            extra_line = f"\n💱 בוצעה המרה לש\"ח בשער {convert_rate} לכל מחירי הדולר בקובץ זה."

        bot.reply_to(msg,
            "✅ הקובץ נקלט בהצלחה.\n"
            f"נוספו לתור: {added}\nכבר היו בתור/כפולים: {already}\nסה\"כ בתור כעת: {total_after}"
            + extra_line +
            "\n\nסטטוס המרה הוחזר ל: כבויה ❌. אפשר להדליק שוב מהתפריט."
        )

    except Exception as e:
        bot.reply_to(msg, f"שגיאה בעיבוד הקובץ: {e}")

# ========= LOOP =========
def run_sender_loop():
    init_pending()
    while True:
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

if __name__ == "__main__":
    print("Bot starting...", flush=True)
    t = threading.Thread(target=run_sender_loop, daemon=True)
    t.start()
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            time.sleep(5)
