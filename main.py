# -*- coding: utf-8 -*-
import os, sys
# לוגים ללא באפר
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

# ========= CONFIG =========
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8371104768:AAHi2lv7CFNFAWycjWeUSJiOn9YR0Qvep_4")  # מומלץ ב-ENV
CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL", "@nisayon121")  # יעד ציבורי ברירת מחדל
ADMIN_USER_IDS = set()  # מומלץ: {123456789}

# נתיבי קבצים (ניתנים להגדרה ב-ENV כדי לעבוד עם Volume כמו /data)
DATA_CSV = os.environ.get("DATA_CSV", "workfile.csv")             # קובץ המקור
PENDING_CSV = os.environ.get("PENDING_CSV", "pending.csv")        # תור הפוסטים
DELAY_FILE = os.environ.get("DELAY_FILE", "post_delay.txt")       # מרווח נבחר
PUBLIC_PRESET_FILE  = os.environ.get("PUBLIC_PRESET_FILE",  "public_target.preset")
PRIVATE_PRESET_FILE = os.environ.get("PRIVATE_PRESET_FILE", "private_target.preset")
SCHEDULE_FLAG_FILE  = os.environ.get("SCHEDULE_FLAG_FILE", "schedule_enforced.flag")
CONSUME_MODE_FILE   = os.environ.get("CONSUME_MODE_FILE", "consume_mode.flag")  # קיים => מוחקים גם מ-workfile.csv
LOCK_PATH = os.environ.get("BOT_LOCK_PATH", "/tmp/bot.lock")  # נעילה למופע יחיד (עדיף לשים על Volume)

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})
IL_TZ = ZoneInfo("Asia/Jerusalem")

# יעד נוכחי
CURRENT_TARGET = CHANNEL_ID

# “התעוררות חמה” ללולאת השידור
DELAY_EVENT = threading.Event()

# מצב בחירת יעד (באמצעות Forward)
EXPECTING_TARGET = {}  # dict[user_id] = "public"|"private"

# מצב העלאת CSV
EXPECTING_UPLOAD = set()  # user_ids שמצפים ל-CSV

# נעילה לפעולות על התור כדי למנוע כפילות בין הלולאה לכפתור ידני
FILE_LOCK = threading.Lock()


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

# ---- PRESET HELPERS (load/save target presets) ----
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
    """@name נשאר מחרוזת; '-100…'/מספר מומר ל-int."""
    try:
        if isinstance(value, int):
            return value
        s = str(value).strip()
        if s.startswith("-"):
            return int(s)
        return s
    except Exception:
        return value

def check_and_probe_target(target):
    """בודק קיום יעד, אדמין, ויכולת פרסום קצרה (למחיקה)."""
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
        now = datetime.now(tz=IL_TZ)
    else:
        now = now.astimezone(IL_TZ)
    wd = now.weekday()  # Mon=0 ... Sun=6 (אצלנו: ראשון=6)
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


# ========= CONSUME MODE (מוחק גם מה-workfile.csv אחרי שליחה) =========
def is_consume_mode() -> bool:
    return os.path.exists(CONSUME_MODE_FILE)

def set_consume_mode(enabled: bool) -> None:
    try:
        if enabled:
            with open(CONSUME_MODE_FILE, "w", encoding="utf-8") as f:
                f.write("on")
        else:
            if os.path.exists(CONSUME_MODE_FILE):
                os.remove(CONSUME_MODE_FILE)
    except Exception as e:
        print(f"[WARN] Failed to set consume mode: {e}", flush=True)

def _key_of_row(r):
    item_id = (r.get("ItemId") or "").strip()
    title   = (r.get("Title") or "").strip()
    buy     = (r.get("BuyLink") or "").strip()
    # כמו בלוגיקת המיזוג: אם יש ItemId משתמשים בו; אחרת Title+BuyLink
    return (item_id if item_id else None, title if not item_id else None, buy)

def remove_item_from_csv(path: str, key_tuple) -> bool:
    """מסיר פריט תואם מ-CSV (אם נמצא), ושומר חזרה. מחזיר True אם הוסר משהו."""
    if not os.path.exists(path):
        return False
    rows = read_products(path)
    before = len(rows)
    rows = [r for r in rows if _key_of_row(r) != key_tuple]
    if len(rows) != before:
        write_products(path, rows)
        return True
    return False


# ========= SAFE EDIT (מניעת 400) =========
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
    # תוכן הפרסום נמשך רק מהקובץ (Opening/Strengths/Title/מחירים וכו')
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
        target = resolve_target(CURRENT_TARGET)
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


# ========= ATOMIC SEND (מניעת כפילות) =========
def send_next_locked(source: str = "loop") -> bool:
    """
    שולח את הפריט הראשון בתור (אם יש), מעדכן את pending.csv ומדפיס לוגים.
    מחזיר True אם נשלח משהו.
    """
    with FILE_LOCK:
        pending = read_products(PENDING_CSV)
        if not pending:
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: no pending", flush=True)
            return False

        item = pending[0]
        item_id = (item.get("ItemId") or "").strip()
        title = (item.get("Title") or "").strip()[:120]
        key_t = _key_of_row(item)
        print(f"[{datetime.now(tz=IL_TZ)}] {source}: sending ItemId={item_id} | Title={title}", flush=True)

        try:
            post_to_channel(item)
        except Exception as e:
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: send FAILED: {e}", flush=True)
            return False

        # קדימת התור
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

        # מחיקה מה-workfile.csv אם מצב צריכה פעיל
        if is_consume_mode():
            removed = remove_item_from_csv(DATA_CSV, key_t)
            print(f"[{datetime.now(tz=IL_TZ)}] {source}: consume_mode=True, removed_from_workfile={removed}", flush=True)

        print(f"[{datetime.now(tz=IL_TZ)}] {source}: sent & advanced queue", flush=True)
        return True


# ========= DELAY (מרווח) =========
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

POST_DELAY_SECONDS = load_delay_seconds(1500)  # 25 דקות


# ========= ADMIN HELPERS =========
def _is_admin(msg) -> bool:
    if not ADMIN_USER_IDS:
        return True
    return msg.from_user and (msg.from_user.id in ADMIN_USER_IDS)


# ========= INLINE MENU =========
def inline_menu():
    kb = types.InlineKeyboardMarkup(row_width=3)

    # פעולות
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

    # יעדים (שמורים)
    kb.add(
        types.InlineKeyboardButton("🎯 ציבורי (השתמש)", callback_data="target_public"),
        types.InlineKeyboardButton("🔒 פרטי (השתמש)", callback_data="target_private"),
    )
    # בחירה דרך Forward
    kb.add(
        types.InlineKeyboardButton("🆕 בחר ערוץ ציבורי", callback_data="choose_public"),
        types.InlineKeyboardButton("🆕 בחר ערוץ פרטי", callback_data="choose_private"),
    )
    # ביטול בחירה
    kb.add(types.InlineKeyboardButton("❌ בטל בחירת יעד", callback_data="choose_cancel"))

    # מצב צריכה
    consume_label = "🗑️ מצב צריכה: פעיל" if is_consume_mode() else "🗑️ מצב צריכה: כבוי"
    kb.add(types.InlineKeyboardButton(consume_label + " (החלפה)", callback_data="toggle_consume"))

    kb.add(types.InlineKeyboardButton(
        f"מרווח: ~{POST_DELAY_SECONDS//60} דק׳ | יעד: {CURRENT_TARGET}", callback_data="noop_info"
    ))
    return kb


# ========= MERGE FROM DATA =========
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


# ========= UPLOAD CSV HELPERS =========
def _decode_csv_bytes(b: bytes) -> str:
    # ניסיון פענוח ידידותי ל-CSV בעברית
    for enc in ("utf-8-sig", "utf-8", "cp1255", "iso-8859-8"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="ignore")

def _read_source_csv_text(csv_text: str):
    """קורא טקסט CSV ומחזיר רשימת dicts כשהעמודות מנורמלות."""
    from io import StringIO
    f = StringIO(csv_text)
    reader = csv.DictReader(f)
    rows = [normalize_row_keys(r) for r in reader]
    return rows

def _save_workfile(rows):
    """כותב את הנתונים ל-workfile.csv בפורמט שהבוט מצפה לו."""
    write_products(DATA_CSV, rows)

def _merge_to_pending_from_rows(rows):
    """
    ממזג rows (כבר מנורמלים) אל pending.csv בלי כפילויות.
    """
    pending_rows = read_products(PENDING_CSV)

    def key_of(r):
        item_id = (r.get("ItemId") or "").strip()
        title   = (r.get("Title") or "").strip()
        buy     = (r.get("BuyLink") or "").strip()
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
    return added, already, len(pending_rows)


# ========= INLINE CALLBACKS =========
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
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="✅ נשלח הפריט הבא בתור.", reply_markup=inline_menu(), cb_id=c.id)

    elif data == "skip_one":
        with FILE_LOCK:
            pending = read_products(PENDING_CSV)
            if not pending:
                bot.answer_callback_query(c.id, "אין מה לדלג – התור ריק.", show_alert=True)
                return
            write_products(PENDING_CSV, pending[1:])
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="⏭ דילגתי על הפריט הבא בתור.", reply_markup=inline_menu(), cb_id=c.id)

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
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text="📝 פוסטים ממתינים:\n\n" + "\n".join(lines),
                          reply_markup=inline_menu(), cb_id=c.id)

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
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=text, reply_markup=inline_menu(), parse_mode='HTML', cb_id=c.id)

    elif data == "reload_merge":
        added, already, total_after = merge_from_data_into_pending()
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🔄 מיזוג הושלם.\nנוספו: {added}\nבעבר בתור: {already}\nסה\"כ בתור כעת: {total_after}",
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

    elif data == "toggle_consume":
        set_consume_mode(not is_consume_mode())
        state = "🗑️ צריכה פעילה (מוחק גם מה-workfile.csv)" if is_consume_mode() else "✅ צריכה כבויה (לא נוגע ב-workfile.csv)"
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"החלפתי מצב: {state}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data.startswith("delay_"):
        try:
            seconds = int(data.split("_", 1)[1])
            if seconds <= 0:
                raise ValueError("מרווח חייב להיות חיובי")
            POST_DELAY_SECONDS = seconds
            save_delay_seconds(seconds)
            DELAY_EVENT.set()
            mins = seconds // 60
            safe_edit_message(bot, chat_id=chat_id, message=c.message,
                              new_text=f"⏱️ עודכן מרווח: ~{mins} דק׳ ({seconds} שניות).",
                              reply_markup=inline_menu(), cb_id=c.id)
        except Exception as e:
            bot.answer_callback_query(c.id, f"שגיאה בעדכון מרווח: {e}", show_alert=True)

    elif data == "target_public":
        v = _load_preset(PUBLIC_PRESET_FILE)
        if v is None:
            bot.answer_callback_query(c.id, "לא הוגדר יעד ציבורי. בחר דרך '🆕 בחר ערוץ ציבורי'.", show_alert=True)
            return
        CURRENT_TARGET = resolve_target(v)
        src_rows = read_products(DATA_CSV)
        with FILE_LOCK:
            write_products(PENDING_CSV, src_rows)
        ok, details = check_and_probe_target(CURRENT_TARGET)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🎯 עברתי לשדר ליעד הציבורי: {v}\n🔄 התור אופס ומתחיל מחדש ({len(src_rows)} פריטים).\n{details}",
                          reply_markup=inline_menu(), cb_id=c.id)

    elif data == "target_private":
        v = _load_preset(PRIVATE_PRESET_FILE)
        if v is None:
            bot.answer_callback_query(c.id, "לא הוגדר יעד פרטי. בחר דרך '🆕 בחר ערוץ פרטי'.", show_alert=True)
            return
        CURRENT_TARGET = resolve_target(v)
        src_rows = read_products(DATA_CSV)
        with FILE_LOCK:
            write_products(PENDING_CSV, src_rows)
        ok, details = check_and_probe_target(CURRENT_TARGET)
        safe_edit_message(bot, chat_id=chat_id, message=c.message,
                          new_text=f"🔒 עברתי לשדר ליעד הפרטי: {v}\n🔄 התור אופס ומתחיל מחדש ({len(src_rows)} פריטים).\n{details}",
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
    src_rows = read_products(DATA_CSV)
    with FILE_LOCK:
        write_products(PENDING_CSV, src_rows)
    ok, details = check_and_probe_target(CURRENT_TARGET)

    EXPECTING_TARGET.pop(msg.from_user.id, None)

    bot.reply_to(msg,
        f"✅ נשמר יעד {label}: {target_value}\n"
        f"🔄 התור אופס ומתחיל מחדש ({len(src_rows)} פריטים).\n"
        f"{details}\n\nאפשר לעבור בין יעדים מהתפריט: 🎯/🔒"
    )


# ========= UPLOAD CSV COMMANDS =========
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
        # לא במצב העלאה מבוקש — מתעלמים
        return

   
