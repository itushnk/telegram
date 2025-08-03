
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
BOT_TOKEN = "8371104768:AAHi2lv7CFNFAWycjWeUSJiOn9YR0Qvep_4"  # ← עדכן כאן
CHANNEL_ID = "@nisayon121"       # ← עדכן כאן (למשל: "@my_channel")
ADMIN_USER_IDS = set()  # ← מומלץ להגדיר user id שלך: {123456789}

# קבצים
DATA_CSV = "workfile.csv"           # קובץ המקור שאתה מכין
PENDING_CSV = "pending.csv"         # תור הפוסטים הממתינים

# מצב עבודה: 'מתוזמן' או 'תמיד-פעיל' באמצעות דגל קובץ
SCHEDULE_FLAG_FILE = "schedule_enforced.flag"  # קיים => מתוזמן (מצב שינה פעיל), לא קיים => תמיד משדר

# מרווח בין פוסטים בשניות
POST_DELAY_SECONDS = 60

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})

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


# ========= BROADCAST WINDOW =========
def should_broadcast(now: datetime | None = None) -> bool:
    if now is None:
        now = datetime.now(tz=IL_TZ)
    else:
        now = now.astimezone(IL_TZ)
    wd = now.weekday()  # Mon=0 ... Sun=6 (אצלנו: ראשון=6)
    t = now.time()
    if wd in (6, 0, 1, 2, 3):  # ראשון–חמישי
        return dtime(6, 0) <= t <= dtime(23, 59)
    if wd == 4:  # שישי
        return dtime(6, 0) <= t <= dtime(17, 59)
    if wd == 5:  # שבת
        return dtime(20, 15) <= t <= dtime(23, 59)
    return False


# ========= MODE: SCHEDULE vs ALWAYS =========
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
        print(f"[WARN] Failed to set schedule mode: {e}")

def is_quiet_now(now: datetime | None = None) -> bool:
    """
    אם מצב "שינה פעיל" (schedule enforced) — נכבד חלונות זמן.
    אם "שינה לא פעיל" — תמיד נשלח (לא שקט לעולם).
    """
    if is_schedule_enforced():
        return not should_broadcast(now)
    return False  # מצב תמיד-פעיל


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
    coupon = product.get('CouponCode', '')
    opening = product.get('Opening', '')

    rating_percent = rating if rating else "אין דירוג"
    orders_num = safe_int(orders, default=0)
    orders_text = f"{orders_num} הזמנות" if orders_num >= 50 else "פריט חדש לחברי הערוץ"
    discount_text = f"💸 חיסכון של {discount}!" if discount and discount != "0%" else ""
    coupon_text = f"🎁 קופון לחברי הערוץ בלבד: {coupon}" if str(coupon).strip() else ""

    post = f"""{opening}

{title}

✨ נוח במיוחד לשימוש יומיומי
🔧 איכות גבוהה ועמידות לאורך זמן
🎨 מגיע במבחר גרסאות – בדקו בקישור!

💰 מחיר מבצע: <a href="{buy_link}">{sale_price} ש"ח</a> (מחיר מקורי: {original_price} ש"ח)
{discount_text}
⭐ דירוג: {rating_percent}
📦 {orders_text}
🚚 משלוח חינם מעל 38 ש"ח או 7.49 ש"ח

{coupon_text}

להזמנה מהירה👈 <a href="{buy_link}">לחצו כאן</a>

מספר פריט: {item_id}
להצטרפות לערוץ לחצו כאן👈 <a href="https://t.me/+LlMY8B9soOdhNmZk">קליק והצטרפתם</a>

👇🛍הזמינו עכשיו🛍👇
<a href="{buy_link}">לחיצה וזה בדרך </a>
"""
    return post, image_url


def post_to_channel(product):
    try:
        post_text, image_url = format_post(product)
        video_url = (product.get('Video Url') or "").strip()
        if video_url.endswith('.mp4'):
            resp = SESSION.get(video_url, timeout=20)
            resp.raise_for_status()
            bot.send_video(CHANNEL_ID, resp.content, caption=post_text)
        else:
            resp = SESSION.get(image_url, timeout=20)
            resp.raise_for_status()
            bot.send_photo(CHANNEL_ID, resp.content, caption=post_text)
    except Exception as e:
        print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] Failed to post: {e}")


# ========= ADMIN COMMANDS =========
def user_is_admin(msg) -> bool:
    if not ADMIN_USER_IDS:
        return True
    return msg.from_user and (msg.from_user.id in ADMIN_USER_IDS)

@bot.message_handler(commands=['list_pending'])
def list_pending(msg):
    pending = read_products(PENDING_CSV)
    if not pending:
        bot.reply_to(msg, "אין פוסטים ממתינים ✅")
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
    bot.reply_to(msg, "פוסטים ממתינים:\n\n" + "\n".join(lines))

@bot.message_handler(commands=['clear_pending'])
def clear_pending(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    write_products(PENDING_CSV, [])
    bot.reply_to(msg, "נוקה התור של הפוסטים הממתינים 🧹")

@bot.message_handler(commands=['reset_pending'])
def reset_pending(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    src = read_products(DATA_CSV)
    write_products(PENDING_CSV, src)
    bot.reply_to(msg, "התור אופס מהקובץ הראשי והכול נטען מחדש 🔄")

@bot.message_handler(commands=['skip_one'])
def skip_one(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    pending = read_products(PENDING_CSV)
    if not pending:
        bot.reply_to(msg, "אין מה לדלג – אין פוסטים ממתינים.")
        return
    write_products(PENDING_CSV, pending[1:])
    bot.reply_to(msg, "דילגתי על הפוסט הבא ✅")

@bot.message_handler(commands=['peek_next'])
def peek_next(msg):
    pending = read_products(PENDING_CSV)
    if not pending:
        bot.reply_to(msg, "אין פוסטים ממתינים ✅")
        return
    nxt = pending[0]
    txt = "<b>הפריט הבא בתור:</b>\n\n" + "\n".join([f"<b>{k}:</b> {v}" for k,v in nxt.items()])
    bot.reply_to(msg, txt, parse_mode='HTML')

@bot.message_handler(commands=['peek_idx'])
def peek_idx(msg):
    text = (msg.text or "").strip()
    parts = text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.reply_to(msg, "שימוש: /peek_idx N  (לדוגמה: /peek_idx 3)")
        return
    idx = int(parts[1])
    pending = read_products(PENDING_CSV)
    if not pending:
        bot.reply_to(msg, "אין פוסטים ממתינים ✅")
        return
    if idx < 1 or idx > len(pending):
        bot.reply_to(msg, f"אינדקס מחוץ לטווח. יש כרגע {len(pending)} פוסטים בתור.")
        return
    item = pending[idx-1]
    txt = f"<b>פריט #{idx} בתור:</b>\n\n" + "\n".join([f"<b>{k}:</b> {v}" for k,v in item.items()])
    bot.reply_to(msg, txt, parse_mode='HTML')

@bot.message_handler(commands=['pending_status'])
def pending_status(msg):
    pending = read_products(PENDING_CSV)
    count = len(pending)
    now_il = datetime.now(tz=IL_TZ)
    schedule_line = "🕰️ מצב: מתוזמן (שינה פעיל)" if is_schedule_enforced() else "🟢 מצב: תמיד-פעיל (שינה כבוי)"
    if count == 0:
        bot.reply_to(msg, f"{schedule_line}\nאין פוסטים ממתינים ✅")
        return
    total_seconds = (count - 1) * POST_DELAY_SECONDS
    eta = now_il + timedelta(seconds=total_seconds)
    eta_str = eta.strftime("%Y-%m-%d %H:%M:%S %Z")
    next_eta = now_il.strftime("%Y-%m-%d %H:%M:%S %Z")
    status_line = "🎙️ שידור אפשרי עכשיו" if not is_quiet_now(now_il) else "⏸️ כרגע מחוץ לחלון השידור"
    msg_text = (
        f"{schedule_line}\n"
        f"{status_line}\n"
        f"יש כרגע <b>{count}</b> פוסטים ממתינים.\n"
        f"⏱️ השידור הבא (תיאוריה לפי מרווח): <b>{next_eta}</b>\n"
        f"🕒 שעת השידור המשוערת של האחרון: <b>{eta_str}</b>\n"
        f"(מרווח בין פוסטים: {POST_DELAY_SECONDS} שניות)"
    )
    bot.reply_to(msg, msg_text, parse_mode='HTML')


# ========= Schedule mode commands =========
@bot.message_handler(commands=['schedule_on'])
def cmd_schedule_on(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_schedule_enforced(True)
    bot.reply_to(msg, "מצב שינה פעיל: הבוט ישדר רק בשעות שהוגדרו.")

@bot.message_handler(commands=['schedule_off'])
def cmd_schedule_off(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_schedule_enforced(False)
    bot.reply_to(msg, "מצב שינה כבוי: הבוט ישדר תמיד.")

@bot.message_handler(commands=['schedule_status'])
def cmd_schedule_status(msg):
    bot.reply_to(msg, "מצב שינה פעיל" if is_schedule_enforced() else "מצב שינה כבוי")


# ========= Force send next =========
@bot.message_handler(commands=['force_send_next'])
def cmd_force_send_next(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    pending = read_products(PENDING_CSV)
    if not pending:
        bot.reply_to(msg, "אין פוסטים ממתינים ✅")
        return
    item = pending[0]
    try:
        post_to_channel(item)
        write_products(PENDING_CSV, pending[1:])
        item_id = item.get("ItemId", "ללא מספר")
        title = (item.get("Title","") or "")[:80]
        bot.reply_to(msg, f"נשלח בכפייה ✅\nמספר פריט: {item_id}\nכותרת: {title}")
    except Exception as e:
        bot.reply_to(msg, f"שגיאה בשליחה כפויה: {e}")


# ========= /start menu =========
@bot.message_handler(commands=['start', 'help', 'menu'])
def cmd_start(msg):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row('/list_pending', '/pending_status')
    kb.row('/peek_next', '/peek_idx')
    kb.row('/skip_one', '/clear_pending')
    kb.row('/reset_pending', '/force_send_next')
    kb.row('/schedule_status')
    kb.row('/schedule_on', '/schedule_off')

    text = """ברוך הבא! מצב עבודה בשתי אפשרויות:
• מצב שינה פעיל (מתוזמן): שידור רק בשעות שהוגדרו.
• מצב שינה כבוי (תמיד-פעיל): הבוט משדר כל הזמן.

פקודות:
• /schedule_on – הפעלת מצב שינה פעיל (כיבוד שעות)
• /schedule_off – ביטול מצב שינה (שידור תמיד)
• /schedule_status – מצב נוכחי
• /list_pending – פוסטים ממתינים
• /pending_status – סטטוס שידור ו-ETA
• /peek_next – הפריט הבא
• /peek_idx N – פריט לפי אינדקס
• /skip_one – דילוג על הבא
• /clear_pending – ניקוי התור
• /reset_pending – טעינה מחדש מהקובץ
• /force_send_next – שליחה כפויה של הפריט הבא (עוקף שקט)

טיפ: פתח את תפריט הפקודות דרך כפתור התפריט או בהקלדת '/'."""
    bot.send_message(msg.chat.id, text, reply_markup=kb)


# ========= SENDER LOOP (BACKGROUND) =========
def run_sender_loop():
    if not os.path.exists(SCHEDULE_FLAG_FILE):
        # ברירת מחדל: שינה פעיל (כיבוד שעות)
        set_schedule_enforced(True)
    while True:
        if is_quiet_now():
            now_il = datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
            print(f"[{now_il}] Quiet (schedule enforced) — not broadcasting.")
            time.sleep(30)
            continue
        pending = read_products(PENDING_CSV)
        if not pending:
            print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] No pending posts.")
            time.sleep(30)
            continue
        product = pending[0]
        post_to_channel(product)
        write_products(PENDING_CSV, pending[1:])
        time.sleep(POST_DELAY_SECONDS)


# ========= MAIN =========
if __name__ == "__main__":
    _lock_handle = acquire_single_instance_lock()
    print_webhook_info()
    try:
        force_delete_webhook()
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        try:
            bot.remove_webhook()
        except Exception as e2:
            print(f"[WARN] remove_webhook failed: {e2}")
    print_webhook_info()
    t = threading.Thread(target=run_sender_loop, daemon=True)
    t.start()
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] Polling error: {e}. Retrying in 5s...")
            time.sleep(5)
