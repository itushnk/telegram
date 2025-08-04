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
BOT_TOKEN = os.getenv("BOT_TOKEN")  # טוקן מהסביבה (לא לשמור בקוד)
CHANNEL_ID = "@nisayon121"         # יעד ברירת מחדל (למשל: "@my_channel")
ADMIN_USER_IDS = {123456789}       # עדכן ל-user id שלך

# יעד שידור נוכחי + קבצי פריסט
CURRENT_TARGET = CHANNEL_ID
PUBLIC_PRESET_FILE = "public_target.preset"
PRIVATE_PRESET_FILE = "private_target.preset"

# קבצים
DATA_CSV = "workfile.csv"          # קובץ המקור
PENDING_CSV = "pending.csv"        # תור הפוסטים

# מצב עבודה: 'מתוזמן' או 'תמיד-פעיל' באמצעות דגל קובץ
SCHEDULE_FLAG_FILE = "schedule_enforced.flag"  # קיים => מתוזמן; לא קיים => תמיד-פעיל

# מרווח בין פוסטים בשניות (ברירת מחדל: 20 דקות)
POST_DELAY_SECONDS = 1200

# ========= INIT =========
if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.3"})

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


# ========= TARGET HELPERS =========
def set_current_target(v):
    global CURRENT_TARGET
    CURRENT_TARGET = v

def save_public_preset(v):
    with open(PUBLIC_PRESET_FILE, "w", encoding="utf-8") as f:
        f.write(str(v))

def save_private_preset(v):
    with open(PRIVATE_PRESET_FILE, "w", encoding="utf-8") as f:
        f.write(str(v))

def load_public_preset():
    if not os.path.exists(PUBLIC_PRESET_FILE):
        return None
    with open(PUBLIC_PRESET_FILE, "r", encoding="utf-8") as f:
        return f.read().strip()

def load_private_preset():
    if not os.path.exists(PRIVATE_PRESET_FILE):
        return None
    with open(PRIVATE_PRESET_FILE, "r", encoding="utf-8") as f:
        return f.read().strip()

def _check_target_permissions(target):
    """
    בדיקת הרשאות/זיהוי יעד: שולח 'typing' (לא פוסט) כדי לוודא שהיעד קיים והרשאות תקינות.
    """
    try:
        bot.send_chat_action(target, 'typing')
        return True, "הרשאה נראית תקינה."
    except Exception as e:
        return False, f"שגיאה בהרשאות/זיהוי היעד: {e}"


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
    out["ImageURL"]   = (out.get("ImageURL") or out.get("Image Url") or "").strip()
    out["Video Url"]  = (out.get("Video Url") or "").strip()
    out["BuyLink"]    = (out.get("BuyLink") or out.get("Promotion Url") or "").strip()

    out["OriginalPrice"] = clean_price_text(out.get("OriginalPrice") or out.get("Origin Price") or "")
    out["SalePrice"]     = clean_price_text(out.get("SalePrice")     or out.get("Discount Price") or "")

    disc = f"{out.get('Discount', '')}".strip().replace("%", "")
    try:
        out["Discount"] = f"{int(round(float(disc)))}%" if disc else ""
    except Exception:
        out["Discount"] = ""

    out["Rating"] = norm_percent(out.get("Rating") or out.get("Positive Feedback") or "",
                                 decimals=1, empty_fallback="")
    out["Orders"] = str(out.get("Orders") or out.get("Sales180Day") or "").strip()

    out["CouponCode"] = (out.get("CouponCode") or out.get("Code Name") or "").strip()
    out["ItemId"]     = (out.get("ItemId") or out.get("ProductId") or "ללא מספר").strip()
    out["Opening"]    = (out.get("Opening") or "").strip()
    out["Title"]      = (out.get("Title") or out.get("Product Desc") or "").strip()
    out["Strengths"]  = (out.get("Strengths") or "").strip()

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
def build_post_text(product):
    item_id = product.get('ItemId', 'ללא מספר')
    title = product.get('Title', '')
    original_price = product.get('OriginalPrice', '')
    sale_price = product.get('SalePrice', '')
    discount = product.get('Discount', '')
    rating = product.get('Rating', '')
    orders = product.get('Orders', '')
    buy_link = product.get('BuyLink', '')
    coupon = product.get('CouponCode', '')
    opening = product.get('Opening', '')
    strengths_src = (product.get("Strengths") or "").strip()

    rating_percent = rating if rating else ""
    orders_num = safe_int(orders, default=0)
    orders_text = f"{orders_num} הזמנות" if orders_num >= 50 else "פריט חדש לחברי הערוץ"
    discount_text = f"💸 חיסכון של {discount}!" if discount and discount != "0%" else ""
    coupon_text = f"🎁 קופון לחברי הערוץ בלבד: {coupon}" if str(coupon).strip() else ""
    rating_line = f"⭐ דירוג: {rating_percent}" if rating_percent else ""

    strengths = []
    if strengths_src:
        for part in [p.strip() for p in strengths_src.replace("|", "\n").replace(";", "\n").split("\n")]:
            if part:
                strengths.append(part)
    else:
        strengths = [
            "✨ נוח במיוחד לשימוש יומיומי",
            "🔧 איכות גבוהה ועמידות לאורך זמן",
            "🎨 מגיע במבחר גרסאות – בדקו בקישור!",
        ]

    price_line = f"💰 מחיר מבצע: <a href=\"{buy_link}\">{sale_price} ש\"ח</a>"
    if original_price:
        price_line += f" (מחיר מקורי: {original_price} ש\"ח)"

    lines = [
        opening,
        "",
        title,
        "",
        *strengths[:3],
        "",
        price_line,
        discount_text,
        rating_line,
        f"📦 {orders_text}",
        "🚚 משלוח חינם מעל 38 ש\"ח או 7.49 ש\"ח",
        "",
        coupon_text,
        "",
        f"להזמנה מהירה👈 <a href=\"{buy_link}\">לחצו כאן</a>",
        "",
        f"מספר פריט: {item_id}",
        "להצטרפות לערוץ לחצו כאן👈 <a href=\"https://t.me/+LlMY8B9soOdhNmZk\">קליק והצטרפתם</a>",
        "",
        "👇🛍הזמינו עכשיו🛍👇",
        f"<a href=\"{buy_link}\">לחיצה וזה בדרך </a>",
    ]
    post = "\n".join([l for l in lines if l is not None and l.strip() != ""])
    return post

def validate_media_fields(product):
    image_url = (product.get('ImageURL') or '').strip()
    video_url = (product.get('Video Url') or '').strip()
    buy_link = (product.get('BuyLink') or '').strip()
    if not buy_link:
        raise ValueError("BuyLink חסר בפריט")
    if not (image_url or video_url):
        raise ValueError("חסר ImageURL או Video Url לפריט")
    return image_url, video_url

def post_to_channel(product):
    try:
        post_text = build_post_text(product)
        image_url, video_url = validate_media_fields(product)

        # שליחה ישירה של ה-URL ל-Telegram (בלי להוריד לשרת)
        if video_url.endswith('.mp4'):
            bot.send_video(CURRENT_TARGET, video_url, caption=post_text)
        else:
            bot.send_photo(CURRENT_TARGET, image_url, caption=post_text)

    except Exception as e:
        ts = datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
        print(f"[{ts}] Failed to post item {product.get('ItemId', 'ללא מספר')} to {CURRENT_TARGET}: {e}")
        raise


# ========= ADMIN HELPERS =========
def user_is_admin(msg) -> bool:
    return (msg.from_user and (msg.from_user.id in ADMIN_USER_IDS))

def reload_pending_from_data():
    """מחדש את התור מהקובץ הראשי."""
    try:
        src = read_products(DATA_CSV)
        write_products(PENDING_CSV, src)
        return True, f"נטענו {len(src)} פריטים מחדש מ-{DATA_CSV}"
    except Exception as e:
        return False, f"שגיאה בטעינה מחדש: {e}"


# ========= ADMIN COMMANDS =========
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

@bot.message_handler(commands=['reset_pending', 'reload_pending'])
def reset_pending(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    ok, info = reload_pending_from_data()
    bot.reply_to(msg, ("✅ " if ok else "❌ ") + info)

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


# ========= TARGET SWITCH COMMANDS =========
@bot.message_handler(commands=['set_public'])
def cmd_set_public(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        m = bot.reply_to(msg, "שלח עכשיו את מזהה הערוץ הציבורי (למשל @name).")
        bot.register_next_step_handler(m, _set_public_from_reply)
        return
    _handle_set_public_value(msg, parts[1].strip())

def _set_public_from_reply(reply_msg):
    if not reply_msg or not reply_msg.text:
        bot.reply_to(reply_msg, "לא התקבל טקסט. נסה שוב: /set_public")
        return
    _handle_set_public_value(reply_msg, reply_msg.text.strip())

def _handle_set_public_value(msg, v):
    save_public_preset(v)
    bot.reply_to(msg, f"נשמר פריסט ציבורי: {v}")

@bot.message_handler(commands=['set_private'])
def cmd_set_private(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        m = bot.reply_to(msg, "שלח עכשיו את מזהה הערוץ הפרטי (לרוב מספר שלילי -100..., או @name אם מוגדר).")
        bot.register_next_step_handler(m, _set_private_from_reply)
        return
    _handle_set_private_value(msg, parts[1].strip())

def _set_private_from_reply(reply_msg):
    if not reply_msg or not reply_msg.text:
        bot.reply_to(reply_msg, "לא התקבל טקסט. נסה שוב: /set_private")
        return
    _handle_set_private_value(reply_msg, reply_msg.text.strip())

def _handle_set_private_value(msg, v):
    try:
        if isinstance(v, str) and v.startswith("-"):
            v = int(v)  # chat_id מספרי
    except ValueError:
        bot.reply_to(msg, "ערך לא חוקי לפריסט פרטי.")
        return
    save_private_preset(v)
    bot.reply_to(msg, f"נשמר פריסט פרטי: {v}")

@bot.message_handler(commands=['use_public'])
def cmd_use_public(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    v = load_public_preset()
    if v is None:
        bot.reply_to(msg, "לא שמור פריסט ציבורי. השתמש /set_public קודם.")
        return
    set_current_target(v)
    ok, details = _check_target_permissions(v)
    if ok:
        bot.reply_to(msg, f"עברתי לשדר ליעד הציבורי: {v} ✅\n{details}")
    else:
        bot.reply_to(msg, f"עודכן יעד ציבורי: {v}, אך יש בעיה בהרשאות/זיהוי היעד ⚠️\n{details}")

@bot.message_handler(commands=['use_private'])
def cmd_use_private(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    v = load_private_preset()
    if v is None:
        bot.reply_to(msg, "לא שמור פריסט פרטי. השתמש /set_private קודם.")
        return
    target = int(v) if (isinstance(v, str) and v.strip().startswith("-")) else v
    set_current_target(target)
    ok, details = _check_target_permissions(target)
    if ok:
        bot.reply_to(msg, f"עברתי לשדר ליעד הפרטי: {target} ✅\n{details}")
    else:
        bot.reply_to(msg, f"עודכן יעד פרטי: {target}, אך יש בעיה בהרשאות/זיהוי היעד ⚠️\n{details}")


# ========= /start without persistent keyboard =========
@bot.message_handler(commands=['start', 'help', 'menu'])
def cmd_start(msg):
    text = """ברוך הבא! מצב עבודה בשתי אפשרויות:
• מצב שינה פעיל (מתוזמן): שידור רק בשעות שהוגדרו.
• מצב שינה כבוי (תמיד-פעיל): הבוט משדר כל הזמן.

פקודות:
/schedule_on, /schedule_off, /schedule_status
/list_pending, /pending_status
/peek_next, /peek_idx N
/skip_one, /clear_pending, /reset_pending, /reload_pending
/set_public @name, /use_public
/set_private -100123..., /use_private

טיפ: הקלד '/' כדי לראות את כל הפקודות."""
    bot.send_message(msg.chat.id, text, reply_markup=types.ReplyKeyboardRemove())


# ========= SENDER LOOP (BACKGROUND) =========
def run_sender_loop():
    # אם אין דגל – ברירת מחדל: שינה פעיל (כיבוד שעות)
    if not os.path.exists(SCHEDULE_FLAG_FILE):
        set_schedule_enforced(True)

    init_pending()

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
        try:
            post_to_channel(product)
            # הסרה מהתור רק אם הצליח או לאחר לוג שגיאה
            write_products(PENDING_CSV, pending[1:])
        except Exception as e:
            ts = datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')
            print(f"[{ts}] Skipping problematic item {product.get('ItemId','ללא מספר')}: {e}")
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
