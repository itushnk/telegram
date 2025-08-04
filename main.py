
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
DATA_CSV = "workfile.csv"            # קובץ המקור שאתה מכין
PENDING_CSV = "pending.csv"          # תור הפוסטים הממתינים

# מצב עבודה: 'מתוזמן' או 'תמיד-פעיל' באמצעות דגל קובץ
SCHEDULE_FLAG_FILE = "schedule_enforced.flag"  # קיים => מתוזמן (שינה פעיל), לא קיים => תמיד משדר

# מרווח בין פוסטים: ברירת מחדל + קובץ הגדרה שנשמר בין הפעלות
POST_DELAY_SECONDS = 60
DELAY_FILE = "post_delay_seconds.cfg"  # נשמר בו המרווח בפועל (שניות)

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})

# אזור זמן ישראל
IL_TZ = ZoneInfo("Asia/Jerusalem")


# ========= SINGLE INSTANCE LOCK =========
def acquire_single_instance_lock(lock_path: str = "bot.lock"):
    """מונע הרצה כפולה על אותה מכונה"""
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


# ========= DYNAMIC CHANNEL TARGET =========
# תמיכה בהחלפת ערוץ בזמן ריצה (פרטי/ציבורי) בלי לפרוס קוד מחדש
CHANNEL_FILE = "channel_id.cfg"  # נשמר בו היעד הנוכחי (@name או -100...)

def load_channel_id():
    """קורא את יעד השידור. קדימות: ENV CHANNEL_ID -> קובץ -> הקבוע בקוד."""
    try:
        env_val = os.getenv("CHANNEL_ID", "").strip()
    except Exception:
        env_val = ""
    if env_val:
        try:
            return int(env_val) if env_val.startswith("-") else env_val
        except Exception:
            return env_val
    try:
        if os.path.exists(CHANNEL_FILE):
            s = open(CHANNEL_FILE, "r", encoding="utf-8").read().strip()
            return int(s) if s.startswith("-") else s
    except Exception:
        pass
    return CHANNEL_ID

def save_channel_id(val):
    """שומר יעד שידור חדש (מחרוזת @name או מספר -100...)."""
    try:
        with open(CHANNEL_FILE, "w", encoding="utf-8") as f:
            f.write(str(val))
    except Exception as e:
        print(f"[WARN] Failed to persist channel id: {e}")

# פריסטים ציבורי/פרטי
PUBLIC_PRESET_FILE = "public_target.cfg"
PRIVATE_PRESET_FILE = "private_target.cfg"

def save_public_preset(val):
    try:
        with open(PUBLIC_PRESET_FILE, "w", encoding="utf-8") as f:
            f.write(str(val))
    except Exception as e:
        print(f"[WARN] Failed to save public preset: {e}")

def save_private_preset(val):
    try:
        with open(PRIVATE_PRESET_FILE, "w", encoding="utf-8") as f:
            f.write(str(val))
    except Exception as e:
        print(f"[WARN] Failed to save private preset: {e}")

def load_public_preset():
    try:
        if os.path.exists(PUBLIC_PRESET_FILE):
            s = open(PUBLIC_PRESET_FILE, "r", encoding="utf-8").read().strip()
            return int(s) if s.startswith("-") else s
    except Exception:
        pass
    env_val = os.getenv("PUBLIC_CHANNEL_ID", "").strip() if os.getenv("PUBLIC_CHANNEL_ID") else ""
    if env_val:
        try:
            return int(env_val) if env_val.startswith("-") else env_val
        except Exception:
            return env_val
    return None

def load_private_preset():
    try:
        if os.path.exists(PRIVATE_PRESET_FILE):
            s = open(PRIVATE_PRESET_FILE, "r", encoding="utf-8").read().strip()
            return int(s) if s.startswith("-") else s
    except Exception:
        pass
    env_val = os.getenv("PRIVATE_CHANNEL_ID", "").strip() if os.getenv("PRIVATE_CHANNEL_ID") else ""
    if env_val:
        try:
            return int(env_val) if env_val.startswith("-") else env_val
        except Exception:
            return env_val
    return None


# ========= DELAY PERSISTENCE =========
def get_post_delay() -> int:
    """קורא מרווח משמירת קובץ, אם קיים. אחרת משתמש ב-POST_DELAY_SECONDS."""
    try:
        if os.path.exists(DELAY_FILE):
            with open(DELAY_FILE, "r", encoding="utf-8") as f:
                v = int(f.read().strip())
                return max(5, v)  # סף מינימום סביר
    except Exception:
        pass
    return POST_DELAY_SECONDS

def set_post_delay(seconds: int) -> None:
    seconds = max(5, int(seconds))
    try:
        with open(DELAY_FILE, "w", encoding="utf-8") as f:
            f.write(str(seconds))
    except Exception as e:
        print(f"[WARN] Failed to persist delay: {e}")


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
    """אם שינה פעיל — נכבד חלונות זמן; אם שינה כבוי — תמיד משדרים."""
    if is_schedule_enforced():
        return not should_broadcast(now)
    return False


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
        target = load_channel_id()

        if video_url.endswith('.mp4'):
            resp = SESSION.get(video_url, timeout=20)
            resp.raise_for_status()
            bot.send_video(target, resp.content, caption=post_text)
        else:
            resp = SESSION.get(image_url, timeout=20)
            resp.raise_for_status()
            bot.send_photo(target, resp.content, caption=post_text)
    except Exception as e:
        print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] Failed to post: {e}")




# ========= Permissions / Debug helpers =========
def _check_target_permissions(target):
    """Return (ok: bool, details: str, status: str|None). Tries getChat, getChatMember."""
    try:
        # getChat
        try:
            chat = bot.get_chat(target)
            chat_info = f"getChat: OK | title={getattr(chat,'title',None)} | type={getattr(chat,'type',None)} | id={getattr(chat,'id',None)}"
        except Exception as e:
            return False, f"getChat: ERROR -> {e}", None

        # getChatMember for this bot
        try:
            bot_id = bot.get_me().id
            member = bot.get_chat_member(target, bot_id)
            status = getattr(member, "status", None)
            can_post = getattr(member, "can_post_messages", None)
            can_edit = getattr(member, "can_edit_messages", None)
            can_invite = getattr(member, "can_invite_users", None)
            details = f"{chat_info}\ngetChatMember: status={status} | can_post={can_post} | can_edit={can_edit} | can_invite={can_invite}"
            # In channels, to post the bot must be 'administrator' (or 'creator'), and can_post_messages True (some libs omit it => assume True if admin)
            ok = (status in ("administrator","creator")) and (can_post in (True, None))
            return ok, details, status
        except Exception as e:
            return False, chat_info + f"\ngetChatMember: ERROR -> {e}", None
    except Exception as e:
        return False, f"check failed: {e}", None



# ========= Quick toggle between presets =========
def _resolve_preset_name(value):
    return "פרטי" if isinstance(value, int) else "ציבורי"

def _pick_other_preset(cur):
    pub = load_public_preset()
    prv = load_private_preset()
    # אם אחד מהם לא מוגדר – אי אפשר לעשות toggle
    if pub is None or prv is None:
        return None, None, pub, prv
    # אם היעד הנוכחי זהה לפריסט פרטי – נעבור לציבורי
    if (isinstance(cur, int) and isinstance(prv, int) and cur == prv):
        return pub, "ציבורי", pub, prv
    # אם היעד הנוכחי זהה לפריסט ציבורי – נעבור לפרטי
    if (not isinstance(cur, int) and not isinstance(pub, int) and cur == pub):
        return prv, "פרטי", pub, prv
    # אם הנוכחי לא תואם לאף פריסט – כברירת מחדל נעבור לפרטי
    return prv, "פרטי", pub, prv

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
    total_seconds = (count - 1) * get_post_delay()
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
        f"(מרווח בין פוסטים: {get_post_delay()//60} דקות)"
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


# ========= Delay commands =========
@bot.message_handler(commands=['set_delay'])
def cmd_set_delay(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split()
    if len(parts) < 2:
        bot.reply_to(msg, "שימוש: /set_delay N  (בדקות). לדוגמה: /set_delay 20")
        return
    try:
        minutes = int(parts[1])
        if minutes < 1 or minutes > 180:
            bot.reply_to(msg, "מרווח צריך להיות בין 1 ל-180 דקות.")
            return
        set_post_delay(minutes * 60)
        bot.reply_to(msg, f"עודכן מרווח בין פוסטים ל-{minutes} דקות.")
    except ValueError:
        bot.reply_to(msg, "ערך לא חוקי. השתמש במספר שלם בדקות.")

@bot.message_handler(commands=['set_delay_10m'])
def cmd_set_delay_10m(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_post_delay(10 * 60)
    bot.reply_to(msg, "עודכן מרווח בין פוסטים ל-10 דקות.")

@bot.message_handler(commands=['set_delay_20m'])
def cmd_set_delay_20m(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_post_delay(20 * 60)
    bot.reply_to(msg, "עודכן מרווח בין פוסטים ל-20 דקות.")

@bot.message_handler(commands=['set_delay_25m'])
def cmd_set_delay_25m(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_post_delay(25 * 60)
    bot.reply_to(msg, "עודכן מרווח בין פוסטים ל-25 דקות.")

@bot.message_handler(commands=['set_delay_30m'])
def cmd_set_delay_30m(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    set_post_delay(30 * 60)
    bot.reply_to(msg, "עודכן מרווח בין פוסטים ל-30 דקות.")


# ========= Channel target commands =========
@bot.message_handler(commands=['set_channel_id'])
def cmd_set_channel_id(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(msg, "שימוש: /set_channel_id @name או -100xxxxxxxxxxxx")
        return
    new_id = parts[1].strip()
    try:
        if new_id.startswith("-"):
            new_id = int(new_id)
    except ValueError:
        bot.reply_to(msg, "chat_id לא חוקי.")
        return
    save_channel_id(new_id)
    ok, details, _ = _check_target_permissions(new_id)
    if ok:
        bot.reply_to(msg, f"עודכן יעד השידור ל־{new_id} ✅\n{details}")
    else:
        bot.reply_to(msg, f"עודכן יעד השידור ל־{new_id}, אך יש בעיה בהרשאות/זיהוי היעד ⚠️\n{details}\nודא שהבוט Admin ושהמספר נכון (לפרטי עדיף -100…).")

@bot.message_handler(commands=['channel_status'])
def cmd_channel_status(msg):
    cur = load_channel_id()
    typ = "פרטי (-100…)" if isinstance(cur, int) else "ציבורי (@name)"
    bot.reply_to(msg, f"Channel target: {cur} ({typ})")

# ========= Public/Private preset commands (improved prompts) =========
@bot.message_handler(commands=['set_public'])
def cmd_set_public(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        m = bot.reply_to(msg, "שלח עכשיו את שם הערוץ הציבורי (@name) או מזהה -100…")
        bot.register_next_step_handler(m, _set_public_from_reply)
        return
    _handle_set_public_value(msg, parts[1].strip())

def _set_public_from_reply(reply_msg):
    if not reply_msg or not reply_msg.text:
        bot.reply_to(reply_msg, "לא התקבל טקסט. נסה שוב: /set_public")
        return
    _handle_set_public_value(reply_msg, reply_msg.text.strip())

def _handle_set_public_value(msg, v):
    try:
        if v.startswith("-"):
            v = int(v)
    except ValueError:
        bot.reply_to(msg, "ערך לא חוקי לפריסט ציבורי.")
        return
    save_public_preset(v)
    bot.reply_to(msg, f"נשמר פריסט ציבורי: {v}")

@bot.message_handler(commands=['set_private'])
def cmd_set_private(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        m = bot.reply_to(msg, "שלח עכשיו את מזהה הערוץ הפרטי (-100… ) או @name (לפרטי מומלץ -100…).")
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
        if v.startswith("-"):
            v = int(v)
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
        bot.reply_to(msg, "לא הוגדר פריסט ציבורי. השתמש ב-/set_public קודם.")
        return
    save_channel_id(v)
    ok, details, _ = _check_target_permissions(v)
    if ok:
        bot.reply_to(msg, f"עברתי לשידור ליעד הציבורי: {v} ✅\n{details}")
    else:
        bot.reply_to(msg, f"עודכן יעד ציבורי: {v}, אך יש בעיה בהרשאות/זיהוי היעד ⚠️\n{details}")

@bot.message_handler(commands=['use_private'])
def cmd_use_private(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    v = load_private_preset()
    if v is None:
        bot.reply_to(msg, "לא הוגדר פריסט פרטי. השתמש ב-/set_private קודם.")
        return
    save_channel_id(v)
    ok, details, _ = _check_target_permissions(v)
    if ok:
        bot.reply_to(msg, f"עברתי לשידור ליעד הפרטי: {v} ✅\n{details}")
    else:
        bot.reply_to(msg, f"עודכן יעד פרטי: {v}, אך יש בעיה בהרשאות/זיהוי היעד ⚠️\n{details}\nודא שהבוט Admin ושהמספר נכון (לפרטי עדיף -100…).")


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




@bot.message_handler(commands=['debug_check_target'])
def cmd_debug_check_target(msg):
    try:
        target = load_channel_id()
        ok, details, status = _check_target_permissions(target)
        prefix = "OK ✅" if ok else "בעיה ⚠️"
        bot.reply_to(msg, f"{prefix}\nTarget: {target} ({'INT' if isinstance(target,int) else 'STR'})\n{details}")
    except Exception as e:
        bot.reply_to(msg, f"debug_check_target failed: {e}")

@bot.message_handler(commands=['debug_send'])
def cmd_debug_send(msg):
    try:
        target = load_channel_id()
        ts = datetime.now(tz=IL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        bot.send_message(target, f"DEBUG PING {ts}")
        bot.reply_to(msg, "ניסיתי לשלוח הודעת טקסט פשוטה ליעד. בדוק אם הופיע בערוץ.")
    except Exception as e:
        bot.reply_to(msg, f"debug_send error: {e}")



@bot.message_handler(commands=['switch_target'])
def cmd_switch_target(msg):
    if not user_is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    cur = load_channel_id()
    nxt, nxt_name, pub, prv = _pick_other_preset(cur)
    if pub is None or prv is None:
        bot.reply_to(msg, "חסר פריסט אחד לפחות. קבע שניהם פעם אחת:\n/set_public @PublicName\n/set_private -100XXXXXXXXXXXX")
        return
    save_channel_id(nxt)
    ok, details, _ = _check_target_permissions(nxt)
    if ok:
        bot.reply_to(msg, f"החלפתי יעד ➜ {nxt_name}: {nxt} ✅\n{details}")
    else:
        bot.reply_to(msg, f"החלפתי יעד ➜ {nxt_name}: {nxt}, אך יש בעיה בהרשאות/זיהוי ⚠️\n{details}")

# ========= /start menu =========
@bot.message_handler(commands=['start', 'help', 'menu'])
def cmd_start(msg):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row('/list_pending', '/pending_status')
    kb.row('/peek_next', '/peek_idx')
    kb.row('/skip_one', '/clear_pending')
    kb.row('/reset_pending', '/force_send_next')
    kb.row('/set_delay_10m', '/set_delay_20m')
    kb.row('/set_delay_25m', '/set_delay_30m')
    kb.row('/schedule_status')
    kb.row('/schedule_on', '/schedule_off')
    kb.row('/channel_status', '/set_channel_id')
    kb.row('/use_public', '/use_private')
    kb.row('/set_public', '/set_private')
    kb.row('/switch_target')
    kb.row('/debug_check_target', '/debug_send')

    text = f"""ברוך הבא! פקודות שימושיות:

מצב עבודה:
• /schedule_on – הפעלת מצב שינה פעיל (כיבוד שעות)
• /schedule_off – ביטול מצב שינה (שידור תמיד)
• /schedule_status – מצב נוכחי

זמני המתנה:
• /set_delay N – להגדיר מרווח בדקות (למשל: /set_delay 20)
• /set_delay_10m / _20m / _25m / _30m – קיצורי דרך

יעד שידור:
• /channel_status – יעד השידור הנוכחי
• /set_channel_id @name או -100xxxxxxxxxxxx – עדכון יעד השידור
• /use_public / /use_private – מעבר מהיר לפריסטים
• /set_public @name או -100xxxxxxxxxxxx – שמירת פריסט ציבורי
• /set_private @name או -100xxxxxxxxxxxx – שמירת פריסט פרטי

ניהול תור:
• /list_pending – פוסטים ממתינים
• /pending_status – סטטוס ו-ETA (מרווח: {get_post_delay()//60} דקות)
• /peek_next – הפריט הבא
• /peek_idx N – פריט לפי אינדקס
• /skip_one – דילוג על הבא
• /clear_pending – ניקוי התור
• /reset_pending – טעינה מחדש מהקובץ
• /force_send_next – שליחה כפויה (עוקף שקט)
• /switch_target – החלפה מהירה בין פרטי ↔ ציבורי
• /debug_check_target – בדיקת יעד והרשאות
• /debug_send – שליחת טקסט בדיקה לערוץ היעד
"""
    bot.send_message(msg.chat.id, text, reply_markup=kb, disable_web_page_preview=True)


# ========= SENDER LOOP (BACKGROUND) =========
def run_sender_loop():
    init_pending()
    # ברירת מחדל: אם אין דגל, הפעל מתוזמן
    if not os.path.exists(SCHEDULE_FLAG_FILE):
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
        time.sleep(get_post_delay())


# ========= MAIN =========
def print_current_channel_target():
    try:
        cur = load_channel_id()
        kind = "INT" if isinstance(cur, int) else "STR"
        print(f"[BOOT] Channel target: {cur} (type={kind})")
    except Exception as e:
        print(f"[BOOT] Channel target check failed: {e}")

if __name__ == "__main__":
    _lock_handle = acquire_single_instance_lock()

    # אבחון webhook לפני ואחרי מחיקה
    print_webhook_info()
    print_current_channel_target()
    try:
        force_delete_webhook()
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        try:
            bot.remove_webhook()
        except Exception as e2:
            print(f"[WARN] remove_webhook failed: {e2}")
    print_webhook_info()

    # חוט רקע לשידור
    t = threading.Thread(target=run_sender_loop, daemon=True)
    t.start()

    # polling עם retry
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"[{datetime.now(tz=IL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}] Polling error: {e}. Retrying in 5s...")
            time.sleep(5)
