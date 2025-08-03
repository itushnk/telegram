import csv
import requests
import time
import telebot
import threading
import os
from datetime import datetime, timedelta
# -*- coding: utf-8 -*-

# ========= CONFIG =========
BOT_TOKEN = "8371104768:AAHi2lv7CFNFAWycjWeUSJiOn9YR0Qvep_4"
CHANNEL_ID = "@nisayon121"  # עדכן לערוץ שלך

# קבצים
DATA_CSV = "workfile.csv"     # קובץ המקור שאתה מכין
PENDING_CSV = "pending.csv"   # תור הפוסטים הממתינים

# מרווח בין פוסטים בשניות
POST_DELAY_SECONDS = 60

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})


# ========= UTILITIES =========
def safe_int(value, default=0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default

def norm_percent(value, decimals=1, empty_fallback=""):
    """
    קולט '91.9', '91.9%', '92' ומחזיר '91.9%' בפורמט קבוע.
    """
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
    """
    מנקה ILS/₪ ותווים לא-ספרתיים, משאיר מספר עם נקודה.
    """
    if s is None:
        return ""
    s = str(s)
    for junk in ["ILS", "₪"]:
        s = s.replace(junk, "")
    out = "".join(ch for ch in s if ch.isdigit() or ch == ".")
    return out.strip()

def normalize_row_keys(row):
    """
    מיישר שמות עמודות נפוצים לשמות הקבועים שהקוד משתמש בהם.
    לא מוחק שדות קיימים—רק משלים אם חסרים.
    """
    out = dict(row)

    # תמונה / וידאו
    if "ImageURL" not in out:
        out["ImageURL"] = out.get("Image Url", "") or out.get("ImageURL", "")
    if "Video Url" not in out:
        out["Video Url"] = out.get("Video Url", "")

    # קישורי רכישה
    if "BuyLink" not in out:
        out["BuyLink"] = out.get("Promotion Url", "") or out.get("BuyLink", "")

    # מחירים
    out["OriginalPrice"] = clean_price_text(out.get("OriginalPrice", "") or out.get("Origin Price", ""))
    out["SalePrice"]     = clean_price_text(out.get("SalePrice", "") or out.get("Discount Price", ""))

    # הנחה / דירוג / הזמנות
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

    # קופון
    if "CouponCode" not in out:
        out["CouponCode"] = out.get("Code Name", "") or out.get("CouponCode", "")

    # מזהה / טקסטים
    if "ItemId" not in out:
        out["ItemId"] = out.get("ProductId", "") or out.get("ItemId", "") or "ללא מספר"
    if "Opening" not in out:
        out["Opening"] = out.get("Opening", "") or ""
    if "Title" not in out:
        out["Title"] = out.get("Title", "") or out.get("Product Desc", "") or ""

    # Strengths אופציונלי — שומר אם קיים, לא חובה לשידור
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
    """
    כותב רשומות ל-CSV. אם הרשימה ריקה, כותב רק כותרות.
    """
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
    """
    אם אין pending.csv – ניצור אותו מתוך workfile.csv.
    """
    if not os.path.exists(PENDING_CSV):
        src = read_products(DATA_CSV)
        write_products(PENDING_CSV, src)


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

    post = f'''{opening}

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
'''
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
        print(f"[{datetime.now()}] Failed to post: {e}")


# ========= ADMIN COMMANDS =========
def format_full_product_text(p):
    fields = [
        ("ItemId", p.get("ItemId", "")),
        ("ImageURL", p.get("ImageURL", "")),
        ("Title", p.get("Title", "")),
        ("OriginalPrice", p.get("OriginalPrice", "")),
        ("SalePrice", p.get("SalePrice", "")),
        ("Discount", p.get("Discount", "")),
        ("Rating", p.get("Rating", "")),
        ("Orders", p.get("Orders", "")),
        ("BuyLink", p.get("BuyLink", "")),
        ("CouponCode", p.get("CouponCode", "")),
        ("Opening", p.get("Opening", "")),
        ("Video Url", p.get("Video Url", "")),
        ("Strengths", p.get("Strengths", "")),
    ]
    lines = [f"<b>{k}:</b> {v if v is not None else ''}" for k, v in fields]
    return "\n".join(lines)

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
    write_products(PENDING_CSV, [])
    bot.reply_to(msg, "נוקה התור של הפוסטים הממתינים 🧹")

@bot.message_handler(commands=['reset_pending'])
def reset_pending(msg):
    src = read_products(DATA_CSV)
    write_products(PENDING_CSV, src)
    bot.reply_to(msg, "התור אופס מהקובץ הראשי והכול נטען מחדש 🔄")

@bot.message_handler(commands=['skip_one'])
def skip_one(msg):
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
    txt = "<b>הפריט הבא בתור:</b>\n\n" + format_full_product_text(nxt)
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
    item = pending[idx-1]  # 1-based
    txt = f"<b>פריט #{idx} בתור:</b>\n\n" + format_full_product_text(item)
    bot.reply_to(msg, txt, parse_mode='HTML')

@bot.message_handler(commands=['pending_status'])
def pending_status(msg):
    pending = read_products(PENDING_CSV)
    count = len(pending)
    if count == 0:
        bot.reply_to(msg, "אין פוסטים ממתינים ✅")
        return

    now = datetime.now()
    total_seconds = (count - 1) * POST_DELAY_SECONDS  # האחרון אחרי (count-1) מרווחים
    eta = now + timedelta(seconds=total_seconds)
    eta_str = eta.strftime("%Y-%m-%d %H:%M:%S")
    next_eta = now.strftime("%Y-%m-%d %H:%M:%S")

    msg_text = (
        f"יש כרגע <b>{count}</b> פוסטים ממתינים.\n"
        f"⏱️ השידור הבא: <b>{next_eta}</b>\n"
        f"🕒 שעת השידור המשוערת של האחרון: <b>{eta_str}</b>\n"
        f"(מרווח בין פוסטים: {POST_DELAY_SECONDS} שניות)"
    )
    bot.reply_to(msg, msg_text, parse_mode='HTML')


# ========= SENDER LOOP (BACKGROUND) =========
def run_sender_loop():
    init_pending()
    while True:
        pending = read_products(PENDING_CSV)
        if not pending:
            print(f"[{datetime.now()}] No pending posts.")
            time.sleep(30)
            continue

        product = pending[0]
        post_to_channel(product)
        write_products(PENDING_CSV, pending[1:])  # הסר את הראשון
        time.sleep(POST_DELAY_SECONDS)


# ========= MAIN =========
if __name__ == "__main__":
    # 0) ננקה Webhook כדי למנוע 409 בעת polling
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        try:
            bot.remove_webhook()
        except Exception as e2:
            print(f"[WARN] remove_webhook failed: {e2}")

    # 1) חוט רקע ששולח פוסטים מהתור
    t = threading.Thread(target=run_sender_loop, daemon=True)
    t.start()

    # 2) polling עם retry כדי להתמודד עם שגיאות זמניות (כולל 409)
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"[{datetime.now()}] Polling error: {e}. Retrying in 5s...")
            time.sleep(5)
