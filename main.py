# -*- coding: utf-8 -*-
import csv
import requests
import time
import telebot
import threading
import os
from datetime import datetime, timedelta

# ========= CONFIG =========
BOT_TOKEN = "PASTE_YOUR_TOKEN_HERE"     # ← החלף לטוקן שלך
CHANNEL_ID = "@nisayon121"              # ← ערוץ היעד

DATA_CSV = "workfile.csv"               # קובץ המקור
PENDING_CSV = "pending.csv"             # תור הפוסטים הממתינים
UPLOAD_MODE_FILE = "upload_mode.txt"    # שמירת מצב העלאה (replace/append/defer)
DEFAULT_UPLOAD_MODE = "replace"         # ברירת מחדל

POST_DELAY_SECONDS = 60                 # מרווח בין פוסטים

# מזהי משתמשים שמורשים לפקודות ניהול/העלאה
ADMIN_IDS = {123456789}                 # ← החלף ל-user_id שלך (אפשר כמה: {111,222})

# ========= INIT =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TelegramPostBot/1.0"})


# ========= HELPERS =========
def is_admin(msg):
    try:
        return msg.from_user and msg.from_user.id in ADMIN_IDS
    except Exception:
        return False

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

    # קישור רכישה
    if "BuyLink" not in out:
        out["BuyLink"] = out.get("Promotion Url", "") or out.get("BuyLink", "")

    # מחירים
    out["OriginalPrice"] = clean_price_text(out.get("OriginalPrice", "") or out.get("Origin Price", ""))
    out["SalePrice"]     = clean_price_text(out.get("SalePrice", "") or out.get("Discount Price", ""))

    # הנחה
    disc = f"{out.get('Discount', '')}".strip()
    if disc and not disc.endswith("%"):
        try:
            disc = f"{int(round(float(disc)))}%"
        except Exception:
            pass
    out["Discount"] = disc

    # דירוג
    out["Rating"] = norm_percent(out.get("Rating", "") or out.get("Positive Feedback", ""), decimals=1, empty_fallback="")

    # הזמנות
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

    # אופציונלי
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

def get_upload_mode():
    if os.path.exists(UPLOAD_MODE_FILE):
        try:
            with open(UPLOAD_MODE_FILE, "r", encoding="utf-8") as f:
                m = f.read().strip().lower()
                if m in {"replace","append","defer"}:
                    return m
        except:
            pass
    return DEFAULT_UPLOAD_MODE

def set_upload_mode(mode):
    with open(UPLOAD_MODE_FILE, "w", encoding="utf-8") as f:
        f.write(mode)


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

@bot.message_handler(commands=['upload_mode'])
def upload_mode_cmd(msg):
    if not is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    parts = (msg.text or "").split()
    if len(parts) == 1:
        bot.reply_to(msg, f"מצב העלאה נוכחי: {get_upload_mode()} (אפשרויות: replace / append / defer)")
        return
    mode = parts[1].lower()
    if mode not in {"replace","append","defer"}:
        bot.reply_to(msg, "שימוש: /upload_mode replace | append | defer")
        return
    set_upload_mode(mode)
    bot.reply_to(msg, f"עודכן מצב העלאה ל: {mode}")

@bot.message_handler(commands=['upload_help'])
def upload_help(msg):
    if not is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    bot.reply_to(msg,
        "שלח/י קובץ CSV (כ-document), והבוט ישמור כ-workfile.csv לפי מצב העלאה:\n"
        "- replace: מחליף ומאפס תור\n- append: מוסיף לסוף התור\n- defer: שומר כ-incoming.csv וייכנס אחרי שהתור הקיים יסתיים.\n"
        "פקודה: /upload_mode להצגת/שינוי מצב."
    )

@bot.message_handler(content_types=['document'])
def handle_document(msg):
    if not is_admin(msg):
        bot.reply_to(msg, "אין הרשאה להעלות קבצים.")
        return
    doc = msg.document
    filename = (doc.file_name or "").lower()
    if not filename.endswith(".csv"):
        bot.reply_to(msg, "נתמך כרגע רק CSV. אנא שלח/י קובץ .csv.")
        return
    try:
        file_info = bot.get_file(doc.file_id)
        file_bytes = bot.download_file(file_info.file_path)
        tmp_name = "workfile_tmp.csv"
        with open(tmp_name, "wb") as f:
            f.write(file_bytes)

        mode = get_upload_mode()

        if mode == "replace":
            if os.path.exists(DATA_CSV):
                os.replace(DATA_CSV, "workfile_backup.csv")
            os.replace(tmp_name, DATA_CSV)
            src = read_products(DATA_CSV)
            write_products(PENDING_CSV, src)
            bot.reply_to(msg, f"✅ הוחלף הקובץ (replace) ואופס התור ({len(src)} פריטים).")

        elif mode == "append":
            new_rows = read_products(tmp_name)
            os.remove(tmp_name)
            base = read_products(DATA_CSV) if os.path.exists(DATA_CSV) else []
            merged_work = base + new_rows
            write_products(DATA_CSV, merged_work)
            pend = read_products(PENDING_CSV)
            merged_pending = pend + new_rows
            write_products(PENDING_CSV, merged_pending)
            bot.reply_to(msg, f"➕ נוסף לסוף התור (append). עכשיו ממתינים: {len(merged_pending)}.")

        elif mode == "defer":
            os.replace(tmp_name, "incoming.csv")
            bot.reply_to(msg, "⏸️ נשמר כ-incoming.csv (defer). ייטען אוטומטית כשהתור הנוכחי יסתיים.")

        else:
            bot.reply_to(msg, f"מצב לא מוכר: {mode}")

    except Exception as e:
        bot.reply_to(msg, f"❌ כשל בהעלאה/שמירה: {e}")

@bot.message_handler(commands=['export_workfile'])
def export_workfile(msg):
    if not is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    if not os.path.exists(DATA_CSV):
        bot.reply_to(msg, "לא קיים workfile.csv על השרת.")
        return
    with open(DATA_CSV, "rb") as f:
        bot.send_document(msg.chat.id, f, visible_file_name="workfile.csv",
                          caption="הנה workfile.csv הנוכחי")

@bot.message_handler(commands=['export_pending'])
def export_pending(msg):
    if not is_admin(msg):
        bot.reply_to(msg, "אין הרשאה.")
        return
    if not os.path.exists(PENDING_CSV):
        bot.reply_to(msg, "לא קיים pending.csv על השרת.")
        return
    with open(PENDING_CSV, "rb") as f:
        bot.send_document(msg.chat.id, f, visible_file_name="pending.csv",
                          caption="הנה pending.csv הנוכחי")

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
    item = pending[idx-1]
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
    total_seconds = (count - 1) * POST_DELAY_SECONDS
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


# ========= SENDER LOOP =========
def run_sender_loop():
    init_pending()
    while True:
        pending = read_products(PENDING_CSV)
        if not pending:
            # מצב defer: אם יש incoming.csv — נטען אותו אוטומטית לתור
            if get_upload_mode() == "defer" and os.path.exists("incoming.csv"):
                incoming_rows = read_products("incoming.csv")
                if incoming_rows:
                    base = read_products(DATA_CSV) if os.path.exists(DATA_CSV) else []
                    merged_work = base + incoming_rows
                    write_products(DATA_CSV, merged_work)
                    write_products(PENDING_CSV, incoming_rows)
                    os.remove("incoming.csv")
                    print(f"[{datetime.now()}] Loaded deferred incoming.csv into pending ({len(incoming_rows)} rows).")
                else:
                    os.remove("incoming.csv")
            else:
                print(f"[{datetime.now()}] No pending posts.")
                time.sleep(30)
                continue

        product = pending[0]
        post_to_channel(product)
        write_products(PENDING_CSV, pending[1:])
        time.sleep(POST_DELAY_SECONDS)


# ========= MAIN =========
if __name__ == "__main__":
    # מניעת 409: ננקה Webhook לפני polling
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        try:
            bot.remove_webhook()
        except Exception as e2:
            print(f"[WARN] remove_webhook failed: {e2}")

    # חוט רקע ששולח פוסטים מהתור
    t = threading.Thread(target=run_sender_loop, daemon=True)
    t.start()

    # Polling עם Retry לטיפול בשגיאות זמניות (כולל 409)
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"[{datetime.now()}] Polling error: {e}. Retrying in 5s...")
            time.sleep(5)
