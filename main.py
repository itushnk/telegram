
# קובץ ראשי של הבוט כולל תפריטים וניהול תור

from telebot import TeleBot, types

bot = TeleBot("YOUR_BOT_TOKEN")

POST_DELAY_SECONDS = 1200
CURRENT_TARGET = "ערוץ ראשי"

# פונקציה לבדיקה אם המשתמש אדמין
def _is_admin(message):
    return message.chat.type in ["private"] or message.from_user.id in [123456789]  # עדכן את ה-ID שלך כאן

# פונקציית עזר לעריכת הודעה
def safe_edit_message(bot, chat_id, message, new_text, reply_markup, cb_id):
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=message.message_id, text=new_text, reply_markup=reply_markup)
        bot.answer_callback_query(cb_id)
    except Exception as e:
        bot.answer_callback_query(cb_id, text="שגיאה בעריכה")

# תפריט ראשי
def inline_menu():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("📋 ניהול תור", callback_data="menu_queue"),
        types.InlineKeyboardButton("⏱️ ניהול מרווחים", callback_data="menu_delays")
    )
    kb.add(
        types.InlineKeyboardButton("🔁 ניהול קבצים", callback_data="menu_files"),
        types.InlineKeyboardButton("🎯 ניהול יעדים", callback_data="menu_targets")
    )
    kb.add(
        types.InlineKeyboardButton("⚙️ מצב אוטומטי (החלפה)", callback_data="toggle_auto_mode")
    )
    kb.add(
        types.InlineKeyboardButton(f"⏳ מרווח: ~{POST_DELAY_SECONDS//60} דק׳ | יעד: {CURRENT_TARGET}", callback_data="noop_info")
    )
    return kb

# תפריט ניהול תור
def menu_queue():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🔍 סטטוס תור", callback_data="queue_status"),
        types.InlineKeyboardButton("👀 הצצה לפוסט הבא", callback_data="peek_post"),
        types.InlineKeyboardButton("🔢 הצצה לפי אינדקס", callback_data="peek_by_index"),
        types.InlineKeyboardButton("⏭ דילוג", callback_data="skip_post"),
        types.InlineKeyboardButton("🔄 איפוס תור", callback_data="reset_queue"),
        types.InlineKeyboardButton("🗑️ ניקוי תור", callback_data="clear_queue")
    )
    kb.add(types.InlineKeyboardButton("🔙 חזרה", callback_data="main_menu"))
    return kb

# תפריט מרווחים
def menu_delays():
    kb = types.InlineKeyboardMarkup(row_width=3)
    for minutes in [1, 5, 10, 15, 20, 30]:
        kb.add(types.InlineKeyboardButton(f"{minutes} דק׳", callback_data=f"set_delay_{minutes}"))
    kb.add(types.InlineKeyboardButton("🔙 חזרה", callback_data="main_menu"))
    return kb

# תפריט ניהול קבצים
def menu_files():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("📥 העלאת CSV", callback_data="upload_csv"),
        types.InlineKeyboardButton("💱 המרת דולר", callback_data="convert_dollars"),
        types.InlineKeyboardButton("🔀 מיזוג קבצים", callback_data="merge_files"),
        types.InlineKeyboardButton("🗑️ מחיקת קובץ", callback_data="delete_file"),
        types.InlineKeyboardButton("🧹 ניקוי תור", callback_data="clear_queue")
    )
    kb.add(types.InlineKeyboardButton("🔙 חזרה", callback_data="main_menu"))
    return kb

# תפריט ניהול יעדים
def menu_targets():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🎯 בחירת יעד", callback_data="choose_target"),
        types.InlineKeyboardButton("❌ ביטול יעד", callback_data="clear_target")
    )
    kb.add(types.InlineKeyboardButton("🔙 חזרה", callback_data="main_menu"))
    return kb

# טיפול בלחיצות תפריט
@bot.callback_query_handler(func=lambda c: True)
def on_inline_click(c):
    global POST_DELAY_SECONDS, CURRENT_TARGET
    if not _is_admin(c.message):
        bot.answer_callback_query(c.id, "אין הרשאה.", show_alert=True)
        return

    data = c.data or ""
    chat_id = c.message.chat.id

    if data == "menu_queue":
        safe_edit_message(bot, chat_id, c.message, "בחר פעולה לניהול התור:", menu_queue(), c.id)
        return

    if data == "menu_delays":
        safe_edit_message(bot, chat_id, c.message, "בחר מרווח זמן בין פוסטים:", menu_delays(), c.id)
        return

    if data == "menu_files":
        safe_edit_message(bot, chat_id, c.message, "בחר פעולה לניהול הקובץ:", menu_files(), c.id)
        return

    if data == "menu_targets":
        safe_edit_message(bot, chat_id, c.message, "בחר פעולה לניהול היעדים:", menu_targets(), c.id)
        return

    if data == "main_menu":
        safe_edit_message(bot, chat_id, c.message, "בחר פעולה ראשית:", inline_menu(), c.id)
        return

# טיפול בפקודת התחלה
@bot.message_handler(commands=["start"])
def start_handler(msg):
    if not _is_admin(msg): return
    bot.send_message(msg.chat.id, "ברוך הבא! בחר פעולה:", reply_markup=inline_menu())

# התחלת הבוט
bot.polling(none_stop=True)
