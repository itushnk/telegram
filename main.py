# -*- coding: utf-8 -*-
# ✅ גרסה מעודכנת עם תמיכה באימוג'ים, קישור Promotion Url בלבד, ולולאת זמן דינמית

import os
import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import telebot

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("PUBLIC_CHANNEL", "@your_channel")
IL_TZ = ZoneInfo("Asia/Jerusalem")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

AUTO_FLAG_FILE = "auto_delay.flag"

AUTO_SCHEDULE = [
    (dtime(6, 0), dtime(9, 0), 1200),
    (dtime(9, 0), dtime(15, 0), 1500),
    (dtime(15, 0), dtime(22, 0), 1200),
]

def read_auto_flag():
    try:
        with open(AUTO_FLAG_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except:
        return "on"

def write_auto_flag(value):
    with open(AUTO_FLAG_FILE, "w", encoding="utf-8") as f:
        f.write(value)

def get_auto_delay():
    now = datetime.now(IL_TZ).time()
    for start, end, delay in AUTO_SCHEDULE:
        if start <= now <= end:
            return delay
    return None  # מחוץ לשעות

@bot.message_handler(commands=['toggle_mode'])
def toggle_mode(msg):
    mode = read_auto_flag()
    new_mode = "off" if mode == "on" else "on"
    write_auto_flag(new_mode)
    bot.reply_to(msg, f"✅ מצב אוטומטי עודכן ל: {'פעיל 🟢' if new_mode == 'on' else 'כבוי 🔴'}")

def send_fake_post():
    bot.send_message(CHANNEL_ID, "🧪 פוסט דוגמה עם אימוג'ים 🎉📦✨")

def auto_post_loop():
    while True:
        if read_auto_flag() == "on":
            delay = get_auto_delay()
            if delay:
                send_fake_post()
                print(f"[{datetime.now()}] פורסם פוסט | המתנה {delay} שניות")
                time.sleep(delay)
            else:
                print(f"[{datetime.now()}] מחוץ לשעות שידור – שינה 60 שניות")
                time.sleep(60)
        else:
            print(f"[{datetime.now()}] מצב ידני מופעל – שינה 5 שניות")
            time.sleep(5)

if __name__ == "__main__":
    import threading
    print("🎯 בוט הופעל עם מצב אוטומטי דינמי")
    threading.Thread(target=auto_post_loop, daemon=True).start()
    bot.infinity_polling()
