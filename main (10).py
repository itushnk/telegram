
import csv
import requests
import time
import telebot
from datetime import datetime
from urllib.parse import quote

# Token and chat/channel setup
BOT_TOKEN = "8371104768:AAE8GYjVBeF0H4fqOur9tMLe4_D4laCBRsk"
CHANNEL_ID = "@LCv_Xuy6z9RjY2I0"  # Use @username format for bot API

bot = telebot.TeleBot(BOT_TOKEN)

def format_post(product):
    item_id = product['ItemId']
    image_url = product['ImageURL']
    title = product['Title']
    original_price = product['OriginalPrice']
    sale_price = product['SalePrice']
    discount = product['Discount']
    rating = product['Rating']
    orders = product['Orders']
    buy_link = product['BuyLink']
    coupon = product['CouponCode']

    rating_percent = f"{float(rating):.0f}%" if rating else "אין דירוג"
    orders_text = f"{orders} הזמנות" if int(orders) >= 50 else "פריט חדש לחברי הערוץ"
    discount_text = f"💸 חיסכון של {discount}!" if discount != "0%" else ""
    coupon_text = f"🎁 קופון לחברי הערוץ בלבד: {coupon}" if coupon.strip() else ""

    post = f"""🔥 כדאי שתראו את זה! 🔥

🛒 {title}

✨ נוח במיוחד לשימוש יומיומי
🔧 איכות גבוהה ועמידות לאורך זמן
🎨 מגיע במבחר גרסאות – בדקו בקישור!

💰 מחיר מבצע: [{sale_price} ש"ח]({buy_link}) (מחיר מקורי: {original_price} ש"ח)
{discount_text}
⭐ דירוג: {rating_percent}
📦 {orders_text}
🚚 משלוח חינם מעל 38 ש"ח או 7.49 ש"ח

{coupon_text}

להזמנה מהירה לחצו כאן👉 {buy_link}

מספר פריט: {item_id}
להצטרפות לערוץ לחצו עליי👉 https://t.me/+LlMY8B9soOdhNmZk

👇🛍הזמינו עכשיו🛍👇
{buy_link}
"""
    return post, image_url

def read_products(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def post_to_channel(product):
    try:
        post_text, image_url = format_post(product)
        response = requests.get(image_url)
        if image_url.endswith('.mp4'):
            bot.send_video(CHANNEL_ID, response.content, caption=post_text, parse_mode='Markdown')
        else:
            bot.send_photo(CHANNEL_ID, response.content, caption=post_text, parse_mode='Markdown')
    except Exception as e:
        print(f"Failed to post: {e}")

def run_bot():
    products = read_products("products.csv")
    for product in products:
        post_to_channel(product)
        time.sleep(1200)  # 20 minutes

if __name__ == "__main__":
    run_bot()
