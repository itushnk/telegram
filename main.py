
import csv
import requests
import time
import telebot
from datetime import datetime

# Token and chat/channel setup
BOT_TOKEN = "8371104768:AAE8GYjVBeF0H4fqOur9tMLe4_D4laCBRsk"
CHANNEL_ID = "@nisayon121"

bot = telebot.TeleBot(BOT_TOKEN)

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
    orders_text = f"{orders} הזמנות" if orders and int(orders) >= 50 else "פריט חדש לחברי הערוץ"
    discount_text = f"💸 חיסכון של {discount}!" if discount != "0%" else ""
    coupon_text = f"🎁 קופון לחברי הערוץ בלבד: {coupon}" if coupon.strip() else ""

    post = '''{opening}

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

להזמנה מהירה לחצו כאן👉 <a href="{buy_link}">לחצו כאן</a>

מספר פריט: {item_id}
להצטרפות לערוץ לחצו עליי👉 <a href="https://t.me/+LlMY8B9soOdhNmZk">לחצו כאן</a>

👇🛍הזמינו עכשיו🛍👇
<a href="{buy_link}">לחצו כאן</a>
'''.format(
        opening=opening,
        title=title,
        sale_price=sale_price,
        buy_link=buy_link,
        original_price=original_price,
        discount_text=discount_text,
        rating_percent=rating_percent,
        orders_text=orders_text,
        coupon_text=coupon_text,
        item_id=item_id
    )

    return post, image_url

def read_products(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def post_to_channel(product):
    try:
        post_text, image_url = format_post(product)
        video_url = product.get('Video Url', '').strip()
        if video_url.endswith('.mp4'):
            response = requests.get(video_url)
            bot.send_video(CHANNEL_ID, response.content, caption=post_text, parse_mode='HTML')
        else:
            response = requests.get(image_url)
            bot.send_photo(CHANNEL_ID, response.content, caption=post_text, parse_mode='HTML')
    except Exception as e:
        print(f"Failed to post: {e}")

def run_bot():
    products = read_products("post_for_video_test.csv")
    for product in products:
        post_to_channel(product)
        time.sleep(60)

if __name__ == "__main__":
    run_bot()
