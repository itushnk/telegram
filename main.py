import csv
import asyncio
import aiohttp
from aiogram import Bot, types
from aiogram.utils import exceptions

API_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'
CHANNEL_ID = 'YOUR_CHANNEL_ID'
CSV_FILE_PATH = 'products.csv'

bot = Bot(token=API_TOKEN, parse_mode=types.ParseMode.MARKDOWN)

async def send_post(row):
    try:
        item_id = row['ItemId']
        image_url = row['ImageUrl']
        product_name = row['Title']
        original_price = row['OriginalPrice']
        sale_price = row['SalePrice']
        discount_percent = row['DiscountPercent']
        shipping_price = row['Shipping']
        orders = row['Orders']
        rating = row['Rating']
        buy_link = row['BuyLink']
        coupon_code = row.get('CouponCode', '').strip()
        video_url = row.get('VideoUrl', '').strip()

        post_lines = []

post_lines.append(f"")
        price_line = f"מחיר מבצע: [{sale_price} ש"ח]({buy_link}) (מחיר מקורי: {original_price} ש"ח)"
        post_lines.append(price_line)
post_lines.append(f"")
post_lines.append(f"")
post_lines.append(f"")

        if coupon_code:
post_lines.append(f"")

        if video_url:
post_lines.append(f"")

        post_lines.append(f"
להזמנה מהירה לחצו כאן👉 [{sale_price} ש"ח]({buy_link})")

        post_lines.append(f"
מספר פריט: {item_id}")
        post_lines.append("להצטרפות לערוץ לחצו עליי👉 https://t.me/+LCv-Xuy6z9RjY2I0")

        caption = '\n'.join(post_lines)

        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as resp:
                if resp.status == 200:
                    photo_data = await resp.read()
                    await bot.send_photo(chat_id=CHANNEL_ID, photo=photo_data, caption=caption)
                else:
                    await bot.send_message(chat_id=CHANNEL_ID, text=caption)

    except exceptions.TelegramAPIError as e:
        print(f"Telegram API error: {e}")
    except Exception as e:
        print(f"Error sending post: {e}")

async def main():
    with open(CSV_FILE_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            await send_post(row)
            await asyncio.sleep(20 * 60)

if __name__ == '__main__':
    asyncio.run(main())