README (V47) - Fix inline buttons

What was fixed:
- Registered the existing on_inline_click(c) handler as a callback query handler:
  @bot.callback_query_handler(func=lambda c: True)

How to run on Railway:
- Variables:
  WEBHOOK_BASE_URL = https://<your-domain>.up.railway.app
  USE_WEBHOOK = 1
- Start Command:
  gunicorn -w 1 -b 0.0.0.0:$PORT main:app

Security:
- If your BOT_TOKEN was pasted into logs or chat, rotate it in BotFather and update Railway variables.
