README (V50) - Railway webhook stable + button callbacks

Fixes:
- Removes any module-level polling loop so gunicorn can import main.py safely.
- Webhook is bound once on worker boot via _startup_webhook_once().
- Keeps the callback_query handler fix so inline buttons respond.

Railway:
- Variables:
  WEBHOOK_BASE_URL = https://<your-domain>.up.railway.app
  USE_WEBHOOK = 1
- Start Command:
  gunicorn -w 1 -b 0.0.0.0:$PORT main:app

If you ever want to disable automatic setWebhook on boot:
- DISABLE_SET_WEBHOOK=1
