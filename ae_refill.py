import os
import csv
import time
import re
import hashlib
import hmac
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, Any, List, Optional

import requests

# לפי מסמכי TOP/Alitrip: router/rest (US/EU) :contentReference[oaicite:2]{index=2}
AE_ENDPOINT = os.getenv("AE_ENDPOINT", "https://api.taobao.com/router/rest")

CSV_FIELDS = [
    "ItemId","ImageURL","Title","Opening","Strengths",
    "OriginalPrice","SalePrice","Discount","Rating","Orders",
    "BuyLink","CouponCode","Video Url"
]

# ---------- File lock ----------
class SimpleFileLock:
    def __init__(self, lock_path: str, timeout_sec: int = 25):
        self.lock_path = lock_path
        self.timeout_sec = timeout_sec
        self.fd = None

    def __enter__(self):
        start = time.time()
        while True:
            try:
                self.fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self.fd, str(os.getpid()).encode("utf-8"))
                return self
            except FileExistsError:
                if time.time() - start > self.timeout_sec:
                    raise TimeoutError(f"Could not acquire lock: {self.lock_path}")
                time.sleep(0.2)

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.fd is not None:
                os.close(self.fd)
            if os.path.exists(self.lock_path):
                os.remove(self.lock_path)
        except Exception:
            pass

# ---------- TOP signing ----------
def _beijing_timestamp_str() -> str:
    bj = datetime.now(timezone.utc) + timedelta(hours=8)
    return bj.strftime("%Y-%m-%d %H:%M:%S")

def _top_sign(params: Dict[str, str], secret: str, sign_method: str = "md5") -> str:
    filtered = {k: v for k, v in params.items() if k != "sign" and v is not None and v != ""}
    base = "".join(f"{k}{filtered[k]}" for k in sorted(filtered.keys()))
    if sign_method.lower() == "hmac":
        return hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.md5).hexdigest().upper()
    raw = f"{secret}{base}{secret}".encode("utf-8")
    return hashlib.md5(raw).hexdigest().upper()

def _extract_products(api_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = None
    for k, v in api_json.items():
        if k.endswith("_response") and isinstance(v, dict):
            resp = v
            break
    if resp is None:
        resp = api_json

    rr = resp.get("resp_result") or resp.get("result") or {}
    result = rr.get("result") if isinstance(rr, dict) else None
    if not isinstance(result, dict):
        result = rr if isinstance(rr, dict) else {}

    products = result.get("products") or result.get("product") or []
    if isinstance(products, dict):
        products = products.get("product") or []
    return products if isinstance(products, list) else []

class AliExpressAffiliateClient:
    def __init__(self, app_key: str, app_secret: str, endpoint: str = AE_ENDPOINT, sign_method: str = "md5"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.endpoint = endpoint
        self.sign_method = sign_method

    def call(self, method: str, api_params: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            "method": method,
            "app_key": self.app_key,
            "sign_method": self.sign_method,
            "timestamp": _beijing_timestamp_str(),
            "format": "json",
            "v": "2.0",
            "simplify": "true",
        }
        for k, v in api_params.items():
            if v is None:
                continue
            params[k] = str(v)

        params["sign"] = _top_sign(params, self.app_secret, self.sign_method)
        r = requests.post(self.endpoint, data=params, timeout=25)
        r.raise_for_status()
        return r.json()

    def fetch_best_sellers(self, *, ship_to_country: str, target_language: str, target_currency: str,
                          tracking_id: Optional[str], keywords: Optional[str],
                          category_ids: Optional[str], page_size: int) -> List[Dict[str, Any]]:
        # sort=LAST_VOLUME_DESC נתמך :contentReference[oaicite:3]{index=3}
        params = {
            "page_no": 1,
            "page_size": page_size,
            "sort": "LAST_VOLUME_DESC",
            "target_language": target_language,
            "target_currency": target_currency,
            "ship_to_country": ship_to_country,
        }
        if tracking_id:
            params["tracking_id"] = tracking_id
        if keywords:
            params["keywords"] = keywords
        if category_ids:
            params["category_ids"] = category_ids

        # API חינמי ולא דורש הרשאה :contentReference[oaicite:4]{index=4}
        data = self.call("aliexpress.affiliate.product.query", params)
        return _extract_products(data)

# ---------- Money helpers ----------
_num_re = re.compile(r"(\d+(?:\.\d+)?)")

def _to_decimal_from_any(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if not s:
        return None
    # לפעמים מגיע "US $3.21 - 4.10" => ניקח את המספר הראשון
    m = _num_re.search(s)
    if not m:
        return None
    try:
        return Decimal(m.group(1))
    except (InvalidOperation, ValueError):
        return None

def _usd_to_ils_str(usd_value: Any, ils_per_usd: Decimal) -> str:
    d = _to_decimal_from_any(usd_value)
    if d is None:
        return ""
    ils = (d * ils_per_usd).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return str(ils)

def _calc_discount_pct(orig_ils: str, sale_ils: str) -> str:
    o = _to_decimal_from_any(orig_ils)
    s = _to_decimal_from_any(sale_ils)
    if o is None or s is None or o <= 0:
        return ""
    pct = (Decimal("1") - (s / o)) * Decimal("100")
    pct = pct.quantize(Decimal("0"), rounding=ROUND_HALF_UP)
    if pct < 0:
        pct = Decimal("0")
    return str(pct)

# ---------- Queue sync ----------
def _read_existing_ids(path: str) -> set:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return set()
    ids = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        dr = csv.DictReader(f)
        for row in dr:
            v = (row.get("ItemId") or "").strip()
            if v:
                ids.add(v)
    return ids

def _count_rows(path: str) -> int:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return 0
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return max(0, sum(1 for _ in f) - 1)

def append_products_to_workfile(path: str, products: List[Dict[str, Any]], ils_per_usd: Decimal) -> int:
    lock_path = path + ".lock"
    with SimpleFileLock(lock_path, timeout_sec=25):
        existing_ids = _read_existing_ids(path)

        rows_to_add = []
        for p in products:
            pid = str(p.get("product_id") or "").strip()
            if not pid or pid in existing_ids:
                continue

            orig_usd = p.get("target_original_price") or p.get("original_price")
            sale_usd = p.get("target_sale_price") or p.get("sale_price")

            orig_ils = _usd_to_ils_str(orig_usd, ils_per_usd)
            sale_ils = _usd_to_ils_str(sale_usd, ils_per_usd)

            rate = str(p.get("evaluate_rate") or "").strip()
            if rate.endswith("%"):
                rate = rate[:-1]

            row = {
                "ItemId": pid,
                "ImageURL": str(p.get("product_main_image_url") or ""),
                "Title": "",         # נשאר ריק כדי שהבוט שלך ימלא עברית (כמו אצלך היום)
                "Opening": "",
                "Strengths": "",
                "OriginalPrice": orig_ils,  # ILS
                "SalePrice": sale_ils,      # ILS
                "Discount": _calc_discount_pct(orig_ils, sale_ils),
                "Rating": rate,
                "Orders": str(p.get("lastest_volume") or ""),
                "BuyLink": str(p.get("promotion_link") or ""),
                "CouponCode": "",
                "Video Url": str(p.get("product_video_url") or ""),
            }
            rows_to_add.append(row)
            existing_ids.add(pid)

        tmp = path + ".tmp"
        file_exists = os.path.exists(path) and os.path.getsize(path) > 0

        with open(tmp, "w", encoding="utf-8-sig", newline="") as wf:
            dw = csv.DictWriter(wf, fieldnames=CSV_FIELDS)
            dw.writeheader()

            if file_exists:
                with open(path, "r", encoding="utf-8-sig", newline="") as rf:
                    dr = csv.DictReader(rf)
                    for old in dr:
                        dw.writerow({k: old.get(k, "") for k in CSV_FIELDS})

            for r in rows_to_add:
                dw.writerow(r)

        os.replace(tmp, path)
        return len(rows_to_add)

def ensure_queue_min_size_once():
    path = os.getenv("AE_QUEUE_FILE", "workfile.csv")
    min_q = int(os.getenv("AE_MIN_QUEUE", "40"))
    batch = int(os.getenv("AE_FETCH_BATCH", "50"))

    app_key = os.getenv("AE_APP_KEY", "")
    app_secret = os.getenv("AE_APP_SECRET", "")
    if not app_key or not app_secret:
        print("AE_APP_KEY / AE_APP_SECRET missing")
        return

    ship = os.getenv("AE_SHIP_TO_COUNTRY", "IL")
    lang = os.getenv("AE_TARGET_LANGUAGE", "he")
    cur = os.getenv("AE_TARGET_CURRENCY", "USD")  # ILS לא נתמך :contentReference[oaicite:5]{index=5}
    tracking_id = os.getenv("AE_TRACKING_ID") or None
    category_ids = os.getenv("AE_CATEGORY_IDS") or None

    ils_per_usd = _to_decimal_from_any(os.getenv("ILS_PER_USD", "3.70")) or Decimal("3.70")

    current = _count_rows(path)
    if current >= min_q:
        print(f"Queue OK: {current} items (>= {min_q})")
        return

    keywords_list = [k.strip() for k in (os.getenv("AE_KEYWORDS", "")).split(",") if k.strip()]

    client = AliExpressAffiliateClient(app_key, app_secret)

    all_products: List[Dict[str, Any]] = []
    if keywords_list:
        for kw in keywords_list[:6]:
            all_products += client.fetch_best_sellers(
                ship_to_country=ship,
                target_language=lang,
                target_currency=cur,
                tracking_id=tracking_id,
                keywords=kw,
                category_ids=category_ids,
                page_size=batch,
            )
    else:
        all_products = client.fetch_best_sellers(
            ship_to_country=ship,
            target_language=lang,
            target_currency=cur,
            tracking_id=tracking_id,
            keywords=None,
            category_ids=category_ids,
            page_size=batch,
        )

    added = append_products_to_workfile(path, all_products, ils_per_usd)
    print(f"Queue refill: added {added} products (current was {current}, min {min_q})")

if __name__ == "__main__":
    ensure_queue_min_size_once()
