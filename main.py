import os
import time
import logging
import requests
from datetime import datetime
import schedule

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Configuration from env
MARKUP_MULTIPLIER = float(os.getenv('MARKUP_MULTIPLIER', '2.8'))
MAX_PRODUCTS_PER_RUN = int(os.getenv('MAX_PRODUCTS_PER_RUN', '10'))
RUN_MODE = os.getenv('RUN_MODE', 'once')  # 'once' or 'scheduled'
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', '')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'


class MakroApi:
    """Minimal Makro API client (stubbed if credentials missing)."""
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = os.getenv('MAKRO_API_BASE', 'https://app-seller-pim.prod.de.metro-marketplace.cloud/openapi/v1')
        self.session = requests.Session()
        if api_key and api_secret:
            self.session.headers.update({
                'X-Api-Key': api_key,
                'X-Api-Secret': api_secret,
                'Content-Type': 'application/json'
            })

    def search_marketplace(self, title):
        logger.info(f"MakroApi.search_marketplace (stub) for: {title}")
        # Real implementation would call Makro search endpoints.
        return []

    def create_listing(self, payload):
        """Create a listing on Makro. Returns dict with result or raises."""
        if not self.api_key or not self.api_secret or DRY_RUN:
            logger.info("DRY RUN: would create listing: %s", payload.get('title'))
            return {'status': 'dry_run', 'id': None}
        # Example POST (not live-tested)
        url = f"{self.base_url}/products"
        resp = self.session.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()


def fetch_takealot_search(query='Air Fryer', limit=10):
    """Attempt to fetch simple product info from Takealot search page.
    This is a best-effort scraper and may need tuning.
    Returns list of {'title','price','url'}.
    """
    q = query.replace(' ', '+')
    url = f"https://www.takealot.com/search?searchTerm={q}"
    try:
        # Comprehensive browser-like headers to bypass anti-scraping
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.takealot.com/',
        }
        
        # Retry logic with exponential backoff
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(1 + attempt * 0.5)  # Progressive delay
                r = requests.get(url, timeout=15, headers=headers)
                r.raise_for_status()                logger.info(f"Takealot request successful on attempt {attempt + 1}")
                break  # Success, exit retry loop
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    logger.warning(f"403 Forbidden on attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        logger.error("All retry attempts failed with 403 Forbidden")
                        return []
                else:
                    raise
            except Exception as e:
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    raise
        text = r.text
        products = []
        # Very lightweight parsing: look for JSON-LD blocks or product-title markers
        # Fallback: return empty list if parsing fails
        # For reliability in Railway runs, allow TEST_PRODUCTS env var
        test_products = os.getenv('TEST_PRODUCTS')
        if test_products:
            # Provide fake products for testing
            for i in range(min(limit, 5)):
                products.append({
                    'title': f'Test Air Fryer {i+1}',
                    'price': 499.99 + i * 50,
                    'url': f'https://www.takealot.com/test-product-{i+1}'
                })
            return products

        # Very naive extraction of titles and prices
        import re
        title_re = re.compile(r'"title"\s*:\s*"([^"]{10,200})"')
        price_re = re.compile(r'"price"\s*:\s*"?([0-9,.]+)"?')
        titles = title_re.findall(text)
        prices = price_re.findall(text)
        for t, p in zip(titles, prices):
            try:
                price = float(p.replace(',', '').replace('R', ''))
            except:
                continue
            products.append({'title': t.strip(), 'price': price, 'url': url})
            if len(products) >= limit:
                break
        return products
    except Exception as e:
        logger.warning('Failed to fetch Takealot search: %s', e)
        return []


def calculate_price(takealot_price):
    return round(takealot_price * MARKUP_MULTIPLIER, 2)


def process_products(makro_client):
    logger.info('process_products() entered')
    perf = {
        'found': 0,
        'created': 0,
        'skipped': 0
    }
    products = fetch_takealot_search(limit=MAX_PRODUCTS_PER_RUN)
    perf['found'] = len(products)
    logger.info('Found %d products', perf['found'])

    for p in products:
        title = p.get('title')
        price = p.get('price')
        if not title or not price:
            perf['skipped'] += 1
            logger.warning('Skipping product with missing data: %s', p)
            continue

        makro_price = calculate_price(price)
        # Check duplicates
        existing = makro_client.search_marketplace(title)
        if existing:
            perf['skipped'] += 1
            logger.info('Skipped (duplicate): %s', title)
            continue

        payload = {
            'title': title,
            'description': f'Imported from Takealot: {p.get("url")}',
            'price': makro_price,
            'status': 'INACTIVE'
        }
        try:
            res = makro_client.create_listing(payload)
            perf['created'] += 1
            logger.info('Created listing for %s (result=%s)', title, res.get('status'))
        except Exception as e:
            perf['skipped'] += 1
            logger.error('Failed to create listing for %s: %s', title, e)

    logger.info('process_products() finished: %s', perf)
    return perf


def job():
    logger.info('Job started')
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)
    try:
        result = process_products(makro)
        logger.info('Job result: %s', result)
    except Exception as e:
        logger.exception('Job failed: %s', e)
    logger.info('Job finished')


def setup_schedule():
    interval = int(os.getenv('SYNC_INTERVAL_MIN', '10'))
    logger.info('Scheduling job every %d minutes', interval)
    schedule.every(interval).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    mode = os.getenv('RUN_MODE', 'once')
    logger.info('Starting main in mode=%s', mode)
    if mode == 'scheduled':
        setup_schedule()
    else:
        job()
    logger.info('main.py exiting')
