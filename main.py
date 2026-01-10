import os
import time
import logging
import requests
import base64
from datetime import datetime
import schedule
import random
import hmac
import hashlib
import json


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


user_agents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]


# Configuration from env
MARKUP_MULTIPLIER = float(os.getenv('MARKUP_MULTIPLIER', '2.8'))
MAX_PRODUCTS_PER_RUN = int(os.getenv('MAX_PRODUCTS_PER_RUN', '10'))
RUN_MODE = os.getenv('RUN_MODE', 'once')  # once or scheduled
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', '')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'


class MakroApi:
    """Makro API client using HMAC signing authentication."""
    
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://seller.makro.co.za/api/listings/v5'
        self.session = requests.Session()
    
    def search_marketplace(self, title):
        """Search marketplace for existing product."""
        logger.info(f'MakroApi.search_marketplace stub for {title}')
        return None
    
    def create_listing(self, payload):
        """Create a listing on Makro."""
        if not self.api_key or not self.api_secret or DRY_RUN:
            logger.info(f'DRY RUN: would create listing {payload.get("title")}')
            return {'status': 'dryrun', 'id': 'DRYRUN_ID'}
        
        # Generate HMAC signature
        timestamp = str(int(time.time()))
        method = 'POST'
        full_url = self.base_url
        body_str = json.dumps(payload, separators=(',', ':'))
        message = f"{method}\n{full_url}\n{body_str}\n{timestamp}"
        signature = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        
        headers = {
            'Accept': 'application/json',
            'X-Client-Id': self.api_key,
            'X-Timestamp': timestamp,
            'X-Signature': signature,
            'Content-Type': 'application/json'
        }
        
        try:
            resp = self.session.post(self.base_url, json=payload, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f'Failed to create listing: {e}')
            return {'status': 'error', 'message': str(e)}


def fetch_takealot_search(query='Air Fryer', limit=10):
    """Production-ready Takealot scraper with anti-bot detection."""
    
    # Test mode
    test_products = os.getenv('TEST_PRODUCTS')
    if test_products:
        products = []
        for i in range(min(limit, 10)):
            products.append({
                'title': f'Test Air Fryer {i+1}',
                'price': 499.99 + (i * 50),
                'url': f'https://www.takealot.com/test-product-{i+1}'
            })
        return products
    
    # Create persistent session
    session = requests.Session()
    user_agent = random.choice(user_agents)
    
    if 'Macintosh' in user_agent:
        platform = 'macOS'
    elif 'Windows' in user_agent:
        platform = 'Windows'
    elif 'Linux' in user_agent:
        platform = 'Linux'
    else:
        platform = 'macOS'  # default
    
    # Step 1: Visit homepage first
    homepage_headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-ZA,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': f'"{platform}"'
    }
    
    try:
        logger.info('Visiting Takealot homepage...')
        resp = session.get('https://www.takealot.com', headers=homepage_headers, timeout=15)
        resp.raise_for_status()
        time.sleep(random.uniform(1, 3))  # Wait like real user
    except Exception as e:
        logger.warning(f'Homepage visit failed: {e}')
    
    time.sleep(random.uniform(0.5, 1.5))  # Additional random delay
    
    # Step 2: Perform search with proper headers
    search_url = f'https://www.takealot.com/search?searchTerm={query}'
    search_headers = {
        **homepage_headers,
        'Referer': 'https://www.takealot.com/',
        'Sec-Fetch-Site': 'same-origin'
    }
    
    max_retries = 3
    r = None
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = random.uniform(2, 4) + (attempt * random.uniform(0.5, 1.5))
                logger.info(f'Waiting {delay:.2f}s before retry {attempt + 1}...')
                time.sleep(delay)
            
            logger.info(f'Fetching Takealot search (attempt {attempt + 1}/{max_retries})...')
            r = session.get(search_url, headers=search_headers, timeout=20)
            
            if r.status_code == 403:
                logger.warning(f'403 Forbidden on attempt {attempt + 1}/{max_retries}')
                if attempt == max_retries - 1:
                    logger.error('All retries exhausted, still getting 403')
                    return []
                continue
            
            r.raise_for_status()
            logger.info(f'Takealot request successful on attempt {attempt + 1}')
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f'Request failed on attempt {attempt + 1}: {e}')
            if attempt == max_retries - 1:
                logger.error('All retries failed')
                return []
    
    if not r or r.status_code != 200:
        return []
    
    # Parse products from HTML
    text = r.text
    products = []
    import re
    
    title_patterns = [
        re.compile(r'title["\'}>](.{10,200})<'),
        re.compile(r'data-product-title["\'}>](.{10,200})<'),
        re.compile(r'<h3[^>]*>(.{10,200})</h3>')
    ]
    
    price_patterns = [
        re.compile(r'[Rr]\s?\d{1,4}[.,]?\d{0,2}'),
        re.compile(r'price["\'']:\s*([\d,.]+)'),
        re.compile(r'[Rr]\d{1,4}(?:[.,]\d{2})?')
    ]
    
    titles = []
    prices = []
    
    for pattern in title_patterns:
        titles = pattern.findall(text)
        if titles:
            logger.info(f'Found {len(titles)} titles using pattern')
            break
    
    for pattern in price_patterns:
        prices = pattern.findall(text)
        if prices:
            logger.info(f'Found {len(prices)} prices using pattern')
            break
    
    # Match titles to prices
    for t, p in zip(titles, prices):
        try:
            price = float(p.replace('R', '').replace(',', '.').replace(' ', ''))
            products.append({
                'title': t.strip(),
                'price': price,
                'url': search_url
            })
        except ValueError:
            continue
    
    if len(products) > limit:
        products = products[:limit]
    
    logger.info(f'Extracted {len(products)} products from Takealot')
    return products


def calculate_price(takealot_price):
    """Calculate Makro listing price based on Takealot."""
    return round(takealot_price * MARKUP_MULTIPLIER, 2)


def process_products(makro_client):
    """Process products from Takealot and create Makro listings."""
    logger.info('process_products entered')
    perf = {'found': 0, 'created': 0, 'skipped': 0}
    
    products = fetch_takealot_search(limit=MAX_PRODUCTS_PER_RUN)
    perf['found'] = len(products)
    logger.info(f'Found {perf["found"]} products')
    
    for p in products:
        title = p.get('title')
        price = p.get('price')
        
        if not title or price is None:
            perf['skipped'] += 1
            logger.warning(f'Skipping product with missing data: {p}')
            continue
        
        makro_price = calculate_price(float(price))
        
        payload = {
            'title': title,
            'description': f'Imported from Takealot: {p.get("url")}',
            'price': makro_price,
            'status': 'INACTIVE'
        }
        
        try:
            res = makro_client.create_listing(payload)
            perf['created'] += 1
            logger.info(f'Created listing for {title}: {res.get("status")}')
        except Exception as e:
            perf['skipped'] += 1
            logger.error(f'Failed to create listing for {title}: {e}')
    
    logger.info(f'process_products finished: {perf}')
    return perf


def job():
    logger.info('Job started')
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)
    try:
        result = process_products(makro)
        logger.info(f'Job result: {result}')
    except Exception as e:
        logger.exception(f'Job failed: {e}')
    logger.info('Job finished')


def setup_schedule():
    interval = int(os.getenv('SYNC_INTERVAL_MIN', '10'))
    logger.info(f'Scheduling job every {interval} minutes')
    schedule.every(interval).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    mode = os.getenv('RUN_MODE', 'once')
    logger.info(f'Starting main in mode: {mode}')
    
    if mode == 'scheduled':
        setup_schedule()
    else:
        job()
    
    logger.info('main.py exiting')
