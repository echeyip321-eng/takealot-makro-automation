import os
import time
import logging
import requests
import base64
from datetime import datetime
import schedule

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration from env
MARKUP_MULTIPLIER = float(os.getenv("MARKUP_MULTIPLIER", "2.8"))
MAX_PRODUCTS_PER_RUN = int(os.getenv("MAX_PRODUCTS_PER_RUN", "10"))
RUN_MODE = os.getenv("RUN_MODE", "once")  # 'once' or 'scheduled'
MAKRO_API_KEY = os.getenv("MAKRO_API_KEY", "")
MAKRO_API_SECRET = os.getenv("MAKRO_API_SECRET", "")
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

class MakroApi:
    """Makro API client using OAuth2 Bearer token authentication."""

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.oauth_url = "https://seller.makro.co.za/api/oauth-service/oauth/token?grant_type=client_credentials&scope=Seller_Api"
        self.base_url = "https://seller.makro.co.za/api/listings/v5/"
        self.session = requests.Session()
        self.access_token = None
        
        if api_key and api_secret:
            self._get_access_token()

    def _get_access_token(self):
        """Get OAuth2 bearer token using Basic Auth."""
        try:
            # Create Basic Auth header
            credentials = f"{self.api_key}:{self.api_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                "Authorization": f"Basic {encoded_credentials}",
                "Content-Type": "application/json"
            }
            
            resp = requests.get(self.oauth_url, headers=headers, timeout=15)
            resp.raise_for_status()
            
            token_data = resp.json()
            self.access_token = token_data.get("access_token")
            
            if self.access_token:
                self.session.headers.update({
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                })
                logger.info("Successfully obtained OAuth access token")
            else:
                logger.error("No access_token in response")
            
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            raise

    def search_marketplace(self, title: str):
        logger.info(f"MakroApi.search_marketplace (stub) for: {title}")
        # Real implementation would call Makro search endpoints.
        return []

    def create_listing(self, payload: dict):
        """Create a listing on Makro. Returns dict with result or raises."""
        if not self.api_key or not self.api_secret or DRY_RUN:
            logger.info("DRY RUN: would create listing: %s", payload.get("title"))
            return {"status": "dry_run", "id": None}
        
        if not self.access_token:
            logger.error("No access token available")
            return {"status": "error", "message": "No access token"}
        
        # Format payload according to Makro API spec
        makro_payload = {
            "listing_records": [
                {
                    "product_id": "TEST_PRODUCT_ID",  # This needs to be a real FSN ID
                    "listing_status": "INACTIVE",
                    "sku_id": f"SKU_{int(time.time())}",
                    "selling_region_pref": "Local",
                    "min_oq": 1,
                    "max_oq": 100,
                    "price": {
                        "base_price": payload.get("price", 0),
                        "selling_price": payload.get("price", 0),
                        "currency": "ZAR"
                    },
                    "fulfillment_profile": "NON_FBM",
                    "fulfillment": {
                        "dispatch_sla": 4,
                        "shipping_provider": "SELLER",
                        "procurement_type": "REGULAR"
                    }
                }
            ]
        }
        
        try:
            resp = self.session.post(self.base_url, json=makro_payload, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to create listing: {e}")
            raise

def fetch_takealot_search(query: str = "Air Fryer", limit: int = 10):
    """
    Production-ready Takealot scraper that appears as a real browser.
    Uses realistic headers, cookies, and timing to avoid 403 blocks.
    Returns list of {'title','price','url'}.
    """
    test_products = os.getenv("TEST_PRODUCTS")
    if test_products:
        # Provide fake products for testing (reliable for Railway)
        products = []
        for i in range(min(limit, 10)):
            products.append(
                {
                    "title": f"Test Air Fryer {i+1}",
                    "price": 499.99 + i * 50,
                    "url": f"https://www.takealot.com/test-product-{i+1}",
                }
            )
        return products
    
    # Create a persistent session with cookies
    session = requests.Session()
    
    # Step 1: Visit homepage first to get cookies (like a real user)
    homepage_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-ZA,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    
    try:
        # Mimic real user: visit homepage first
        logger.info("Visiting Takealot homepage to establish session...")
        homepage_resp = session.get(
            "https://www.takealot.com/", 
            headers=homepage_headers, 
            timeout=15
        )
        homepage_resp.raise_for_status()
        
        # Wait like a real user would
        time.sleep(2 + (hash(query) % 3))  # Random-ish delay 2-5 seconds
        
    except Exception as e:
        logger.warning(f"Homepage visit failed: {e}")
        # Continue anyway, might still work
    
    # Step 2: Now perform the search with proper referer
    q = query.replace(" ", "+")
    search_url = f"https://www.takealot.com/search?searchTerm={q}"
    
    search_headers = {
        "User-Agent": homepage_headers["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-ZA,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": "https://www.takealot.com/",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "TE": "trailers",
    }
    
    max_retries = 3
    r = None
    
    for attempt in range(max_retries):
        try:
            # Exponential backoff with jitter
            if attempt > 0:
                delay = (2 ** attempt) + (hash(str(time.time())) % 3)
                logger.info(f"Waiting {delay}s before retry {attempt + 1}...")
                time.sleep(delay)
            
            logger.info(f"Fetching Takealot search (attempt {attempt + 1}/{max_retries})...")
            r = session.get(search_url, headers=search_headers, timeout=20)
            
            if r.status_code == 403:
                logger.warning(f"403 Forbidden on attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    continue
                else:
                    logger.error("All retries exhausted, still getting 403")
                    return []
            
            r.raise_for_status()
            logger.info(f"✓ Takealot request successful on attempt {attempt + 1}")
            break
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                continue
            else:
                logger.error("All retries failed")
                return []
    
    if not r or r.status_code != 200:
        return []
    
    text = r.text
    products = []
    
    # Parse product data from HTML/JSON
    import re
    
    # Try multiple patterns (Takealot may use different formats)
    title_patterns = [
        re.compile(r'"title"\s*:\s*"([^"]{10,200})"'),
        re.compile(r'data-product-title="([^"]{10,200})"'),
        re.compile(r'<h3[^>]*>([^<]{10,200})</h3>'),
    ]
    
    price_patterns = [
        re.compile(r'"price"\s*:"?([0-9][0-9,\.]*)\s*"?'),
        re.compile(r'data-price="([0-9][0-9,\.]*)"'),
        re.compile(r'R\s*([0-9][0-9,\.]+)'),
    ]
    
    titles = []
    prices = []
    
    for pattern in title_patterns:
        titles = pattern.findall(text)
        if titles:
            logger.info(f"Found {len(titles)} titles using pattern")
            break
    
    for pattern in price_patterns:
        prices = pattern.findall(text)
        if prices:
            logger.info(f"Found {len(prices)} prices using pattern")
            break
    
    # Match titles to prices
    for t, p in zip(titles, prices):
        try:
            price = float(p.replace(",", "").replace(" ", ""))
        except ValueError:
            continue
        
        products.append(
            {
                "title": t.strip(),
                "price": price,
                "url": search_url,
            }
        )
        
        if len(products) >= limit:
            break
    
    logger.info(f"Extracted {len(products)} products from Takealot")
    return products

def calculate_price(takealot_price: float) -> float:
    return round(takealot_price * MARKUP_MULTIPLIER, 2)

def process_products(makro_client: MakroApi):
    logger.info("process_products() entered")
    perf = {"found": 0, "created": 0, "skipped": 0}

    products = fetch_takealot_search(limit=MAX_PRODUCTS_PER_RUN)
    perf["found"] = len(products)
    logger.info("Found %d products", perf["found"])

    for p in products:
        title = p.get("title")
        price = p.get("price")

        if not title or price is None:
            perf["skipped"] += 1
            logger.warning("Skipping product with missing data: %s", p)
            continue

        makro_price = calculate_price(float(price))

        existing = makro_client.search_marketplace(title)
        if existing:
            perf["skipped"] += 1
            logger.info("Skipped (duplicate): %s", title)
            continue

        payload = {
            "title": title,
            "description": f"Imported from Takealot: {p.get('url')}",
            "price": makro_price,
            "status": "INACTIVE",
        }

        try:
            res = makro_client.create_listing(payload)
            perf["created"] += 1
            logger.info("Created listing for %s (result=%s)", title, res.get("status"))
        except Exception as e:
            perf["skipped"] += 1
            logger.error("Failed to create listing for %s: %s", title, e)

    logger.info("process_products() finished: %s", perf)
    return perf

def job():
    logger.info("Job started")
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)

    try:
        result = process_products(makro)
        logger.info("Job result: %s", result)
    except Exception as e:
        logger.exception("Job failed: %s", e)

    logger.info("Job finished")

def setup_schedule():
    interval = int(os.getenv("SYNC_INTERVAL_MIN", "10"))
    logger.info("Scheduling job every %d minutes", interval)
    schedule.every(interval).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    mode = os.getenv("RUN_MODE", "once")
    logger.info("Starting main in mode=%s", mode)

    if mode == "scheduled":
        setup_schedule()
    else:
        job()

    logger.info("main.py exiting")
