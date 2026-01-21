import os
import re
import time
import logging
import requests
from datetime import datetime
import json
import csv
import io

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration from env
MARKUP_MULTIPLIER = float(os.getenv('MARKUP_MULTIPLIER', 2.8))
MIN_MARGIN_THRESHOLD = float(os.getenv('MIN_MARGIN_THRESHOLD', 0.3))
MAX_CANDIDATES_PER_RUN = int(os.getenv('MAX_CANDIDATES_PER_RUN', 10))
RUN_MODE = os.getenv('RUN_MODE', 'once')
MAKRO_APP_ID = os.getenv('MAKRO_API_KEY', '')
MAKRO_APP_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'
GOOGLE_SHEETS_CSV_URL = os.getenv('GOOGLE_SHEETS_CSV_URL', '')
MODE = os.getenv('MODE', 'ingest')


def to_float(value, default=0.0):
    """Safely parse float from string, handling commas and currency symbols"""
    try:
        s = (value or '').strip().replace('R', '').replace(',', '')
        return float(s) if s else default
    except Exception:
        return default


class MakroFSNFinder:
    """Automatically find FSN IDs by searching Makro's website"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })

    def search_makro(self, product_title):
        try:
            search_query = (
                product_title
                .replace('DH - ', '')
                .replace('Cappuccino', '')
                .strip()
            )

            logger.info(f"Searching Makro for: {search_query}")

            search_url = f"https://www.makro.co.za/search?q={requests.utils.quote(search_query)}"
            resp = self.session.get(search_url, timeout=30)
            resp.raise_for_status()

            fsn_matches = re.findall(r'pid=([A-Z0-9]{13,16})', resp.text)

            if fsn_matches:
                fsn = list(dict.fromkeys(fsn_matches))[0]
                logger.info(f"  ✅ Found FSN: {fsn}")
                return fsn

            logger.warning(f"  ⚠️ No FSN found for: {product_title}")
            return None

        except Exception as e:
            logger.error(f"Error searching Makro: {e}")
            return None


class MakroAuth:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None
        self.expiry = 0
        self.token_url = 'https://seller.makro.co.za/api/oauth-service/oauth/token'

    def get_token(self):
        if self.token and time.time() < self.expiry:
            return self.token

        logger.info("Fetching new OAuth access token...")

        resp = requests.get(
            self.token_url,
            params={
                'grant_type': 'client_credentials',
                'scope': 'Seller_Api'
            },
            auth=(self.app_id, self.app_secret),
            timeout=30
        )
        resp.raise_for_status()

        data = resp.json()
        self.token = data['access_token']
        self.expiry = time.time() + data.get('expires_in', 3600) - 60

        logger.info("Successfully obtained OAuth token")
        return self.token


class MakroApi:
    def __init__(self, auth: MakroAuth):
        self.auth = auth
        self.base_url = 'https://seller.makro.co.za/api'
        self.session = requests.Session()

    def _headers(self):
        return {
            'Authorization': f'Bearer {self.auth.get_token()}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def _request(self, method, endpoint, json_body=None):
        url = f"{self.base_url}{endpoint}"

        resp = self.session.request(
            method,
            url,
            headers=self._headers(),
            json=json_body,
            timeout=30,
            allow_redirects=False
        )

        # Check for redirects (3xx status codes)
        if 300 <= resp.status_code < 400:
            logger.error(f"REDIRECT: {resp.status_code} {method} {url}")
            logger.error(f"Location: {resp.headers.get('Location')}")
            raise RuntimeError("Makro API redirected to a different host. Blocked for safety")

        if resp.status_code >= 400:
            logger.error(f"HTTP {resp.status_code}: {method} {url}")
            logger.error(f"Body: {resp.text[:10000]}")
            resp.raise_for_status()

        return resp.json() if resp.text else None

    def create_listing(self, payload):
        """Create a new listing"""
        return self._request('POST', '/listings/v5/', json_body=payload)


class ReviewQueue:
    def __init__(self, csv_url):
        self.csv_url = csv_url

    def get_approved_items(self):
        if not self.csv_url:
            logger.error("GOOGLE_SHEETS_CSV_URL not configured")
            return []

        try:
            logger.info("Fetching candidate items from Google Sheets...")
            resp = requests.get(self.csv_url, timeout=30)
            resp.raise_for_status()

            csv_data = csv.DictReader(io.StringIO(resp.text))
            approved = []

            for row in csv_data:
                status = row.get('Status', '').strip().lower()
                sku = row.get('Takealot SKU', '').strip()

                logger.info(f"Row: SKU={sku} Status={status}")

                if status in ['approved', 'candidate']:
                    approved.append({
                        'takealot_sku': sku,
                        'fsn': row.get('FSN', '').strip(),
                        'title': row.get('Title', '').strip(),
                        'takealot_price': to_float(row.get('Takealot Price')),
                        'suggested_price': to_float(row.get('Suggested Makro Price')),
                        'margin': to_float(row.get('Margin %')),
                    })

            logger.info(f"Found {len(approved)} approved items")
            return approved

        except Exception as e:
            logger.error(f"Failed to fetch approved items: {e}")
            return []

    def mark_as_listed(self, takealot_sku, listing_id):
        logger.info(f"Would mark {takealot_sku} as listed with Makro listing {listing_id}")


class TakealotScraper:
    def get_product_info(self, sku):
        return {
            'title': f'Sample Product {sku}',
            'price': 199.99,
            'available': True,
            'image_url': 'https://example.com/image.jpg'
        }


def ingest_mode(makro_api, takealot_scraper):
    logger.info("=== INGEST MODE ===")
    logger.info("Populate Google Sheet with candidates")


def build_makro_listing(fsn: str, sku: str, price: float, location_id: str, inventory: int = 10):
        """Build Makro SA v5 API listing payload (snake_case as per Seller API guide)"""
        return {
        "listing_records": [{
            "product_id": fsn,
            "listing_status": "ACTIVE",
            "sku_id": sku,
            "selling_region_pref": "National",
            "min_oq": 1,
            "max_oq": 100,
            "price": {
                "base_price": price,
                "selling_price": price,
                "currency": "ZAR"
            },
            "shipping_fees": {
                "local": 1,
                "zonal": 1,
                "national": 1
            },
            "fulfillment_profile": "NON_FBM",
            "fulfillment": {
                "dispatch_sla": 3,
                "shipping_provider": "SELLER",
                "procurement_type": "REGULAR"
            },
            "packages": [{
                "name": "standard",
                "dimensions": {
                    "length": 30,
                    "breadth": 30,
                    "height": 30
                },
                "weight": 5,
                "description": "Standard package",
                "handling": {
                    "fragile": False
                }
            }],
                "locations": [{
                                        "id": location_id,
                    "status": "Active",
                    "inventory": inventory
                                    }]
                        }]
                    }
                }
                

def activate_mode(makro_api, review_queue, takealot_scraper, fsn_finder):
        """Process approved items and create Makro listings"""

    # Safety check for missing API credentials
    if not makro_api and not DRY_RUN:
        logger.error("Makro API not initialized but DRY_RUN=False. Set credentials or enable DRY_RUN.")
        return
    
    approved_items = review_queue.get_approved_items()
    if not approved_items:
        logger.info("No approved items to process")
        return
    
    for item in approved_items:
        sku = item['takealot_sku']
        title = item['title']
                price = item['suggested_price']
        fsn = item['fsn']

        logger.info(f"\n{'=' * 60}")
        logger.info(f"Processing SKU={sku}")
        logger.info(f"Title: {title}")
        logger.info(f"Price: R{price}")

        # Skip items with invalid price
        if price <= 0:
            logger.warning(f"Outcome=SKIPPED reason=INVALID_PRICE SKU={sku}")
            continue

        # Auto-find FSN if missing
        if not fsn:
            logger.info("FSN not provided, searching Makro...")
            fsn = fsn_finder.search_makro(title)
            if not fsn:
                logger.warning("Outcome=SKIPPED reason=NO_FSN")
                continue

        payload = build_makro_listing(
            fsn=fsn,
            sku=sku,
            price=price,
            location_id=os.getenv('MAKRO_LOCATION_ID', 'LOC4cef7f9b88a14df79646ba1c9dca25e9')
        )

        if DRY_RUN:
            logger.info(f"[DRY RUN] Would create listing with payload: {json.dumps(payload, indent=2)}")
            logger.info("Outcome=DRY_RUN_SUCCESS")
        else:
            try:
                logger.info("Creating Makro listing...")
                result = makro_api.create_listing(payload) or {}
                listing_id = result.get('listing_id', 'unknown')
                logger.info("✅ Successfully created listing")
                logger.info(f"Listing ID: {listing_id}")
                logger.info(f"Outcome=CREATED listing_id={listing_id}")

                review_queue.mark_as_listed(sku, listing_id)

            except Exception as e:
                logger.error(f"❌ Failed to create listing: {e}")
                logger.error(f"Outcome=FAILED error={str(e)[:200]}")


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("=== Starting Takealot-Makro Automation ===")
    logger.info(f"Mode: {MODE}")
    logger.info(f"DRY RUN: {DRY_RUN}")
    logger.info(f"Google Sheets URL configured: {bool(GOOGLE_SHEETS_CSV_URL)}")
    logger.info(f"Makro credentials configured: {bool(MAKRO_APP_ID and MAKRO_APP_SECRET)}")
    logger.info("=" * 60)

    # Initialize components
    fsn_finder = MakroFSNFinder()
    review_queue = ReviewQueue(GOOGLE_SHEETS_CSV_URL)
    takealot_scraper = TakealotScraper()

    # Initialize Makro API if credentials provided
    makro_api = None
    if MAKRO_APP_ID and MAKRO_APP_SECRET:
        try:
            auth = MakroAuth(MAKRO_APP_ID, MAKRO_APP_SECRET)
            makro_api = MakroApi(auth)
            logger.info("Makro API initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Makro API: {e}")
    else:
        logger.warning("Makro API credentials not provided")

    # Run based on MODE
    if MODE == 'ingest':
        ingest_mode(makro_api, takealot_scraper)
    elif MODE == 'activate':
        activate_mode(makro_api, review_queue, takealot_scraper, fsn_finder)
    else:
        logger.error(f"Unknown MODE: {MODE}")

    logger.info("=" * 60)
    logger.info("=== Finished ===")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
