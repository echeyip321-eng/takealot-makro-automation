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
            timeout=30
        )
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def search_listings(self, sku=None, fsn=None):
        params = []
        if sku:
            params.append(f'sku={sku}')
        if fsn:
            params.append(f'fsn={fsn}')
        query = '&'.join(params)
        return self._request('GET', f'/listings/v5/?{query}')

    def create_listing(self, payload):
        return self._request('POST', '/listings/v5/', json_body=payload)


class ReviewQueue:
    def __init__(self, csv_url):
        self.csv_url = csv_url

    def get_approved_items(self):
        if not self.csv_url:
            logger.error("GOOGLE_SHEETS_CSV_URL not configured")
            return []

        resp = requests.get(self.csv_url, timeout=30)
        resp.raise_for_status()

        csv_data = csv.DictReader(io.StringIO(resp.text))
        approved = []

        for row in csv_data:
            status = row.get('Status', '').strip().lower()
            logger.info(f"Row: {row.get('Takealot SKU', '')} - Status: {status}")

            if status in ['approved', 'candidate']:
                approved.append({
                    'takealot_sku': row.get('Takealot SKU', '').strip(),
                    'fsn': row.get('FSN', '').strip(),
                    'title': row.get('Title', '').strip(),
                    'takealot_price': float(row.get('Takealot Price', 0)),
                    'suggested_price': float(row.get('Suggested Makro Price', 0)),
                    'margin': float(row.get('Margin %', 0)),
                })

        logger.info(f"Found {len(approved)} approved items")
        return approved

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


def activate_mode(makro_api, review_queue, takealot_scraper, fsn_finder):
    approved_items = review_queue.get_approved_items()

    if not approved_items:
        logger.info("No approved items to process")
        return

    for item in approved_items:
        sku = item['takealot_sku']
        fsn = item['fsn']
        title = item['title']
        price = item['suggested_price']

        logger.info(f"Processing {sku}: {title}")

        if not fsn:
            fsn = fsn_finder.search_makro(title)
            if not fsn:
                continue

        if makro_api:
            existing = makro_api.search_listings(sku=sku)
            if existing.get('listings'):
                logger.info("Already listed")
                continue

        payload = {
            'sku': sku,
            'fsn': fsn,
            'price': price,
            'quantity': 10,
            'enabled': True
        }

        if DRY_RUN:
            logger.info(f"[DRY RUN] Would create listing: {payload}")
        else:
            result = makro_api.create_listing(payload)
            listing_id = result.get('listing_id', 'unknown')
            review_queue.mark_as_listed(sku, listing_id)


def main():
    """Main entry point"""
    logger.info("=== Starting Takealot-Makro Automation ===")
    logger.info(f"Mode: {MODE}")
    logger.info(f"DRY RUN: {DRY_RUN}")

    # Initialize components
    fsn_finder = MakroFSNFinder()
    review_queue = ReviewQueue(GOOGLE_SHEETS_CSV_URL)
    takealot_scraper = TakealotScraper()

    # Initialize Makro API if credentials provided
    makro_api = None
    if MAKRO_APP_ID and MAKRO_APP_SECRET:
        auth = MakroAuth(MAKRO_APP_ID, MAKRO_APP_SECRET)
        makro_api = MakroApi(auth)

    # Run based on MODE
    if MODE == 'ingest':
        ingest_mode(makro_api, takealot_scraper)
    elif MODE == 'activate':
        activate_mode(makro_api, review_queue, takealot_scraper, fsn_finder)
    else:
        logger.error(f"Unknown MODE: {MODE}")

    logger.info("=== Finished ===")


if __name__ == "__main__":
    main()
