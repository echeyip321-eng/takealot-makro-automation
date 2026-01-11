import os
import re

class MakroFSNFinder:
    """Automatically find FSN IDs by searching Makro's website"""
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def search_makro(self, product_title):
        """Search Makro website and extract FSN from first result"""
        try:
            # Clean up the title for search
            search_query = product_title.replace('DH - ', '').replace('Cappuccino', '').strip()
            
            logger.info(f"Searching Makro for: {search_query}")
            
            # Search Makro
            search_url = f"https://www.makro.co.za/search?q={requests.utils.quote(search_query)}"
            resp = self.session.get(search_url, timeout=30)
            resp.raise_for_status()
            
            # Look for pid= parameter in the response HTML
            # Match FSN pattern: pid=XXXXXXXXXXXXXXXX (13-16 chars)
            fsn_matches = re.findall(r'pid=([A-Z0-9]{13,16})', resp.text)
            
            if fsn_matches:
                # Get the first unique FSN
                unique_fsns = list(dict.fromkeys(fsn_matches))  # Remove duplicates
                fsn = unique_fsns[0]
                logger.info(f"  ✅ Found FSN: {fsn}")
                return fsn
            else:
                logger.warning(f"  ⚠️ No FSN found for: {product_title}")
                return None
                
        except Exception as e:
            logger.error(f"Error searching Makro: {e}")
            return None
import time
import logging
import requests
import base64
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
MIN_MARGIN_THRESHOLD = float(os.getenv('MIN_MARGIN_THRESHOLD', 0.3))  # 30% minimum margin
MAX_CANDIDATES_PER_RUN = int(os.getenv('MAX_CANDIDATES_PER_RUN', 10))
RUN_MODE = os.getenv('RUN_MODE', 'once')  # 'once' or 'scheduled'
MAKRO_APP_ID = os.getenv('MAKRO_API_KEY', '')
MAKRO_APP_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'
GOOGLE_SHEETS_CSV_URL = os.getenv('GOOGLE_SHEETS_CSV_URL', '')  # Published CSV URL
MODE = os.getenv('MODE', 'ingest')  # 'ingest' or 'activate'

class MakroAuth:
    """OAuth 2.0 authentication for Makro Marketplace API"""
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None
        self.expiry = 0
        self.token_url = 'https://seller.makro.co.za/api/oauth-service/oauth/token'
    
    def get_token(self):
        # Return cached token if still valid
        if self.token and time.time() < self.expiry:
            logger.debug("Using cached access token")
            return self.token
        
        logger.info("Fetching new OAuth access token...")
        try:
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
            # Set expiry with 60 second buffer
            self.expiry = time.time() + data.get('expires_in', 3600) - 60
            
            logger.info(f"Successfully obtained access token (expires in {data.get('expires_in')} seconds)")
            return self.token
            
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            raise

class MakroApi:
    """Makro Marketplace API client using OAuth 2.0"""
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
        
        logger.debug(f"{method} {url}")
        resp = self.session.request(
            method, 
            url, 
            headers=self._headers(), 
            json=json_body if json_body else None,
            timeout=30
        )
        resp.raise_for_status()
        return resp.json() if resp.text else {}
    
    def get_listings(self, limit=100, offset=0):
        """Get seller's active listings"""
        return self._request('GET', f'/listings/v5/?limit={limit}&offset={offset}')
    
    def search_listings(self, sku=None, fsn=None):
        """Search for listings by SKU or FSN"""
        params = []
        if sku:
            params.append(f'sku={sku}')
        if fsn:
            params.append(f'fsn={fsn}')
        query = '&'.join(params)
        return self._request('GET', f'/listings/v5/?{query}')
    
    def create_listing(self, payload):
        """Create a new listing"""
        return self._request('POST', '/listings/v5/', json_body=payload)
    
    def update_listing(self, listing_id, payload):
        """Update an existing listing"""
        return self._request('PUT', f'/listings/v5/{listing_id}', json_body=payload)

class ReviewQueue:
    def __init__(self, csv_url):
        self.csv_url = csv_url
    
    def get_approved_items(self):
        if not self.csv_url:
            logger.error("GOOGLE_SHEETS_CSV_URL not configured")
            return []
        
        try:
            logger.info("Fetching approved items from Google Sheets...")
            resp = requests.get(self.csv_url, timeout=30)
            resp.raise_for_status()
            
            csv_data = csv.DictReader(io.StringIO(resp.text))
            approved = []
            
            for row in csv_data:
                status = row.get('Status', '').strip().lower()
                if status == 'approved':
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
            
        except Exception as e:
            logger.error(f"Failed to fetch approved items: {e}")
            return []
    
    def mark_as_listed(self, takealot_sku, listing_id):
        logger.info(f"Would mark {takealot_sku} as listed with Makro listing {listing_id}")
        # In production, update the Google Sheet via Sheets API

class TakealotScraper:
    """Placeholder - integrate your Takealot scraping logic"""
    def get_product_info(self, sku):
        # Mock data - replace with actual scraping
        return {
            'title': f'Sample Product {sku}',
            'price': 199.99,
            'available': True,
            'image_url': 'https://example.com/image.jpg'
        }

def ingest_mode(makro_api, takealot_scraper):
    """Scan Takealot, find candidates, and populate Google Sheet"""
    logger.info("=== INGEST MODE ===")
    logger.info("This would scan Takealot products and populate candidates in the Google Sheet")
    logger.info("For now, use the FSN finder in activate mode - it will auto-find FSNs")    
    # Implementation:
    # 1. Scan Takealot categories/products
    # 2. Calculate margins with MARKUP_MULTIPLIER
    # 3. Filter by MIN_MARGIN_THRESHOLD
    # 4. Write to Google Sheet with Status='Pending Review'

def activate_mode(makro_api, review_queue, takealot_scrap, fsn_finderer):
    """Process approved items from Google Sheet and create Makro listings"""
    logger.info("=== ACTIVATE MODE ===")
    
    approved_items = review_queue.get_approved_items()
    
    if not approved_items:
        logger.info("No approved items to process")
        return
    
    logger.info(f"Processing {len(approved_items)} approved items...")
    
    for idx, item in enumerate(approved_items[:MAX_CANDIDATES_PER_RUN], 1):
        logger.info(f"\n[{idx}/{len(approved_items)}] Processing: {item['title']}")
        logger.info(f"  Takealot SKU: {item['takealot_sku']}")
        logger.info(f"  FSN: {item['fsn']}")
        logger.info(f"  Suggested Price: R{item['suggested_price']:.2f}")
        
        if not item['fsn']:
                        # Try to find FSN automatically
            logger.info(f"  🔍 No FSN provided - searching Makro automatically...")
            found_fsn = fsn_finder.search_makro(item['title'])
            
            if found_fsn:
                item['fsn'] = found_fsn
                logger.info(f"  ✅ Auto-found FSN: {found_fsn}")
            else:
            logger.warning("  ⚠️ No FSN provided - skipping")
            continue
        
        # Check if already listed
        try:
            existing = makro_api.search_listings(sku=item['takealot_sku'])
            if existing.get('listings'):
                logger.info("  ℹ️ Already listed on Makro")
                continue
        except Exception as e:
            logger.warning(f"  Could not check existing listings: {e}")
        
        # Create listing payload
        payload = {
            'fsn': item['fsn'],
            'seller_sku': item['takealot_sku'],
            'price': item['suggested_price'],
            'stock': 10,  # Default stock
            'procurement_sla': 2,  # Days to fulfill
            'listing_status': 'ACTIVE'
        }
        
        if DRY_RUN:
            logger.info(f"  [DRY RUN] Would create listing: {json.dumps(payload, indent=2)}")
            else:
            try:
                result = makro_api.create_listing(payload)
                listing_id = result.get('listing_id')
                logger.info(f"  ✅ Successfully created listing {listing_id}")
                review_queue.mark_as_listed(item['takealot_sku'], listing_id)
            except requests.exceptions.HTTPError as e:
                logger.error(f"  ❌ Failed to create listing: {e}")
                logger.error(f"  Response: {e.response.text}")
            except Exception as e:
                logger.error(f"  ❌ Error: {e}")
        
        time.sleep(2)  # Rate limiting
    
    logger.info("\n=== ACTIVATION COMPLETE ===")

def main():
    logger.info("Takealot → Makro Automation Starting...")
    logger.info(f"Mode: {MODE}")
    logger.info(f"Dry Run: {DRY_RUN}")
    
    # Initialize components
    auth = MakroAuth(MAKRO_APP_ID, MAKRO_APP_SECRET)
    makro_api = MakroApi(auth)
    review_queue = ReviewQueue(GOOGLE_SHEETS_CSV_URL)
    takealot_scraper = TakealotScraper()
        fsn_finder = MakroFSNFinder()
    
    # Test authentication
    try:
        logger.info("Testing Makro API authentication...")
        token = auth.get_token()
        logger.info("✅ Authentication successful")
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        return
    
    # Run mode
    if MODE == 'ingest':
        ingest_mode(makro_api, takealot_scraper, fsn_finder)
    elif MODE == 'activate':
        activate_mode(makro_api, review_queue, takealot_scraper, fsn_finder)
    else:
        logger.error(f"Unknown mode: {MODE}")
    
    logger.info("\nmain.py exiting")

if __name__ == '__main__':
    main()
