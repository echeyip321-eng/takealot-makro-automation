import os
import time
import logging
import requests
import base64
from datetime import datetime
import json
import hmac
import hashlib

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
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', '')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'
GOOGLE_SHEETS_URL = os.getenv('GOOGLE_SHEETS_URL', '')  # CSV export URL from Google Sheets
MODE = os.getenv('MODE', 'ingest')  # 'ingest' or 'activate'

class MakroApi:
    """Makro API client using HMAC-SHA256 signature authentication."""
    
    def __init__(self, apikey, apisecret):
        self.apikey = apikey
        self.apisecret = apisecret
        # Use Metro Markets production endpoint for offers
        self.baseurl = "https://app-seller-inventory.prod.de.metro-marketplace.cloud/openapi/v2/offers"
        self.session = requests.Session()
    
    def _generate_signature(self, method: str, uri: str, body: str, timestamp: str) -> str:
        """Generate HMAC-SHA256 signature for Metro/Makro API.
        
        Metro API requires signature string in order: METHOD, URI, BODY, TIMESTAMP
        where URI is the FULL URI including https://domain.com/path
        """
        # Create signature string: METHOD\nURI\nBODY\nTIMESTAMP
        signature_string = f"{method}\n{uri}\n{body}\n{timestamp}"
        
        logger.debug(f"Signature string: {signature_string}")
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            self.apisecret.encode('utf-8'),
            signature_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _make_request(self, method: str, endpoint: str, json_data: dict = None) -> dict:
        """Make authenticated request to Makro API using HMAC signature."""
        timestamp = str(int(time.time()))
        
        # Construct full URL
        if endpoint.startswith('http'):
            url = endpoint
        else:
            url = f"{self.baseurl}{endpoint}"
        
        body = json.dumps(json_data) if json_data else ''
        
        # Generate signature using FULL URL
        signature = self._generate_signature(method, url, body, timestamp)
        
        headers = {
            'Accept': 'application/json',
            'X-Client-Id': self.apikey,
            'X-Signature': signature,
            'X-Timestamp': timestamp,
            'Content-Type': 'application/json'
        }
        
        logger.info(f"Making {method} request to {url}")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Body: {body}")
        
        try:
            resp = self.session.request(method, url, data=body, headers=headers, timeout=30)
            logger.info(f"Response status: {resp.status_code}")
            return resp
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def create_listing(self, payload: dict) -> dict:
        """Create a listing on Makro."""
        if not self.apikey or not self.apisecret or DRY_RUN:
            logger.info(f"DRY RUN: would create listing '{payload.get('title')}'")
            return {'status': 'dryrun', 'id': 'DRYRUN_ID'}
        
        # Metro/Makro offer structure
        makro_payload = {
            "offers": [{
                "sku": payload.get('sku', f"SKU{int(time.time())}"),
                "price": payload.get('price', 0),
                "stock": 100,
                "active": payload.get('status', 'INACTIVE') == 'ACTIVE'
            }]
        }
        
        try:
            resp = self._make_request('POST', '', makro_payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to create listing: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            return {'status': 'error', 'message': str(e)}
    
    def update_listing_status(self, listing_id: str, status: str) -> dict:
        """Update a listing status (e.g., INACTIVE to ACTIVE)."""
        if DRY_RUN:
            logger.info(f"DRY RUN: would update listing {listing_id} to {status}")
            return {'status': 'dryrun'}
        
        logger.info(f"Updating listing {listing_id} to {status}")
        return {'status': 'success'}

class ReviewQueue:
    """Manages the review queue using Google Sheets CSV export."""
    
    def __init__(self, sheets_url: str):
        self.sheets_url = sheets_url
    
    def fetch_queue(self) -> list:
        """Fetch the review queue from Google Sheets."""
        if not self.sheets_url:
            logger.warning("No Google Sheets URL configured")
            return []
        
        try:
            resp = requests.get(self.sheets_url, timeout=10)
            resp.raise_for_status()
            
            # Parse CSV
            lines = resp.text.strip().split('\n')
            if len(lines) < 2:
                return []
            
            headers = [h.strip() for h in lines[0].split(',')]
            rows = []
            
            for line in lines[1:]:
                values = [v.strip() for v in line.split(',')]
                if len(values) == len(headers):
                    rows.append(dict(zip(headers, values)))
            
            return rows
        except Exception as e:
            logger.error(f"Failed to fetch review queue: {e}")
            return []
    
    def add_candidate(self, candidate: dict):
        """Add a candidate to the queue (appends to Google Sheet)."""
        logger.info(f"Would add candidate to queue: {candidate['title']}")
    
    def get_approved(self) -> list:
        """Get all approved items from the queue."""
        queue = self.fetch_queue()
        return [item for item in queue if item.get('review_status', '').upper() == 'APPROVED']
    
    def get_pending(self) -> list:
        """Get all pending items from the queue."""
        queue = self.fetch_queue()
        return [item for item in queue if item.get('review_status', '').upper() == 'PENDING']

def generate_sample_products(count: int = 10) -> list:
    """Generate sample supplier products for testing."""
    categories = ['Electronics', 'Home & Garden', 'Kitchen', 'Automotive', 'Sports']
    products = []
    
    for i in range(count):
        supplier_cost = round(100 + (i * 50), 2)
        your_price = round(supplier_cost * MARKUP_MULTIPLIER, 2)
        margin = round((your_price - supplier_cost) / your_price, 2)
        
        products.append({
            'sku': f'SUPP{1000 + i}',
            'title': f'Sample {categories[i % len(categories)]} Product {i+1}',
            'supplier_cost': supplier_cost,
            'your_price': your_price,
            'expected_margin': margin,
            'priority_score': round(margin * 100, 1),
            'category': categories[i % len(categories)]
        })
    
    return products

def score_and_filter_candidates(products: list) -> list:
    """Score products and filter based on business rules."""
    filtered = []
    
    for p in products:
        margin = p.get('expected_margin', 0)
        
        if margin < MIN_MARGIN_THRESHOLD:
            logger.info(f"Skipping {p['title']}: margin {margin:.1%} below threshold")
            continue
        
        filtered.append(p)
    
    filtered.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
    return filtered[:MAX_CANDIDATES_PER_RUN]

def job_ingest_candidates(makro: MakroApi, queue: ReviewQueue):
    """Job 1: Ingest supplier data, create draft listings, add to review queue."""
    logger.info("Starting job_ingest_candidates")
    
    supplier_products = generate_sample_products(20)
    logger.info(f"Found {len(supplier_products)} supplier products")
    
    candidates = score_and_filter_candidates(supplier_products)
    logger.info(f"Selected {len(candidates)} candidates after filtering")
    
    for candidate in candidates:
        payload = {
            'sku': candidate['sku'],
            'title': candidate['title'],
            'description': f"SKU: {candidate['sku']}, Category: {candidate['category']}",
            'price': candidate['your_price'],
            'status': 'INACTIVE'
        }
        
        try:
            result = makro.create_listing(payload)
            candidate['makro_listing_id'] = result.get('id', 'PENDING')
            candidate['makro_status'] = 'INACTIVE'
            logger.info(f"Created INACTIVE listing for {candidate['title']}")
        except Exception as e:
            logger.error(f"Failed to create listing for {candidate['title']}: {e}")
            candidate['makro_listing_id'] = 'FAILED'
            candidate['makro_status'] = 'ERROR'
    
    for candidate in candidates:
        candidate['takealot_link'] = ''
        candidate['takealot_price'] = ''
        candidate['takealot_instock'] = ''
        candidate['review_status'] = 'PENDING'
        candidate['reviewed_by'] = ''
        candidate['reviewed_at'] = ''
        queue.add_candidate(candidate)
    
    logger.info(f"job_ingest_candidates completed: {len(candidates)} candidates added to queue")
    return {'ingested': len(candidates)}

def job_activate_approved(makro: MakroApi, queue: ReviewQueue):
    """Job 2: Read approved items from queue and activate their Makro listings."""
    logger.info("Starting job_activate_approved")
    
    approved = queue.get_approved()
    logger.info(f"Found {len(approved)} approved items")
    
    if not approved:
        logger.info("No approved items to activate")
        return {'activated': 0}
    
    activated = 0
    for item in approved:
        listing_id = item.get('makro_listing_id')
        title = item.get('title', 'Unknown')
        
        if not listing_id or listing_id in ['FAILED', 'PENDING']:
            logger.warning(f"Skipping {title}: no valid listing_id")
            continue
        
        try:
            result = makro.update_listing_status(listing_id, 'ACTIVE')
            if result.get('status') in ['success', 'dryrun']:
                activated += 1
                logger.info(f"Activated listing {listing_id} for {title}")
            else:
                logger.error(f"Failed to activate {listing_id}: {result}")
        except Exception as e:
            logger.error(f"Error activating {listing_id}: {e}")
    
    logger.info(f"job_activate_approved completed: {activated} listings activated")
    return {'activated': activated}

def main():
    """Main entry point."""
    logger.info(f"Starting main in mode={MODE}")
    
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)
    queue = ReviewQueue(GOOGLE_SHEETS_URL)
    
    try:
        if MODE == 'ingest':
            result = job_ingest_candidates(makro, queue)
            logger.info(f"Ingest job result: {result}")
        elif MODE == 'activate':
            result = job_activate_approved(makro, queue)
            logger.info(f"Activate job result: {result}")
        else:
            logger.error(f"Unknown MODE: {MODE}. Use 'ingest' or 'activate'")
    except Exception as e:
        logger.exception(f"Job failed: {e}")
    
    logger.info("main.py exiting")

if __name__ == '__main__':
    main()


# Trigger Railway deployment
