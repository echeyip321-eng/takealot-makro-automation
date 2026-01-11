import os
import time
import logging
import requests
import base64
from datetime import datetime
import json
import hmac
import hashlib
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
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', '')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '')
DRY_RUN = os.getenv('DRY_RUN', '1') == '1'
GOOGLE_SHEETS_CSV_URL = os.getenv('GOOGLE_SHEETS_CSV_URL', '')  # Published CSV URL
MODE = os.getenv('MODE', 'ingest')  # 'ingest' or 'activate'

class MakroApi:
    def __init__(self, apikey, apisecret):
        self.apikey = apikey
        self.apisecret = apisecret
        self.base_url = 'https://app-seller-inventory.prod.de.metro-marketplace.cloud/openapi/v2'
        self.session = requests.Session()
        
    def _sign_request(self, method, uri, body, timestamp):
        message = f"{method}{uri}{body}{timestamp}"
        signature = hmac.new(
            self.apisecret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _request(self, method, endpoint, json_body=None):
        uri = f"{self.base_url}{endpoint}"
        timestamp = int(time.time())
        body = ''
        if json_body:
            body = json.dumps(json_body, separators=(',', ':'))
        
        signature = self._sign_request(method.upper(), uri, body, timestamp)
        
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Client-Id': self.apikey,
            'X-Timestamp': str(timestamp),
            'X-Signature': signature,
        }
        
        logger.debug(f"{method} {uri}")
        resp = self.session.request(method, uri, headers=headers, data=body if body else None, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.text else {}
    
    def get_products(self, limit=100, offset=0):
        return self._request('GET', f'/offers?limit={limit}&offset={offset}')
    
    def search_marketplace(self, title):
        try:
            products = self.get_products(limit=100)
            for p in products.get('data', []):
                if title.lower() in p.get('title', '').lower():
                    return p
            return None
        except Exception as e:
            logger.warning(f"Search failed: {e}")
            return None
    
    def create_listing(self, payload):
        return self._request('POST', '/offers', json_body=payload)
    
    def update_listing(self, product_id, payload):
        return self._request('PATCH', f'/offers/{product_id}', json_body=payload)

class ReviewQueue:
    def __init__(self, csv_url):
        self.csv_url = csv_url
    
    def get_approved_items(self):
        if not self.csv_url:
            logger.error("GOOGLE_SHEETS_CSV_URL not configured")
            return []
        
        try:
            logger.info(f"Fetching CSV from: {self.csv_url}")
            response = requests.get(self.csv_url, timeout=15)
            response.raise_for_status()
            
            csv_data = io.StringIO(response.text)
            reader = csv.DictReader(csv_data)
            
            approved = [row for row in reader if row.get('review_status', '').upper() == 'APPROVED']
            logger.info(f"Found {len(approved)} approved items")
            return approved
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            return []
    
    def log_candidate(self, candidate):
        logger.info(f"CANDIDATE: {candidate.get('sku')}, {candidate.get('title')}, {candidate.get('supplier_cost')}, {candidate.get('your_price')}, {candidate.get('expected_margin')}%, {candidate.get('priority_score')}, {candidate.get('category')}, {candidate.get('makro_listing_id')}, {candidate.get('makro_status')}")

def generate_sample_products():
    return [
        {'sku': f'SAMPLE-{i:03d}', 'title': f'Sample Electronics Product {i}', 'cost': 100 + i*10, 'category': 'Electronics'}
        for i in range(1, MAX_CANDIDATES_PER_RUN + 1)
    ]

def calculate_margin(cost, price):
    if price <= 0:
        return 0
    return round((price - cost) / price * 100, 2)

def score_product(candidate):
    margin = candidate.get('expected_margin', 0)
    score = margin * 1.5
    return round(score, 2)

def job_ingest_candidates():
    logger.info("JOB: Ingest Candidates")
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)
    queue = ReviewQueue(GOOGLE_SHEETS_CSV_URL)
    supplier_products = generate_sample_products()
    logger.info(f"Fetched {len(supplier_products)} products from supplier")
    candidates = []
    
    for p in supplier_products[:MAX_CANDIDATES_PER_RUN]:
        cost = p.get('cost', 0)
        price = round(cost * MARKUP_MULTIPLIER, 2)
        margin = calculate_margin(cost, price)
        
        if margin < MIN_MARGIN_THRESHOLD * 100:
            logger.info(f"Skipping {p['title']} - margin too low ({margin}%)")
            continue
        
        existing = makro.search_marketplace(p['title'])
        if existing:
            logger.info(f"Skipping {p['title']} - already on Makro")
            continue
        
        payload = {'title': p['title'], 'description': f"Auto-imported. SKU: {p['sku']}", 'price': price, 'status': 'INACTIVE', 'sku': p['sku']}
        
        try:
            if not DRY_RUN:
                result = makro.create_listing(payload)
                makro_id = result.get('id')
            else:
                logger.info(f"DRY RUN: Would create {p['title']} at {price}")
                makro_id = f"DRYRUN-{p['sku']}"
            
            candidate = {'sku': p['sku'], 'title': p['title'], 'supplier_cost': cost, 'your_price': price, 'expected_margin': margin, 'priority_score': 0, 'category': p.get('category', ''), 'makro_listing_id': makro_id, 'makro_status': 'INACTIVE'}
            candidate['priority_score'] = score_product(candidate)
            candidates.append(candidate)
            logger.info(f"Created draft: {p['title']} (margin={margin}%)")
        except Exception as e:
            logger.error(f"Failed to create {p['title']}: {e}")
    
    candidates.sort(key=lambda x: x['priority_score'], reverse=True)
    logger.info("COPY THESE CANDIDATES TO GOOGLE SHEET:")
    for c in candidates:
        queue.log_candidate(c)
    logger.info(f"Total: {len(candidates)} candidates logged")
    return {'ingested': len(candidates)}

def job_publish_approved():
    logger.info("JOB: Publish Approved")
    makro = MakroApi(MAKRO_API_KEY, MAKRO_API_SECRET)
    queue = ReviewQueue(GOOGLE_SHEETS_CSV_URL)
    approved = queue.get_approved_items()
    
    if not approved:
        logger.info("No approved items to publish")
        return {'activated': 0}
    
    activated = 0
    for item in approved:
        makro_id = item.get('makro_listing_id')
        if not makro_id or makro_id.startswith('DRYRUN'):
            logger.warning(f"Skipping {item.get('title', 'Unknown')} - no valid Makro ID")
            continue
        
        takealot_price = item.get('takealot_price')
        final_price = float(item.get('your_price', 0))
        if takealot_price:
            try:
                competitor_price = float(takealot_price)
                final_price = round(competitor_price * 0.95, 2)
            except ValueError:
                pass
        
        update_payload = {'status': 'ACTIVE', 'price': final_price, 'description': f"{item.get('title', '')} - vs Takealot: {item.get('takealot_link', 'N/A')}"}
        
        try:
            if not DRY_RUN:
                makro.update_listing(makro_id, update_payload)
                logger.info(f"Activated: {item.get('title', 'Unknown')} at R{final_price}")
                activated += 1
            else:
                logger.info(f"DRY RUN: Would activate {item.get('title', 'Unknown')} at R{final_price}")
                activated += 1
        except Exception as e:
            logger.error(f"Failed to activate {item.get('title', 'Unknown')}: {e}")
    
    logger.info(f"Activated {activated} of {len(approved)} approved listings")
    return {'activated': activated, 'total_approved': len(approved)}

def main():
    logger.info("Starting Takealot-Makro automation")
    logger.info(f"Mode: {MODE}, Dry Run: {DRY_RUN}")
    
    if MODE == 'ingest':
        result = job_ingest_candidates()
        logger.info(f"Ingest job result: {result}")
    elif MODE == 'activate':
        result = job_publish_approved()
        logger.info(f"Activate job result: {result}")
    else:
        logger.error(f"Unknown MODE: {MODE}. Use 'ingest' or 'activate'")
    
    logger.info("main.py exiting")

if __name__ == '__main__':
    main()
