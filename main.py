import requests
import time
import os
from bs4 import BeautifulSoup
import json
import schedule

# Makro API Configuration
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', 'ff05c866-2a98-4f55-b5f0-6a92e40f8e93')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '6e18b3ec-be5d-46e3-ab3e-28d8f6b8fb3a')
MAKRO_SELLER_ID = '2303'
MAKRO_BASE_URL = 'https://api.makromarketplace.co.za/rest/v2/'

# Configuration
TARGET_CATEGORY = 'Air Fryers'
MARKUP_MULTIPLIER = 2.8
MAX_PRODUCTS_PER_RUN = 10

class TakealotScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def search_products(self, category, limit=10):
        """Search for products on Takealot"""
        url = f'https://www.takealot.com/all?qsearch={category.replace(" ", "+")}&via=search'
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            products = []
            
            # Find product listings
            product_cards = soup.find_all('div', class_='product-card', limit=limit)
            
            for card in product_cards:
                try:
                    title_elem = card.find('h3', class_='product-title')
                    price_elem = card.find('span', class_='currency-value')
                    link_elem = card.find('a', href=True)
                    
                    if title_elem and price_elem and link_elem:
                        product = {
                            'title': title_elem.get_text(strip=True),
                            'price': float(price_elem.get_text(strip=True).replace(',', '')),
                            'url': 'https://www.takealot.com' + link_elem['href'],
                            'plid': link_elem['href'].split('/')[-1] if '/' in link_elem['href'] else None
                        }
                        products.append(product)
                except Exception as e:
                    print(f'Error parsing product card: {e}')
                    continue
            
            return products
        except Exception as e:
            print(f'Error searching Takealot: {e}')
            return []
    
    def get_product_details(self, product_url):
        """Get detailed product information"""
        try:
            response = self.session.get(product_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            details = {}
            
            # Extract specifications
            specs_section = soup.find('div', class_='product-specs')
            if specs_section:
                specs = {}
                spec_items = specs_section.find_all('div', class_='spec-item')
                for item in spec_items:
                    key = item.find('span', class_='spec-key')
                    value = item.find('span', class_='spec-value')
                    if key and value:
                        specs[key.get_text(strip=True)] = value.get_text(strip=True)
                details['specifications'] = specs
            
            # Extract description
            desc_elem = soup.find('div', class_='product-description')
            if desc_elem:
                details['description'] = desc_elem.get_text(strip=True)
            
            return details
        except Exception as e:
            print(f'Error getting product details: {e}')
            return {}

class MakroAPI:
    def __init__(self, api_key, api_secret, seller_id):
        self.api_key = api_key
        self.api_secret = api_secret
        self.seller_id = seller_id
        self.base_url = MAKRO_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'api_key': self.api_key,
            'api_secret': self.api_secret
        })
    
    def get_fsn_from_category(self, category_name):
        """Get FSN for a category - placeholder, needs actual category mapping"""
        # Air Fryers category FSN
        category_map = {
            'Air Fryers': '3310',  # Placeholder - need to get actual FSN
        }
        return category_map.get(category_name, '3310')
    
    def check_duplicate(self, title, model_id=None):
        """Check if listing already exists"""
        try:
            # Get all listings
            url = f'{self.base_url}listings'
            response = self.session.get(url)
            
            if response.status_code == 200:
                listings = response.json()
                for listing in listings:
                    if listing.get('title', '').lower() == title.lower():
                        return True
                    if model_id and listing.get('model_id', '').lower() == model_id.lower():
                        return True
            return False
        except Exception as e:
            print(f'Error checking duplicates: {e}')
            return False
    
    def create_listing(self, product_data):
        """Create a new listing on Makro"""
        try:
            url = f'{self.base_url}listings'
            
            listing_payload = {
                'fsn': product_data.get('fsn'),
                'title': product_data.get('title'),
                'description': product_data.get('description', product_data.get('title')),
                'listing_status': 'INACTIVE',  # Draft status
                'fulfilled_by': 'seller',
                'mrp': product_data.get('mrp'),
                'selling_price': product_data.get('selling_price'),
                'stock': product_data.get('stock', 0),
                'sku': product_data.get('sku'),
                'manufacturer_details': 'N/A',
                'packer_details': 'N/A',
                'hsn': product_data.get('hsn', ''),
                'brand': product_data.get('brand', 'Generic'),
            }
            
            response = self.session.post(url, json=listing_payload)
            
            if response.status_code in [200, 201]:
                print(f'✓ Created listing: {product_data.get("title")}')
                return response.json()
            else:
                print(f'✗ Failed to create listing: {response.status_code} - {response.text}')
                return None
        except Exception as e:
            print(f'Error creating listing: {e}')
            return None

class AutomationEngine:
    def __init__(self):
        self.takealot = TakealotScraper()
        self.makro = MakroAPI(MAKRO_API_KEY, MAKRO_API_SECRET, MAKRO_SELLER_ID)
        self.processed_plids = set()
    
    def process_products(self):
        """Main automation logic"""
        print(f'\n=== Starting product sync at {time.strftime("%Y-%m-%d %H:%M:%S")} ===')
        
        # Search for products
        products = self.takealot.search_products(TARGET_CATEGORY, MAX_PRODUCTS_PER_RUN)
        print(f'Found {len(products)} products on Takealot')
        
        created_count = 0
        skipped_count = 0
        
        for product in products:
            # Skip if already processed
            if product.get('plid') in self.processed_plids:
                skipped_count += 1
                continue
            
            # Check for duplicates on Makro
            if self.makro.check_duplicate(product['title'], product.get('plid')):
                print(f'⊘ Duplicate found, skipping: {product["title"]}')
                skipped_count += 1
                self.processed_plids.add(product.get('plid'))
                continue
            
            # Get detailed information
            details = self.takealot.get_product_details(product['url'])
            
            # Calculate pricing
            takealot_price = product['price']
            makro_price = round(takealot_price * MARKUP_MULTIPLIER, 2)
            
            # Prepare listing data
            listing_data = {
                'fsn': self.makro.get_fsn_from_category(TARGET_CATEGORY),
                'title': product['title'],
                'description': details.get('description', product['title']),
                'mrp': makro_price,
                'selling_price': makro_price,
                'stock': 0,  # No stock initially
                'sku': f'TA-{product.get("plid", "UNKNOWN")}-{int(time.time())}',
                'brand': 'Generic',
                'hsn': '',
            }
            
            # Create listing
            result = self.makro.create_listing(listing_data)
            
            if result:
                created_count += 1
                self.processed_plids.add(product.get('plid'))
                time.sleep(2)  # Rate limiting
            
            # Stop if we hit the limit
            if created_count >= MAX_PRODUCTS_PER_RUN:
                break
        
        print(f'\n=== Summary ===')
        print(f'Created: {created_count}')
        print(f'Skipped: {skipped_count}')
        print(f'=================\n')

def main():
    print('Takealot-Makro Automation Starting...')
    print(f'Target Category: {TARGET_CATEGORY}')
    print(f'Markup: {MARKUP_MULTIPLIER}x')
    print(f'Max products per run: {MAX_PRODUCTS_PER_RUN}')
    
    engine = AutomationEngine()
    
    # Run immediately
    engine.process_products()
    
    # Schedule every 10 minutes
    schedule.every(10).minutes.do(engine.process_products)
    
    print('\nScheduler running... (every 10 minutes)')
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    main()
