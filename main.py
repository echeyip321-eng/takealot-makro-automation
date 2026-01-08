import requests
import time
import os
import re
from bs4 import BeautifulSoup
import json
import schedule
from io import BytesIO

# Makro API Configuration
MAKRO_API_KEY = os.getenv('MAKRO_API_KEY', 'ff05c866-2a98-4f55-b5f0-6a92e40f8e93')
MAKRO_API_SECRET = os.getenv('MAKRO_API_SECRET', '6e18b3ec-be5d-46e3-ab3e-28d8f6b8fb3a')
MAKRO_SELLER_ID = '2303'
MAKRO_BASE_URL = 'https://api.makromarketplace.co.za/rest/v2/'

# Configuration
TARGET_CATEGORY = 'Air Fryers'
MARKUP_MULTIPLIER = 2.8
MAX_PRODUCTS_PER_RUN = 10
MIN_RATING = 3.9
MIN_PRICE_FOR_CHEAP_PRODUCTS = 450  # Products under R200 must sell for at least R450

def apply_charm_pricing(price):
    """Apply consumer psychology to pricing"""
    # Round to nearest charm price
    if price < 100:
        return round(price - 1)  # R99
    elif price < 300:
        return round(price / 10) * 10 - 1  # R299
    elif price < 500:
        return round(price / 5) * 5 - 0.05  # R499.95
    elif price < 1000:
        return round(price / 10) * 10 - 1  # R999
    elif price < 2000:
        return round(price / 50) * 50 - 1  # R1999
    else:
        return round(price / 100) * 100 - 1  # R2999

class TakealotScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def search_products(self, category, limit=10):
        """Search for products on Takealot with rating filter"""
        url = f'https://www.takealot.com/all?qsearch={category.replace(" ", "+")}&via=search'
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            products = []
            
            product_cards = soup.find_all('div', class_='product-card', limit=limit*2)
            
            for card in product_cards:
                try:
                    title_elem = card.find('h3', class_='product-title')
                    price_elem = card.find('span', class_='currency-value')
                    link_elem = card.find('a', href=True)
                    
                    # Extract rating
                    rating_elem = card.find('div', class_='rating-stars')
                    rating = 0
                    if rating_elem:
                        rating_text = rating_elem.get('title', '0')
                        rating_match = re.search(r'([0-9.]+)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                    
                    # Skip if rating is below minimum
                    if rating < MIN_RATING:
                        continue
                    
                    # Extract actual price (not discounted)
                    actual_price_elem = card.find('span', class_='price-wrapper')
                    original_price_elem = card.find('span', class_='old-price')
                    
                    if original_price_elem:
                        # Use original price if available
                        price_text = original_price_elem.get_text(strip=True)
                    elif price_elem:
                        price_text = price_elem.get_text(strip=True)
                    else:
                        continue
                    
                    price = float(price_text.replace(',', '').replace('R', '').strip())
                    
                    if title_elem and link_elem:
                        product = {
                            'title': title_elem.get_text(strip=True),
                            'price': price,
                            'rating': rating,
                            'url': 'https://www.takealot.com' + link_elem['href'],
                            'plid': link_elem['href'].split('/')[-1] if '/' in link_elem['href'] else None
                        }
                        products.append(product)
                        
                        if len(products) >= limit:
                            break
                            
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
            
            details = {'image_urls': []}
            
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
            
            # Extract images
            gallery = soup.find('div', class_='gallery')
            if gallery:
                img_elements = gallery.find_all('img')
                for img in img_elements:
                    img_url = img.get('src') or img.get('data-src')
                    if img_url and 'http' in img_url:
                        details['image_urls'].append(img_url)
            
            return details
        except Exception as e:
            print(f'Error getting product details: {e}')
            return {'image_urls': []}
    
    def download_images(self, image_urls):
        """Download product images"""
        images = []
        for idx, url in enumerate(image_urls[:5]):  # Limit to 5 images
            try:
                response = self.session.get(url)
                if response.status_code == 200:
                    images.append({
                        'content': response.content,
                        'filename': f'image_{idx}.jpg'
                    })
            except Exception as e:
                print(f'Error downloading image {idx}: {e}')
        return images

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
        """Get FSN for a category"""
        category_map = {
            'Air Fryers': '3310',
        }
        return category_map.get(category_name, '3310')
    
    def check_duplicate(self, title, model_id=None):
        """Check if listing already exists"""
        try:
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
        """Create a new ACTIVE listing on Makro"""
        try:
            url = f'{self.base_url}listings'
            
            listing_payload = {
                'fsn': product_data.get('fsn'),
                'title': product_data.get('title'),
                'description': product_data.get('description', product_data.get('title')),
                'listing_status': 'ACTIVE',  # ACTIVE status for immediate listing
                'fulfilled_by': 'seller',
                'mrp': product_data.get('mrp'),
                'selling_price': product_data.get('selling_price'),
                'stock': 100,  # Set stock to 100
                'sku': product_data.get('sku'),
                'manufacturer_details': 'N/A',
                'packer_details': 'N/A',
                'hsn': product_data.get('hsn', ''),
                'brand': product_data.get('brand', 'Generic'),
            }
            
            response = self.session.post(url, json=listing_payload)
            
            if response.status_code in [200, 201]:
                print(f'✓ Created ACTIVE listing: {product_data.get("title")}')
                return response.json()
            else:
                print(f'✗ Failed to create listing: {response.status_code} - {response.text}')
                return None
        except Exception as e:
            print(f'Error creating listing: {e}')
            return None
    
    def upload_images(self, listing_id, images):
        """Upload images to Makro listing"""
        try:
            url = f'{self.base_url}listings/{listing_id}/images'
            
            uploaded_count = 0
            for idx, img in enumerate(images):
                files = {
                    'image': (img['filename'], img['content'], 'image/jpeg')
                }
                data = {
                    'image_type': 'MAIN' if idx == 0 else 'ADDITIONAL'
                }
                
                response = self.session.post(url, files=files, data=data)
                
                if response.status_code in [200, 201]:
                    uploaded_count += 1
                    print(f'  ✓ Uploaded image {idx + 1}/{len(images)}')
                else:
                    print(f'  ✗ Failed to upload image {idx + 1}')
            
            return uploaded_count > 0
        except Exception as e:
            print(f'Error uploading images: {e}')
            return False
    
    def update_stock(self, listing_id, stock_quantity):
        """Update stock quantity for a listing"""
        try:
            url = f'{self.base_url}listings/{listing_id}/inventory'
            payload = {'stock': stock_quantity}
            
            response = self.session.put(url, json=payload)
            
            if response.status_code in [200, 201]:
                print(f'✓ Stock updated to {stock_quantity}')
                return True
            else:
                print(f'✗ Failed to update stock')
                return False
        except Exception as e:
            print(f'Error updating stock: {e}')
            return False

class AutomationEngine:
    def __init__(self):
        self.takealot = TakealotScraper()
        self.makro = MakroAPI(MAKRO_API_KEY, MAKRO_API_SECRET, MAKRO_SELLER_ID)
        self.processed_plids = set()
    
    def process_products(self):
        """Main automation logic with all add-on requirements"""
        print(f'\n=== Starting product sync at {time.strftime("%Y-%m-%d %H:%M:%S")} ===')
        print(f'Category: {TARGET_CATEGORY}')
        print(f'Min Rating: {MIN_RATING}+ stars')
        print(f'Price Rule: Products <R200 must be ≥R{MIN_PRICE_FOR_CHEAP_PRODUCTS}')
        
        # Search for products
        products = self.takealot.search_products(TARGET_CATEGORY, MAX_PRODUCTS_PER_RUN)
        print(f'Found {len(products)} products matching criteria')
        
        created_count = 0
        skipped_count = 0
        
        for product in products:
            print(f'\nProcessing: {product["title"][:50]}...')
            print(f'  Rating: {product["rating"]} stars')
            print(f'  Takealot Price: R{product["price"]}')
            
            # Skip if already processed
            if product.get('plid') in self.processed_plids:
                print(f'  ⊘ Skipped: Already processed')
                skipped_count += 1
                continue
            
            # Check for duplicates on Makro
            if self.makro.check_duplicate(product['title'], product.get('plid')):
                print(f'  ⊘ Skipped: Duplicate listing')
                skipped_count += 1
                self.processed_plids.add(product.get('plid'))
                continue
            
            # Get detailed information
            details = self.takealot.get_product_details(product['url'])
            
            # Calculate pricing with minimum price rule
            takealot_price = product['price']
            
            # Apply minimum price rule for cheap products
            if takealot_price < 200:
                base_price = max(MIN_PRICE_FOR_CHEAP_PRODUCTS, takealot_price * MARKUP_MULTIPLIER)
                print(f'  Applying min price rule: R{takealot_price} → R{base_price}')
            else:
                base_price = takealot_price * MARKUP_MULTIPLIER
            
            # Apply consumer psychology pricing
            makro_price = apply_charm_pricing(base_price)
            print(f'  Final Makro Price: R{makro_price} (charm pricing applied)')
            
            # Prepare listing data
            listing_data = {
                'fsn': self.makro.get_fsn_from_category(TARGET_CATEGORY),
                'title': product['title'],
                'description': details.get('description', product['title']),
                'mrp': makro_price,
                'selling_price': makro_price,
                'sku': f'TA-{product.get("plid", "UNKNOWN")}-{int(time.time())}',
                'brand': 'Generic',
                'hsn': '',
            }
            
            # Create listing
            result = self.makro.create_listing(listing_data)
            
            if result:
                listing_id = result.get('id') or result.get('listing_id')
                
                # Download and upload images
                if details.get('image_urls') and listing_id:
                    print(f'  Downloading {len(details["image_urls"])} images...')
                    images = self.takealot.download_images(details['image_urls'])
                    
                    if images:
                        print(f'  Uploading {len(images)} images to Makro...')
                        self.makro.upload_images(listing_id, images)
                
                # Update stock to 100
                if listing_id:
                    self.makro.update_stock(listing_id, 100)
                
                created_count += 1
                self.processed_plids.add(product.get('plid'))
                time.sleep(2)  # Rate limiting
            
            # Stop if we hit the limit
            if created_count >= MAX_PRODUCTS_PER_RUN:
                break
        
        print(f'\n=== Summary ===')
        print(f'Created: {created_count} ACTIVE listings')
        print(f'Skipped: {skipped_count}')
        print(f'Stock: 100 units per listing')
        print(f'=================\n')

def main():
    print('Takealot-Makro Automation Starting...')
    print(f'Target Category: {TARGET_CATEGORY}')
    print(f'Minimum Rating: {MIN_RATING}+ stars')
    print(f'Min Price Rule: <R200 → ≥R{MIN_PRICE_FOR_CHEAP_PRODUCTS}')
    print(f'Listing Status: ACTIVE (with stock: 100)')
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
