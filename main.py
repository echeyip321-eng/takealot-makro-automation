import requests
import time
import os
import re
import schedule
import json
from datetime import datetime
from bs4 import BeautifulSoup
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enhanced Configuration
MIN_PROFIT_MARGIN = float(os.getenv('MIN_PROFIT_MARGIN', '0.15'))  # 15% minimum profit
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # seconds
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', '2'))  # seconds between requests

# Webhook for notifications (optional)
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')  # For Slack/Discord notifications


class NotificationService:
    """Sends notifications for critical events"""
    
    @staticmethod
    def send_webhook(message, level='info'):
        """Send webhook notification"""
        if not WEBHOOK_URL:
            return
            
        try:
            payload = {
                'text': f"[{level.upper()}] {message}",
                'timestamp': datetime.now().isoformat()
            }
            requests.post(WEBHOOK_URL, json=payload, timeout=5)
        except:
            pass
    
    @staticmethod
    def notify_success(count, category):
        """Notify successful listings"""
        message = f"✅ Successfully listed {count} products in {category}"
        logger.info(message)
        NotificationService.send_webhook(message, 'success')
    
    @staticmethod
    def notify_error(error_msg):
        """Notify critical errors"""
        message = f"🚨 Critical Error: {error_msg}"
        logger.error(message)
        NotificationService.send_webhook(message, 'error')


class ProfitCalculator:
    """Calculate and validate profit margins"""
    
    @staticmethod
    def calculate_margin(cost_price, selling_price):
            """Calculate and validate profit margins with Makro fees included"""
    
    # Makro fee structure (from official documentation)
    PLATFORM_FEE_MONTHLY = 230  # R230/month (prorated per product for calculation)
    
    # Commission fees by category (percentage of selling price)
    COMMISSION_RATES = {
        'default': 0.12,  # 12% default
        'appliances': 0.10,
        'electronics': 0.08,
        'home_garden': 0.12,
        'sports': 0.12,
        'toys': 0.12,
        'health_beauty': 0.10,
        'books': 0.08,
        'baby': 0.10
    }
    
    # Transport fees based on weight (in grams) and distance
    # Using average rates for calculation
    @staticmethod
    def estimate_transport_fee(weight_grams=1000):
        """Estimate transport fee based on product weight"""
        # Weight brackets (in grams) with average fees
        if weight_grams <= 1000:
            return 35  # Small: ~R35 average
        elif weight_grams <= 15000:
            return 50  # Medium: ~R50 average  
        elif weight_grams <= 30000:
            return 75  # Medium: ~R75 average
        elif weight_grams <= 60000:
            return 100  # Large: ~R100 average
        elif weight_grams <= 150000:
            return 135  # X-Large: ~R135 average
        else:
            return 200  # Bulky/Oversized: ~R200 average

            @staticmethod
    def calculate_makro_fees(selling_price, category='default', weight_grams=1000, monthly_sales=30):
        """Calculate all Makro fees for a single product"""
        # 1. Commission fee (percentage of selling price)
        commission_rate = ProfitCalculator.COMMISSION_RATES.get(category, 0.12)
        commission_fee = selling_price * commission_rate
        
        # 2. Transport fee (based on weight)
        transport_fee = ProfitCalculator.estimate_transport_fee(weight_grams)
        
        # 3. Platform fee (prorated per product based on estimated monthly sales)
        # If you sell 30 products/month, each product bears R230/30 = R7.67
        platform_fee_per_product = ProfitCalculator.PLATFORM_FEE_MONTHLY / max(monthly_sales, 1)
        
        total_fees = commission_fee + transport_fee + platform_fee_per_product
        
        return {
            'commission': commission_fee,
            'transport': transport_fee,
            'platform_prorated': platform_fee_per_product,
            'total': total_fees,
            'breakdown': f"Commission: R{commission_fee:.2f} + Transport: R{transport_fee:.2f} + Platform: R{platform_fee_per_product:.2f}"
        }


        """Calculate profit margin percentage"""
        if cost_price <= 0:
            return 0
        return ((selling_price - cost_price) / cost_price) * 100
    
    @staticmethod
    def calculate_optimal_price(takealot_price, min_margin=MIN_PROFIT_MARGIN):
        """Calculate optimal selling price with minimum margin"""
        # Takealot price includes their margin, treat as cost
        target_price = takealot_price * (1 + min_margin)
                
        # Calculate Makro fees to ensure we meet minimum profit AFTER fees
        # We need to work backwards: final_profit = selling_price - cost - makro_fees
        # Iterative approach to find optimal price that meets profit margin after fees
        
        attempt_price = target_price
        for _ in range(10):  # Max 10 iterations to find optimal price
            fees = ProfitCalculator.calculate_makro_fees(attempt_price)
            net_profit = attempt_price - takealot_price - fees['total']
            actual_margin = (net_profit / takealot_price) if takealot_price > 0 else 0
            
            if actual_margin >= min_margin:
                target_price = attempt_price
                break
            
            # Increase price to compensate for fees
            attempt_price = takealot_price * (1 + min_margin) + fees['total']
        
        
        # Apply charm pricing
        if target_price < 100:
            return round(target_price - 0.01, 2)
        elif target_price < 1000:
            # Round to .99
            return int(target_price) + 0.99
        else:
            # Round to nearest 9.99
            return (int(target_price / 10) * 10) + 9.99
    
    @staticmethod
    def is_profitable(takealot_price, makro_price, min_margin=MIN_PROFIT_MARGIN):
        """Check if listing meets minimum profit requirement"""
        margin = ProfitCalculator.calculate_margin(takealot_price, makro_price)
        return margin >= (min_margin * 100)


class RetryHandler:
    """Handle retries with exponential backoff"""
    
    @staticmethod
    def retry_with_backoff(func, *args, max_attempts=MAX_RETRY_ATTEMPTS, **kwargs):
        """Retry function with exponential backoff"""
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Failed after {max_attempts} attempts: {str(e)}")
                    raise
                
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s...")
                time.sleep(wait_time)


class CompetitionMonitor:
    """Monitor and analyze competition on Makro"""
    
    def __init__(self, makro_api):
        self.makro = makro_api
        self.competition_data = {}
    
    def check_existing_listings(self, product_title):
        """Check if similar products exist and their prices"""
        try:
            # Search Makro for similar products
            search_results = self.makro.search_marketplace(product_title)
            
            if search_results:
                prices = [p.get('price', 0) for p in search_results]
                return {
                    'has_competition': True,
                    'competitor_count': len(search_results),
                    'lowest_price': min(prices) if prices else 0,
                    'average_price': sum(prices) / len(prices) if prices else 0
                }
            
            return {'has_competition': False}
        except:
            return {'has_competition': False}
    
    def adjust_price_for_competition(self, base_price, competition_data):
        """Adjust price based on competition"""
        if not competition_data.get('has_competition'):
            return base_price
        
        lowest = competition_data.get('lowest_price', 0)
        if lowest > 0:
            # Price slightly below lowest competitor (2-5%)
            competitive_price = lowest * 0.97
            return max(competitive_price, base_price * 0.9)  # Don't go below 90% of base
        
        return base_price


class PerformanceTracker:
    """Track and log performance metrics"""
    
    def __init__(self):
        self.metrics = {
            'total_processed': 0,
            'successful_listings': 0,
            'failed_listings': 0,
            'total_profit_potential': 0.0,
            'categories_processed': set(),
            'start_time': datetime.now()
        }
    
    def log_success(self, product, profit_margin):
        """Log successful listing"""
        self.metrics['successful_listings'] += 1
        self.metrics['total_processed'] += 1
        self.metrics['total_profit_potential'] += profit_margin
        logger.info(f"✓ Listed: {product['title'][:50]}... | Margin: {profit_margin:.1f}%")
    
    def log_failure(self, product, reason):
        """Log failed listing"""
        self.metrics['failed_listings'] += 1
        self.metrics['total_processed'] += 1
        logger.warning(f"✗ Skipped: {product.get('title', 'Unknown')[:50]}... | Reason: {reason}")
    
    def get_summary(self):
        """Get performance summary"""
        runtime = (datetime.now() - self.metrics['start_time']).total_seconds() / 60
        
        return {
            'runtime_minutes': round(runtime, 2),
            'success_rate': (self.metrics['successful_listings'] / max(self.metrics['total_processed'], 1)) * 100,
            **self.metrics
        }
    
    def print_summary(self):
        """Print performance summary"""
        summary = self.get_summary()
        logger.info(f"\n{'='*50}")
        logger.info(f"PERFORMANCE SUMMARY")
        logger.info(f"{'='*50}")
        logger.info(f"Runtime: {summary['runtime_minutes']:.1f} minutes")
        logger.info(f"Total Processed: {summary['total_processed']}")
        logger.info(f"Successful: {summary['successful_listings']}")
        logger.info(f"Failed: {summary['failed_listings']}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        logger.info(f"Avg Profit Margin: {summary['total_profit_potential']:.1f}%")
        logger.info(f"{'='*50}\n")



class SATrendAnalyzer:
    """Analyzes South African market trends on Takealot"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.trending_categories = []
        self.seasonal_products = {}
    
    def get_trending_products(self):
        """Scrape Takealot's trending/best sellers section"""
        trending_urls = [
            'https://www.takealot.com/best-sellers',
            'https://www.takealot.com/deals/daily-deals'
        ]
        
        trending_products = []
        
        for url in trending_urls:
            try:
                response = self.session.get(url)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract trending product categories
                category_links = soup.find_all('a', class_='category-link')
                for link in category_links[:10]:  # Top 10 trending categories
                    cat_name = link.get_text(strip=True)
                    if cat_name not in self.trending_categories:
                        self.trending_categories.append(cat_name)
                
                # Extract best-selling products
                product_cards = soup.find_all('div', class_='product-card', limit=20)
                for card in product_cards:
                    try:
                        title_elem = card.find('h3', class_='product-title')
                        price_elem = card.find('span', class_='currency-value')
                        
                        if title_elem and price_elem:
                            price = float(price_elem.get_text(strip=True).replace(',', '').replace('R', ''))
                            trending_products.append({
                                'title': title_elem.get_text(strip=True),
                                'price': price,
                                'source': 'trending'
                            })
                    except:
                        continue
            except Exception as e:
                print(f'Error fetching trending: {e}')
        
        return trending_products
    
    def analyze_seasonal_trends(self):
        """Determine what products are hot based on SA seasons"""
        import datetime
        month = datetime.datetime.now().month
        
        # South African seasons (opposite to Northern Hemisphere)
        if month in [12, 1, 2]:  # Summer
            return ['Fans', 'Portable Coolers', 'Swimming Pools', 'Camping Gear', 'Braai Equipment', 'Outdoor Furniture']
        elif month in [3, 4, 5]:  # Autumn
            return ['Heaters', 'Blankets', 'Indoor Appliances', 'Back to School', 'Home Office']
        elif month in [6, 7, 8]:  # Winter
            return ['Heaters', 'Electric Blankets', 'Indoor Heating', 'Winter Sports', 'Indoor Entertainment', 'Coffee Makers']
        else:  # Spring (9, 10, 11)
            return ['Garden Tools', 'Outdoor Equipment', 'Spring Cleaning', 'Fitness Equipment', 'Bicycle']
    
    def get_sa_market_sweet_spots(self):
        """Price points that sell well in South African market"""
        return [
            {'range': (100, 299), 'psychology': 'Impulse buy', 'charm': 299},
            {'range': (300, 799), 'psychology': 'Sweet spot - most popular', 'charm': 499},
            {'range': (800, 1499), 'psychology': 'Considered purchase', 'charm': 999},
            {'range': (1500, 2999), 'psychology': 'Premium segment', 'charm': 1999},
        ]
    
    def score_product_viability(self, product, takealot_competition_count):
        """Score product based on SA market factors"""
        score = 0
        
        # Check if in trending categories
        for cat in self.trending_categories:
            if cat.lower() in product['title'].lower():
                score += 30
                break
        
        # Check seasonal relevance
        seasonal = self.analyze_seasonal_trends()
        for season_cat in seasonal:
            if season_cat.lower() in product['title'].lower():
                score += 25
                break
        
        # Price point analysis
        price = product.get('price', 0)
        sweet_spots = self.get_sa_market_sweet_spots()
        for spot in sweet_spots:
            if spot['range'][0] <= price <= spot['range'][1]:
                if 'Sweet spot' in spot['psychology']:
                    score += 20
                else:
                    score += 10
                break
        
        # Rating boost
        if product.get('rating', 0) >= 4.5:
            score += 15
        elif product.get('rating', 0) >= 4.0:
            score += 10
        
        # Competition penalty
        if takealot_competition_count > 50:
            score -= 20
        elif takealot_competition_count > 20:
            score -= 10
        
        return score
    
    def recommend_best_categories(self):
        """Get recommended categories to focus on"""
        seasonal = self.analyze_seasonal_trends()
        
        recommendations = {
            'seasonal_hot': seasonal,
            'trending': self.trending_categories[:5],
            'evergreen': ['Air Fryers', 'Power Tools', 'Kitchen Appliances', 'Phone Accessories', 'Home Security'],
            'sa_specific': ['Load Shedding Solutions', 'Solar Products', 'Power Banks', 'Generators', 'Inverters']
        }
        
        return recommendations
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
