# Takealot-Makro Automation

Automated product arbitrage system that scrapes products from Takealot and creates listings on Makro Marketplace via API.

## Features

- **Automated Product Discovery**: Scrapes Takealot for products in target category (Air Fryers)
- **Smart Pricing**: Applies 2.8x markup automatically for optimal margins
- **Duplicate Detection**: Checks existing Makro listings to avoid duplicates
- **Draft Mode**: Creates listings as drafts (INACTIVE) for manual photo upload
- **Scheduled Sync**: Runs every 10 minutes to continuously find new products
- **API Integration**: Uses Makro Marketplace API for reliable listing creation

## Configuration

The system is pre-configured with:
- **Category**: Air Fryers
- **Markup**: 2.8x
- **Products per run**: 10
- **Sync frequency**: Every 10 minutes
- **Manufacturer/Packer**: N/A (as required)

## Setup on Railway

### 1. Deploy to Railway

1. Go to [Railway](https://railway.app)
2. Click "New Project" → "Deploy from GitHub repo"
3. Connect your GitHub account and select `echeyip321-eng/takealot-makro-automation`
4. Railway will automatically detect the Python project

### 2. Configure Environment Variables

Add these variables in Railway dashboard:

```
MAKRO_API_KEY=ff05c866-2a98-4f55-b5f0-6a92e40f8e93
MAKRO_API_SECRET=6e18b3ec-be5d-46e3-ab3e-28d8f6b8fb3a
```

### 3. Set Start Command

In Railway settings, set the start command:
```
python main.py
```

### 4. Deploy

Railway will:
- Install dependencies from `requirements.txt`
- Start the automation
- Keep it running 24/7

## How It Works

1. **Search Takealot**: Finds Air Fryer products
2. **Extract Details**: Gets title, price, product URL
3. **Check Duplicates**: Verifies product doesn't exist on Makro
4. **Calculate Price**: Applies 2.8x markup to Takealot price
5. **Create Listing**: Posts to Makro API as INACTIVE (draft)
6. **Manual Photos**: You add photos and activate listing
7. **Repeat**: Runs every 10 minutes automatically

## Product Details Created

- **Title**: From Takealot
- **Description**: From Takealot
- **Price (MRP & Selling)**: Takealot price × 2.8
- **SKU**: Auto-generated (TA-{PLID}-{timestamp})
- **Brand**: Generic
- **Stock**: 0 (no inventory initially)
- **Status**: INACTIVE (draft for photo upload)
- **Manufacturer**: N/A
- **Packer**: N/A

## Monitoring

The system logs:
- Products found on Takealot
- Duplicates skipped
- Listings created successfully
- API errors
- Sync schedule

## Workflow

1. System finds new Air Fryer on Takealot
2. Creates draft listing on Makro
3. You receive notification (check Makro dashboard)
4. Open Takealot product page (kept open in tabs)
5. Download photos from Takealot
6. Upload to Makro draft listing
7. Activate listing
8. Profit!

## Customization

To change settings, edit `main.py`:

```python
TARGET_CATEGORY = 'Air Fryers'  # Change category
MARKUP_MULTIPLIER = 2.8          # Change markup
MAX_PRODUCTS_PER_RUN = 10        # Products per sync
```

## Support

API Credentials: From Makro Developer Access
Seller ID: 2303
API Base URL: https://api.makromarketplace.co.za/rest/v2/

---

**Status**: Ready for Railway deployment
**Goal**: 20-50 products/day automated
