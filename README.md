# Jophil Makro Marketplace Automation — safe rebuild

> **TEST VERSION ONLY. NOT DEPLOYED.**
>
> The application produces an offline product shortlist and Makro listing previews.
> **It cannot submit live Makro listings:** `--mode upload` deliberately exits with an error.
> This is a staging rebuild for owner approval, not an autonomous dropshipping robot.

## Critical credential warning

An earlier commit to this public repository included an apparent Makro API key and secret.
Those credentials may still be retrievable from Git history even though this README
no longer displays them. **Immediately revoke/rotate that key pair** in Makro
Developer Access and replace any corresponding Railway environment variables.
Do not paste keys into chat, issues, screenshots, CSV files, or git commits.

## What currently works

1. Read a CSV of product candidates supplied through **authorized, permitted research**.
2. Calculate a proposed **2x purchase price** in South African rand (configurable).
3. Deduct **provided** supplier shipping, Makro fees, fulfillment cost, and returns reserve.
4. Reject unverified resale permission, missing costs, and unavailable supplier stock.
5. Rank products when researchers provide TikTok South Africa, Takealot popularity,
   and Makro competition scores (0–100). These are **researcher-entered signals**,
   not measured sales counts or scraped data.
6. Require explicit **Approved** status, verified Makro product FSN, package
   measurements, location ID and realistic dispatch information before preparing a draft.
7. Export `output/shortlist.csv`, `output/inactive_listing_previews.json` and
   `output/summary.json` for review. No data is sent to Makro in this version.

## What is not implemented yet

- Autonomous TikTok product discovery or fetching Takealot popularity data.
- Live stock feeds or supplier API connections; buyer/seller authorization is required.
- Automatic daily schedules, dashboard, notifications, or persistent inventory syncing.
- Publishing to Makro or introducing products not already in its catalog.
- Order fulfillment and fulfillment timing verification.

Takealot/TikTok pages must not be scraped or copied without relevant permissions.
The seller must confirm sourcing arrangements and Makro's current fulfillment rules.

## How to test

Use Python 3.11+:

```bash
python -m unittest discover -s tests -v
python main.py --input data/example_candidates.csv --output output
```

The three demo records in `data/example_candidates.csv` are **illustrations only**.
They are intentionally unapproved, have no verified stock, and will not produce
Makro listing previews. Results are in the `output/` directory.

For real evaluation, create a copy of the CSV with supplier-approved products and
actual verified cost/stock/fulfillment data. Fill out the following columns:

- Identification: Product ID, Title, Category, Source, Source URL
- Cost: Source Price, Supplier Shipping, Makro Fees, Fulfillment Cost, Returns Reserve
- Eligibility: Supplier Authorized (yes/no), Supplier Stock
- Optional demand scores: TikTok Score, Takealot Score, Competition Score (0-100)
- Makro preview (requires all): Approval (Approved), FSN, FSN Verified (yes),
  Pickup Location ID, Length CM, Width CM, Height CM, Weight KG,
  Dispatch SLA Days, Shipping Provider (SELLER or MAKRO),
  Local Shipping Fee, Zonal Shipping Fee, National Shipping Fee
- Optional: Fragile (yes/no)

Do **not** put credentials into the CSV.

### Pricing

Defaults:
- Markup: 2x source product price
- Minimum **estimated** profit: R50
- Minimum **estimated** margin: 15%

Set `MARKUP_MULTIPLIER`, `MIN_NET_PROFIT_ZAR`, and
`MIN_MARGIN_PERCENT` to change the defaults.

**Example:** A product purchased for R200 is proposed at R400.
After R10 supplier shipping, R50 Makro fees, R20 fulfillment and
R10 returns allowance, estimated net profit is R110 (27.5%).
Real fees, VAT obligations and actual order costs must be verified before selling.

Ranking score = 40% Takealot observation + 30% TikTok observation
+ 20% estimated margin + 10% low-competition score.
Missing data is labeled unknown rather than being invented.

### Makro API

The Makro seller documentation supports creating and updating **listings attached to
existing catalog products** through a verified `product_id` (FSN). It specifies a
maximum of 10 listing records in a request. The code retains a Makro authentication
and inactive-listing client adapter for eventual approved use, but this staging
version has **no executable live-upload path**.

Any prepared payload uses `INACTIVE` and zero inventory. Nothing should be activated
or advertised as available until stock, procurement and dispatch are verified.

## Railway and account safety

Do not point Railway to this testing branch or merge the draft PR without owner approval.
The seller account previously displayed an `ONHOLD` notice; resolve that separately
before any live automation. Do not attempt to bypass account restrictions.

## Tests and status

GitHub Actions runs a syntax check, 12 unit tests, and an offline demo preview.
All tests are designed to run without any live supplier or Makro network requests.

Current staging PR: [View the rebuild](https://github.com/echeyip321-eng/takealot-makro-automation/pull/1)
