"""Prepare Makro product candidates from seller-authorized research data.

Default behavior is OFFLINE and READ-ONLY. The application never researches
Takealot by scraping it or uploads listings without explicit opt-in.
"""
import argparse
import csv
import json
import logging
import os
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import base64

LOG = logging.getLogger("makro_agent")
ZAR = Decimal("0.01")
FSN_PATTERN = re.compile(r"^[A-Z0-9]{13,16}$")


def number(value):
    """Read numeric CSV data; missing/invalid values remain unknown."""
    if value is None or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).strip().replace(",", "").replace("R", "").replace("%", ""))
    except InvalidOperation:
        return None


def amount(value):
    value = number(value)
    return value.quantize(ZAR, rounding=ROUND_HALF_UP) if value is not None else None


def yes(value):
    return str(value or "").strip().lower() in ("yes", "true", "1")


def whole(value, minimum=0):
    parsed = number(value)
    if parsed is None or parsed != parsed.to_integral_value() or parsed < minimum:
        return None
    return int(parsed)


def score_field(row, name):
    v = number(row.get(name))
    return float(v) if v is not None and 0 <= v <= 100 else None


def candidate(row, markup=Decimal("2"), minimum_profit=Decimal("50"),
              minimum_margin=Decimal("15")):
    """Evaluate product eligibility without guessing missing costs or demand."""
    title = (row.get("Title") or "").strip()
    ref = (row.get("Product ID") or "").strip()
    source = (row.get("Source URL") or "").strip()
    cost = amount(row.get("Source Price"))
    shipping = amount(row.get("Supplier Shipping"))
    fees = amount(row.get("Makro Fees"))
    fulfillment = amount(row.get("Fulfillment Cost"))
    returns = amount(row.get("Returns Reserve"))
    price = (cost * markup).quantize(ZAR, rounding=ROUND_HALF_UP) if cost is not None else None
    notes = []

    if not ref or not title:
        notes.append("missing product ID/title")
    if not source.startswith(("https://", "http://")):
        notes.append("missing supplier product URL")
    if not yes(row.get("Supplier Authorized")):
        notes.append("resale/fulfillment permission unverified")
    stock = whole(row.get("Supplier Stock"), minimum=0)
    if stock is None:
        notes.append("supplier stock unverified")
    elif stock == 0:
        notes.append("supplier out of stock")
    if cost is None or cost <= 0:
        notes.append("missing/invalid supplier price")
    for label, val in (("supplier shipping", shipping), ("Makro fees", fees),
                       ("fulfillment cost", fulfillment), ("returns reserve", returns)):
        if val is None or val < 0:
            notes.append("missing/invalid " + label)

    profit = None
    margin = None
    if price is not None and price > 0 and all(
            v is not None and v >= 0 for v in (shipping, fees, fulfillment, returns)):
        profit = price - cost - shipping - fees - fulfillment - returns
        margin = (profit / price * 100).quantize(ZAR, rounding=ROUND_HALF_UP)
        if profit < minimum_profit:
            notes.append("estimated profit below threshold")
        if margin < minimum_margin:
            notes.append("estimated margin below threshold")

    tiktok = score_field(row, "TikTok Score")
    takealot = score_field(row, "Takealot Score")
    competition = score_field(row, "Competition Score")
    # Scores are analyst-entered observations, NOT verified sales volumes.
    rank = None
    if all(v is not None for v in (tiktok, takealot, competition, margin)):
        rank = round(.3 * tiktok + .4 * takealot +
                     .2 * min(100, max(0, float(margin))) +
                     .1 * (100 - competition), 1)

    return {
        "product_id": ref, "title": title, "category": row.get("Category", ""),
        "source": row.get("Source", ""), "source_url": source,
        "source_price_zar": str(cost) if cost is not None else "",
        "selling_price_zar": str(price) if price is not None else "",
        "estimated_profit_zar": str(profit) if profit is not None else "",
        "estimated_margin_pct": str(margin) if margin is not None else "",
        "demand_score": rank if rank is not None else "",
        "status": "QUALIFIED" if not notes else "NEEDS_REVIEW",
        "issues": "; ".join(notes),
    }


def listing_payload(row, review):
    """Only a manually approved, qualified row can become an INACTIVE preview."""
    if review["status"] != "QUALIFIED" or (row.get("Approval") or "").strip().lower() != "approved":
        return None, "not qualified and explicitly approved"
    fsn = (row.get("FSN") or "").strip()
    if not FSN_PATTERN.fullmatch(fsn) or not yes(row.get("FSN Verified")):
        return None, "exact Makro catalog FSN has not been verified"
    location = (row.get("Pickup Location ID") or "").strip()
    if not location:
        return None, "pickup location ID missing"
    dimensions = [whole(row.get(k), 1) for k in ("Length CM", "Width CM", "Height CM")]
    weight = number(row.get("Weight KG"))
    sla = whole(row.get("Dispatch SLA Days"), 1)
    if any(x is None for x in dimensions) or weight is None or weight <= 0 or sla is None:
        return None, "real package dimensions, weight or dispatch SLA missing"
    provider = (row.get("Shipping Provider") or "").strip().upper()
    if provider not in ("SELLER", "MAKRO"):
        return None, "shipping provider missing or unsupported"
    shipping_fees = [whole(row.get(k)) for k in
                     ("Local Shipping Fee", "Zonal Shipping Fee", "National Shipping Fee")]
    if any(x is None for x in shipping_fees):
        return None, "Makro shipping fees missing"
    sku = review["product_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,60}", sku):
        return None, "product ID cannot be used as a Makro SKU"
    price = float(Decimal(review["selling_price_zar"]))
    payload = {
        "product_id": fsn,
        "listing_status": "INACTIVE",
        "sku_id": sku,
        "selling_region_pref": "National",
        "min_oq": 1,
        "max_oq": 1,
        "price": {"base_price": price, "selling_price": price, "currency": "ZAR"},
        "shipping_fees": dict(zip(("local", "zonal", "national"), shipping_fees)),
        "fulfillment_profile": "NON_FBM",
        "fulfillment": {"dispatch_sla": sla, "shipping_provider": provider,
                        "procurement_type": "REGULAR"},
        "packages": [{"name": "seller_verified",
                      "dimensions": dict(zip(("length", "breadth", "height"), dimensions)),
                      "weight": float(weight),
                      "handling": {"fragile": yes(row.get("Fragile"))}}],
        # Only use seller-verified physical inventory. Never advertise inferred stock.
        "locations": {"id": location, "status": "Active", "inventory": 0},
    }
    return payload, ""


def read_candidates(path):
    with open(path, newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def prepare(rows, markup=Decimal("2"), min_profit=Decimal("50"),
            min_margin=Decimal("15")):
    results, drafts = [], []
    seen_skus, seen_fsns = set(), set()
    for row in rows:
        review = candidate(row, markup, min_profit, min_margin)
        if review["product_id"] in seen_skus:
            review["status"] = "NEEDS_REVIEW"
            review["issues"] += "; duplicate product ID"
        seen_skus.add(review["product_id"])
        payload, reason = listing_payload(row, review)
        if payload and payload["product_id"] in seen_fsns:
            payload, reason = None, "duplicate Makro FSN"
        if payload:
            seen_fsns.add(payload["product_id"])
            drafts.append(payload)
        review["listing_preview"] = "READY" if payload else "BLOCKED: " + reason
        results.append(review)
    results.sort(key=lambda x: (x["demand_score"] == "",
                                -(float(x["demand_score"]) if x["demand_score"] != "" else -1),
                                x["title"]))
    return results, drafts


def save(results, drafts, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    with (outdir / "shortlist.csv").open("w", newline="", encoding="utf-8") as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=list(results[0]))
            writer.writeheader()
            writer.writerows(results)
        else:
            f.write("product_id,title,status,issues\n")
    # A PREVIEW only; this file is NOT transmitted to Makro.
    (outdir / "inactive_listing_previews.json").write_text(
        json.dumps({"listing_records": drafts}, indent=2), encoding="utf-8")
    summary = {
        "researched": len(results),
        "qualified": sum(r["status"] == "QUALIFIED" for r in results),
        "approved_inactive_previews": len(drafts),
        "live_posts": 0
    }
    (outdir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


class MakroClient:
    """Connection adapter. Never invoked by normal prep mode or automated tests."""

    def __init__(self, app_id, secret):
        if not app_id or not secret:
            raise ValueError("Makro API credentials must be supplied securely")
        self.app_id, self.secret = app_id, secret

    def token(self):
        auth = base64.b64encode(f"{self.app_id}:{self.secret}".encode()).decode()
        req = Request(
            "https://seller.makro.co.za/api/oauth-service/oauth/token?" +
            urlencode({"grant_type": "client_credentials", "scope": "Seller_Api"}),
            headers={"Authorization": "Basic " + auth},
            method="GET",
        )
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())["access_token"]

    def upload_inactive(self, records, token):
        if any(r.get("listing_status") != "INACTIVE" or
               r.get("locations", {}).get("inventory") != 0 for r in records):
            raise ValueError("Only inactive, zero-inventory listings are permitted")
        if not records or len(records) > 10:
            raise ValueError("Makro batches must contain 1-10 listings")
        req = Request(
            "https://seller.makro.co.za/api/listings/v5/",
            data=json.dumps({"listing_records": records}).encode(),
            headers={"Authorization": "Bearer " + token,
                     "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline Makro listing preparation")
    parser.add_argument("--input", default="data/example_candidates.csv")
    parser.add_argument("--output", default="output")
    parser.add_argument("--mode", choices=("prepare", "upload"), default="prepare")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.mode == "upload":
        # Deliberately locked until owner separately approves deployment/publishing.
        raise SystemExit("LIVE UPLOAD DISABLED. Request a separate release approval.")

    markup = number(os.getenv("MARKUP_MULTIPLIER", "2"))
    min_profit = number(os.getenv("MIN_NET_PROFIT_ZAR", "50"))
    min_margin = number(os.getenv("MIN_MARGIN_PERCENT", "15"))
    if any(v is None or v <= 0 for v in (markup, min_profit, min_margin)):
        raise SystemExit("Invalid pricing configuration")
    rows = read_candidates(args.input)
    results, drafts = prepare(rows, markup, min_profit, min_margin)
    summary = save(results, drafts, Path(args.output))
    LOG.info("Offline preview finished: %s", summary)
    return summary


if __name__ == "__main__":
    main()
