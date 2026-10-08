"""Offline tests. No Makro or supplier network calls are made."""
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import main


def sample(**updates):
    row = {
        "Product ID": "JOP-TEST001", "Title": "Example Storage Organizer",
        "Category": "Home", "Source": "Approved wholesale test source",
        "Source URL": "https://example.org/product",
        "Source Price": "200", "Supplier Shipping": "10",
        "Makro Fees": "50", "Fulfillment Cost": "20",
        "Returns Reserve": "10", "Supplier Authorized": "yes",
        "Supplier Stock": "5", "TikTok Score": "80",
        "Takealot Score": "70", "Competition Score": "20",
        "Approval": "Approved", "FSN": "CKSH5HYVPX2WXFXH",
        "FSN Verified": "yes", "Pickup Location ID": "LOC-TEST",
        "Length CM": "25", "Width CM": "20", "Height CM": "10",
        "Weight KG": "0.8", "Dispatch SLA Days": "3",
        "Shipping Provider": "SELLER",
        "Local Shipping Fee": "12", "Zonal Shipping Fee": "15",
        "National Shipping Fee": "20",
    }
    row.update(updates)
    return row


class TestAgent(unittest.TestCase):
    def test_twice_price_and_net_profit(self):
        result = main.candidate(sample())
        self.assertEqual(result["selling_price_zar"], "400.00")
        self.assertEqual(result["estimated_profit_zar"], "110.00")
        self.assertEqual(result["estimated_margin_pct"], "27.50")
        self.assertEqual(result["status"], "QUALIFIED")

    def test_score_uses_verified_fields_without_fake_sales(self):
        result = main.candidate(sample())
        self.assertEqual(result["demand_score"], 65.5)

    def test_unverified_supplier_blocked(self):
        result, drafts = main.prepare([sample(**{"Supplier Authorized": "no"})])
        self.assertEqual(drafts, [])
        self.assertIn("permission unverified", result[0]["issues"])

    def test_missing_costs_not_assumed_zero(self):
        result = main.candidate(sample(**{"Makro Fees": ""}))
        self.assertEqual(result["estimated_profit_zar"], "")
        self.assertEqual(result["status"], "NEEDS_REVIEW")

    def test_out_of_stock_blocked(self):
        result, drafts = main.prepare([sample(**{"Supplier Stock": "0"})])
        self.assertEqual(drafts, [])

    def test_unapproved_blocked(self):
        results, drafts = main.prepare([sample(**{"Approval": "Candidate"})])
        self.assertEqual(drafts, [])
        self.assertIn("BLOCKED", results[0]["listing_preview"])

    def test_fsn_must_be_verified(self):
        results, drafts = main.prepare([sample(**{"FSN Verified": "no"})])
        self.assertFalse(drafts)
        self.assertIn("FSN", results[0]["listing_preview"])

    def test_dimensions_required(self):
        results, drafts = main.prepare([sample(**{"Weight KG": ""})])
        self.assertFalse(drafts)

    def test_good_listing_is_inactive_without_claimed_inventory(self):
        _, drafts = main.prepare([sample()])
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["listing_status"], "INACTIVE")
        self.assertEqual(drafts[0]["locations"]["inventory"], 0)
        self.assertEqual(drafts[0]["fulfillment"]["dispatch_sla"], 3)

    def test_duplicate_fsn_does_not_create_two_previews(self):
        _, drafts = main.prepare([sample(), sample(**{"Product ID": "JOP-TEST002"})])
        self.assertEqual(len(drafts), 1)

    def test_disallows_upload_even_with_credentials(self):
        with patch.object(main, "urlopen", side_effect=AssertionError("Network called")):
            with self.assertRaises(SystemExit) as ctx:
                main.main(["--mode", "upload"])
        self.assertIn("DISABLED", str(ctx.exception))

    def test_outputs_are_only_local_files(self):
        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory)
            file = dest / "input.csv"
            with file.open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(sample()))
                writer.writeheader()
                writer.writerow(sample())
            with patch.object(main, "urlopen", side_effect=AssertionError("Network called")):
                summary = main.main(["--mode", "prepare", "--input", str(file),
                                     "--output", str(dest / "out")])
            self.assertEqual(summary["approved_inactive_previews"], 1)
            data = json.loads((dest / "out" / "inactive_listing_previews.json").read_text())
            self.assertEqual(data["listing_records"][0]["listing_status"], "INACTIVE")


if __name__ == "__main__":
    unittest.main()
