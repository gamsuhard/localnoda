from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = PROJECT_ROOT / "tests" / "block06" / "fixtures" / "sample_transfer_record.json"
FILESINK_FIXTURE_PATH = PROJECT_ROOT / "tests" / "block06" / "fixtures" / "sample_transfer_record_filesink.json"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loader.normalizer.tron_usdt_transfer_normalizer import normalize_records


class NormalizerTest(unittest.TestCase):
    def test_normalizes_single_transfer_into_one_event_and_two_legs(self) -> None:
        payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        segment_manifest = {
            "segment_id": "demo-segment-1",
            "record_count": 1,
        }
        events, legs = normalize_records([payload], segment_manifest, load_run_id="demo-run")
        self.assertEqual(len(events), 1)
        self.assertEqual(len(legs), 2)
        event = events[0]
        self.assertEqual(event["block_number"], 54300010)
        self.assertEqual(event["from_address"], "41aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(event["to_address"], "41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        self.assertEqual(event["amount_raw"], 2500000)
        self.assertEqual(event["amount_decimal"], "2.500000")
        self.assertEqual({leg["direction"] for leg in legs}, {"outbound", "inbound"})

    def test_normalizes_real_filesink_shape_into_one_event_and_two_legs(self) -> None:
        payload = json.loads(FILESINK_FIXTURE_PATH.read_text(encoding="utf-8"))
        segment_manifest = {
            "segment_id": "demo-segment-2",
            "record_count": 1,
        }
        events, legs = normalize_records([payload], segment_manifest, load_run_id="demo-run")
        self.assertEqual(len(events), 1)
        self.assertEqual(len(legs), 2)
        event = events[0]
        self.assertEqual(event["block_number"], 54298720)
        self.assertEqual(event["contract_address"], "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
        self.assertEqual(event["from_address"], "4190b1b9b199eb1c45db90f420c831a04b6236ee69")
        self.assertEqual(event["to_address"], "41057cc0a8b19526af883ad8d3ebad043a2aaf7167")
        self.assertEqual(event["amount_raw"], 752000000)
        self.assertEqual(event["amount_decimal"], "752.000000")
        self.assertEqual(event["log_index"], 1)


if __name__ == "__main__":
    unittest.main()
