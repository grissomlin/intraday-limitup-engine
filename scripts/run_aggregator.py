# scripts/run_aggregator.py
import sys
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from markets.tw.aggregator import aggregate  # noqa: E402

payload_path = ROOT / "data" / "cache" / "tw" / "2026-01-18" / "midday.payload.json"
payload = json.loads(payload_path.read_text(encoding="utf-8"))

out = aggregate(payload)

print("ymd =", payload.get("ymd"))
print("ymd_effective =", out.get("ymd_effective"))
print("limitup_count =", len(out.get("limitup", [])))
print("peers_sectors =", len(out.get("peers_by_sector", {})))
print("peers_flat_count =", len(out.get("peers_not_limitup", [])))

out_path = payload_path.with_suffix(".agg.json")
out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
print("✅ written:", out_path)
