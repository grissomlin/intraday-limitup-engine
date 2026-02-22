# scripts/test_us_local.py
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
import json
from datetime import datetime

# =============================================================================
# 0) 強制把 repo root 加入 sys.path
# =============================================================================
# 檔案位置：repo_root/scripts/test_us_local.py
# parents[1] = repo_root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =============================================================================
# 1) Imports（現在 markets.* 一定找得到）
# =============================================================================
from markets.us.downloader import run_intraday as run_us_intraday
from markets.tw.aggregator import aggregate  # 直接借用 TW 的 open_limit / 興櫃邏輯


# =============================================================================
# 2) Main
# =============================================================================
def main():
    # 使用今天日期
    ymd = datetime.now().strftime("%Y-%m-%d")

    print("🚀 Running US local test")
    print("   ymd =", ymd)
    print("   repo root =", ROOT)

    # -------------------------------------------------------------
    # (1) 產生 US raw snapshot
    #     - snapshot_main = []
    #     - snapshot_open = ALL US stocks (open_limit universe)
    # -------------------------------------------------------------
    raw = run_us_intraday(
        slot="close",   # 對 US 只是命名用
        asof="16:00",   # 顯示用
        ymd=ymd,
    )

    print("📦 RAW snapshot generated")
    print("   snapshot_open_count =", len(raw.get("snapshot_open", [])))

    # -------------------------------------------------------------
    # (2) 套用 TW aggregator（興櫃 / open_limit）
    #     - open_limit_watchlist
    #     - open_limit_sector_summary
    # -------------------------------------------------------------
    payload = aggregate(raw)

    print("🧠 Aggregated with TW open_limit logic")
    print("   stats =", payload.get("stats", {}))

    # -------------------------------------------------------------
    # (3) 輸出到 data/cache/us/YYYY-MM-DD/close.payload.json
    # -------------------------------------------------------------
    out_dir = ROOT / "data" / "cache" / "us" / ymd
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "close.payload.json"
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("✅ wrote:", out_path)


# =============================================================================
# Entry
# =============================================================================
if __name__ == "__main__":
    main()
