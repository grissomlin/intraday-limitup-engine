# main.py (intraday-limitup-engine)
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# Env helpers
# =============================================================================
def is_github_actions() -> bool:
    return os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true"


def parse_bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


# -----------------------------------------------------------------------------
# Market timezone (for ymd/asof correctness across regions)
# -----------------------------------------------------------------------------
MARKET_TZ: Dict[str, str] = {
    "tw": "Asia/Taipei",
    "cn": "Asia/Shanghai",
    "jp": "Asia/Tokyo",
    "kr": "Asia/Seoul",
    "th": "Asia/Bangkok",
    "us": "America/New_York",
    "ca": "America/Toronto",
    "uk": "Europe/London",
    "au": "Australia/Sydney",
    "in": "Asia/Kolkata",
}


def today_ymd_market(market: str) -> str:
    """
    Compute YYYY-MM-DD in market local timezone (when zoneinfo available).
    Fallback to system local time if zoneinfo missing.
    """
    m = (market or "").strip().lower()
    tzname = MARKET_TZ.get(m, "Asia/Taipei")

    if ZoneInfo is None:
        return datetime.now().strftime("%Y-%m-%d")

    try:
        return datetime.now(ZoneInfo(tzname)).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


# =============================================================================
# Cache helpers
# =============================================================================
def cache_paths(base_dir: Path, market: str, slot: str, ymd: str) -> dict:
    cache_dir = base_dir / "data" / "cache" / market / ymd
    cache_dir.mkdir(parents=True, exist_ok=True)
    return {
        "marker": cache_dir / f"{slot}.done.json",
        "payload": cache_dir / f"{slot}.payload.json",
    }


def should_skip_by_cache(marker_path: Path) -> bool:
    return marker_path.exists()


def write_marker(marker_path: Path, payload_path: Path, meta: dict, payload: dict) -> None:
    marker = {
        "meta": meta,
        "payload_path": str(payload_path),
        "written_at": datetime.now().isoformat(timespec="seconds"),
    }
    payload_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    marker_path.write_text(
        json.dumps(marker, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# =============================================================================
# Stock list updater (daily) - TW only
# =============================================================================
def maybe_update_stock_list_tw(base_dir: Path) -> None:
    auto_update = parse_bool_env("TW_AUTO_UPDATE_STOCKLIST", True)
    if not auto_update:
        return

    script_path = (base_dir / "scripts" / "update_tw_stock_list.py").resolve()
    if not script_path.exists():
        print(f"⚠️ TW_AUTO_UPDATE_STOCKLIST=1 but missing script: {script_path}")
        print("   (continue without updating stock list)")
        return

    timeout_s = int(os.getenv("TW_STOCKLIST_UPDATE_TIMEOUT", "120"))

    try:
        print(f"🔄 Updating tw_stock_list.json via {script_path.name} ...")
        subprocess.run(
            ["python", str(script_path)],
            check=True,
            timeout=timeout_s,
        )
        print("✅ Stock list updated.")
    except subprocess.TimeoutExpired:
        print(f"⚠️ Stock list update timeout after {timeout_s}s (continue).")
    except Exception as e:
        print(f"⚠️ Stock list update failed (continue): {e}")


# -----------------------------------------------------------------------------
# Backward-compat alias for markets/runners.py
# (it imports: from main import parse_bool_env, maybe_update_tw_stock_list)
# -----------------------------------------------------------------------------
def maybe_update_tw_stock_list(base_dir: Path) -> None:
    return maybe_update_stock_list_tw(base_dir)


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    # Import runner registry (lazy import to avoid heavy deps at module import)
    from markets.runners import RUNNERS  # type: ignore

    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--slot",
        choices=["open", "midday", "close"],
        default="midday",
        help="open=開盤快照；midday=盤中快照；close=收盤快照",
    )
    ap.add_argument(
        "--asof",
        default="11:00",
        help="只作為顯示/記錄用途，例如 11:00、13:45",
    )

    # ✅ allow external override ymd (useful for CI/testing)
    ap.add_argument(
        "--ymd",
        default="",
        help="交易日 YYYY-MM-DD（留空則用市場時區的今天）",
    )

    ap.add_argument("--no-cache", action="store_true", help="強制停用快取（測試用）")
    ap.add_argument("--force", action="store_true", help="忽略快取，強制重跑（測試用）")

    ap.add_argument(
        "--market",
        default="tw",
        choices=sorted(RUNNERS.keys()),
        help=f"市場：{', '.join(sorted(RUNNERS.keys()))}",
    )

    # Debug：只輸出 raw snapshot
    ap.add_argument("--raw-only", action="store_true", help="只跑 downloader，不跑 aggregator")

    # Runners may use these (open movers markets)
    ap.add_argument("--start", default=None, help="sync 起始日 (YYYY-MM-DD，可選)")
    ap.add_argument("--end", default=None, help="sync 結束日 (YYYY-MM-DD，可選)")
    ap.add_argument("--no-refresh-list", action="store_true", help="sync 時不要刷新股票清單（可選）")

    # Trading guard control
    ap.add_argument(
        "--allow-nontrading",
        action="store_true",
        help="允許非交易日/非交易時段也產出（不 raise）",
    )

    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Cache policy
    # ------------------------------------------------------------------
    default_cache = False if is_github_actions() else True
    enable_cache = parse_bool_env("INTRADAY_ENABLE_CACHE", default_cache)
    if args.no_cache:
        enable_cache = False

    # ymd
    ymd = (args.ymd or "").strip()
    if not ymd:
        ymd = today_ymd_market(args.market)

    base_dir = Path(__file__).resolve().parent
    paths = cache_paths(base_dir, args.market, args.slot, ymd)

    # market-specific test_mode（沿用你原本 TW env，其他市場也能用）
    test_mode = parse_bool_env("TW_TEST_MODE", False)

    meta: Dict[str, Any] = {
        "market": args.market,
        "slot": args.slot,
        "asof": args.asof,
        "ymd": ymd,
        "enable_cache": enable_cache,
        "github_actions": is_github_actions(),
        "raw_only": bool(args.raw_only),
        "test_mode": test_mode,
    }

    # ------------------------------------------------------------------
    # 1) Cache skip
    # ------------------------------------------------------------------
    if enable_cache and (not args.force) and should_skip_by_cache(paths["marker"]):
        print(f"⏭️  Skip (cache hit): {paths['marker']}")
        print("    用 --force 強制重跑，或 --no-cache 停用快取")
        return 0

    # ------------------------------------------------------------------
    # 1.5) Update stock list (daily) - TW only
    # ------------------------------------------------------------------
    if args.market == "tw":
        maybe_update_stock_list_tw(base_dir)

    # ------------------------------------------------------------------
    # 2) Run via runner registry
    # ------------------------------------------------------------------
    runner = RUNNERS.get(args.market)
    if runner is None:
        raise ValueError(f"Unsupported market: {args.market}")

    print(f"🚀 Run market runner: {args.market} | {meta}")
    payload = runner(args, base_dir, ymd, meta)

    # ------------------------------------------------------------------
    # 3) Always write payload file (run_shorts depends on it)
    # ------------------------------------------------------------------
    paths["payload"].write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"✅ Wrote payload: {paths['payload']}")

    # ------------------------------------------------------------------
    # 3.1) Write marker only when cache enabled
    # ------------------------------------------------------------------
    if enable_cache:
        marker = {
            "meta": meta,
            "payload_path": str(paths["payload"]),
            "written_at": datetime.now().isoformat(timespec="seconds"),
        }
        paths["marker"].write_text(
            json.dumps(marker, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"✅ Wrote marker: {paths['marker']}")
    else:
        print("✅ Done (cache disabled; marker not written)")
        try:
            print(f"   stats: {json.dumps(payload.get('stats', {}), ensure_ascii=False)}")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
