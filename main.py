# main.py (intraday-limitup-engine)
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import argparse
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict


# =============================================================================
# Env helpers
# =============================================================================
def is_github_actions() -> bool:
    return os.getenv("GITHUB_ACTIONS", "").lower() == "true"


def parse_bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def parse_int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def parse_float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def today_ymd() -> str:
    tz_tw = timezone(timedelta(hours=8))
    return datetime.now(tz_tw).strftime("%Y-%m-%d")


# =============================================================================
# Time helpers (UTC + fixed offset)
# - Supports fractional offsets like +5.5 (India)
# =============================================================================
DEFAULT_TZ_OFFSETS = {
    # Open movers / global
    "us": -5,     # default Eastern; DST can be overridden by env
    "ca": -5,     # TSX/TSXV (Toronto) uses Eastern
    "uk": 0,      # London
    "au": +10,    # ASX (Sydney) AEST/AEDT; DST can be overridden by env

    # Limit markets
    "tw": +8,
    "cn": +8,
    "jp": +9,
    "kr": +9,

    # New markets
    "in": +5.5,   # India IST = UTC+5:30
    "th": +7,     # Thailand ICT = UTC+7
}


def _tz_offset_hours(market: str) -> float:
    market = (market or "").strip().lower()
    env_name = f"INTRADAY_TZ_OFFSET_{market.upper()}"
    return parse_float_env(env_name, float(DEFAULT_TZ_OFFSETS.get(market, 0.0)))


def _tz_offset_str(hours: float) -> str:
    # hours may be fractional (e.g. 5.5)
    sign = "+" if hours >= 0 else "-"
    ah = abs(float(hours))
    hh = int(ah)
    mm = int(round((ah - hh) * 60))
    # normalize rounding edge case (e.g. 9.999 -> 10:00)
    if mm >= 60:
        hh += 1
        mm -= 60
    return f"{sign}{hh:02d}:{mm:02d}"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def build_time_meta(market: str, *, finished_utc: datetime) -> Dict[str, Any]:
    off_h = _tz_offset_hours(market)
    tz = timezone(timedelta(hours=off_h))
    market_dt = finished_utc.astimezone(tz)

    off_str = _tz_offset_str(off_h)
    return {
        "finished_at_utc": finished_utc.isoformat(timespec="seconds"),
        "market_tz": f"UTC{off_str}",
        # keep both (hours can be float)
        "market_utc_offset_hours": float(off_h),
        "market_utc_offset": off_str,
        "market_finished_at": market_dt.strftime("%Y-%m-%d %H:%M"),
        "market_finished_hm": market_dt.strftime("%H:%M"),
    }


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
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    marker_path.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")


# =============================================================================
# Stock list updater (TW daily)
# =============================================================================
def maybe_update_tw_stock_list(base_dir: Path) -> None:
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
        subprocess.run(["python", str(script_path)], check=True, timeout=timeout_s)
        print("✅ Stock list updated.")
    except subprocess.TimeoutExpired:
        print(f"⚠️ Stock list update timeout after {timeout_s}s (continue).")
    except Exception as e:
        print(f"⚠️ Stock list update failed (continue): {e}")


# =============================================================================
# Main
# =============================================================================
def main():
    from markets.guard import guard_enabled_default
    from markets.runners import RUNNERS

    ap = argparse.ArgumentParser()
    ap.add_argument("--market", default="tw", choices=list(RUNNERS.keys()))
    ap.add_argument("--slot", default="midday")
    ap.add_argument("--asof", default="11:00")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--raw-only", action="store_true")
    ap.add_argument("--allow-nontrading", action="store_true")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--no-refresh-list", action="store_true")
    args = ap.parse_args()

    default_cache = False if is_github_actions() else True
    enable_cache = parse_bool_env("INTRADAY_ENABLE_CACHE", default_cache)
    if args.no_cache:
        enable_cache = False

    ymd = today_ymd()
    base_dir = Path(__file__).resolve().parent
    paths = cache_paths(base_dir, args.market, args.slot, ymd)

    meta: Dict[str, Any] = {
        "market": args.market,
        "slot": args.slot,
        "asof": args.asof,
        "ymd": ymd,
        "enable_cache": enable_cache,
        "github_actions": is_github_actions(),
        "raw_only": bool(args.raw_only),
        "allow_nontrading": bool(args.allow_nontrading),
        "guard_enabled_default": bool(guard_enabled_default()),
    }

    if enable_cache and (not args.force) and should_skip_by_cache(paths["marker"]):
        print(f"⏭️  Skip (cache hit): {paths['marker']}")
        return

    runner = RUNNERS.get(args.market)
    if not runner:
        raise RuntimeError(f"Unknown market: {args.market}")

    started_utc = _now_utc()
    payload = runner(args, base_dir, ymd, meta)
    finished_utc = _now_utc()

    payload.setdefault("market", args.market)
    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    payload.setdefault("meta", {})
    payload["meta"].setdefault("time", {})
    payload["meta"]["time"].update(build_time_meta(args.market, finished_utc=finished_utc))
    payload["meta"]["time"]["started_at_utc"] = started_utc.isoformat(timespec="seconds")
    payload["meta"]["time"]["duration_seconds"] = int(max(0.0, (finished_utc - started_utc).total_seconds()))

    if enable_cache:
        write_marker(paths["marker"], paths["payload"], meta, payload)
        print(f"✅ Cached: {paths['payload']}")
    else:
        print("✅ Done (cache disabled)")
        try:
            print(f"   stats: {json.dumps(payload.get('stats', {}), ensure_ascii=False)}")
        except Exception:
            pass


if __name__ == "__main__":
    main()
