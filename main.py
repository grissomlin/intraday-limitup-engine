# main.py (intraday-limitup-engine)
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import argparse
import subprocess
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from markets.timekit import (
    market_today_ymd,
    build_market_time_meta,
)


# =============================================================================
# Env helpers
# =============================================================================
def is_github_actions() -> bool:
    v = (os.getenv("GITHUB_ACTIONS", "") or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


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


def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# Debug helpers
# =============================================================================
def _debug_enabled(cli_no_debug: bool) -> bool:
    # Default ON (your request). Can disable by --no-debug or INTRADAY_DEBUG=0.
    if cli_no_debug:
        return False
    return parse_bool_env("INTRADAY_DEBUG", True)


def _dbg(msg: str, *, enabled: bool) -> None:
    if enabled:
        print(msg, flush=True)


def _tree(
    root: Path,
    *,
    enabled: bool,
    max_depth: int = 5,
    max_items: int = 300,
) -> None:
    if not enabled:
        return
    root = Path(root)
    print(f"[debug-tree] {root}", flush=True)
    if not root.exists():
        print("  (missing)", flush=True)
        return

    items_printed = 0
    root_depth = len(root.resolve().parts)

    def _depth(p: Path) -> int:
        return len(p.resolve().parts) - root_depth

    try:
        for p in sorted(root.rglob("*")):
            if items_printed >= max_items:
                print(f"  ... (truncated, max_items={max_items})", flush=True)
                break
            d = _depth(p)
            if d > max_depth:
                continue
            rel = p.relative_to(root).as_posix()
            if p.is_dir():
                print(f"  [D] {rel}/", flush=True)
            else:
                try:
                    sz = p.stat().st_size
                except Exception:
                    sz = -1
                print(f"  [F] {rel} ({sz} bytes)", flush=True)
            items_printed += 1
    except Exception as e:
        print(f"  (tree error: {e})", flush=True)


# =============================================================================
# Cache helpers
# =============================================================================
def cache_paths(base_dir: Path, market: str, slot: str, ymd: str) -> dict:
    cache_dir = base_dir / "data" / "cache" / market / ymd
    cache_dir.mkdir(parents=True, exist_ok=True)
    return {
        "dir": cache_dir,
        "marker": cache_dir / f"{slot}.done.json",
        "payload": cache_dir / f"{slot}.payload.json",
    }


def should_skip_by_cache(marker_path: Path, payload_path: Path) -> bool:
    # Cache "hit" means marker exists AND payload exists.
    return marker_path.exists() and payload_path.exists()


def write_payload(payload_path: Path, payload: dict) -> None:
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_marker(marker_path: Path, payload_path: Path, meta: dict) -> None:
    marker = {
        "meta": meta,
        "payload_path": str(payload_path),
        "written_at_utc": _now_utc().isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
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

    # ✅ Default ON debug (your request)
    ap.add_argument("--no-debug", action="store_true", help="Disable main.py debug prints/tree")
    ap.add_argument("--debug-tree-depth", type=int, default=5)
    ap.add_argument("--debug-tree-max", type=int, default=250)

    args = ap.parse_args()

    dbg_on = _debug_enabled(args.no_debug)

    # Cache default policy:
    # - IMPORTANT: payload MUST be written for downstream scripts (run_shorts.py).
    # - "cache" here only controls marker + skip-on-hit behavior.
    default_cache = True
    enable_cache = parse_bool_env("INTRADAY_ENABLE_CACHE", default_cache)
    if args.no_cache:
        enable_cache = False

    # ✅ ymd is market-local (unified for all markets)
    ymd = market_today_ymd(args.market)

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
        "cwd": str(Path.cwd()),
        "base_dir": str(base_dir),
        "platform": platform.platform(),
        "python": platform.python_version(),
    }

    _dbg(f"[debug] market={args.market} slot={args.slot} asof={args.asof} ymd(market-local)={ymd}", enabled=dbg_on)
    _dbg(f"[debug] cache_dir={paths['dir']}", enabled=dbg_on)
    _dbg(f"[debug] payload_path={paths['payload']}", enabled=dbg_on)
    _dbg(f"[debug] marker_path={paths['marker']}", enabled=dbg_on)
    _dbg(f"[debug] enable_cache={enable_cache} force={bool(args.force)}", enabled=dbg_on)

    if enable_cache and (not args.force) and should_skip_by_cache(paths["marker"], paths["payload"]):
        print(f"⏭️  Skip (cache hit): {paths['marker']}")
        if dbg_on:
            _tree(paths["dir"], enabled=True, max_depth=args.debug_tree_depth, max_items=args.debug_tree_max)
        return

    runner = RUNNERS.get(args.market)
    if not runner:
        raise RuntimeError(f"Unknown market: {args.market}")

    started_utc = _now_utc()
    payload = runner(args, base_dir, ymd, meta)
    finished_utc = _now_utc()

    # Normalize required fields
    payload.setdefault("market", args.market)
    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)

    # ✅ generated_at should be UTC Z to avoid local machine leakage
    payload.setdefault("generated_at", finished_utc.isoformat(timespec="seconds").replace("+00:00", "Z"))

    payload.setdefault("meta", {})
    payload["meta"].setdefault("time", {})

    # ✅ Unified, DST-aware when possible
    payload["meta"]["time"].update(
        build_market_time_meta(args.market, started_utc=started_utc, finished_utc=finished_utc)
    )

    # -------------------------------------------------------------------------
    # ✅ CRITICAL FIX:
    # Always write payload json (run_shorts.py depends on it).
    # Cache flag only controls marker + skip behavior.
    # -------------------------------------------------------------------------
    write_payload(paths["payload"], payload)

    if enable_cache:
        write_marker(paths["marker"], paths["payload"], meta)
        print(f"✅ Cached: {paths['payload']}")
    else:
        print(f"✅ Payload written (cache disabled): {paths['payload']}")
        try:
            print(f"   stats: {json.dumps(payload.get('stats', {}), ensure_ascii=False)}")
        except Exception:
            pass

    if dbg_on:
        _tree(paths["dir"], enabled=True, max_depth=args.debug_tree_depth, max_items=args.debug_tree_max)


if __name__ == "__main__":
    main()
