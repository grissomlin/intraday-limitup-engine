# markets/runners.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Callable

# NOTE:
# - Imports are done inside each function to avoid heavy import cost / circular deps.


# =============================================================================
# TW (limit-up market)
# =============================================================================
def run_market_tw(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.tw.downloader import run_intraday
    from markets.tw.aggregator import aggregate
    from markets.guard import run_nontrading_guard_or_raise
    from main import parse_bool_env, maybe_update_tw_stock_list

    test_mode = parse_bool_env("TW_TEST_MODE", False)
    meta["test_mode"] = test_mode

    maybe_update_tw_stock_list(base_dir)

    raw_payload = run_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["test_mode"] = test_mode
    if test_mode:
        raw_payload["filters"]["test_mode_note"] = "SIMULATION (weekend / non-trading hours)"

    raw_payload = run_nontrading_guard_or_raise(
        market="tw",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# US (open movers market)
# =============================================================================
def run_market_us(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.us.downloader_us import run_sync as run_us_sync
    from markets.us.downloader_us import run_intraday as run_us_intraday
    from markets.us.aggregator import aggregate as aggregate_us
    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_us_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_us_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["us_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="us",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_us(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# UK (open movers market)
# =============================================================================
def run_market_uk(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.uk.downloader_uk import run_sync as run_uk_sync
    from markets.uk.downloader_uk import run_intraday as run_uk_intraday
    from markets.uk.aggregator import aggregate as aggregate_uk
    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_uk_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_uk_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["uk_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="uk",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_uk(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# CA (Canada open movers market)
# =============================================================================
def run_market_ca(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.ca.downloader_ca import run_sync as run_ca_sync
    from markets.ca.downloader_ca import run_intraday as run_ca_intraday
    from markets.ca.aggregator import aggregate as aggregate_ca
    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_ca_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_ca_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["ca_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="ca",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_ca(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# AU (Australia open movers market)
# =============================================================================
def run_market_au(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.au.downloader_au import run_sync as run_au_sync
    from markets.au.downloader_au import run_intraday as run_au_intraday
    from markets.au.aggregator import aggregate as aggregate_au
    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_au_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_au_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["au_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="au",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_au(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# IN (India open movers market) ✅ NEW (markets/india/)
# =============================================================================
def run_market_in(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    """
    India (NSE/BSE) = open movers market like UK/CA/AU.

    We keep folder name as: markets/india/
    So import via:
      markets.india.downloader_in
      markets.india.aggregator_in
    """
    import importlib

    mod_dl = importlib.import_module("markets.india.downloader_in")
    mod_ag = importlib.import_module("markets.india.aggregator_in")

    run_in_sync = getattr(mod_dl, "run_sync")
    run_in_intraday = getattr(mod_dl, "run_intraday")
    aggregate_in = getattr(mod_ag, "aggregate")

    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_in_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_in_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["in_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="in",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_in(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# TH (Thailand ceiling+bigmove hybrid)
# =============================================================================
def run_market_th(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    """
    Thailand (SET) = hybrid:
    - ceiling touch/locked (30% default)
    - bigmove >=10% (includes touch-only)
    """
    from markets.th.downloader import run_sync as run_th_sync
    from markets.th.downloader import run_intraday as run_th_intraday
    from markets.th.aggregator import aggregate as aggregate_th
    from markets.guard import run_nontrading_guard_or_raise

    start_date = args.start
    end_date = args.end
    refresh_list = not args.no_refresh_list

    res_sync = run_th_sync(start_date, end_date, refresh_list=refresh_list)
    raw_payload = run_th_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"].setdefault("enable_open_watchlist", True)
    raw_payload["filters"]["th_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="th",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_th(raw_payload)

    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# CN / JP / KR (limit markets)
# =============================================================================
def run_market_cn(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.cn.downloader import run_sync as run_cn_sync
    from markets.cn.downloader import run_intraday as run_cn_intraday
    from markets.cn.aggregator import aggregate
    from markets.guard import run_nontrading_guard_or_raise

    res_sync = run_cn_sync(start_date=None, end_date=None, refresh_list=True)
    raw_payload = run_cn_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["cn_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="cn",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate(raw_payload)
    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


def run_market_jp(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.jp.downloader import run_sync as run_jp_sync
    from markets.jp.downloader import run_intraday as run_jp_intraday
    from markets.jp.aggregator import aggregate
    from markets.guard import run_nontrading_guard_or_raise

    res_sync = run_jp_sync(start_date=None, end_date=None, refresh_list=True)
    raw_payload = run_jp_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["jp_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="jp",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate(raw_payload)
    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


def run_market_kr(args: argparse.Namespace, base_dir: Path, ymd: str, meta: dict) -> Dict[str, Any]:
    from markets.kr.downloader import run_sync as run_kr_sync
    from markets.kr.downloader import run_intraday as run_kr_intraday
    from markets.kr.aggregator import aggregate as aggregate_kr
    from markets.guard import run_nontrading_guard_or_raise

    res_sync = run_kr_sync(start_date=None, end_date=None, refresh_list=True)
    raw_payload = run_kr_intraday(slot=args.slot, asof=args.asof, ymd=ymd)

    raw_payload.setdefault("ymd", ymd)
    raw_payload.setdefault("slot", args.slot)
    raw_payload.setdefault("asof", args.asof)
    raw_payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["kr_sync"] = res_sync

    raw_payload = run_nontrading_guard_or_raise(
        market="kr",
        today=ymd,
        raw_payload=raw_payload,
        allow_nontrading_flag=bool(args.allow_nontrading),
    )

    payload = raw_payload if args.raw_only else aggregate_kr(raw_payload)
    payload.setdefault("ymd", ymd)
    payload.setdefault("slot", args.slot)
    payload.setdefault("asof", args.asof)
    payload.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))
    return payload


# =============================================================================
# Runner registry
# =============================================================================
RUNNERS: Dict[str, Callable[[argparse.Namespace, Path, str, dict], Dict[str, Any]]] = {
    "tw": run_market_tw,
    "us": run_market_us,
    "uk": run_market_uk,
    "ca": run_market_ca,
    "au": run_market_au,

    # open movers / hybrid
    "in": run_market_in,
    "th": run_market_th,

    # limit markets
    "cn": run_market_cn,
    "jp": run_market_jp,
    "kr": run_market_kr,
}

# -----------------------------------------------------------------------------
# ✅ Alias keys (so argparse choices include them, and they map to the same runner)
# main.py uses: choices=list(RUNNERS.keys())
# So we add aliases directly in RUNNERS (all point to run_market_in).
# -----------------------------------------------------------------------------
RUNNERS.update(
    {
        "india": run_market_in,
        "nse": run_market_in,
        "bse": run_market_in,
    }
)
