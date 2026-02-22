# markets/guard.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple


# =============================================================================
# Env helpers
# =============================================================================
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


def is_github_actions() -> bool:
    return os.getenv("GITHUB_ACTIONS", "").lower() == "true"


# =============================================================================
# Normalization
# =============================================================================
def normalize_ymd(s: Any) -> str:
    """
    Normalize to YYYY-MM-DD
    """
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    return s[:10] if len(s) >= 10 else s


# =============================================================================
# Guard settings
# =============================================================================
def guard_enabled_default() -> bool:
    """
    ✅ GitHub Actions 預設開 guard
    ✅ 本機預設關 guard
    env 可覆蓋：INTRADAY_GUARD_NONTRADING=0/1
    """
    default_on = True if is_github_actions() else False
    return parse_bool_env("INTRADAY_GUARD_NONTRADING", default_on)


def allow_nontrading(args: Any = None, *, allow_nontrading_flag: Optional[bool] = None) -> bool:
    """
    優先序：
    1) allow_nontrading_flag（main.py 直接傳）
    2) args.allow_nontrading（CLI）
    3) env INTRADAY_ALLOW_NONTRADING
    """
    if allow_nontrading_flag is not None:
        return bool(allow_nontrading_flag)

    if args is not None and bool(getattr(args, "allow_nontrading", False)):
        return True

    return parse_bool_env("INTRADAY_ALLOW_NONTRADING", False)


# =============================================================================
# Lag rules (KEY CHANGE)
# =============================================================================
def guard_lag_days_for_market(market: str) -> int:
    """
    允許 ymd_effective 落後 requested_ymd 的天數

    env 可覆蓋：
        INTRADAY_GUARD_LAG_DAYS_US=1
        INTRADAY_GUARD_LAG_DAYS_CA=1

    ✅ 預設策略（台灣晚上 9 點跑）：

    - US / CA：可能跨日（收盤在台灣凌晨）
      → 預設允許 lag=1

    - UK / AU / IN / TH / JP / KR / TW / CN：
      → 預設 lag=0（同一天必須一致）
    """
    m = (market or "").strip().lower()

    # --- ENV override first ---
    env_key = f"INTRADAY_GUARD_LAG_DAYS_{m.upper()}"
    v = os.getenv(env_key)
    if v is not None and str(v).strip() != "":
        try:
            return max(0, int(str(v).strip()))
        except Exception:
            pass

    # ✅ Only US + Canada need lag=1 by default
    if m in {"us", "ca"}:
        return 1

    return 0


# =============================================================================
# Core check (pure function)
# =============================================================================
def check_nontrading(
    *,
    market: str,
    requested_ymd: str,
    raw_payload: Dict[str, Any],
    enabled: Optional[bool] = None,
    allow: bool = False,
) -> Tuple[bool, Dict[str, Any]]:
    """
    回傳 (should_skip, info)

    should_skip=True => 視為放假/資料未更新

    requested_ymd：
    - main.py 算出來的「market 當地今天」
    guard 比較 requested_ymd vs ymd_effective
    """
    enabled_eff = guard_enabled_default() if enabled is None else bool(enabled)

    if not enabled_eff:
        return (False, {"guard_enabled": False, "decision": "disabled"})

    if allow:
        return (False, {"guard_enabled": True, "allow_nontrading": True, "decision": "allow_nontrading"})

    ymd_eff = normalize_ymd(raw_payload.get("ymd_effective") or raw_payload.get("ymd"))
    req = normalize_ymd(requested_ymd)

    if not ymd_eff or not req:
        return (
            False,
            {
                "guard_enabled": True,
                "decision": "skip_guard_missing_ymd",
                "note": "missing ymd",
                "requested_ymd": req,
                "ymd_effective": ymd_eff,
            },
        )

    lag_days = guard_lag_days_for_market(market)

    try:
        req_dt = datetime.strptime(req, "%Y-%m-%d").date()
        eff_dt = datetime.strptime(ymd_eff, "%Y-%m-%d").date()
    except Exception:
        return (
            False,
            {
                "guard_enabled": True,
                "decision": "skip_guard_bad_ymd_format",
                "note": "bad ymd format",
                "requested_ymd": req,
                "ymd_effective": ymd_eff,
            },
        )

    diff_days = (req_dt - eff_dt).days

    expect_dt = req_dt - timedelta(days=int(lag_days))
    expect_ymd = expect_dt.strftime("%Y-%m-%d")

    # diff_days:
    #  0 => same day OK
    #  1 => allowed if lag_days=1 (US/CA)
    # <0 => ymd_effective is in future (bad)
    should_skip = (diff_days < 0) or (diff_days > lag_days)

    info = {
        "guard_enabled": True,
        "allow_nontrading": False,
        "market": (market or "").strip().lower(),
        "requested_ymd": req,
        "expected_ymd_effective": expect_ymd,
        "ymd_effective": ymd_eff,
        "diff_days": int(diff_days),
        "lag_allow_days": int(lag_days),
        "is_trading_day": bool(not should_skip),
        "decision": "skip_non_trading" if should_skip else "allow",
    }
    return (should_skip, info)


# =============================================================================
# Main.py friendly API (raise on non-trading)
# =============================================================================
def run_nontrading_guard_or_raise(
    *,
    market: str,
    today: str,
    raw_payload: Dict[str, Any],
    allow_nontrading_flag: bool = False,
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    main.py 用：不符合就 raise

    guard info 會寫入 raw_payload["meta"]["guard"]
    """
    allow = allow_nontrading(args=None, allow_nontrading_flag=allow_nontrading_flag)

    should_skip, info = check_nontrading(
        market=market,
        requested_ymd=today,
        raw_payload=raw_payload,
        enabled=enabled,
        allow=allow,
    )

    raw_payload.setdefault("meta", {})
    raw_payload["meta"]["guard"] = info

    if should_skip:
        raise RuntimeError(
            f"NON_TRADING_GUARD: market={market} today={normalize_ymd(today)} "
            f"expected_ymd_effective={info.get('expected_ymd_effective')} got={info.get('ymd_effective')}"
        )

    return raw_payload


# =============================================================================
# Optional: skip payload (if you prefer not to raise)
# =============================================================================
def make_skip_payload(
    *,
    market: str,
    requested_ymd: str,
    args: Any = None,
    raw_payload: Dict[str, Any] = None,
    guard_info: Dict[str, Any] = None,
    reason: str = "holiday_or_data_not_updated",
) -> Dict[str, Any]:
    """
    產出「直接跳過」payload（不中止 pipeline）
    """
    raw_payload = raw_payload or {}
    guard_info = guard_info or {}

    ymd_eff = normalize_ymd(raw_payload.get("ymd_effective") or raw_payload.get("ymd"))
    req = normalize_ymd(requested_ymd)

    skip_payload: Dict[str, Any] = dict(raw_payload)

    skip_payload.setdefault("filters", {})
    skip_payload["filters"]["skip_reason"] = reason
    skip_payload["filters"]["guard"] = guard_info

    # clear render inputs
    skip_payload["snapshot_main"] = []
    skip_payload["snapshot_open"] = []
    skip_payload["limitup"] = []
    skip_payload["sector_summary"] = []
    skip_payload["peers_by_sector"] = {}
    skip_payload["peers_not_limitup"] = []

    skip_payload.setdefault("stats", {})
    skip_payload["stats"]["snapshot_main_count"] = 0
    skip_payload["stats"]["snapshot_open_count"] = 0
    skip_payload["stats"]["is_market_open"] = 0

    skip_payload["market"] = market
    skip_payload["ymd"] = req
    skip_payload["ymd_effective"] = ymd_eff

    if args is not None:
        skip_payload["slot"] = getattr(args, "slot", skip_payload.get("slot"))
        skip_payload["asof"] = getattr(args, "asof", skip_payload.get("asof"))

    skip_payload["generated_at"] = datetime.now().isoformat(timespec="seconds")
    return skip_payload
