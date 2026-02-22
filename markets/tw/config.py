# markets/tw/config.py
# -*- coding: utf-8 -*-
"""
TW Config / Tunables
-------------------
集中管理所有：
- env 開關
- 數值門檻
- 行為選項（debug / fallback）

⚠️ 原則：
- 任何 os.getenv(...) 不應散落在其他檔案
- builders / limitup_flags / aggregator 只能 import 這裡
- 未來 US / HK 可複製此檔，或抽成 markets/common/config.py
"""

from __future__ import annotations

import os
from typing import Set


# =============================================================================
# Helpers
# =============================================================================
def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default


def _env_set(name: str) -> Set[str]:
    raw = os.getenv(name, "")
    return {s.strip() for s in raw.split(",") if s.strip()}


# =============================================================================
# Core thresholds (business meaning)
# =============================================================================

# standard 漲停外，no_limit 題材要進榜的最小漲幅
# 例如：新上市、特殊制度
NO_LIMIT_THEME_RET: float = _env_float("TW_NO_LIMIT_THEME_RET", 0.10)

# 興櫃 / 無漲跌幅市場的「強勢觀察門檻」
EMERGING_STRONG_RET: float = _env_float("TW_EMERGING_STRONG_RET", 0.10)

# 每個產業最多顯示幾檔「同業未漲停」
PEERS_BY_SECTOR_CAP: int = _env_int("TW_PEERS_BY_SECTOR_CAP", 50)


# =============================================================================
# Debug / compatibility switches
# =============================================================================

# ⚠️ 不建議：強制 ret >= 10% 視為漲停（僅 debug 用）
FORCE_RET_GE_10_AS_LIMITUP: bool = _env_bool(
    "TW_FORCE_RET_GE_10_AS_LIMITUP", False
)

# 是否啟用「價格異常 → 自動轉 no_limit」
AUTO_INFER_NO_LIMIT_FROM_PRICE: bool = _env_bool(
    "TW_AUTO_INFER_NO_LIMIT_FROM_PRICE", True
)

# high 超過「漲停價 + 幾個 tick」才算異常
AUTO_NO_LIMIT_EXCEED_TICKS: int = _env_int(
    "TW_AUTO_NO_LIMIT_EXCEED_TICKS", 2
)

# 同時要求 ret >= 多少才觸發異常轉換（避免誤判）
AUTO_NO_LIMIT_MIN_RET: float = _env_float(
    "TW_AUTO_NO_LIMIT_MIN_RET", 0.11
)


# =============================================================================
# Explicit no-limit symbols (manual override)
# =============================================================================

# 例如：
#   TW_NO_LIMIT_SYMBOLS="7795.TW,xxxx.TWO"
NO_LIMIT_SYMBOLS: Set[str] = _env_set("TW_NO_LIMIT_SYMBOLS")


# =============================================================================
# Test / runtime mode
# =============================================================================

_TW_TEST_MODE_RAW = os.getenv("TW_TEST_MODE", "").strip().lower()
if _TW_TEST_MODE_RAW in ("1", "true", "yes", "y", "on"):
    TEST_MODE_DEFAULT = True
elif _TW_TEST_MODE_RAW in ("0", "false", "no", "n", "off"):
    TEST_MODE_DEFAULT = False
else:
    TEST_MODE_DEFAULT = None  # auto by slot


# =============================================================================
# Sanity export (optional, for debugging)
# =============================================================================
__all__ = [
    # thresholds
    "NO_LIMIT_THEME_RET",
    "EMERGING_STRONG_RET",
    "PEERS_BY_SECTOR_CAP",
    # switches
    "FORCE_RET_GE_10_AS_LIMITUP",
    "AUTO_INFER_NO_LIMIT_FROM_PRICE",
    "AUTO_NO_LIMIT_EXCEED_TICKS",
    "AUTO_NO_LIMIT_MIN_RET",
    # no-limit
    "NO_LIMIT_SYMBOLS",
    # test mode
    "TEST_MODE_DEFAULT",
]
