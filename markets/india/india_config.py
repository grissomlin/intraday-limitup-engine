# markets/india/india_config.py
# -*- coding: utf-8 -*-
"""
Stable config module name for INDIA.

downloader.py imports many private helpers like _batch_size, _db_path, ...
So we must explicitly re-export them even if upstream module uses __all__.
"""

from __future__ import annotations

# Import the real implementation (currently in in_config.py)
from . import in_config as _impl

# Explicitly re-export the exact names that downloader.py expects
_db_path = _impl._db_path
_rolling_trading_days = _impl._rolling_trading_days
_calendar_ticker = _impl._calendar_ticker
_calendar_lookback_cal_days = _impl._calendar_lookback_cal_days
_fallback_rolling_cal_days = _impl._fallback_rolling_cal_days
_batch_size = _impl._batch_size
_batch_sleep_sec = _impl._batch_sleep_sec
_fallback_single_enabled = _impl._fallback_single_enabled
_yf_threads_enabled = _impl._yf_threads_enabled
_single_sleep_sec = _impl._single_sleep_sec
_yf_suffix = _impl._yf_suffix
_master_csv_path = _impl._master_csv_path
_cal_cache_root = _impl._cal_cache_root
log = _impl.log