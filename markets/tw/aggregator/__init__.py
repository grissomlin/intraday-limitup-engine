# markets/tw/aggregator/__init__.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import importlib.util
from pathlib import Path
from typing import Any, Dict, Optional


def aggregate(
    payload: Dict[str, Any],
    *,
    enable_open_watchlist: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    TW aggregator package entry.

    IMPORTANT:
    - 避免 markets.tw.aggregator (package) 與 markets/tw/aggregator.py (single-file)
      同名導致的 recursive import。
    - 用檔案路徑載入 single-file aggregator 並呼叫其 aggregate()。
    """
    # markets/tw/aggregator/__init__.py
    # parent is markets/tw/aggregator/ (dir), so parent.parent is markets/tw/
    tw_dir = Path(__file__).resolve().parent.parent
    single_file = tw_dir / "aggregator.py"  # markets/tw/aggregator.py

    if not single_file.exists():
        raise FileNotFoundError(f"single-file aggregator not found: {single_file}")

    mod_name = "markets.tw._aggregator_singlefile"

    # ✅ cache module to avoid re-loading each call
    mod = sys.modules.get(mod_name)
    if mod is None:
        spec = importlib.util.spec_from_file_location(mod_name, str(single_file))
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load module spec from: {single_file}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)

    if not hasattr(mod, "aggregate"):
        raise ImportError(f"{single_file} does not define aggregate()")

    agg_fn = getattr(mod, "aggregate")

    if enable_open_watchlist is None:
        return agg_fn(payload)

    # temporary env override (restore after)
    key = "ENABLE_OPEN_WATCHLIST"
    old = os.environ.get(key)
    try:
        os.environ[key] = "1" if bool(enable_open_watchlist) else "0"
        return agg_fn(payload)
    finally:
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old
