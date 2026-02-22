# markets/tw/builders/__init__.py
# -*- coding: utf-8 -*-
"""
TW builders package.

Public API re-export (so callers can import from either):
- markets.tw.builders (shim)
- markets.tw.builders.* (package)

Keep names stable for renderers/aggregator.
"""

from .limitup import build_limitup

from .open_limit import (
    normalize_open_limit_rows,
    build_open_limit_watchlist,
    build_emerging_watchlist,
    build_sector_summary_open_limit,
    build_sector_summary_emerging,
)

from .sector_summary import (
    merge_open_limit_into_limitup_df,
    build_sector_summary_main,
)

from .peers import (
    build_peers_by_sector,
    flatten_peers,
)

__all__ = [
    "build_limitup",
    "normalize_open_limit_rows",
    "build_open_limit_watchlist",
    "build_emerging_watchlist",
    "build_sector_summary_open_limit",
    "build_sector_summary_emerging",
    "merge_open_limit_into_limitup_df",
    "build_sector_summary_main",
    "build_peers_by_sector",
    "flatten_peers",
]
