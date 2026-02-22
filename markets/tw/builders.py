# markets/tw/builders.py
# -*- coding: utf-8 -*-
"""
Backward-compat shim.

We moved implementations to the package: markets/tw/builders/*

Keep old imports working:
  from markets.tw.builders import build_limitup, build_open_limit_watchlist, ...

Do NOT add logic here; all implementations live in markets/tw/builders/ modules.
"""
from .builders import *  # noqa: F401,F403
