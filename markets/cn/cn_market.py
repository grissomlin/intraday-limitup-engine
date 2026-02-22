# markets/cn/cn_market.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Tuple

def classify_cn_market(symbol: str) -> Tuple[str, str]:
    """
    symbol: 600000.SS / 000001.SZ / 300001.SZ / 688001.SS
    return (market, market_detail)
    """
    code = str(symbol).split(".")[0].zfill(6)

    if code.startswith("688"):
        return "SSE", "star"
    if code.startswith(("300", "301")):
        return "SZSE", "chinext"
    if symbol.endswith(".SS"):
        return "SSE", "main"
    if symbol.endswith(".SZ"):
        return "SZSE", "main"
    return "CN", "unknown"

def is_main(sym: str) -> bool:
    code = sym.split(".")[0]
    return not code.startswith(("300", "301", "688"))

def is_chinext(sym: str) -> bool:
    code = sym.split(".")[0]
    return code.startswith(("300", "301"))

def is_star(sym: str) -> bool:
    code = sym.split(".")[0]
    return code.startswith("688")
