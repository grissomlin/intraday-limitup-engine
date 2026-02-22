# markets/tw/labels.py
# -*- coding: utf-8 -*-
"""
TW labels
---------
集中放「顯示用標籤」：
- market_detail -> market_label（中文顯示）
- 也可放其他 UI 文字 mapping（避免散落在 downloader/aggregator）
"""

from __future__ import annotations


# 你目前 stock_list 的 market_detail 會用到的顯示名稱
MARKET_DETAIL_TO_LABEL = {
    "listed": "上市",
    "otc": "上櫃",
    "dr": "DR",
    "innovation_a": "創新A",
    "innovation_c": "創新C",
    "emerging": "興櫃",
}


def market_label_from_detail(market_detail: str) -> str:
    """
    將 market_detail 轉成顯示用 market_label（中文）
    - 允許 None/空值
    - 未知值回傳空字串（由上游決定要不要 fallback）
    """
    key = (market_detail or "").strip()
    return MARKET_DETAIL_TO_LABEL.get(key, "")
