# scripts/render_images_common/overview/footer_i18n.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Tuple


def is_zh_cn(lang: str, market: str = "") -> bool:
    l = (lang or "").strip().lower()
    m = (market or "").strip().upper()
    if m == "CN":
        return True
    return l.startswith("zh-cn") or l.startswith("zh-hans")


def is_zh_any(lang: str) -> bool:
    l = (lang or "").strip().lower()
    return l.startswith("zh")


def labels_for_market(mkt: str, lang: str) -> Tuple[str, str]:
    m = (mkt or "").upper()

    if m == "KR":
        return "상한가", "터치"
    if m == "JP":
        return "ストップ高", "タッチ"
    if m == "TH":
        return "ติดซิลลิ่ง", "แตะซิลลิ่ง"

    if m in {"TW", "CN", "HK", "MO"} or is_zh_any(lang):
        if is_zh_cn(lang, m):
            return "涨停", "触及"
        return "漲停", "觸及"

    return "Limit-Up", "Touched"


def market_word(lang: str, market: str = "") -> str:
    l = (lang or "").strip().lower()
    m = (market or "").strip().upper()

    if m == "TH" or l == "th":
        return "ตลาด"
    if m == "JP" or l == "ja":
        return "市場"
    if m == "KR" or l == "ko":
        return "시장"

    if is_zh_cn(l, m):
        return "市场"
    if l.startswith("zh"):
        return "市場"

    return "Market"
