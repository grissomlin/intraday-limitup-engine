# scripts/render_images_common/i18n.py
# -*- coding: utf-8 -*-
"""
Simple i18n dictionary for render layer.

Supported langs (initial):
- zh_hant (TW)
- zh_hans (CN)
- ja      (JP)
- ko      (KR)
- en      (US)

Usage:
    from scripts.render_images_common.i18n import t
    s = t("zh_hant", "prev_day", default="前日")
"""

from __future__ import annotations

from typing import Any, Dict


# -----------------------------------------------------------------------------
# Language packs
# -----------------------------------------------------------------------------
PACKS: Dict[str, Dict[str, str]] = {
    # =========================
    # Traditional Chinese (TW)
    # =========================
    "zh_hant": {
        "prev_day": "前日",
        "term_limitup": "漲停",
        "term_touched": "觸及",

        # ✅ short bucket label (legacy: 10–20%)
        "term_bigmove10": "大漲",

        # ✅ move bands (shared across no-limit markets)
        # band 0..5 => 10–20, 20–30, 30–40, 40–50, 50–100, >=100
        "move_band_0": "大漲",
        "move_band_1": "急漲",
        "move_band_2": "強漲",
        "move_band_3": "猛漲",
        "move_band_4": "狂漲",
        "move_band_5": "噴出",

        # subtitles
        "subtitle_fallback": "{limitup} {hit} 家｜{touched} {touch} 家",
        "subtitle_precise_1": "{limitup} 顯示 {hs}/{ht} ｜ {touched} 顯示 {ts}/{tt}",
        "subtitle_precise_2": "{limitup} {hs}/{ht} ｜ {touched} {ts}/{tt}",

        # box titles
        "box_title_top": "{limitup} / {touched}",
        "box_title_bottom": "同行業今日無漲停股",

        # empty / hints
        "empty_limitup": "（本頁無{limitup}/{touched}資料）",
        "empty_peer": "（本頁無資料）",
        "more_hint": "（尚有更多資料未顯示）",

        # badges
        "badge_streak": "{badge}{streak}連",

        # ✅ footer (no provider naming)
        "footer_disclaimer": "資料來源：公開市場資訊整理｜僅供資訊參考，非投資建議",
    },

    # =========================
    # Simplified Chinese (CN)
    # =========================
    "zh_hans": {
        "prev_day": "前日",
        "term_limitup": "涨停",
        "term_touched": "炸板",

        # ✅ short bucket label (legacy: 10–20%)
        "term_bigmove10": "大涨",

        # ✅ move bands
        "move_band_0": "大涨",
        "move_band_1": "急涨",
        "move_band_2": "强涨",
        "move_band_3": "猛涨",
        "move_band_4": "狂涨",
        "move_band_5": "喷出",

        "subtitle_fallback": "{limitup} {hit} 家｜{touched} {touch} 家",
        "subtitle_precise_1": "{limitup} 显示 {hs}/{ht} ｜ {touched} 显示 {ts}/{tt}",
        "subtitle_precise_2": "{limitup} {hs}/{ht} ｜ {touched} {ts}/{tt}",

        "box_title_top": "{limitup} / {touched}",
        "box_title_bottom": "同行业今日未涨停",

        "empty_limitup": "（本页无{limitup}/{touched}资料）",
        "empty_peer": "（本页无资料）",
        "more_hint": "（还有更多资料未显示）",

        "badge_streak": "{badge}{streak}连",

        # ✅ footer (no provider naming)
        "footer_disclaimer": "资料来源：公开市场信息整理｜仅供资讯参考，非投资建议",
    },

    # =========================
    # English (US / default)
    # =========================
    "en": {
        "prev_day": "prev. session",
        "term_limitup": "Limit-Up",
        "term_touched": "Touched",

        # ✅ short bucket label (legacy: 10–20%)
        "term_bigmove10": "big",

        # ✅ move bands (short, UI-friendly)
        "move_band_0": "big",
        "move_band_1": "surge",
        "move_band_2": "rally",
        "move_band_3": "spike",
        "move_band_4": "moon",
        "move_band_5": "parabolic",

        "subtitle_fallback": "{limitup} {hit} ｜ {touched} {touch}",
        "subtitle_precise_1": "{limitup} shown {hs}/{ht} ｜ {touched} shown {ts}/{tt}",
        "subtitle_precise_2": "{limitup} {hs}/{ht} ｜ {touched} {ts}/{tt}",

        "box_title_top": "{limitup} / {touched}",
        "box_title_bottom": "No sector movers ({prev_day})",

        "empty_limitup": "(No {limitup}/{touched} on this page)",
        "empty_peer": "(No data on this page)",
        "more_hint": "(More items not shown)",

        "badge_streak": "{badge} x{streak}",

        # ✅ footer (generic)
        "footer_disclaimer": "Source: Public market data | For information only. Not financial advice.",
    },

    # =========================
    # Japanese (JP)
    # =========================
    "ja": {
        "prev_day": "前日",
        "term_limitup": "ストップ高",
        "term_touched": "タッチ",

        # ✅ short bucket label (legacy: 10–20%)
        "term_bigmove10": "急騰",

        # ✅ move bands
        "move_band_0": "急騰",
        "move_band_1": "急伸",
        "move_band_2": "爆上げ",
        "move_band_3": "急騰",
        "move_band_4": "超急騰",
        "move_band_5": "パラボリック",

        "subtitle_fallback": "{limitup} {hit} ｜ {touched} {touch}",
        "subtitle_precise_1": "{limitup} 表示 {hs}/{ht} ｜ {touched} 表示 {ts}/{tt}",
        "subtitle_precise_2": "{limitup} {hs}/{ht} ｜ {touched} {ts}/{tt}",

        "box_title_top": "{limitup} / {touched}",
        "box_title_bottom": "同業 {prev_day} 強い銘柄なし",

        "empty_limitup": "（本ページは{limitup}/{touched}なし）",
        "empty_peer": "（本ページはデータなし）",
        "more_hint": "（未表示のデータあり）",

        "badge_streak": "{badge}{streak}連",

        # ✅ footer (generic)
        "footer_disclaimer": "出所：公開市場データ｜参考情報であり、投資助言ではありません",
    },

    # =========================
    # Korean (KR)
    # =========================
    "ko": {
        "prev_day": "전일",
        "term_limitup": "상한가",
        "term_touched": "터치",

        # ✅ short bucket label (legacy: 10–20%)
        "term_bigmove10": "급등",

        # ✅ move bands
        "move_band_0": "급등",
        "move_band_1": "급등",
        "move_band_2": "폭등",
        "move_band_3": "폭등",
        "move_band_4": "초급등",
        "move_band_5": "파라볼릭",

        "subtitle_fallback": "{limitup} {hit} ｜ {touched} {touch}",
        "subtitle_precise_1": "{limitup} 표시 {hs}/{ht} ｜ {touched} 표시 {ts}/{tt}",
        "subtitle_precise_2": "{limitup} {hs}/{ht} ｜ {touched} {ts}/{tt}",

        "box_title_top": "{limitup} / {touched}",
        "box_title_bottom": "동종업계 {prev_day} 강세주 없음",

        "empty_limitup": "({limitup}/{touched} 없음)",
        "empty_peer": "(데이터 없음)",
        "more_hint": "(추가 항목 있음)",

        "badge_streak": "{badge}{streak}연",

        # ✅ footer (generic)
        "footer_disclaimer": "출처: 공개 시장 데이터 | 정보 제공 목적이며 투자 조언이 아닙니다.",
    },
}


# -----------------------------------------------------------------------------
# Helper
# -----------------------------------------------------------------------------
def t(lang: str, key: str, default: str = "", **kwargs: Any) -> str:
    """
    Fetch translation and apply .format(**kwargs).

    - lang fallback: en -> zh_hant -> key itself
    - missing key: use default (if provided), else key
    """
    lang = (lang or "en").strip().lower()
    pack = PACKS.get(lang) or PACKS.get("en") or {}

    if key in pack:
        template = pack[key]
    else:
        template = default if default else key

    # Common variables
    common = {
        "prev_day": (
            PACKS.get(lang, {}).get("prev_day")
            or PACKS.get("en", {}).get("prev_day")
            or "prev. session"
        ),
        "limitup": (
            PACKS.get(lang, {}).get("term_limitup")
            or PACKS.get("en", {}).get("term_limitup")
            or "Limit-Up"
        ),
        "touched": (
            PACKS.get(lang, {}).get("term_touched")
            or PACKS.get("en", {}).get("term_touched")
            or "Touched"
        ),
        # legacy short label for 10–20%
        "bigmove10": (
            PACKS.get(lang, {}).get("term_bigmove10")
            or PACKS.get("en", {}).get("term_bigmove10")
            or "big"
        ),
    }
    merged = {**common, **kwargs}

    try:
        return template.format(**merged)
    except Exception:
        return str(template)
