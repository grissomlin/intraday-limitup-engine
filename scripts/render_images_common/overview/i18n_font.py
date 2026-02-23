# scripts/render_images_common/overview/i18n_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

__all__ = [
    "normalize_market",
    "resolve_lang",
    "setup_cjk_font",
    "has_hangul",
    "has_kana",
    "has_han",
    "has_thai",
    "has_cjk",
]

# =============================================================================
# Text detectors
# =============================================================================
def has_hangul(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if (0xAC00 <= o <= 0xD7A3) or (0x1100 <= o <= 0x11FF) or (0x3130 <= o <= 0x318F):
            return True
    return False


def has_kana(text: str) -> bool:
    """Hiragana + Katakana (and extensions)."""
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if (0x3040 <= o <= 0x309F) or (0x30A0 <= o <= 0x30FF) or (0x31F0 <= o <= 0x31FF):
            return True
    return False


def has_han(text: str) -> bool:
    """CJK Unified Ideographs (Han/Chinese characters)"""
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x4E00 <= o <= 0x9FFF:
            return True
    return False


def has_thai(text: str) -> bool:
    """Thai block: 0E00-0E7F"""
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x0E00 <= o <= 0x0E7F:
            return True
    return False


def has_cjk(text: str) -> bool:
    """Backward-compat helper: Han + Hiragana/Katakana."""
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if (0x4E00 <= o <= 0x9FFF) or (0x3040 <= o <= 0x30FF):
            return True
    return False


# =============================================================================
# Market normalization
# =============================================================================
def normalize_market(m: str | None) -> str:
    m = (m or "").strip().upper()
    alias = {
        "TWN": "TW",
        "TAIWAN": "TW",
        "HKG": "HK",
        "HKEX": "HK",
        "CHN": "CN",
        "CHINA": "CN",
        "USA": "US",
        "NASDAQ": "US",
        "NYSE": "US",
        # JP aliases
        "JPN": "JP",
        "JAPAN": "JP",
        "JPX": "JP",
        "TSE": "JP",
        "TOSE": "JP",
        "TOKYO": "JP",
        # KR aliases
        "KOR": "KR",
        "KOREA": "KR",
        "KRX": "KR",
        # CA/AU/UK
        "CAN": "CA",
        "CANADA": "CA",
        "TSX": "CA",
        "TSXV": "CA",
        "AUS": "AU",
        "AUSTRALIA": "AU",
        "ASX": "AU",
        "GBR": "UK",
        "GB": "UK",
        "UNITED KINGDOM": "UK",
        "LSE": "UK",
        "LONDON": "UK",
        # IN
        "IND": "IN",
        "INDIA": "IN",
        "NSE": "IN",
        "BSE": "IN",
        # TH
        "THA": "TH",
        "THAILAND": "TH",
        "SET": "TH",
        # optional EU-ish aliases
        "EUR": "EU",
        "EUROPE": "EU",
        "EUN": "EU",
    }
    return alias.get(m, m or "TW")


# =============================================================================
# Env helpers
# =============================================================================
def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _get_font_profile() -> str:
    """
    OVERVIEW_FONT_PROFILE:
      - "TH"      : force TH-like font order (to copy TH "字感" into US/CA/AU/UK)
      - "DEFAULT" : normal per-market strategy (default)
    """
    v = (os.getenv("OVERVIEW_FONT_PROFILE") or "").strip().upper()
    if v in {"TH", "DEFAULT"}:
        return v
    return "DEFAULT"


def _debug_print_fonts(market: str, profile: str, font_list: List[str]) -> None:
    if not _env_on("OVERVIEW_DEBUG_FONTS"):
        return
    try:
        print("[OVERVIEW_FONT_DEBUG]")
        print("  market =", market)
        print("  profile =", profile)
        print("  selected_font_list =", font_list)
        print("  rcParams.font.family =", plt.rcParams.get("font.family"))
        print("  rcParams.font.sans-serif =", plt.rcParams.get("font.sans-serif"))
    except Exception:
        pass


# =============================================================================
# Font setup
# =============================================================================
def setup_cjk_font(payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Configure matplotlib fonts for CJK/KR/JP/TH rendering.

    IMPORTANT (why you saw tons of 'Glyph missing'):
    - Matplotlib sometimes effectively uses the FIRST font for text measurement / bbox.
    - If Thai text exists but Thai fonts are not early enough, you get:
        Glyph xxxx missing from font(s) Noto Sans
    - If you hard-pick a Thai-only family elsewhere, you can also get:
        Glyph 'A' missing from font(s) Noto Sans Thai
      (that second one is usually from sector_blocks forcing FontProperties; i18n_font helps
       only if caller lets rcParams fallback work.)
    """
    try:
        available = {f.name for f in fm.fontManager.ttflist}

        market = ""
        if payload:
            market = str(payload.get("market", "") or "").upper()
        market = normalize_market(market)

        # Detect KR need (existing logic)
        need_kr = False
        if market == "KR":
            need_kr = True
        elif payload:
            ss = payload.get("sector_summary", []) or []
            if isinstance(ss, list):
                for r in ss[:80]:
                    if has_hangul(str((r or {}).get("sector", "") or "")):
                        need_kr = True
                        break

        # ✅ Detect Thai need as well (NEW, symmetrical to KR detection)
        need_th = False
        if market == "TH":
            need_th = True
        elif payload:
            ss = payload.get("sector_summary", []) or []
            if isinstance(ss, list):
                for r in ss[:80]:
                    if has_thai(str((r or {}).get("sector", "") or "")):
                        need_th = True
                        break

        # Prefer CJK family names that exist on Ubuntu CI (fonts-noto-cjk)
        primary_cn = ["Noto Sans CJK SC", "Noto Sans SC", "Microsoft YaHei", "SimHei", "WenQuanYi Zen Hei"]
        primary_tw = ["Noto Sans CJK TC", "Noto Sans TC", "Noto Sans HK", "Microsoft JhengHei", "PingFang TC"]
        primary_jp = ["Noto Sans CJK JP", "Noto Sans JP", "Yu Gothic", "Meiryo"]
        primary_kr = ["Noto Sans CJK KR", "Noto Sans KR", "Malgun Gothic"]

        # Thai families (Thai glyph coverage)
        primary_th = [
            "Noto Sans Thai",
            "Noto Sans Thai UI",
            "Noto Looped Thai",
            "Noto Looped Thai UI",
            "Tahoma",
            "Leelawadee UI",
            "TH Sarabun New",
            "Angsana New",
        ]

        # Latin-capable fonts
        latin = ["Noto Sans", "DejaVu Sans", "Arial Unicode MS"]

        # General fallback
        fallback = ["DejaVu Sans", "Noto Sans", "Arial Unicode MS"]

        zh_primary = primary_cn if market == "CN" else primary_tw
        profile = _get_font_profile()

        # ---------------------------------------------------------------------
        # Font order strategy (KEY FIX)
        # - If Thai is needed, Thai fonts must be BEFORE Latin to avoid Thai glyph missing.
        # - But keep Latin close (right after Thai) so English/symbols still render well.
        # ---------------------------------------------------------------------
        if profile == "TH" and market in {"US", "CA", "AU", "UK"}:
            # "TH feel" for EN markets: Thai first (for style) BUT keep Latin immediately after
            order = primary_th + latin + zh_primary + primary_kr + primary_jp + fallback
        else:
            if need_th:
                # ✅ TH (or content has Thai): Thai FIRST, then Latin, then CJK
                order = primary_th + latin + zh_primary + primary_kr + primary_jp + fallback
            elif need_kr:
                # KR content: KR first (or at least early), keep Latin near front
                order = primary_kr + latin + zh_primary + primary_jp + fallback
            else:
                if market == "JP":
                    order = primary_jp + latin + zh_primary + primary_kr + fallback
                elif market == "CN":
                    order = primary_cn + latin + primary_jp + primary_kr + fallback
                else:
                    # Default: Latin first is fine for EN/most CJK
                    order = latin + zh_primary + primary_kr + primary_jp + primary_th + fallback

        font_list: List[str] = []
        for n in order:
            if n in available and n not in font_list:
                font_list.append(n)

        if not font_list:
            return None

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = font_list
        plt.rcParams["axes.unicode_minus"] = False

        _debug_print_fonts(market, profile, font_list)

        return font_list[0]
    except Exception:
        return None


# =============================================================================
# Language resolution
# =============================================================================
def _get_payload_lang(payload: Dict[str, Any]) -> str:
    try:
        v = (payload.get("lang") or (payload.get("meta") or {}).get("lang") or "")
        v = str(v).strip().lower()
        if v in {"en", "zh-tw", "zh-cn", "ja", "ko", "th", "zh_hant", "zh_hans"}:
            if v == "zh_hant":
                return "zh-tw"
            if v == "zh_hans":
                return "zh-cn"
            return v
    except Exception:
        pass
    return ""


def _infer_lang_from_sectors(payload: Dict[str, Any]) -> str:
    ss = payload.get("sector_summary", []) or []
    if isinstance(ss, list):
        for r in ss[:80]:
            s = str((r or {}).get("sector", "") or "")
            if has_thai(s):
                return "th"
            if has_hangul(s):
                return "ko"
            if has_kana(s):
                return "ja"
            if has_han(s):
                return "zh-tw"
    return "en"


def _get_market_lang(market: str) -> str:
    market = normalize_market(market)

    if market == "KR":
        return "ko"
    if market == "JP":
        return "ja"
    if market == "CN":
        return "zh-cn"
    if market == "TH":
        return "th"
    if market == "TW":
        return "zh-tw"

    EN_MARKETS = {"US", "CA", "AU", "UK", "EU", "IN", "SG", "MY", "PH", "ID", "VN", "HK"}
    if market in EN_MARKETS:
        return "en"

    return "zh-tw"


def resolve_lang(payload: Dict[str, Any], market: str) -> str:
    v = _get_payload_lang(payload)
    if v:
        return v
    v = _get_market_lang(market)
    if v:
        return v
    return _infer_lang_from_sectors(payload)
