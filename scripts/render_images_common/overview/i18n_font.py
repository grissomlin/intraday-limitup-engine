# scripts/render_images_common/overview/i18n_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


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
    """
    Hiragana + Katakana
    - Hiragana: 3040-309F
    - Katakana: 30A0-30FF
    - Katakana Phonetic Extensions: 31F0-31FF (optional, but safe)
    """
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if (0x3040 <= o <= 0x309F) or (0x30A0 <= o <= 0x30FF) or (0x31F0 <= o <= 0x31FF):
            return True
    return False


def has_han(text: str) -> bool:
    """
    CJK Unified Ideographs (Han/Chinese characters)
    """
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x4E00 <= o <= 0x9FFF:
            return True
    return False


def has_thai(text: str) -> bool:
    """
    Thai block: 0E00-0E7F
    """
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x0E00 <= o <= 0x0E7F:
            return True
    return False


def has_cjk(text: str) -> bool:
    """
    Backward-compat helper: Han + Hiragana/Katakana
    (kept because some callers might still use it)
    """
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        # Han + Hiragana/Katakana
        if (0x4E00 <= o <= 0x9FFF) or (0x3040 <= o <= 0x30FF):
            return True
    return False


# =============================================================================
# Market normalization
# =============================================================================
def normalize_market(m: str) -> str:
    m = (m or "").strip().upper()
    alias = {
        "TWN": "TW", "TAIWAN": "TW",
        "HKG": "HK", "HKEX": "HK",
        "CHN": "CN", "CHINA": "CN",
        "USA": "US", "NASDAQ": "US", "NYSE": "US",

        # ✅ JP aliases (important: payload might be JPX / TSE)
        "JPN": "JP", "JAPAN": "JP", "JPX": "JP", "TSE": "JP", "TOSE": "JP", "TOKYO": "JP",

        # ✅ KR aliases
        "KOR": "KR", "KOREA": "KR", "KRX": "KR",

        "CAN": "CA", "CANADA": "CA", "TSX": "CA", "TSXV": "CA",
        "AUS": "AU", "AUSTRALIA": "AU", "ASX": "AU",
        "GBR": "UK", "GB": "UK", "UNITED KINGDOM": "UK", "LSE": "UK", "LONDON": "UK",
        "IND": "IN", "INDIA": "IN", "NSE": "IN", "BSE": "IN",

        # ✅ TH aliases
        "THA": "TH", "THAILAND": "TH", "SET": "TH",

        # optional EU-ish aliases
        "EUR": "EU", "EUROPE": "EU", "EUN": "EU",
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


# =============================================================================
# Font setup
# =============================================================================
def setup_cjk_font(payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Configure matplotlib fonts for CJK/KR/JP/TH rendering based on payload.market
    and detected characters in sector_summary.

    ✅ Debug:
      - set OVERVIEW_DEBUG_FONTS=1 to print selected font order and rcParams.

    ✅ Optional profile:
      - set OVERVIEW_FONT_PROFILE=TH to reuse TH font "feel" for non-TH markets.
    """
    try:
        available = {f.name for f in fm.fontManager.ttflist}

        market = ""
        if payload:
            market = str(payload.get("market", "") or "").upper()
        market = normalize_market(market)

        # Detect KR need
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

        primary_cn = ["Noto Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", "SimHei", "WenQuanYi Zen Hei"]
        primary_tw = ["Noto Sans TC", "Noto Sans HK", "Microsoft JhengHei", "PingFang TC"]
        primary_jp = ["Noto Sans JP", "Noto Sans CJK JP", "Yu Gothic", "Meiryo"]
        primary_kr = ["Malgun Gothic", "Noto Sans KR", "Noto Sans CJK KR"]

        # ✅ Thai font candidates (best-effort; may not exist on all machines)
        primary_th = [
            "Noto Sans Thai",
            "Noto Sans Thai UI",
            "Tahoma",
            "Leelawadee UI",
            "TH Sarabun New",
            "Angsana New",
        ]

        fallback = ["Arial Unicode MS", "DejaVu Sans", "Noto Sans"]

        # Choose zh primary by market
        zh_primary = primary_cn if market == "CN" else primary_tw

        profile = _get_font_profile()

        # ---------------------------------------------------------------------
        # Font order strategy
        # ---------------------------------------------------------------------
        if profile == "TH" and market in {"US", "CA", "AU", "UK"}:
            # ✅ Force TH-like feel into EN markets (for your Sector/Market label look)
            order = primary_th + zh_primary + primary_kr + primary_jp + fallback
        else:
            if market == "TH":
                order = primary_th + zh_primary + primary_kr + primary_jp + fallback
            elif need_kr:
                order = primary_kr + zh_primary + primary_jp + fallback
            else:
                if market == "JP":
                    order = primary_jp + zh_primary + primary_kr + fallback
                else:
                    order = zh_primary + primary_kr + primary_jp + fallback

        font_list: List[str] = []
        for n in order:
            if n in available and n not in font_list:
                font_list.append(n)

        if not font_list:
            return None

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = font_list
        plt.rcParams["axes.unicode_minus"] = False

        if _env_on("OVERVIEW_DEBUG_FONTS"):
            try:
                print("[OVERVIEW_FONT_DEBUG]")
                print("  market =", market)
                print("  profile =", profile)
                print("  selected_font_list =", font_list)
                print("  rcParams.font.family =", plt.rcParams.get("font.family"))
                print("  rcParams.font.sans-serif =", plt.rcParams.get("font.sans-serif"))
            except Exception:
                pass

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
        if v in {"en", "zh-tw", "zh-cn", "ja", "ko", "th"}:
            return v
    except Exception:
        pass
    return ""


def _infer_lang_from_sectors(payload: Dict[str, Any]) -> str:
    """
    Infer from sector text (best-effort):
    - Thai => th
    - Hangul => ko
    - Kana => ja
    - Han (no kana) => zh-tw
    - else => en
    """
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

    EN_MARKETS = {"US", "CA", "AU", "UK", "EU", "IN", "SG", "MY", "PH", "ID", "VN"}
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
