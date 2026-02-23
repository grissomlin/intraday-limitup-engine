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
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if (0x3040 <= o <= 0x309F) or (0x30A0 <= o <= 0x30FF) or (0x31F0 <= o <= 0x31FF):
            return True
    return False


def has_han(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x4E00 <= o <= 0x9FFF:
            return True
    return False


def has_thai(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x0E00 <= o <= 0x0E7F:
            return True
    return False


def has_cjk(text: str) -> bool:
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
def normalize_market(m: str) -> str:
    m = (m or "").strip().upper()
    alias = {
        "TWN": "TW", "TAIWAN": "TW",
        "HKG": "HK", "HKEX": "HK",
        "CHN": "CN", "CHINA": "CN",
        "USA": "US", "NASDAQ": "US", "NYSE": "US",

        "JPN": "JP", "JAPAN": "JP", "JPX": "JP", "TSE": "JP",
        "TOSE": "JP", "TOKYO": "JP",

        "KOR": "KR", "KOREA": "KR", "KRX": "KR",

        "CAN": "CA", "CANADA": "CA", "TSX": "CA", "TSXV": "CA",
        "AUS": "AU", "AUSTRALIA": "AU", "ASX": "AU",
        "GBR": "UK", "GB": "UK", "UNITED KINGDOM": "UK",
        "LSE": "UK", "LONDON": "UK",
        "IND": "IN", "INDIA": "IN", "NSE": "IN", "BSE": "IN",

        "THA": "TH", "THAILAND": "TH", "SET": "TH",

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
    v = (os.getenv("OVERVIEW_FONT_PROFILE") or "").strip().upper()
    if v in {"TH", "DEFAULT"}:
        return v
    return "DEFAULT"


# =============================================================================
# Font setup
# =============================================================================
def setup_cjk_font(payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    try:
        available = {f.name for f in fm.fontManager.ttflist}

        market = ""
        if payload:
            market = str(payload.get("market", "") or "").upper()
        market = normalize_market(market)

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

        primary_cn = ["Noto Sans SC", "Noto Sans CJK SC", "Microsoft YaHei", "SimHei"]
        primary_tw = ["Noto Sans TC", "Microsoft JhengHei", "PingFang TC"]
        primary_jp = ["Noto Sans JP", "Noto Sans CJK JP", "Yu Gothic", "Meiryo"]
        primary_kr = ["Malgun Gothic", "Noto Sans KR", "Noto Sans CJK KR"]

        # ✅ 修正重點：避免 Looped Thai 當第一主字型
        primary_th = [
            # 先用 Sans Thai（通常含 Latin glyph）
            "Noto Sans Thai",
            "Noto Sans Thai UI",

            # Looped Thai 往後排（避免缺字警告）
            "Noto Looped Thai",
            "Noto Looped Thai UI",

            # Latin fallback
            "Noto Sans",

            # Windows
            "Tahoma",
            "Leelawadee UI",
            "TH Sarabun New",
            "Angsana New",
        ]

        fallback = ["Arial Unicode MS", "DejaVu Sans", "Noto Sans"]

        zh_primary = primary_cn if market == "CN" else primary_tw
        profile = _get_font_profile()

        if profile == "TH" and market in {"US", "CA", "AU", "UK"}:
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
            print("[OVERVIEW_FONT_DEBUG]")
            print("  market =", market)
            print("  selected_font_list =", font_list)

        return font_list[0]
    except Exception:
        return None
