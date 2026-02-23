# scripts/render_images_common/overview/i18n_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.font_manager import FontProperties

__all__ = [
    "normalize_market",
    "resolve_lang",
    "setup_cjk_font",
    "has_hangul",
    "has_kana",
    "has_han",
    "has_thai",
    "has_cjk",
    "fontprops_for_text",
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
    """CJK Unified Ideographs (Han/Chinese characters)."""
    if not text:
        return False
    for ch in text:
        o = ord(ch)
        if 0x4E00 <= o <= 0x9FFF:
            return True
    return False


def has_thai(text: str) -> bool:
    """Thai block: 0E00-0E7F."""
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


def _debug_print_fonts(market: str, profile: str, font_list: List[str], chosen: Optional[str]) -> None:
    if not _env_on("OVERVIEW_DEBUG_FONTS"):
        return
    try:
        print("[OVERVIEW_FONT_DEBUG]")
        print("  market =", market)
        print("  profile =", profile)
        print("  chosen_primary =", chosen)
        print(
            "  selected_font_list =",
            font_list[:20],
            ("... (len=%d)" % len(font_list) if len(font_list) > 20 else ""),
        )
        print("  rcParams.font.family =", plt.rcParams.get("font.family"))
        print("  rcParams.font.sans-serif (head) =", (plt.rcParams.get("font.sans-serif") or [])[:15])
    except Exception:
        pass


def _debug_print_noto_paths() -> None:
    if not _env_on("OVERVIEW_DEBUG_FONTS"):
        return
    try:
        paths = sorted({f.fname for f in fm.fontManager.ttflist})
        noto = [p for p in paths if "noto" in p.lower()]
        print("[OVERVIEW_FONT_DEBUG_PATHS]")
        print("  matplotlib knows noto paths =", len(noto))
        for p in noto[:30]:
            print("   ", p)
    except Exception:
        pass


# =============================================================================
# Force-register TTC faces (critical on some CI images)
# =============================================================================
_CJK_TTC_PATHS = [
    # Most important: Sans CJK
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Light.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-DemiLight.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Medium.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Thin.ttc",
    # Serif CJK (optional)
    "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",
]


def _try_add_noto_cjk_ttc() -> None:
    """
    On some GitHub runner + Matplotlib setups, fontManager scans TTC but only
    registers one family name (often JP). However fc-list shows KR/SC/TC exist.

    Force-add TTC via fontManager.addfont() to register all faces.
    Safe/idempotent.
    """
    try:
        for p in _CJK_TTC_PATHS:
            if os.path.exists(p):
                try:
                    fm.fontManager.addfont(p)
                except Exception:
                    pass
    except Exception:
        pass


# =============================================================================
# Small helpers
# =============================================================================
def _available_font_names() -> set[str]:
    # Ensure TTC faces are registered before we read names
    _try_add_noto_cjk_ttc()
    return {f.name for f in fm.fontManager.ttflist}


def _dedup_keep_order(xs: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in xs:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _filter_available(order: List[str], available: set[str]) -> List[str]:
    out: List[str] = []
    for n in order:
        if n in available and n not in out:
            out.append(n)
    return out


def _pick_available_list(candidates: List[str]) -> List[str]:
    try:
        available = _available_font_names()
    except Exception:
        return []
    out: List[str] = []
    for n in candidates:
        if n in available and n not in out:
            out.append(n)
    return out


# =============================================================================
# Font setup (rcParams)
# =============================================================================
def setup_cjk_font(payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """
    Configure matplotlib fonts for rendering + bbox measurement.

    Key rule for stability:
    - For markets that actually DISPLAY CJK/TH text (TW/CN/JP/KR/TH),
      the FIRST font in rcParams['font.sans-serif'] MUST be able to render that script.
      Otherwise matplotlib will measure with a font that lacks glyphs (=> warnings + wrong widths),
      and layout/ellipsize will drift.

    - For pure EN markets, keep Latin-first for clean digits/punct, but include CJK/TH as fallbacks.
    """
    try:
        # critical: ensure TTC faces available in fontManager
        _try_add_noto_cjk_ttc()

        available = _available_font_names()

        market = ""
        if payload:
            market = str(payload.get("market", "") or "").upper()
        market = normalize_market(market)

        ss = []
        if payload:
            ss = payload.get("sector_summary", []) or []
        if not isinstance(ss, list):
            ss = []

        # Detect KR need (for mixed payloads)
        need_kr = (market == "KR")
        if not need_kr and ss:
            for r in ss[:80]:
                if has_hangul(str((r or {}).get("sector", "") or "")):
                    need_kr = True
                    break

        # Detect Thai need (for mixed payloads)
        need_th = (market == "TH")
        if not need_th and ss:
            for r in ss[:80]:
                if has_thai(str((r or {}).get("sector", "") or "")):
                    need_th = True
                    break

        # Detect JP need (for mixed payloads)
        need_jp = (market == "JP")
        if not need_jp and ss:
            for r in ss[:80]:
                if has_kana(str((r or {}).get("sector", "") or "")):
                    need_jp = True
                    break

        # Detect Han need (for mixed payloads)
        need_han = (market in {"TW", "CN"})
        if not need_han and ss:
            for r in ss[:80]:
                if has_han(str((r or {}).get("sector", "") or "")):
                    need_han = True
                    break

        # Prefer CJK family names that exist on Ubuntu CI (fonts-noto-cjk)
        # NOTE: runner family name may be "Noto Sans CJK XX" not "Noto Sans XX"
        primary_cn = ["Noto Sans CJK SC", "Noto Sans SC", "Microsoft YaHei", "SimHei", "WenQuanYi Zen Hei"]
        primary_tw = ["Noto Sans CJK TC", "Noto Sans TC", "Noto Sans CJK HK", "Noto Sans HK", "Microsoft JhengHei", "PingFang TC"]
        primary_jp = ["Noto Sans CJK JP", "Noto Sans JP", "Yu Gothic", "Meiryo"]
        primary_kr = ["Noto Sans CJK KR", "Noto Sans KR", "Malgun Gothic"]

        # Thai families
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

        latin = ["Noto Sans", "DejaVu Sans", "Arial Unicode MS"]

        profile = _get_font_profile()
        zh_primary = primary_cn if market == "CN" else primary_tw

        # ---- Build order ----
        if market in {"TW", "CN", "JP", "KR", "TH"} or need_han or need_jp or need_kr or need_th:
            if market == "TH" or need_th:
                order = primary_th + latin + zh_primary + primary_jp + primary_kr
            elif market == "KR" or need_kr:
                order = primary_kr + latin + zh_primary + primary_jp + primary_th
            elif market == "JP" or need_jp:
                order = primary_jp + latin + zh_primary + primary_kr + primary_th
            elif market == "CN":
                order = primary_cn + latin + primary_jp + primary_kr + primary_th
            else:
                order = primary_tw + latin + primary_jp + primary_kr + primary_th

            order = order + ["DejaVu Sans", "Noto Sans"]
        else:
            if profile == "TH" and market in {"US", "CA", "AU", "UK"}:
                order = latin + primary_th + zh_primary + primary_kr + primary_jp + ["DejaVu Sans", "Noto Sans"]
            else:
                order = latin + zh_primary + primary_jp + primary_kr + primary_th + ["DejaVu Sans", "Noto Sans"]

        order = _dedup_keep_order(order)
        font_list = _filter_available(order, available)
        if not font_list:
            return None

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = font_list
        plt.rcParams["axes.unicode_minus"] = False

        chosen = font_list[0] if font_list else None
        _debug_print_fonts(market, profile, font_list, chosen)
        _debug_print_noto_paths()

        return chosen
    except Exception:
        return None


# =============================================================================
# Per-text FontProperties chooser (CRITICAL for bbox measurement)
# =============================================================================
def _pick_first_available(candidates: List[str]) -> Optional[str]:
    try:
        available = _available_font_names()
    except Exception:
        return None
    for n in candidates:
        if n in available:
            return n
    return None


def fontprops_for_text(
    text: str,
    *,
    market: str = "",
    payload: Optional[Dict[str, Any]] = None,
    weight: Optional[str] = None,
) -> FontProperties:
    """
    Keep script detection logic.
    But return a family fallback list so Matplotlib can fall back without switching
    to DejaVu (missing glyph) and without bbox drift.
    """
    # Ensure rcParams configured (idempotent)
    try:
        setup_cjk_font(payload or {"market": market})
    except Exception:
        pass

    m = normalize_market(market or (payload or {}).get("market", "") if payload else market)

    if has_thai(text):
        primary = [
            "Noto Sans Thai",
            "Noto Sans Thai UI",
            "Noto Looped Thai",
            "Noto Looped Thai UI",
            "Tahoma",
            "Leelawadee UI",
            "DejaVu Sans",
        ]
    elif has_hangul(text):
        primary = [
            "Noto Sans CJK KR",
            "Noto Sans KR",
            "Malgun Gothic",
            "DejaVu Sans",
        ]
    elif has_kana(text):
        primary = [
            "Noto Sans CJK JP",
            "Noto Sans JP",
            "Yu Gothic",
            "Meiryo",
            "DejaVu Sans",
        ]
    elif has_han(text):
        if m == "CN":
            primary = [
                "Noto Sans CJK SC",
                "Noto Sans SC",
                "Microsoft YaHei",
                "SimHei",
                "WenQuanYi Zen Hei",
                "DejaVu Sans",
            ]
        else:
            primary = [
                "Noto Sans CJK TC",
                "Noto Sans TC",
                "Noto Sans CJK HK",
                "Noto Sans HK",
                "Microsoft JhengHei",
                "PingFang TC",
                "DejaVu Sans",
            ]
    else:
        primary = [
            "Noto Sans",
            "DejaVu Sans",
            "Arial Unicode MS",
        ]

    families = _pick_available_list(primary)
    if not families:
        base = _pick_first_available(primary) or "sans-serif"
        families = [base]

    w = (weight or "").strip().lower() or None
    if w is None:
        return FontProperties(family=families)

    if w in {"regular", "normal"}:
        w = "regular"
    elif w in {"medium"}:
        w = "medium"
    elif w in {"bold", "heavy", "black"}:
        w = "bold"

    return FontProperties(family=families, weight=w)


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
