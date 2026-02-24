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

# ✅ KR-only: when family names are not registered (common on CI),
# use TTC file directly for Hangul rendering.
_KR_TTC_BY_WEIGHT = {
    "bold": "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "black": "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
    "medium": "/usr/share/fonts/opentype/noto/NotoSansCJK-Medium.ttc",
    "light": "/usr/share/fonts/opentype/noto/NotoSansCJK-Light.ttc",
    "regular": "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
}


def _try_add_noto_cjk_ttc() -> None:
    """
    On some GitHub runner + Matplotlib setups, fontManager scans TTC but only
    registers one family name (often JP). However the TTC file itself still
    contains KR glyphs.

    Force-add TTC via fontManager.addfont() to register faces.
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


def _pick_kr_ttc_path(weight: Optional[str]) -> Optional[str]:
    """
    ✅ KR-only: pick a TTC font file path that exists on disk.
    This bypasses "family name not registered" issues and prevents DejaVu fallback.
    """
    w = (weight or "").strip().lower()
    if w in {"heavy", "black"}:
        w = "black"
    elif w in {"bold"}:
        w = "bold"
    elif w in {"medium"}:
        w = "medium"
    elif w in {"light"}:
        w = "light"
    else:
        w = "regular"

    # Try requested weight first, then fall back to regular
    cand = [_KR_TTC_BY_WEIGHT.get(w), _KR_TTC_BY_WEIGHT.get("regular")]
    for p in cand:
        if p and os.path.exists(p):
            return p
    return None


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

    NOTE: This sets rcParams fallback list. For KR tofu on CI, we additionally
    use fontprops_for_text(fname=...) for Hangul, so KR fix does NOT rely on
    family-name registration.
    """
    try:
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

        need_kr = (market == "KR")
        if not need_kr and ss:
            for r in ss[:80]:
                if has_hangul(str((r or {}).get("sector", "") or "")):
                    need_kr = True
                    break

        need_th = (market == "TH")
        if not need_th and ss:
            for r in ss[:80]:
                if has_thai(str((r or {}).get("sector", "") or "")):
                    need_th = True
                    break

        need_jp = (market == "JP")
        if not need_jp and ss:
            for r in ss[:80]:
                if has_kana(str((r or {}).get("sector", "") or "")):
                    need_jp = True
                    break

        need_han = (market in {"TW", "CN"})
        if not need_han and ss:
            for r in ss[:80]:
                if has_han(str((r or {}).get("sector", "") or "")):
                    need_han = True
                    break

        primary_cn = ["Noto Sans CJK SC", "Noto Sans SC", "Microsoft YaHei", "SimHei", "WenQuanYi Zen Hei"]
        primary_tw = ["Noto Sans CJK TC", "Noto Sans TC", "Noto Sans CJK HK", "Noto Sans HK", "Microsoft JhengHei", "PingFang TC"]
        primary_jp = ["Noto Sans CJK JP", "Noto Sans JP", "Yu Gothic", "Meiryo"]
        primary_kr = ["Noto Sans CJK KR", "Noto Sans KR", "Malgun Gothic"]

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
# Per-text FontProperties chooser (KR tofu-safe)
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
    Return FontProperties that avoids tofu.

    ✅ KR-only enhancement:
    - If Hangul detected, try using TTC file path directly via FontProperties(fname=...).
      This bypasses CI cases where family name "Noto Sans CJK KR" is not registered,
      which otherwise causes fallback to DejaVu (missing glyphs).
    """
    # Ensure rcParams configured (idempotent)
    try:
        setup_cjk_font(payload or {"market": market})
    except Exception:
        pass

    m = normalize_market(market or (payload or {}).get("market", "") if payload else market)

    w_in = (weight or "").strip().lower() or None
    w_norm = w_in
    if w_norm in {"regular", "normal"}:
        w_norm = "regular"
    elif w_norm in {"medium"}:
        w_norm = "medium"
    elif w_norm in {"bold", "heavy", "black"}:
        w_norm = "bold"

    # ✅ KR-only: Hangul -> use TTC path if possible
    if has_hangul(text):
        p = _pick_kr_ttc_path(w_norm)
        if p:
            # fname forces the actual font file, avoiding DejaVu fallback
            if w_norm is None:
                return FontProperties(fname=p)
            return FontProperties(fname=p, weight=w_norm)

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
        # (If TTC path not found, fall back to family list)
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

    if w_in is None:
        return FontProperties(family=families)

    return FontProperties(family=families, weight=w_norm)


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
