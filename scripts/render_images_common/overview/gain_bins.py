# scripts/render_images_common/overview/gain_bins.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from .footer import get_market_universe_total
from .strings import disclaimer_one_line


# =============================================================================
# Helpers
# =============================================================================
def _env_on(name: str, default: str = "1") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _exclude_touch_default_on(_payload: Dict[str, Any]) -> bool:
    """
    ✅ 預設排除 touched/locked（default ON）
    若你未來要恢復「不排除」：
      set GAINBINS_EXCLUDE_TOUCH=0
    """
    return _env_on("GAINBINS_EXCLUDE_TOUCH", "1")


def pick_ret(row: Dict[str, Any]) -> Optional[float]:
    """
    取個股漲跌幅（比例值）
    例：0.10 = +10%

    ✅ 防呆：有些來源會給 ret_pct=15.4（百分比）而不是 0.154（比例）
    若不處理，後面 ret*100 會變 1540% → 造成 100%+ 假象
    """
    keys = (
        "ret",
        "return",
        "pct",
        "pct_change",
        "chg_pct",
        "change_pct",
        "ret_pct",
        "pctChg",
        "pct_chg",
        "changePercent",
    )

    for k in keys:
        if k in row and row[k] is not None:
            try:
                v = float(row[k])
            except Exception:
                continue

            # Heuristic:
            # - ratio 通常在 [-1, +1] 附近
            # - percent 通常像 10, 15.4, 120 ...
            # 對「看起來像百分比」的欄位做 /100 轉回 ratio
            if k in {
                "ret_pct",
                "pct",
                "pct_change",
                "chg_pct",
                "change_pct",
                "pctChg",
                "pct_chg",
                "changePercent",
            }:
                if abs(v) > 2.0:
                    v = v / 100.0

            return v

    return None


def get_snapshot_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    統一取出可用的行情 rows（依常見優先序）：
    snapshot_all -> snapshot_main -> snapshot_open -> snapshot
    """
    for key in ("snapshot_all", "snapshot_main", "snapshot_open", "snapshot"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            return [r for r in rows if isinstance(r, dict)]
    return []


def _resolve_lang(payload: Dict[str, Any]) -> str:
    """
    gain_bins 只需要粗略語系（ja/ko/zh-tw/zh-cn/th/en）
    """
    market = str(payload.get("market", "") or "").strip().upper()
    lang = str((payload.get("lang") or (payload.get("meta") or {}).get("lang") or "")).strip().lower()

    if lang not in {"en", "zh-tw", "zh-cn", "ja", "ko", "th"}:
        if market in {"JP", "JPX", "JPN"}:
            lang = "ja"
        elif market in {"KR", "KOR"}:
            lang = "ko"
        elif market in {"TH", "THA", "SET"}:
            lang = "th"
        else:
            lang = "en"
    return lang


def _band_label(lang: str, lo: int, hi: int) -> str:
    if lang in {"ko", "ja"}:
        return f"{lo}–{hi}%"
    if lang.startswith("zh") or lang == "th":
        return f"{lo}–{hi}%"
    return f"{lo}-{hi}%"


def _over_label(lang: str) -> str:
    if lang in {"ko", "ja", "th"}:
        return "100%+"
    if lang.startswith("zh"):
        return "100%+"
    return ">100%"


def _tagline(lang: str, *, band: str) -> str:
    if lang == "th":
        # ✅ 短一點、一般人看得懂，不要太長爆版
        return {
            "10-20": "ปรับขึ้น",
            "20-30": "พุ่งแรง",
            "30-40": "พุ่งแรง",
            "40-50": "ทะยาน",
            "50-100": "พุ่งมาก",
            "100+": "ดับเบิล",
        }.get(band, "")

    if lang == "ja":
        return {
            "10-20": "上昇",
            "20-30": "急騰",
            "30-40": "急騰",
            "40-50": "暴騰",
            "50-100": "大暴騰",
            "100+": "大化け",
        }.get(band, "")

    if lang == "ko":
        return {
            "10-20": "상승",
            "20-30": "급등",
            "30-40": "급등",
            "40-50": "폭등",
            "50-100": "대폭등",
            "100+": "초급등",
        }.get(band, "")

    if lang == "zh-cn":
        return {
            "10-20": "上涨",
            "20-30": "大涨",
            "30-40": "暴涨",
            "40-50": "暴涨",
            "50-100": "飙涨",
            "100+": "翻倍",
        }.get(band, "")

    if lang.startswith("zh"):
        return {
            "10-20": "上漲",
            "20-30": "大漲",
            "30-40": "暴漲",
            "40-50": "暴漲",
            "50-100": "飆漲",
            "100+": "翻倍",
        }.get(band, "")

    return {
        "10-20": "Gainers",
        "20-30": "Surge",
        "30-40": "Spike",
        "40-50": "Soaring",
        "50-100": "Rocket",
        "100+": "Doublers",
    }.get(band, "")


def _pct_str_1dp(p: float) -> str:
    try:
        return f"{float(p) * 100.0:.1f}%"
    except Exception:
        return "0.0%"


def _penny_enabled(payload: Dict[str, Any]) -> bool:
    """
    ✅ TH penny filter switch (compat)
      - NEW: filters.th_filter_penny
      - OLD: filters.th_penny_exclude
    """
    f = payload.get("filters") or {}
    if not isinstance(f, dict):
        return False

    # new key first
    v = f.get("th_filter_penny")
    if isinstance(v, bool):
        return v
    if v is not None and str(v).strip().lower() in {"1", "true", "yes", "y", "on"}:
        return True

    # fallback old key
    v2 = f.get("th_penny_exclude")
    if isinstance(v2, bool):
        return v2
    if v2 is None:
        return False
    return str(v2).strip().lower() in {"1", "true", "yes", "y", "on"}


def _penny_price_max(payload: Dict[str, Any]) -> Optional[float]:
    f = payload.get("filters") or {}
    if not isinstance(f, dict):
        return None
    mx = f.get("th_penny_price_max")
    if mx is None:
        return None
    try:
        return float(mx)
    except Exception:
        return None


def _is_penny_row(payload: Dict[str, Any], row: Dict[str, Any]) -> bool:
    """
    向下相容：
    - 若 row 已帶 is_penny_stock / is_penny → 直接用
    - 否則可用 close + th_penny_price_max 推斷（如果 payload 有提供）
    """
    for k in ("is_penny_stock", "is_penny"):
        if k in row and row[k] is not None:
            try:
                return bool(row[k])
            except Exception:
                pass

    mxv = _penny_price_max(payload)
    if mxv is None:
        return False

    try:
        price = float(row.get("close") or 0.0)
        return (price > 0.0) and (price <= mxv)
    except Exception:
        return False


def _penny_excluded_count(payload: Dict[str, Any], rows: List[Dict[str, Any]]) -> Optional[int]:
    """
    嘗試計算被篩掉的 penny 股數量（只在 TH + penny filter enabled 時有意義）
    """
    if not _penny_enabled(payload):
        return None
    try:
        c = 0
        for r in rows:
            if _is_penny_row(payload, r):
                c += 1
        return int(c)
    except Exception:
        return None


# =============================================================================
# Core: build rows for gain-bins page
# =============================================================================
def build_gain_bins_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    只統計 ret >= 0.10（10%+，不含下跌 / 不含 0–10%）

    ✅ 預設排除 touched/locked（可用 env 關閉）
    ✅ penny filter：支援 th_filter_penny（新）+ th_penny_exclude（舊）
    """
    universe_total = int(get_market_universe_total(payload) or 0)
    if universe_total <= 0:
        return []

    rows = get_snapshot_rows(payload)
    if not rows:
        return []

    counts = {
        "10-20": 0,
        "20-30": 0,
        "30-40": 0,
        "40-50": 0,
        "50-100": 0,
        "100+": 0,
    }

    lang = _resolve_lang(payload)
    use_penny_filter = _penny_enabled(payload)
    exclude_touch = _exclude_touch_default_on(payload)

    for r in rows:
        if use_penny_filter and _is_penny_row(payload, r):
            continue

        # ✅ default exclude touched/locked (avoid touch inflation)
        if exclude_touch and (bool(r.get("is_limitup_touch")) or bool(r.get("is_limitup_locked"))):
            continue

        ret = pick_ret(r)
        if ret is None:
            continue
        if ret < 0.10:
            continue

        pct = ret * 100.0
        if pct >= 100.0:
            counts["100+"] += 1
        elif pct >= 50.0:
            counts["50-100"] += 1
        elif pct >= 40.0:
            counts["40-50"] += 1
        elif pct >= 30.0:
            counts["30-40"] += 1
        elif pct >= 20.0:
            counts["20-30"] += 1
        else:
            counts["10-20"] += 1

    out: List[Dict[str, Any]] = []
    order = ["100+", "50-100", "40-50", "30-40", "20-30", "10-20"]

    for key in order:
        c = int(counts.get(key, 0))
        if c <= 0:
            continue

        if key == "100+":
            label = _over_label(lang)
            tag = _tagline(lang, band="100+")
        elif key == "50-100":
            label = _band_label(lang, 50, 100)
            tag = _tagline(lang, band="50-100")
        elif key == "40-50":
            label = _band_label(lang, 40, 50)
            tag = _tagline(lang, band="40-50")
        elif key == "30-40":
            label = _band_label(lang, 30, 40)
            tag = _tagline(lang, band="30-40")
        elif key == "20-30":
            label = _band_label(lang, 20, 30)
            tag = _tagline(lang, band="20-30")
        else:
            label = _band_label(lang, 10, 20)
            tag = _tagline(lang, band="10-20")

        sector_label = f"{label}  {tag}".strip() if tag else label
        out.append({"sector": sector_label, "cnt": c, "pct": float(c) / float(universe_total)})

    return out


# =============================================================================
# Footer lines for gain-bins page
# =============================================================================
def gainbins_footer_center_lines(payload: Dict[str, Any], lang: str) -> Tuple[str, str, str, str]:
    universe_total = int(get_market_universe_total(payload) or 0)
    rows = get_snapshot_rows(payload)

    use_penny_filter = _penny_enabled(payload)
    exclude_touch = _exclude_touch_default_on(payload)

    pos_cnt = 0
    for r in rows:
        if use_penny_filter and _is_penny_row(payload, r):
            continue
        if exclude_touch and (bool(r.get("is_limitup_touch")) or bool(r.get("is_limitup_locked"))):
            continue
        ret = pick_ret(r)
        if ret is not None and ret >= 0.10:
            pos_cnt += 1

    p = (float(pos_cnt) / float(universe_total)) if universe_total > 0 else 0.0
    pct = _pct_str_1dp(p)

    if lang == "th":
        line1 = f"ทั้งหมด {universe_total} ตัว มี {pos_cnt} ตัว ({pct})"
        line2 = "※ 10%+ = ปิด ≥ +10% (ไม่รวม แตะซิลลิ่ง/ติดซิลลิ่ง)"
    elif lang == "en":
        line1 = f"{pos_cnt} / {universe_total} ({pct}) are 10%+ gainers"
        line2 = "※ 10%+ = return ≥ +10% (incl. limit-up/touched)"
    elif lang == "ko":
        line1 = f"{universe_total}개 중 {pos_cnt}개（{pct}）"
        line2 = "※ 10%+ = 상승률 10% 이상 (상한가/터치 포함)"
    elif lang == "ja":
        line1 = f"{universe_total}社中{pos_cnt}社（{pct}）"
        line2 = "※ 10%+ = 上昇率10%以上（ストップ高/タッチ含む）"
    elif lang == "zh-cn":
        line1 = f"{universe_total}家中{pos_cnt}家（{pct}）"
        line2 = "※ 10%+ = 涨幅≥10%（含涨停/触及）"
    else:
        line1 = f"{universe_total}家中{pos_cnt}家（{pct}）"
        line2 = "※ 10%+ = 漲幅≥10%（含漲停/觸及）"

    # ✅ line3: TH penny filter note (only for TH)
    line3 = ""
    if lang == "th" and use_penny_filter:
        mxv = _penny_price_max(payload)
        exc = _penny_excluded_count(payload, rows)
        if mxv is not None and exc is not None:
            line3 = f"คัดกรองหุ้นต่ำกว่า {mxv:.2f} บาท: {exc} ตัว"
        elif mxv is not None:
            line3 = f"คัดกรองหุ้นต่ำกว่า {mxv:.2f} บาท"
        else:
            line3 = "มีการคัดกรองหุ้นราคาต่ำ (penny)"

    try:
        line4 = disclaimer_one_line(lang)
    except Exception:
        line4 = ""

    return line1, line2, line3, line4
