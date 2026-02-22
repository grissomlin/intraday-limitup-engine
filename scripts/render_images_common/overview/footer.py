# scripts/render_images_common/overview/footer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

# ✅ ALL counting/market-specific semantics live here:
from .footer_calc import (
    get_market_total as _get_market_total_calc,
    pick_bigmove10_ex,
    pick_bigmove10_inclusive,
    pick_locked_total,
    pick_touched_total,
    pick_mix_total,
)

# =============================================================================
# Market groups
# =============================================================================
# ✅ No daily limit-up制度的英文市場：footer 不顯示 Limit-Up / Touched，只顯示 10%+
NO_LIMIT_MARKETS = {"US", "CA", "AU", "UK", "EU"}

# =============================================================================
# Env helpers
# =============================================================================
def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _debug_any(name: str) -> bool:
    """
    Unified debug switch:
      OVERVIEW_DEBUG=1        -> enable ALL debug
      OVERVIEW_DEBUG_FOOTER=1 -> enable footer debug only
      OVERVIEW_DEBUG_FOOTER_SHOW=1 -> also show 1 dbg line in footer (⚠️ we no longer draw it)
    """
    return _env_on("OVERVIEW_DEBUG") or _env_on(name)


def _dbg(msg: str) -> None:
    try:
        print(msg)
    except Exception:
        pass


# =============================================================================
# Safe helpers
# =============================================================================
def _get_dict(d: Any) -> Dict[str, Any]:
    return d if isinstance(d, dict) else {}


def _get_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _safe_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, bool):
            return int(x)
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0


def _market_of(payload: Dict[str, Any]) -> str:
    return str(payload.get("market") or (_get_dict(payload.get("meta")).get("market")) or "").strip().upper()


def _lang_of(payload: Dict[str, Any]) -> str:
    return str(payload.get("lang") or (_get_dict(payload.get("meta")).get("lang")) or "").strip().lower()


def _filters_of(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_dict(payload.get("filters"))


def _stats_of(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_dict(payload.get("stats"))


def _meta_totals(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = _get_dict(payload.get("meta"))
    return _get_dict(meta.get("totals"))


def _meta_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = _get_dict(payload.get("meta"))
    return _get_dict(meta.get("metrics"))


def _pick_meta_int(payload: Dict[str, Any], keys: Tuple[str, ...]) -> Tuple[int, str]:
    """
    Try meta.totals first, then meta.metrics.
    Returns: (value, source_string) ; source_string is "missing" if none found.
    """
    totals = _meta_totals(payload)
    metrics = _meta_metrics(payload)
    for k in keys:
        if k in totals:
            return _safe_int(totals.get(k)), f"totals.{k}"
        if k in metrics:
            return _safe_int(metrics.get(k)), f"metrics.{k}"
    return 0, "missing"


def _universe_total_fallback(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    Best-effort universe total (full market coverage), used ONLY when footer_calc total == 0.

    Priority:
      1) filters.<mkt>_sync.total / success  (e.g., filters.us_sync.total)
      2) filters.sync.total / success (generic)
      3) payload.universe (if you ever store it)
      4) stats.universe_count / market_total (if exists)
      5) len(snapshot_open) / stats.snapshot_open_count  (NOT full universe, last resort)
    """
    mkt = _market_of(payload).lower()
    f = _filters_of(payload)

    # (1) filters.<mkt>_sync.*
    if mkt:
        key = f"{mkt}_sync"
        sd = _get_dict(f.get(key))
        tot = _safe_int(sd.get("total"))
        if tot > 0:
            return tot, f"filters.{key}.total"
        suc = _safe_int(sd.get("success"))
        if suc > 0:
            return suc, f"filters.{key}.success"

    # (2) filters.sync.*
    sd2 = _get_dict(f.get("sync"))
    tot2 = _safe_int(sd2.get("total"))
    if tot2 > 0:
        return tot2, "filters.sync.total"
    suc2 = _safe_int(sd2.get("success"))
    if suc2 > 0:
        return suc2, "filters.sync.success"

    # (3) payload.universe (optional)
    u = _safe_int(payload.get("universe"))
    if u > 0:
        return u, "payload.universe"

    # (4) stats.*
    st = _stats_of(payload)
    for k in ("universe", "universe_total", "universe_count", "market_total"):
        v = _safe_int(st.get(k))
        if v > 0:
            return v, f"stats.{k}"

    # (5) last resort (not full universe)
    snap_open = _get_list(payload.get("snapshot_open"))
    if len(snap_open) > 0:
        return len(snap_open), "len(snapshot_open)"
    so = _safe_int(st.get("snapshot_open_count"))
    if so > 0:
        return so, "stats.snapshot_open_count"

    return 0, ""


# =============================================================================
# Backward-compat API (required by gain_bins.py)
# =============================================================================
def get_market_universe_total(payload: Dict[str, Any]) -> int:
    # keep backward behavior: still footer_calc
    return int(_get_market_total_calc(payload))


def get_market_total(payload: Dict[str, Any]) -> int:
    # keep backward behavior: still footer_calc
    return int(_get_market_total_calc(payload))


# =============================================================================
# i18n helpers (ZH)
# =============================================================================
def _is_zh_cn(lang: str, market: str = "") -> bool:
    l = (lang or "").strip().lower()
    m = (market or "").strip().upper()
    if m == "CN":
        return True
    return l.startswith("zh-cn") or l.startswith("zh-hans")


def _is_zh_any(lang: str) -> bool:
    l = (lang or "").strip().lower()
    return l.startswith("zh")


# =============================================================================
# i18n strings (KR / JP / TH / ZH)
# =============================================================================
def _labels_for_market(mkt: str, lang: str) -> Tuple[str, str]:
    """
    Return (locked_label, touched_label)

    - ZH markets: touched -> "涨停失败/漲停失敗"
    - CN locked label -> "涨停(含ST)" to avoid confusion
    """
    m = (mkt or "").upper()

    if m == "KR":
        return "상한가", "터치"
    if m == "JP":
        return "ストップ高", "タッチ"
    if m == "TH":
        return "ติดซิลลิ่ง", "แตะซิลลิ่ง"

    if m in {"TW", "CN", "HK", "MO"} or _is_zh_any(lang):
        # ✅ CN 一律简体
        if m == "CN" or _is_zh_cn(lang, m):
            if m == "CN":
                return "涨停(含ST)", "涨停失败"
            return "涨停", "涨停失败"

        # zh-tw / others
        if m == "CN":
            return "漲停(含ST)", "漲停失敗"
        return "漲停", "漲停失敗"

    return "Limit-Up", "Touched"


def _market_word(lang: str, market: str = "") -> str:
    lang = (lang or "").strip().lower()
    m = (market or "").strip().upper()

    if m == "TH" or lang == "th":
        return "ตลาด"
    if m == "JP" or lang == "ja":
        return "市場"
    if m == "KR" or lang == "ko":
        return "시장"

    if _is_zh_cn(lang, m):
        return "市场"
    if lang.startswith("zh"):
        return "市場"

    return "Market"


def _note_lines(market: str, lang: str, *, note_mode: str = "exclusive") -> List[str]:
    """
    note_mode:
      - "exclusive": 10%+ excludes stop/touch  (overview / sector pages)
      - "inclusive": 10%+ includes stop/touch  (gain-bins page)
    """
    m = (market or "").upper()
    mode = (note_mode or "exclusive").strip().lower()
    lng = (lang or "").strip().lower()

    # ✅ NO-LIMIT markets: fixed English note + disclaimer
    if m in NO_LIMIT_MARKETS:
        note = "※ 10%+ = Close ≥ +10%"
        disclaimer = "Disclaimer: For learning only. Not financial advice."
        return [note, "", disclaimer]

    if m == "TH":
        note = (
            "※ 10%+ = ≥ +10% (รวม แตะซิลลิ่ง/ติดซิลลิ่ง)"
            if mode == "inclusive"
            else "※ 10%+ = ปิด ≥ +10% (ไม่รวม แตะซิลลิ่ง/ติดซิลลิ่ง)"
        )
        disclaimer = "คำเตือน: เพื่อการเรียนรู้ ไม่ใช่คำแนะนำการลงทุน"
        return [note, "", disclaimer]

    if m == "KR":
        note = (
            "※ 10%+ = 상승률 10% 이상 (상한가/터치 포함)"
            if mode == "inclusive"
            else "※ 10%+ = 종가 +10% 이상 (상한가/터치 제외)"
        )
        disclaimer = "면책: 학습용이며 투자 조언이 아닙니다"
        return [note, "", disclaimer]

    if m == "JP":
        note = (
            "※ 10%+ = 上昇率 10%以上（ストップ高/タッチ含む）"
            if mode == "inclusive"
            else "※ 10%+ = 終値 +10%以上（ストップ高/タッチ除外）"
        )
        disclaimer = "免責：学習用であり投資助言ではありません"
        return [note, "", disclaimer]

    if m in {"TW", "CN", "HK", "MO"} or _is_zh_any(lng):
        # ✅ CN 一律简体
        is_cn = True if m == "CN" else _is_zh_cn(lng, m)
        if mode == "inclusive":
            note = "※ 10%+ = 涨幅 ≥ +10%（含涨停/涨停失败）" if is_cn else "※ 10%+ = 漲幅 ≥ +10%（含漲停/漲停失敗）"
        else:
            note = "※ 10%+ = 收盘 ≥ +10%（不含涨停/涨停失败）" if is_cn else "※ 10%+ = 收盤 ≥ +10%（不含漲停/漲停失敗）"
        disclaimer = "免责声明：仅供学习参考，非投资建议" if is_cn else "免責：僅供學習參考，非投資建議"
        return [note, "", disclaimer]

    return []


def _pack_4(lines: List[str]) -> Tuple[str, str, str, str]:
    l = list(lines or [])
    while len(l) < 4:
        l.append("")
    return (l[0], l[1], l[2], l[3])


# =============================================================================
# Public API
# =============================================================================
def build_footer_center_lines(
    payload: Dict[str, Any],
    *,
    metric: str = "",
    market: Optional[str] = None,
    lang: Optional[str] = None,
    normalize_market: Any = None,  # keep signature compatibility
    note_mode: str = "",           # "exclusive" / "inclusive"
    **kwargs: Any,                 # ignore future extra kwargs safely
) -> Tuple[str, str, str, str]:
    """
    Returns exactly 4 strings (line1..line4).

    ✅ This file MUST NOT implement market-specific counting rules.
    ✅ All counting/semantics are delegated to footer_calc.py.
    (Only presentation / labels live here.)
    """
    mkt = (market or _market_of(payload) or "").upper()
    lng = (lang or _lang_of(payload) or "").lower()
    met = (metric or "").strip().lower()

    # ------------------------------------------------------------------
    # Pick numbers (ALL semantics live in footer_calc.py)
    # ------------------------------------------------------------------
    m_total_calc = _safe_int(_get_market_total_calc(payload))
    m_total_src = "footer_calc.get_market_total"

    # ✅ fallback ONLY when calc total is 0
    if m_total_calc <= 0:
        fb, fb_src = _universe_total_fallback(payload)
        if fb > 0:
            m_total_calc = fb
            m_total_src = f"fallback:{fb_src}"

    locked, locked_src = pick_locked_total(payload)
    touched, touched_src = pick_touched_total(payload)

    big10_ex, big10_ex_src = pick_bigmove10_ex(payload)
    big10_total, big10_total_src = pick_bigmove10_inclusive(payload)

    mix, mix_src = pick_mix_total(payload)  # kept for debug; CN line1 will not show it

    # default note_mode:
    nm = (note_mode or "").strip().lower()
    if not nm:
        nm = "inclusive" if ("gain" in met) else "exclusive"

    # ------------------------------------------------------------------
    # ✅ EN no-limit markets: footer only shows Market + 10%+ (exclusive)
    # ------------------------------------------------------------------
    if mkt in NO_LIMIT_MARKETS:
        parts = [
            f"{_market_word(lng, mkt)} {int(m_total_calc)}",
            f"10%+:{int(big10_ex)}",
        ]
        lines: List[str] = [" | ".join(parts)]
        lines.extend(_note_lines(mkt, lng, note_mode="exclusive"))

        if _debug_any("OVERVIEW_DEBUG_FOOTER"):
            _dbg("[OVERVIEW_DEBUG_FOOTER]")
            _dbg(f"  market={mkt} lang={lng} metric={met} note_mode=exclusive (forced for no-limit markets)")
            _dbg(f"  picked market_total={int(m_total_calc)} (source={m_total_src})")
            _dbg(f"  picked big10_ex={int(big10_ex)} (source={big10_ex_src})")
            _dbg(f"  picked big10_total={int(big10_total)} (source={big10_total_src})")
            _dbg(f"  picked locked={int(locked)} (source={locked_src}) [hidden]")
            _dbg(f"  picked touched={int(touched)} (source={touched_src}) [hidden]")
            _dbg(f"  picked mix={int(mix)} (source={mix_src}) [debug only]")
            _dbg(f"  extra_kwargs={sorted(list(kwargs.keys()))}")
            _dbg("")
            if _debug_any("OVERVIEW_DEBUG_FOOTER_SHOW"):
                _dbg(
                    f"[FOOTER_SHOW] big10_total={int(big10_total)}({big10_total_src}) | "
                    f"big10_ex={int(big10_ex)}({big10_ex_src}) | "
                    f"market_total={int(m_total_calc)}({m_total_src})"
                )

        return _pack_4(lines)

    # ------------------------------------------------------------------
    # Other markets (incl. CN): always show Market | locked | touched | 10%+
    # ✅ CN change: if touched doesn't exist in sector rows, replace it by ST封板 on line1
    # ------------------------------------------------------------------
    locked_label, touched_label = _labels_for_market(mkt, lng)

    if mkt == "CN":
        st_locked, st_src = _pick_meta_int(payload, ("st_locked_total",))
        parts = [
            f"{_market_word(lng, mkt)} {int(m_total_calc)}",
            f"{locked_label}:{int(locked)}",
            f"ST封板:{int(st_locked)}",
            f"10%+:{int(big10_ex)}",
        ]
    else:
        parts = [
            f"{_market_word(lng, mkt)} {int(m_total_calc)}",
            f"{locked_label}:{int(locked)}",
            f"{touched_label}:{int(touched)}",
            f"10%+:{int(big10_ex)}",
        ]

    lines: List[str] = [" | ".join(parts)]
    lines.extend(_note_lines(mkt, lng, note_mode=nm))

    if _debug_any("OVERVIEW_DEBUG_FOOTER"):
        _dbg("[OVERVIEW_DEBUG_FOOTER]")
        _dbg(f"  market={mkt} lang={lng} metric={met} note_mode={nm}")
        _dbg(f"  picked market_total={int(m_total_calc)} (source={m_total_src})")
        _dbg(f"  picked locked={int(locked)} (source={locked_src})")
        _dbg(f"  picked touched={int(touched)} (source={touched_src}) {'[hidden on CN line1]' if mkt=='CN' else ''}")
        _dbg(f"  picked big10_ex={int(big10_ex)} (source={big10_ex_src})")
        _dbg(f"  picked big10_total={int(big10_total)} (source={big10_total_src})")
        _dbg(f"  picked mix={int(mix)} (source={mix_src}) {'[NOT SHOWN on CN line1]' if mkt=='CN' else ''}")
        if mkt == "CN":
            _dbg(f"  picked cn_st_locked={int(st_locked)} (source={st_src}) [SHOWN on CN line1]")
        _dbg(f"  extra_kwargs={sorted(list(kwargs.keys()))}")
        _dbg("")
        if _debug_any("OVERVIEW_DEBUG_FOOTER_SHOW"):
            if mkt == "CN":
                _dbg(
                    f"[FOOTER_SHOW] big10_total={int(big10_total)}({big10_total_src}) | "
                    f"big10_ex={int(big10_ex)}({big10_ex_src}) | "
                    f"locked={int(locked)}({locked_src}) | "
                    f"st_locked={int(st_locked)}({st_src}) | "
                    f"market_total={int(m_total_calc)}({m_total_src})"
                )
            else:
                _dbg(
                    f"[FOOTER_SHOW] big10_total={int(big10_total)}({big10_total_src}) | "
                    f"big10_ex={int(big10_ex)}({big10_ex_src}) | "
                    f"locked={int(locked)}({locked_src}) | "
                    f"touched={int(touched)}({touched_src}) | "
                    f"market_total={int(m_total_calc)}({m_total_src})"
                )

    return _pack_4(lines)


def _normalize_source_text(src: str) -> str:
    s = (src or "").strip()
    if not s:
        return ""
    low = s.lower()
    if "yahoo" in low or "google" in low or "investing" in low or "tradingview" in low:
        return "public market data"
    return s


def build_footer_right_text(payload: Dict[str, Any], *, market: Optional[str] = None, **_kwargs: Any) -> str:
    mkt = (market or _market_of(payload) or "").upper()
    lng = _lang_of(payload)

    meta = _get_dict(payload.get("meta"))
    src_raw = str(meta.get("source") or meta.get("data_source") or "").strip()
    src = _normalize_source_text(src_raw)

    if mkt in NO_LIMIT_MARKETS:
        disclaimer = "Disclaimer: For learning only. Not financial advice."
        return f"Data: {src} | {disclaimer}" if src else disclaimer

    # ✅ CN 一律简体
    if mkt == "CN" or _is_zh_cn(lng, mkt):
        disclaimer = "免责声明：仅供学习参考，非投资建议"
        return f"数据：{src} | {disclaimer}" if src else disclaimer

    if lng.startswith("zh"):
        disclaimer = "免責：僅供學習參考，非投資建議"
        return f"資料：{src} | {disclaimer}" if src else disclaimer

    disclaimer = "Disclaimer: For learning only. Not financial advice."
    return f"Data: {src} | {disclaimer}" if src else disclaimer


# Backward-compat alias
build_footer_right = build_footer_right_text