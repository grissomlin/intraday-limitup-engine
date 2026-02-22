# scripts/render_images_common/overview/metrics.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Value pickers (compat)
# =============================================================================
def _pick_int(row: Dict[str, Any], keys: Tuple[str, ...], default: int = 0) -> int:
    """
    從 row 中依序挑第一個能轉成 int 的欄位
    """
    for k in keys:
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except Exception:
                pass
    return default


def _pick_float(
    row: Dict[str, Any],
    keys: Tuple[str, ...],
    default: Optional[float] = None,
) -> Optional[float]:
    """
    從 row 中依序挑第一個能轉成 float 的欄位
    """
    for k in keys:
        if k in row and row[k] is not None:
            try:
                return float(row[k])
            except Exception:
                pass
    return default


# =============================================================================
# Row type guards (IMPORTANT)
# =============================================================================
def _looks_like_bins_row(row: Dict[str, Any]) -> bool:
    """
    ✅ 用來判斷這個 row 是不是「分箱統計」(gain_bins/bins) 那種 row。

    - bins row 典型：
        {"sector": "10–20%", "cnt": 72, "pct": 0.014}
    - sector_summary 典型（JP/KR/...）：
        {"sector": "...", "sector_total": 123, "locked_cnt": 2, ...}

    目標：
    - 只允許 bins row 直接吃 row["pct"]
    - sector_summary 禁止直接吃 row["pct"]（避免被 1.0 之類的欄位污染，造成 100%）
    """
    if not isinstance(row, dict):
        return False

    # bins rows almost always have cnt + pct
    has_bins = ("cnt" in row) and ("pct" in row)

    # sector_summary has these kinds of keys; if present, treat as NOT bins
    has_sector_summary_shape = any(
        k in row
        for k in (
            "sector_total",
            "sector_cnt",
            "total_cnt",
            "locked_cnt",
            "touched_cnt",
            "bigmove10_cnt",
            "mix_cnt",
            "display_limitup_count",
            "peers_count",
            "locked_pct",
            "touched_pct",
            "bigmove10_pct",
            "mix_pct",
            "all_pct",
            "value_pct",
        )
    )

    return bool(has_bins and (not has_sector_summary_shape))


# =============================================================================
# Raw metric fields
# =============================================================================
def pick_locked(row: Dict[str, Any]) -> int:
    return _pick_int(row, ("locked_cnt", "limitup_locked", "locked", "lock_cnt"))


def pick_touched(row: Dict[str, Any]) -> int:
    return _pick_int(row, ("touched_cnt", "limitup_touched", "touched", "touch_cnt"))


def pick_bigmove10(row: Dict[str, Any]) -> int:
    return _pick_int(row, ("bigmove10_cnt", "bigmove_10_cnt", "move10_cnt", "ret10_cnt", "gt10_cnt"))


def pick_display_cnt(row: Dict[str, Any]) -> int:
    # sector_summary 常見：display_limitup_count
    return _pick_int(row, ("display_limitup_count", "display_cnt", "display_count"))


def pick_peers_cnt(row: Dict[str, Any]) -> int:
    # sector_summary 常見：peers_count
    return _pick_int(row, ("peers_count", "peers_cnt", "peer_cnt", "others_cnt", "non_trigger_cnt"))


# =============================================================================
# TW fallback: derive bigmove10_cnt from snapshot_main when missing
# =============================================================================
def _normalize_market(market: str) -> str:
    return (market or "").strip().upper()


def _needs_bigmove10(metric: str) -> bool:
    m = (metric or "").strip().lower()
    return m in {"bigmove10", "mix", "all", "bigmove10+locked+touched"}


def _tw_has_bigmove10_cnt(sector_summary: List[Dict[str, Any]]) -> bool:
    try:
        return any(int((r or {}).get("bigmove10_cnt", 0) or 0) > 0 for r in sector_summary if isinstance(r, dict))
    except Exception:
        return False


def _derive_tw_bigmove10_per_sector(payload: Dict[str, Any]) -> Dict[str, int]:
    """
    TW only:
      10%+ = ret >= 0.10 (收盤) 且 不含漲停/觸及（不含 locked/touched）
    """
    snap = payload.get("snapshot_main", []) or []
    if not isinstance(snap, list) or not snap:
        return {}

    per: Dict[str, int] = {}
    for row in snap:
        if not isinstance(row, dict):
            continue

        sec = str(row.get("sector", "") or "").strip()
        if not sec:
            continue

        try:
            ret = float(row.get("ret", 0.0) or 0.0)
        except Exception:
            ret = 0.0

        is_locked = bool(row.get("is_limitup_locked", False))
        is_touch = bool(row.get("is_limitup_touch", False))

        if (ret >= 0.10) and (not is_locked) and (not is_touch):
            per[sec] = per.get(sec, 0) + 1

    return per


def _ensure_tw_bigmove10_cnt_on_rows(payload: Dict[str, Any], sector_summary: List[Dict[str, Any]], metric: str) -> None:
    """
    ✅ 最保守：只對 TW + (mix/bigmove10/all) 才補 bigmove10_cnt。
    - 若已經有 bigmove10_cnt 且有人 >0，則完全不覆蓋。
    - 以 snapshot_main 推導後回填 sector_summary（in-place）。
    """
    if _normalize_market(str(payload.get("market", ""))) != "TW":
        return
    if not _needs_bigmove10(metric):
        return
    if not isinstance(sector_summary, list) or not sector_summary:
        return
    if _tw_has_bigmove10_cnt(sector_summary):
        return

    per = _derive_tw_bigmove10_per_sector(payload)
    if not per:
        return

    for r in sector_summary:
        if not isinstance(r, dict):
            continue
        sec = str(r.get("sector", "") or "").strip()
        if not sec:
            continue
        r["bigmove10_cnt"] = int(per.get(sec, 0))


def _tw_bigmove10_from_payload(payload: Dict[str, Any], sector_name: str) -> int:
    """
    只在真的缺 bigmove10_cnt 時，才用 payload 推導單一 sector 的 10%+ 數量。
    """
    sec = (sector_name or "").strip()
    if not sec:
        return 0
    per = _derive_tw_bigmove10_per_sector(payload)
    return int(per.get(sec, 0))


# =============================================================================
# Metric compute
# =============================================================================
def compute_value(row: Dict[str, Any], metric: str, payload: Optional[Dict[str, Any]] = None) -> int:
    """
    統一計算某一行 sector 的 value

    ✅ 兼容：
    - gainbins：使用 row["cnt"]（由 gain_bins.py 產出）
    - bins 或自定義 rows：若 row 帶 value 則優先用 value

    ✅ TW fallback（可選）：
    - 若 payload 提供且 sector_summary 缺 bigmove10_cnt，會從 snapshot_main 推導（只在 TW + mix/bigmove10/all）
    """
    metric = (metric or "auto").strip().lower()

    # ✅ gain bins page: value is the band count
    # row shape: {"sector": "10–20%", "cnt": 72, "pct": 0.014}
    if metric == "gainbins":
        try:
            return int(row.get("cnt", 0) or 0)
        except Exception:
            return 0

    # ✅ bins / custom rows: directly use row["value"] if present
    if isinstance(row, dict) and ("value" in row) and (row["value"] is not None):
        try:
            return int(row["value"])
        except Exception:
            pass

    locked = pick_locked(row)
    touched = pick_touched(row)
    big10 = pick_bigmove10(row)

    # ✅ TW fallback: if big10 is missing and payload provided, derive it
    if (big10 <= 0) and payload and _normalize_market(str(payload.get("market", ""))) == "TW" and _needs_bigmove10(metric):
        try:
            sec = str(row.get("sector", "") or "")
        except Exception:
            sec = ""
        big10 = _tw_bigmove10_from_payload(payload, sec)

    if metric == "locked":
        return locked

    if metric == "touched":
        return touched

    if metric == "bigmove10":
        return big10

    if metric in ("locked+touched", "locked_plus_touched"):
        return locked + touched

    if metric in ("bigmove10+locked+touched", "all", "mix"):
        return big10 + locked + touched

    # fallback
    return locked


# =============================================================================
# Payload override / auto metric
# =============================================================================
def payload_metric_override(payload: Dict[str, Any]) -> str:
    """
    允許 payload/meta 指定固定 metric，避免 renderer 自己猜：
      payload["meta"]["overview_metric"] = "mix"/"locked"/"touched"/"bigmove10"/"locked+touched"
    """
    try:
        meta = payload.get("meta") or {}
        v = meta.get("overview_metric") or payload.get("overview_metric")
        v = str(v).strip().lower()

        allowed = {
            "locked",
            "touched",
            "bigmove10",
            "locked+touched",
            "locked_plus_touched",
            "mix",
            "all",
            "bigmove10+locked+touched",
        }

        if v in allowed:
            if v == "locked_plus_touched":
                return "locked+touched"
            if v in ("all", "bigmove10+locked+touched"):
                return "mix"
            return v
    except Exception:
        pass

    return ""


def auto_metric(payload: Dict[str, Any], normalize_market) -> str:
    """
    Auto 選 metric 策略：
    0) payload override 優先
    1) KR：mix
    2) JP：mix
    3) TW：mix
    4) US/CA/AU/UK：bigmove10
    5) 其他：若有 locked -> locked
       否則 locked+touched
       否則 mix
    """
    ov = payload_metric_override(payload)
    if ov:
        return ov

    market = normalize_market(str(payload.get("market", "")))
    ss = payload.get("sector_summary", []) or []

    if market == "KR":
        return "mix"

    if market == "JP":
        return "mix"

    if market == "TW":
        return "mix"

    if market in ("US", "CA", "AU", "UK"):
        return "bigmove10"

    any_locked = any(pick_locked(x) > 0 for x in ss)
    if any_locked:
        return "locked"

    any_lt = any((pick_locked(x) + pick_touched(x)) > 0 for x in ss)
    if any_lt:
        return "locked+touched"

    return "mix"


# =============================================================================
# Breadth helpers (pct badge)
# =============================================================================
def fmt_pct(p: float) -> str:
    """
    將比例 (0.071) 格式化成 "7.1%"
    """
    try:
        v = float(p) * 100.0
    except Exception:
        return ""

    if v >= 10:
        return f"{v:.0f}%"
    if v >= 1:
        return f"{v:.1f}%"
    return f"{v:.1f}%"


def _derive_sector_pct(row: Dict[str, Any], metric: str) -> Optional[float]:
    """
    當 payload 沒提供 *_pct 時，嘗試用 sector_summary 的統計欄位推導「占該產業比例」

    sector_total ≈ peers_count + display_limitup_count
    """
    try:
        v = int(compute_value(row, metric) or 0)
    except Exception:
        v = 0
    if v <= 0:
        return None

    # 若 row 本身就有 sector_total/sector_cnt 類欄位，優先用
    sector_total = _pick_int(row, ("sector_total", "sector_cnt", "total_cnt"), default=0)

    if sector_total <= 0:
        peers = pick_peers_cnt(row)
        disp = pick_display_cnt(row)
        if peers > 0 or disp > 0:
            sector_total = peers + disp

    if sector_total <= 0:
        return None

    return float(v) / float(sector_total)


def compute_pct(row: Dict[str, Any], metric: str) -> Optional[float]:
    """
    對應 pct 欄位：
    - locked         -> locked_pct
    - touched        -> touched_pct
    - bigmove10      -> bigmove10_pct
    - locked+touched -> locked_touched_pct（或 lt_pct）
    - mix/all        -> mix_pct（或 all_pct）

    ✅ bins 兼容：
    - 只在「bins row / gainbins」才允許直接讀 row["pct"]（避免 sector_summary 被 pct=1.0 汙染）

    ✅ 若都沒有，就用 sector_summary 欄位推導「占該產業比例」
    """
    m = (metric or "").lower()

    # ✅ bins / gainbins：才允許直接吃 row["pct"]
    if m == "gainbins" or _looks_like_bins_row(row):
        p_direct = _pick_float(row, ("pct", "value_pct"), None)
        if p_direct is not None:
            return p_direct

    if m == "locked":
        p = _pick_float(row, ("locked_pct", "limitup_locked_pct"))
        return p if (p is not None) else _derive_sector_pct(row, m)

    if m == "touched":
        p = _pick_float(row, ("touched_pct", "limitup_touched_pct"))
        return p if (p is not None) else _derive_sector_pct(row, m)

    if m == "bigmove10":
        p = _pick_float(row, ("bigmove10_pct", "move10_pct"))
        return p if (p is not None) else _derive_sector_pct(row, m)

    if m in ("locked+touched", "locked_plus_touched"):
        p = _pick_float(row, ("locked_touched_pct", "lt_pct"))
        return p if (p is not None) else _derive_sector_pct(row, "locked+touched")

    if m in ("mix", "all", "bigmove10+locked+touched"):
        p = _pick_float(row, ("mix_pct", "all_pct"))
        return p if (p is not None) else _derive_sector_pct(row, "mix")

    # fallback：也嘗試推導
    return _derive_sector_pct(row, m)


# =============================================================================
# Badge label (更直覺：全部用「佔比/占比/割合/비중/สัดส่วน」)
# =============================================================================
def _pct_prefix(lang: str) -> str:
    """
    給 badge 的百分比前綴用字（非英文）：
      - zh-tw: 佔比
      - zh-cn: 占比
      - ja   : 割合
      - ko   : 비중
      - th   : สัดส่วน
    """
    l = (lang or "en").strip().lower()

    if l == "th":
        return "สัดส่วน"
    if l == "ja":
        return "割合"
    if l == "zh-tw":
        return "佔比"
    if l == "zh-cn":
        return "占比"
    if l == "ko":
        return "비중"
    return "Share"


def badge_text(row: Dict[str, Any], metric: str, lang: str) -> Tuple[str, Optional[str]]:
    """
    Badge 文字：
    count: "3"
    pct  :
      - en: "6.1% of sector" / "0.6% of market"
      - 其他語系: "佔比 6.1%" / "占比 6.1%" / "割合 6.1%" ...

    ✅ 修正：
    - 不要再出現「10%+ 產業 x%」
    - 全部改成「佔比 x%」（語意更清楚）
    """
    m = (metric or "").strip().lower()

    v = compute_value(row, m)
    p = compute_pct(row, m)

    if p is None:
        return str(v), None

    pct_str = fmt_pct(p)

    # English: natural phrasing
    if (lang or "").strip().lower() == "en":
        if m == "gainbins":
            return str(v), f"{pct_str} of market"
        return str(v), f"{pct_str} of sector"

    prefix = _pct_prefix(lang)
    return str(v), f"{prefix} {pct_str}"


def should_show_breadth_legend(metric: str, sector_rows: List[Dict[str, Any]]) -> bool:
    """
    只要本頁任何 row 有 pct，就顯示 legend
    """
    m = (metric or "").lower()
    for r in sector_rows:
        if compute_pct(r, m) is not None:
            return True
    return False