# markets/cn/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

import pandas as pd

from markets.tw.builders import (
    build_limitup,
    build_sector_summary_main,
    build_peers_by_sector,
    flatten_peers,
)

# =============================================================================
# CN Debug defaults (ON by default)
# =============================================================================
# - CN_BIG10_DEBUG: print per-stock candidates on 20% board with close>=10% and why excluded
# - CN_OVERVIEW_BUCKET_DEBUG: print per-sector bucket accounting (locked/touch/big10/mix/diff)
os.environ.setdefault("CN_BIG10_DEBUG", "1")
os.environ.setdefault("CN_OVERVIEW_BUCKET_DEBUG", "1")


# =============================================================================
# Helpers
# =============================================================================
def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _to_bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s
    return s.fillna(0).astype(int).astype(bool)


def _infer_market_tag(symbol: Any, name: Any, limit_rate: Any) -> str:
    sym = str(symbol or "").strip()
    nm = str(name or "").strip().upper()

    try:
        lr = float(limit_rate)
    except Exception:
        lr = None

    code = sym.split(".")[0]

    if "ST" in nm:
        return "ST"
    if code.startswith(("8", "4")):
        return "北交"
    if code.startswith("688"):
        return "科创"
    if code.startswith(("300", "301")):
        return "创业"

    # fallback by limit_rate
    if lr is not None and abs(lr - 0.20) < 1e-9:
        if code.startswith(("300", "301", "30")):
            return "创业"
        if code.startswith(("688", "68")):
            return "科创"
    return ""


def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    snapshot_builder 給的 is_limitup_touch = touch_any（含封板）
    這裡做兩件事：
      1) 保存 touch_any 到 is_limitup_touch_any
      2) 把 is_limitup_touch 改成 炸板（触及未封）= touch_any & ~locked
    """
    out = df.copy()

    for c, default in [
        ("sector", "未分類"),
        ("ret", 0.0),
        ("streak", 0),
        ("streak_prev", 0),
        ("prev_was_limitup_locked", False),
        ("prev_was_limitup_touch", False),
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("limit_rate", None),
        ("market_detail", ""),
        # optional fields (for debug readability)
        ("last_close", None),
        ("limit_price", None),
        ("high", None),
        ("close", None),
    ]:
        if c not in out.columns:
            out[c] = default

    for c in ["symbol", "name"]:
        if c not in out.columns:
            out[c] = ""

    out["sector"] = out["sector"].fillna("未分類").astype(str)
    out.loc[out["sector"].isin(["", "A-Share", "—", "-", "--", "－", "–"]), "sector"] = "未分類"

    out["ret"] = pd.to_numeric(out["ret"], errors="coerce").fillna(0.0)  # 0.10 = 10%
    out["streak"] = pd.to_numeric(out["streak"], errors="coerce").fillna(0).astype(int)
    out["streak_prev"] = pd.to_numeric(out["streak_prev"], errors="coerce").fillna(0).astype(int)

    out["prev_was_limitup_locked"] = _to_bool_series(out["prev_was_limitup_locked"])
    out["prev_was_limitup_touch"] = _to_bool_series(out["prev_was_limitup_touch"])
    out["is_limitup_touch"] = _to_bool_series(out["is_limitup_touch"])
    out["is_limitup_locked"] = _to_bool_series(out["is_limitup_locked"])

    # 保存 raw touch_any
    out["is_limitup_touch_any"] = out["is_limitup_touch"]

    # ✅ 炸板（触及未封）
    out["is_limitup_touch"] = out["is_limitup_touch_any"] & (~out["is_limitup_locked"])

    # market_tag
    if "market_tag" not in out.columns:
        out["market_tag"] = ""
    out["market_tag"] = [
        _infer_market_tag(sym, nm, lr)
        for sym, nm, lr in zip(out["symbol"].tolist(), out["name"].tolist(), out["limit_rate"].tolist())
    ]

    return out


def _hm_from_payload(payload: Dict[str, Any]) -> str:
    s = str(payload.get("asof") or payload.get("generated_at") or "")
    if not s:
        return ""
    if "T" in s:
        s = s.split("T", 1)[1]
    elif " " in s:
        s = s.split(" ", 1)[1]
    return s[:5] if len(s) >= 5 and s[2] == ":" else ""


def _ensure_meta_time(payload: Dict[str, Any]) -> None:
    payload.setdefault("meta", {})
    payload["meta"].setdefault("time", {})
    t = payload["meta"]["time"]

    hm = _hm_from_payload(payload)
    ymd_trade = str(payload.get("ymd_effective") or payload.get("ymd") or "")[:10]

    t.setdefault("market_tz", "UTC+08:00")
    t.setdefault("market_utc_offset", "+08:00")
    if hm:
        t.setdefault("market_finished_hm", hm)
    if ymd_trade and hm:
        t.setdefault("market_finished_at", f"{ymd_trade} {hm}")


def _is_20_board(df: pd.DataFrame) -> pd.Series:
    tag = df.get("market_tag", pd.Series([""] * len(df))).astype(str)
    md = df.get("market_detail", pd.Series([""] * len(df))).astype(str).str.lower()
    lr = pd.to_numeric(df.get("limit_rate", pd.Series([None] * len(df))), errors="coerce")
    return tag.isin(["创业", "科创"]) | md.isin(["chinext", "star"]) | (lr.notna() & (lr - 0.20).abs() < 1e-9)


def _build_sector_total_by_sector(dfS: pd.DataFrame) -> Dict[str, int]:
    g = dfS.groupby("sector").size()
    return {str(k): int(v) for k, v in g.items()}


def _build_bigmove10_by_sector(dfS: pd.DataFrame) -> Dict[str, int]:
    """
    CN big mover（只針對 20% 板）:
      ret >= 10% 且 沒有摸過漲停價(touch_any) 且 沒封板

    這樣 big mover / 炸板 / 封板 三者互斥，可直接加總做 mix
    """
    df = dfS.copy()
    ret = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    locked = df["is_limitup_locked"].astype(bool)
    touch_any = df.get("is_limitup_touch_any", pd.Series([False] * len(df))).astype(bool)

    mask = _is_20_board(df) & (ret >= 0.10) & (~locked) & (~touch_any)
    g = df.loc[mask].groupby("sector").size()
    return {str(k): int(v) for k, v in g.items()}


def _build_locked_split_by_sector(dfS: pd.DataFrame) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    回傳 (locked_non_st_by_sector, locked_st_by_sector)
    - locked: is_limitup_locked == True
    - ST: market_tag == "ST"
    """
    df = dfS.copy()

    locked = df.get("is_limitup_locked", pd.Series([False] * len(df)))
    locked = locked.fillna(False).astype(bool)

    tag = df.get("market_tag", pd.Series([""] * len(df)))
    tag = tag.fillna("").astype(str)
    is_st = tag.eq("ST")

    g_non = df[locked & (~is_st)].groupby("sector").size()
    g_st = df[locked & is_st].groupby("sector").size()

    non_map = {str(k): int(v) for k, v in g_non.items()}
    st_map = {str(k): int(v) for k, v in g_st.items()}
    return non_map, st_map


def _get_int(r: Dict[str, Any], keys: Tuple[str, ...], default: int = 0) -> int:
    for k in keys:
        if k in r and r[k] is not None:
            try:
                return int(r[k])
            except Exception:
                pass
    return default


def _ensure_sector_summary_includes_big10(
    sector_summary: List[Dict[str, Any]],
    *,
    big10_by_sector: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    ✅ 关键修正：
    CN 的 sector_summary 原本是从 build_sector_summary_main(limitup_df) 来的，
    只包含 locked/touch/no_limit 的行业。

    但 big10_ex（20%板收盘>=10%且非touch/locked）根本不在 limitup_df，
    所以「只有10%+」的行业会完全消失，导致 overview sector_rows 永远不会出现纯 10%+ 行。

    这里把 big10_by_sector 的 sector 补进来（locked/touch/no_limit 先填 0），
    让后续 _merge_overview_fields 能算出 bigmove10_cnt / mix_cnt。
    """
    ss = list(sector_summary or [])
    have = set()
    for r in ss:
        if isinstance(r, dict):
            have.add(str(r.get("sector", "") or ""))

    for sec, cnt in (big10_by_sector or {}).items():
        if int(cnt or 0) <= 0:
            continue
        s = str(sec or "")
        if not s:
            s = "未分類"
        if s in have:
            continue

        # minimal compatible row for downstream (renderer reads these keys defensively)
        ss.append(
            {
                "sector": s,
                "locked_cnt": 0,
                "touch_cnt": 0,          # build_sector_summary_main uses touch_cnt (含locked)；这里先 0
                "touched_cnt": 0,        # 某些渲染/计算也会读 touched_cnt
                "no_limit_cnt": 0,
                "total_cnt": 0,
                "count": 0,
            }
        )
        have.add(s)

    return ss


def _merge_overview_fields(
    sector_summary: List[Dict[str, Any]],
    *,
    sector_total: Dict[str, int],
    big10_by_sector: Dict[str, int],
    locked_non_st_by_sector: Dict[str, int],
    locked_st_by_sector: Dict[str, int],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in (sector_summary or []):
        sec = str(r.get("sector", "") or "")
        rr = dict(r)

        total = int(sector_total.get(sec, 0) or 0)
        big_cnt = int(big10_by_sector.get(sec, 0) or 0)

        locked_cnt = _get_int(rr, ("locked_cnt", "limitup_locked", "locked"), 0)
        touched_cnt = _get_int(rr, ("touched_cnt", "touch_cnt", "limitup_touched", "touched"), 0)

        # ✅ CN extra split
        locked_non_st_cnt = int(locked_non_st_by_sector.get(sec, 0) or 0)
        locked_st_cnt = int(locked_st_by_sector.get(sec, 0) or 0)

        rr["sector_total_cnt"] = total
        rr["bigmove10_cnt"] = big_cnt

        # 原 mix（保留）：big + locked(all) + touched
        rr["mix_cnt"] = int(big_cnt + locked_cnt + touched_cnt)

        # ✅ CN mix_ex_st：排除 ST 封板污染強度
        rr["locked_non_st_cnt"] = locked_non_st_cnt
        rr["locked_st_cnt"] = locked_st_cnt
        rr["mix_ex_st_cnt"] = int(big_cnt + touched_cnt + locked_non_st_cnt)

        if total > 0:
            rr["bigmove10_pct"] = float(big_cnt) / float(total)
            rr["locked_pct"] = float(locked_cnt) / float(total)
            rr["touched_pct"] = float(touched_cnt) / float(total)
            rr["locked_touched_pct"] = float(locked_cnt + touched_cnt) / float(total)
            rr["mix_pct"] = float(rr["mix_cnt"]) / float(total)

            rr["locked_non_st_pct"] = float(locked_non_st_cnt) / float(total)
            rr["locked_st_pct"] = float(locked_st_cnt) / float(total)
            rr["mix_ex_st_pct"] = float(rr["mix_ex_st_cnt"]) / float(total)
        else:
            rr["bigmove10_pct"] = 0.0
            rr["locked_pct"] = 0.0
            rr["touched_pct"] = 0.0
            rr["locked_touched_pct"] = 0.0
            rr["mix_pct"] = 0.0

            rr["locked_non_st_pct"] = 0.0
            rr["locked_st_pct"] = 0.0
            rr["mix_ex_st_pct"] = 0.0

        out.append(rr)
    return out


def _post_sort_peers(peers_by_sector: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    peers 排除「今日觸及過漲停價」（用 touch_any，避免把封板/炸板放進 peers）
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sec, rows in (peers_by_sector or {}).items():
        arr = list(rows or [])

        def t_any(r: Dict[str, Any]) -> bool:
            if "is_limitup_touch_any" in r:
                return bool(r.get("is_limitup_touch_any", False))
            return bool(r.get("is_limitup_touch", False))

        arr = [r for r in arr if not t_any(r)]

        def key(r: Dict[str, Any]) -> Tuple[int, int, int, float]:
            sp = int(r.get("streak_prev", 0) or 0)
            pl = 1 if bool(r.get("prev_was_limitup_locked", False)) else 0
            pt = 1 if bool(r.get("prev_was_limitup_touch", False)) else 0
            ret = float(r.get("ret", 0.0) or 0.0)
            return (sp, pl, pt, ret)

        arr.sort(key=key, reverse=True)
        out[str(sec)] = arr
    return out


def _ensure_meta_totals(payload: Dict[str, Any], dfS: pd.DataFrame, big10_by_sector: Dict[str, int]) -> None:
    """
    ✅ footer_calc reads meta.totals (then meta.metrics).

    Must provide:
      - locked_total
      - touched_total (touch-only)
      - mix_total
      - bigmove10_ex_total

    CN extra (for mix_ex_st, footer ST line):
      - st_locked_total
      - locked_non_st_total
      - mix_ex_st_total
    """
    payload.setdefault("meta", {})
    payload["meta"].setdefault("totals", {})
    t = payload["meta"]["totals"]

    locked_total = int(dfS["is_limitup_locked"].astype(bool).sum())
    touched_total = int(dfS["is_limitup_touch"].astype(bool).sum())  # ✅ 炸板（touch-only）
    big10_ex_total = int(sum(int(v) for v in big10_by_sector.values()))
    mix_total = int(locked_total + touched_total + big10_ex_total)

    tag = dfS.get("market_tag", pd.Series([""] * len(dfS))).astype(str)
    is_st = tag.eq("ST")
    locked = dfS["is_limitup_locked"].astype(bool)

    st_locked_total = int((locked & is_st).sum())
    locked_non_st_total = int((locked & (~is_st)).sum())
    mix_ex_st_total = int(locked_non_st_total + touched_total + big10_ex_total)

    t.setdefault("locked_total", locked_total)
    t.setdefault("touched_total", touched_total)
    t.setdefault("mix_total", mix_total)

    # ✅ KEY for footer_calc.pick_bigmove10_ex()
    t.setdefault("bigmove10_ex_total", big10_ex_total)

    # optional (inclusive fallback chain)
    t.setdefault("bigmove10_total", big10_ex_total)
    t.setdefault("bigmove10_ge10_total", big10_ex_total)

    # ✅ CN extras
    t.setdefault("st_locked_total", st_locked_total)
    t.setdefault("locked_non_st_total", locked_non_st_total)
    t.setdefault("mix_ex_st_total", mix_ex_st_total)


def _ensure_stats_totals(payload: Dict[str, Any], dfS: pd.DataFrame) -> None:
    """
    Stats are not used by footer_calc big10 pickers (it reads meta.totals/metrics),
    but we keep them for debugging / other modules that might read stats.*.
    """
    payload.setdefault("stats", {})
    st = payload["stats"]

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    mt = meta.get("totals") if isinstance(meta.get("totals"), dict) else {}

    # 1) universe_total (best-effort)
    uni = st.get("universe_total", None)
    try:
        uni = int(uni) if uni is not None else None
    except Exception:
        uni = None

    if not uni or uni <= 0:
        try:
            u2 = int(payload.get("universe_total", 0) or 0)
        except Exception:
            u2 = 0
        uni = u2 if u2 > 0 else int(len(dfS))

    st["universe_total"] = int(uni)

    def _i(x: Any) -> int:
        try:
            return int(x)
        except Exception:
            try:
                return int(float(x))
            except Exception:
                return 0

    locked_total = _i(mt.get("locked_total", 0))
    touched_total = _i(mt.get("touched_total", 0))
    big10_ex_total = _i(mt.get("bigmove10_ex_total", 0))
    mix_total = _i(mt.get("mix_total", 0))

    st["cn_locked_total"] = locked_total
    st["cn_touch_only_total"] = touched_total
    st["cn_bigmove10_ex_total"] = big10_ex_total
    st["cn_mix_total"] = mix_total

    # CN extras
    st["cn_st_locked_total"] = _i(mt.get("st_locked_total", 0))
    st["cn_locked_non_st_total"] = _i(mt.get("locked_non_st_total", 0))
    st["cn_mix_ex_st_total"] = _i(mt.get("mix_ex_st_total", 0))

    # optional aliases
    st["cn_bigmove10_total"] = big10_ex_total
    st["cn_bigmove10_inclusive_total"] = big10_ex_total


# =============================================================================
# DEBUG helpers
# =============================================================================
def _cn_big10_debug(dfS: pd.DataFrame) -> None:
    """
    印出：所有「20%板 + 收盤>=10%」候選股，並解釋為何被 big10 排除（locked / touch_any）。
    """
    if not _env_on("CN_BIG10_DEBUG", "0"):
        return

    if dfS is None or dfS.empty:
        print("[CN_BIG10_DEBUG] dfS empty")
        return

    d = dfS.copy()
    ret = pd.to_numeric(d.get("ret", 0.0), errors="coerce").fillna(0.0)

    cand = d[_is_20_board(d) & (ret >= 0.10)].copy()
    if cand.empty:
        print("[CN_BIG10_DEBUG] no candidates: 20% board & close>=10%")
        return

    for c in ["symbol", "name", "sector", "last_close", "close", "high", "limit_rate", "limit_price", "market_tag"]:
        if c not in cand.columns:
            cand[c] = None

    print("\n" + "=" * 120)
    print("[CN_BIG10_DEBUG] candidates (20% board & close>=10%) and exclusion reason")
    print("-" * 120)

    cand["_ret"] = pd.to_numeric(cand.get("ret", 0.0), errors="coerce").fillna(0.0)
    cand = cand.sort_values("_ret", ascending=False)

    for _, r in cand.iterrows():
        sym = str(r.get("symbol", "") or "")
        nm = str(r.get("name", "") or "")
        sec = str(r.get("sector", "") or "未分類")
        rr = float(r.get("ret") or 0.0)

        lk = bool(r.get("is_limitup_locked") or False)
        ta = bool(r.get("is_limitup_touch_any") or False)
        to = bool(r.get("is_limitup_touch") or False)
        tag = str(r.get("market_tag", "") or "")

        reason: List[str] = []
        if lk:
            reason.append("EXCLUDE:locked")
        if ta:
            reason.append("EXCLUDE:touch_any")
        if (not lk) and (not ta):
            reason.append("OK:big10_ex")

        print(
            f"{sym:<10} {rr*100:>6.2f}%  {sec}  "
            f"tag={tag:<4} locked={int(lk)} touch_any={int(ta)} touch_only={int(to)}  "
            f"{', '.join(reason)}"
        )
        print(
            f"    last_close={r.get('last_close')} close={r.get('close')} high={r.get('high')} "
            f"limit_price={r.get('limit_price')} limit_rate={r.get('limit_rate')} name={nm}"
        )

    print("=" * 120 + "\n")


def _cn_bucket_debug(payload: Dict[str, Any]) -> None:
    """
    印出：sector_summary 的 locked/touched/big10/mix 是否對帳。
    也印出 CN extras（ST locked / non-ST locked / mix_ex_st totals）對帳。
    """
    if not _env_on("CN_OVERVIEW_BUCKET_DEBUG", "0"):
        return

    ss = payload.get("sector_summary") or []
    if not ss:
        print("[CN_OVERVIEW_BUCKET_DEBUG] sector_summary empty")
        return

    print("\n" + "=" * 120)
    print("[CN_OVERVIEW_BUCKET_DEBUG] sector bucket accounting")
    print("-" * 120)
    print(
        f"{'sector':<18} {'locked':>6} {'touch':>6} {'big10':>6} {'mix':>6} {'diff':>6} || "
        f"{'stL':>4} {'nonST':>6} {'mix_ex':>6} {'d2':>4}"
    )
    print("-" * 120)

    sum_locked = 0
    sum_touch = 0
    sum_big10 = 0
    sum_mix = 0
    sum_stL = 0
    sum_nonST = 0
    sum_mix_ex = 0

    for r in ss:
        if not isinstance(r, dict):
            continue
        sec = str(r.get("sector", "") or "未分類")

        locked = int(r.get("locked_cnt", r.get("limitup_locked", r.get("locked", 0))) or 0)
        touch = int(r.get("touched_cnt", r.get("touch_cnt", r.get("limitup_touched", r.get("touched", 0)))) or 0)
        big10 = int(r.get("bigmove10_cnt", 0) or 0)
        mix = int(r.get("mix_cnt", 0) or 0)
        diff = mix - (locked + touch + big10)

        stL = int(r.get("locked_st_cnt", 0) or 0)
        nonST = int(r.get("locked_non_st_cnt", 0) or 0)
        mix_ex = int(r.get("mix_ex_st_cnt", 0) or 0)
        d2 = mix_ex - (nonST + touch + big10)

        sum_locked += locked
        sum_touch += touch
        sum_big10 += big10
        sum_mix += mix
        sum_stL += stL
        sum_nonST += nonST
        sum_mix_ex += mix_ex

        print(
            f"{sec:<18} {locked:>6} {touch:>6} {big10:>6} {mix:>6} {diff:>6} || "
            f"{stL:>4} {nonST:>6} {mix_ex:>6} {d2:>4}"
        )

    mt = (payload.get("meta") or {}).get("totals") or {}
    try:
        L = int(mt.get("locked_total", 0) or 0)
        T = int(mt.get("touched_total", 0) or 0)
        B = int(mt.get("bigmove10_ex_total", 0) or 0)
        M = int(mt.get("mix_total", 0) or 0)

        stL0 = int(mt.get("st_locked_total", 0) or 0)
        nonST0 = int(mt.get("locked_non_st_total", 0) or 0)
        mixex0 = int(mt.get("mix_ex_st_total", 0) or 0)

        print("-" * 120)
        print(
            f"{'SUM_ROWS':<18} {sum_locked:>6} {sum_touch:>6} {sum_big10:>6} {sum_mix:>6} "
            f"{sum_mix-(sum_locked+sum_touch+sum_big10):>6} || "
            f"{sum_stL:>4} {sum_nonST:>6} {sum_mix_ex:>6} {sum_mix_ex-(sum_nonST+sum_touch+sum_big10):>4}"
        )
        print(
            f"{'META_TOTALS':<18} {L:>6} {T:>6} {B:>6} {M:>6} {M-(L+T+B):>6} || "
            f"{stL0:>4} {nonST0:>6} {mixex0:>6} {mixex0-(nonST0+T+B):>4}"
        )
        print(
            f"{'CHECK':<18} {'':>6} {'':>6} {'':>6} {'':>6} {'':>6} || "
            f"{'L==st+non':>10} {int(L==(stL0+nonST0)):>6} {'':>6} {'':>4}"
        )
    except Exception:
        pass

    print("=" * 120 + "\n")


# =============================================================================
# Public
# =============================================================================
def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload or {})

    # normalize market tag (renderer/footer often uses this)
    payload["market"] = "CN"

    dfS = pd.DataFrame(payload.get("snapshot_main") or [])

    if dfS.empty:
        payload["limitup"] = []
        payload["sector_summary"] = []
        payload["peers_by_sector"] = {}
        payload["peers_not_limitup"] = []
        payload.setdefault("filters", {})
        payload["filters"].update({"market": "CN", "disclaimer": "非券商資料；各市場/個股漲停制度不同，結果僅供資訊參考"})
        _ensure_meta_time(payload)
        payload.setdefault("stats", {})
        payload["stats"].update({"snapshot_main_count": 0, "limitup_count": 0, "peers_sectors": 0, "peers_flat_count": 0})
        payload["stats"].setdefault("universe_total", 0)
        return payload

    dfS = _ensure_cols(dfS)
    _ensure_meta_time(payload)

    # ✅ DEBUG: show big10 candidates and why excluded
    _cn_big10_debug(dfS)

    # 用 TW builders（但不會用到 TW tick rules，因為 CN snapshot 已算好 locked/touch）
    limitup_df = build_limitup(dfS)
    payload["limitup"] = [] if limitup_df is None or limitup_df.empty else limitup_df.to_dict(orient="records")

    # 1) base sector_summary (from limitup_df only)
    sector_summary = build_sector_summary_main(limitup_df) or []
    payload["sector_summary"] = sector_summary

    # 2) compute CN extra buckets from full snapshot
    sector_total = _build_sector_total_by_sector(dfS)
    big10_by_sector = _build_bigmove10_by_sector(dfS)
    locked_non_st_by_sector, locked_st_by_sector = _build_locked_split_by_sector(dfS)

    # ✅ KEY FIX: include big10-only sectors into sector_summary BEFORE merging overview fields
    payload["sector_summary"] = _ensure_sector_summary_includes_big10(
        payload["sector_summary"],
        big10_by_sector=big10_by_sector,
    )

    # 3) merge overview fields (mix/big10/pcts etc.)
    payload["sector_summary"] = _merge_overview_fields(
        payload["sector_summary"],
        sector_total=sector_total,
        big10_by_sector=big10_by_sector,
        locked_non_st_by_sector=locked_non_st_by_sector,
        locked_st_by_sector=locked_st_by_sector,
    )

    # ✅ IMPORTANT: write meta.totals.bigmove10_ex_total + mix_ex_st_total + st_locked_total for footer_calc/render
    _ensure_meta_totals(payload, dfS, big10_by_sector)

    # ✅ stats kept for debugging / compatibility
    _ensure_stats_totals(payload, dfS)

    # ✅ DEBUG: bucket accounting per-sector (includes mix_ex_st)
    _cn_bucket_debug(payload)

    peers_by_sector = build_peers_by_sector(dfS, limitup_df)
    peers_by_sector = _post_sort_peers(peers_by_sector)
    payload["peers_by_sector"] = peers_by_sector
    payload["peers_not_limitup"] = flatten_peers(peers_by_sector)

    payload["snapshot_main"] = dfS.to_dict(orient="records")
    payload.setdefault("filters", {})
    payload["filters"].update({"market": "CN", "disclaimer": "非券商資料；各市場/個股漲停制度不同，結果僅供資訊參考"})

    payload.setdefault("stats", {})
    payload["stats"].update(
        {
            "snapshot_main_count": int(len(dfS)),
            "limitup_count": int(len(payload.get("limitup") or [])),
            "peers_sectors": int(len(payload.get("peers_by_sector") or {})),
            "peers_flat_count": int(len(payload.get("peers_not_limitup") or [])),
        }
    )

    return payload