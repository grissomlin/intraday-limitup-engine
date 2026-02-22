# markets/kr/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Tuple
import os
import sqlite3
from datetime import datetime

import pandas as pd


# =============================================================================
# Env helpers
# =============================================================================
def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


# =============================================================================
# Database connection for checking new listings
# =============================================================================
def _get_db_connection():
    """DB 연결을 가져오기"""
    db_path = os.getenv("KR_DB_PATH", os.path.join(os.path.dirname(__file__), "kr_stock_warehouse.db"))
    if not os.path.exists(db_path):
        return None
    return sqlite3.connect(db_path)


def _get_listing_info(symbol: str, ymd: str) -> Tuple[bool, int, str]:
    """
    신규 상장 여부 체크
    반환: (is_new, days_since_listing, listing_date)

    ✅ 개선: 신규상장 판정 강화
    1) 거래량이 있는 첫 날짜를 상장일로 사용
    2) 상장 후 30일 미만이면 신규상장으로 표시
    """
    conn = _get_db_connection()
    if not conn:
        return False, 0, ""

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT MIN(date)
            FROM stock_prices
            WHERE symbol = ?
              AND close IS NOT NULL
              AND volume > 0
            """,
            (symbol,),
        )
        result = cursor.fetchone()

        if not result or not result[0]:
            return False, 0, ""

        listing_date = str(result[0])

        try:
            listing_dt = datetime.strptime(listing_date, "%Y-%m-%d")
            current_dt = datetime.strptime(ymd, "%Y-%m-%d")
            days_since_listing = (current_dt - listing_dt).days
            is_new = days_since_listing < 30
            return is_new, days_since_listing, listing_date
        except ValueError:
            return False, 0, listing_date

    finally:
        conn.close()


def _detect_new_listing_pattern(symbol: str, ymd: str) -> Tuple[bool, str]:
    """
    ✅ 신규 상장(또는 상장 직후) 패턴 탐지 (가격/거래량 패턴 기반)

    규칙:
    1) 여러 날 연속 종가가 완전히 동일(거래 거의 없음) -> 이후 급변
    2) 초기 거래량 매우 작다가 최근 급증

    반환: (is_new_by_pattern, reason_ko)
    """
    conn = _get_db_connection()
    if not conn:
        return False, ""

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT date, close, volume, high, low
            FROM stock_prices
            WHERE symbol = ?
              AND date <= ?
            ORDER BY date DESC
            LIMIT 30
            """,
            (symbol, ymd),
        )

        rows = cursor.fetchall()
        if len(rows) < 10:
            return False, ""

        rows = list(reversed(rows))

        prices = [row[1] for row in rows[:-5] if row[1] is not None]
        volumes = [row[2] for row in rows[:-5] if row[2] is not None]

        if len(prices) < 10:
            return False, ""

        unique_prices = set(prices)
        if len(unique_prices) == 1:
            recent_prices = [row[1] for row in rows[-5:] if row[1] is not None]
            if recent_prices and recent_prices[-1] != prices[0]:
                return True, f"종가가 {len(prices)}일 연속 동일 후 급변"

        if volumes:
            avg_early_volume = sum(volumes[:15]) / len(volumes[:15]) if len(volumes) >= 15 else 0
            recent_volumes = [row[2] for row in rows[-5:] if row[2] is not None]
            avg_recent_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0

            if avg_early_volume < 1000 and avg_recent_volume > avg_early_volume * 10:
                return True, f"초기 거래량 매우 작음({avg_early_volume:.0f}) → 최근 급증({avg_recent_volume:.0f})"

        return False, ""

    finally:
        conn.close()


# =============================================================================
# ✅ Korea unified limit rate: 30%
# =============================================================================
def _get_limit_rate_for_row(row: Dict[str, Any]) -> float:
    _ = row
    return 0.30


# =============================================================================
# Dataframe normalize
# =============================================================================
def _df(snapshot: Any, ymd: str = "") -> pd.DataFrame:
    """
    snapshot_main(raw/agg 모두 가능)을 DataFrame으로 정규화.

    ✅ 중요:
    - snapshot_builder가 이미 is_limitup30_locked/touch/is_bigup10 을 계산함
    - aggregator에서 ret_high가 없으면 touch 재계산이 깨질 수 있으므로
      snapshot_builder 플래그를 우선 사용하고,
      없을 때만 fallback으로 ret/ret_high 기반 계산을 사용한다.
    """
    if not snapshot or not isinstance(snapshot, list):
        return pd.DataFrame()
    df = pd.DataFrame(snapshot)
    if df.empty:
        return df

    defaults = [
        ("symbol", ""),
        ("name", "Unknown"),
        ("sector", "미분류"),
        ("ret", 0.0),
        ("ret_high", None),
        ("ymd", ""),
        ("market", ""),
        ("market_detail", ""),
        ("streak30", 0),
        ("streak30_prev", 0),
        ("streak10", 0),
        ("streak10_prev", 0),
        ("status_line1", ""),
        ("status_line2", ""),
        ("status", ""),
    ]
    for c, dv in defaults:
        if c not in df.columns:
            df[c] = dv

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df = df[df["symbol"].ne("")].copy()

    if "ymd" not in df.columns or df["ymd"].isnull().all():
        if ymd:
            df["ymd"] = ymd

    df["ret"] = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)
    df["sector"] = df["sector"].fillna("").replace("", "미분류")
    df["name"] = df["name"].fillna("Unknown")

    for c in ["streak30", "streak30_prev", "streak10", "streak10_prev"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # ✅ 고정 상한 기준 30%
    df["limit_rate"] = 0.30

    # ret_high normalize (있으면 사용, 없으면 NaN 유지)
    if "ret_high" in df.columns:
        df["ret_high"] = pd.to_numeric(df["ret_high"], errors="coerce")
    else:
        df["ret_high"] = pd.NA

    # -------------------------------------------------------------------------
    # ✅ 이벤트 플래그: snapshot_builder 플래그 우선 사용
    # -------------------------------------------------------------------------
    has_sb_locked = "is_limitup30_locked" in df.columns
    has_sb_touch = "is_limitup30_touch" in df.columns
    has_sb_big10 = "is_bigup10" in df.columns

    if has_sb_locked:
        df["is_limitup_locked"] = df["is_limitup30_locked"].astype(bool)
    else:
        df["is_limitup_locked"] = df["ret"] >= 0.30

    if has_sb_touch:
        df["is_limitup_touch"] = df["is_limitup30_touch"].astype(bool)
    else:
        # fallback: ret_high 없으면 touch 정확도 떨어짐
        rh = df["ret_high"].fillna(df["ret"])
        df["is_limitup_touch"] = (rh >= 0.30) & (df["ret"] < 0.30)

    if has_sb_big10:
        df["is_bigup"] = df["is_bigup10"].astype(bool)
    else:
        df["is_bigup"] = df["ret"] >= 0.10

    # ✅ 신규상장 탐지
    df["is_new_listing"] = False
    df["new_listing_days"] = 0
    df["new_listing_date"] = ""
    df["new_listing_reason"] = ""

    if not df.empty and "ymd" in df.columns:
        for idx, row in df.iterrows():
            symbol = row["symbol"]
            current_ymd = row["ymd"] if pd.notna(row["ymd"]) else ymd
            if not symbol or not current_ymd:
                continue

            is_new_db, days_since, listing_date = _get_listing_info(symbol, str(current_ymd)[:10])
            is_new_pattern, pattern_reason = _detect_new_listing_pattern(symbol, str(current_ymd)[:10])

            if is_new_db or is_new_pattern:
                df.at[idx, "is_new_listing"] = True
                df.at[idx, "new_listing_days"] = int(days_since or 0)
                df.at[idx, "new_listing_date"] = str(listing_date or "")

                if is_new_db and is_new_pattern:
                    df.at[idx, "new_listing_reason"] = f"상장 {days_since}일 + {pattern_reason}"
                elif is_new_db:
                    df.at[idx, "new_listing_reason"] = f"상장 {days_since}일"
                else:
                    df.at[idx, "new_listing_reason"] = pattern_reason

    return df.reset_index(drop=True)


# =============================================================================
# Event definition
# =============================================================================
def _is_event_row(dfS: pd.DataFrame) -> pd.Series:
    """
    이벤트 정의: 상한가(>=30%) 또는 터치(장중>=30% & 종가<30%) 또는 급등(>=10%)
    """
    if dfS is None or dfS.empty:
        return pd.Series([], dtype=bool)
    return (dfS["is_limitup_locked"] | dfS["is_limitup_touch"] | dfS["is_bigup"])


# =============================================================================
# Status builders (KR)
# =============================================================================
def _event_status_lines(row: pd.Series) -> tuple[str, str, str]:
    """
    반환: (status_line1, status_line2, status)
    """
    locked = bool(row.get("is_limitup_locked"))
    touch = bool(row.get("is_limitup_touch"))
    bigup = bool(row.get("is_bigup"))
    ret = float(row.get("ret") or 0.0)

    is_new = bool(row.get("is_new_listing", False))
    new_date = str(row.get("new_listing_date", "")).strip()

    def _new_suffix() -> str:
        if not is_new:
            return ""
        return f" 신규상장({new_date})" if new_date else " 신규상장"

    # ✅ 상한가(종가 30% 이상)
    if locked:
        prev30 = int(row.get("streak30_prev") or 0)
        line1 = "상한가"
        line2 = "전일 상한가(30%) 없음" if prev30 <= 0 else f"전일 상한가 {prev30}연속(30%)"
        line2 = f"{line2}{_new_suffix()}".strip()
        status = f"{line1} | {line2}".strip()
        return line1, line2, status

    # ✅ 터치(장중 30% 도달, 종가 미달)
    if touch:
        prev30 = int(row.get("streak30_prev") or 0)
        line1 = "터치"
        line2 = "전일 상한가(30%) 없음" if prev30 <= 0 else f"전일 상한가 {prev30}연속(30%)"
        line2 = f"{line2}{_new_suffix()}".strip()
        status = f"{line1} | {line2}".strip()
        return line1, line2, status

    # ✅ 10%+ 급등/강세
    if bigup:
        prev10 = int(row.get("streak10_prev") or 0)
        line1 = "급등" if ret >= 0.20 else "강세"
        line2 = "전일 10%+ 없음" if prev10 <= 0 else f"전일 10%+ {prev10}일"
        line2 = f"{line2}{_new_suffix()}".strip()
        status = f"{line1} | {line2}".strip()
        return line1, line2, status

    return "", "", ""


def _peer_status_lines(row: pd.Series) -> tuple[str, str, str]:
    """
    peers: 오늘은 이벤트가 아니지만, 전일 강세/연속 상태를 보여줌
    """
    prev30 = int(row.get("streak30_prev") or 0)
    prev10 = int(row.get("streak10_prev") or 0)
    is_new = bool(row.get("is_new_listing", False))
    new_date = str(row.get("new_listing_date", "")).strip()

    line1 = "동일 업종"
    if prev30 > 0:
        line2 = f"전일 상한가 {prev30}연속"
    elif prev10 > 0:
        line2 = f"전일 10%+ {prev10}일"
    else:
        line2 = "전일 10%+ 없음"

    if is_new:
        line2 = f"{line2} 신규상장({new_date})" if new_date else f"{line2} 신규상장"

    status = f"{line1} | {line2}".strip()
    return line1, line2, status


# =============================================================================
# Build limitup/events
# =============================================================================
def _build_limitup(dfS: pd.DataFrame) -> pd.DataFrame:
    if dfS is None or dfS.empty:
        return pd.DataFrame()

    m = _is_event_row(dfS)
    out = dfS.loc[m].copy()

    s = out.apply(
        lambda r: pd.Series(_event_status_lines(r), index=["status_line1", "status_line2", "status"]),
        axis=1,
    )
    out["status_line1"] = s["status_line1"].astype(str)
    out["status_line2"] = s["status_line2"].astype(str)
    out["status"] = s["status"].astype(str)

    # ✅ 정렬: 신규상장 우선 → 상한가 우선 → 터치 → ret 내림차순
    out["_rank_new"] = out["is_new_listing"].astype(int)
    out["_rank_locked"] = out["is_limitup_locked"].astype(int)
    out["_rank_touch"] = out["is_limitup_touch"].astype(int)
    out = out.sort_values(
        ["_rank_new", "_rank_locked", "_rank_touch", "ret"],
        ascending=[False, False, False, False],
    ).drop(columns=["_rank_new", "_rank_locked", "_rank_touch"])

    return out.reset_index(drop=True)


# =============================================================================
# ✅ Sector summary (JP-compatible): totals + pct with proper denominator
# =============================================================================
def _sector_summary(df_all: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    섹터별 집계(overview용, JP 규격 호환)

    각 row 최소 포함:
      - sector
      - sector_total
      - locked_cnt, locked_pct（상한가 locked）
      - touched_cnt, touched_pct（touch-only: 터치 but not locked）
      - bigmove10_cnt, bigmove10_pct（✅ pure 10%+: exclude locked/touch）
      - mix_cnt, mix_pct（locked + touched + bigmove10_pure）

    ⚠️ 매우 중요:
      - sector_summary row 에서는 절대 'pct' 라는 key 를 포함하지 않는다.
        (render 쪽에서 pct 를 "공용"으로 해석 + fallback 1.0 하면 전부 100%로 뜸)
    """
    if df_all is None or df_all.empty:
        return []

    df = df_all.copy()

    for c in ["sector", "symbol", "is_limitup_locked", "is_limitup_touch", "is_bigup"]:
        if c not in df.columns:
            if c == "sector":
                df[c] = "미분류"
            elif c == "symbol":
                df[c] = ""
            else:
                df[c] = False

    df["sector"] = df["sector"].fillna("").replace("", "미분류")

    # ✅ pure 10%+: exclude any limitup touch/locked to avoid mix overlap
    df["_bigmove10_pure"] = df["is_bigup"] & (~df["is_limitup_locked"]) & (~df["is_limitup_touch"])

    g = df.groupby("sector", as_index=False).agg(
        sector_total=("symbol", "count"),
        locked_cnt=("is_limitup_locked", "sum"),
        touched_cnt=("is_limitup_touch", "sum"),
        bigmove10_cnt=("_bigmove10_pure", "sum"),
    )

    # ints
    for c in ["sector_total", "locked_cnt", "touched_cnt", "bigmove10_cnt"]:
        g[c] = pd.to_numeric(g[c], errors="coerce").fillna(0).astype(int)

    g["mix_cnt"] = (g["locked_cnt"] + g["touched_cnt"] + g["bigmove10_cnt"]).astype(int)

    # pct (denominator = sector_total)
    def _pct(cnt: pd.Series, tot: pd.Series) -> pd.Series:
        totf = tot.astype(float)
        out = cnt.astype(float) / totf.where(totf > 0.0, 1.0)
        out = out.where(totf > 0.0, 0.0)
        return out

    g["locked_pct"] = _pct(g["locked_cnt"], g["sector_total"])
    g["touched_pct"] = _pct(g["touched_cnt"], g["sector_total"])
    g["bigmove10_pct"] = _pct(g["bigmove10_cnt"], g["sector_total"])
    g["mix_pct"] = _pct(g["mix_cnt"], g["sector_total"])

    # sort: mix_cnt desc then locked then touched then big10
    g = g.sort_values(
        ["mix_cnt", "locked_cnt", "touched_cnt", "bigmove10_cnt"],
        ascending=[False, False, False, False],
    )

    rows = g.to_dict(orient="records")

    # ✅ HARD GUARD: sector_summary rows must never contain 'pct'
    for r in rows:
        if isinstance(r, dict):
            r.pop("pct", None)

    return rows


# =============================================================================
# Build peers (same sector, non-event)
# =============================================================================
def _build_peers_by_sector(
    dfS: pd.DataFrame,
    limitup_df: pd.DataFrame,
    *,
    ret_min: float,
    max_peers_per_sector: int,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    peers_by_sector: '동일 섹터의 비이벤트 종목' 보충 리스트
    """
    if dfS is None or dfS.empty or limitup_df is None or limitup_df.empty:
        return {}

    event_syms = set(limitup_df["symbol"].astype(str).tolist())
    sectors = sorted(set(limitup_df["sector"].astype(str).tolist()))

    out: Dict[str, List[Dict[str, Any]]] = {}
    for sec in sectors:
        dsec = dfS[dfS["sector"].astype(str) == sec].copy()
        if dsec.empty:
            continue

        dsec = dsec[~dsec["symbol"].astype(str).isin(event_syms)].copy()
        if dsec.empty:
            continue

        dsec["ret"] = pd.to_numeric(dsec["ret"], errors="coerce").fillna(0.0)
        dsec = dsec[dsec["ret"] >= float(ret_min)].copy()
        if dsec.empty:
            continue

        # ✅ peers는 비이벤트로 고정(렌더 단에서 오판 방지)
        for c in ["is_limitup_locked", "is_limitup_touch", "is_bigup"]:
            if c in dsec.columns:
                dsec[c] = False

        s = dsec.apply(
            lambda r: pd.Series(_peer_status_lines(r), index=["status_line1", "status_line2", "status"]),
            axis=1,
        )
        dsec["status_line1"] = s["status_line1"].astype(str)
        dsec["status_line2"] = s["status_line2"].astype(str)
        dsec["status"] = s["status"].astype(str)

        dsec = dsec.sort_values("ret", ascending=False).head(int(max_peers_per_sector)).copy()
        out[sec] = dsec.to_dict(orient="records")

    return out


def _flatten_peers(peers_by_sector: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not peers_by_sector:
        return []
    rows: List[Dict[str, Any]] = []
    for sec, items in peers_by_sector.items():
        for r in (items or []):
            rr = dict(r)
            rr.setdefault("sector", sec)
            rows.append(rr)
    return rows


# =============================================================================
# ✅ Meta totals builder (JP-compatible): footer.py reads meta.totals + meta.universe_total
# =============================================================================
def _build_meta_totals(dfS: pd.DataFrame) -> Dict[str, Any]:
    if dfS is None or dfS.empty:
        return {
            "universe_total": 0,
            "totals": {
                "locked_total": 0,
                "touched_total": 0,
                "bigmove10_total": 0,
                "bigmove10_ex_locked_total": 0,
                "mix_total": 0,
            },
        }

    locked_total = int(pd.to_numeric(dfS["is_limitup_locked"], errors="coerce").fillna(0).astype(int).sum())
    touched_total = int(pd.to_numeric(dfS["is_limitup_touch"], errors="coerce").fillna(0).astype(int).sum())
    bigmove10_total = int(pd.to_numeric(dfS["is_bigup"], errors="coerce").fillna(0).astype(int).sum())

    # ✅ pure 10%+: exclude locked/touch (the number footer expects for KR/TH policy)
    bigmove10_ex_locked_total = int(
        (
            dfS["is_bigup"].astype(bool)
            & (~dfS["is_limitup_locked"].astype(bool))
            & (~dfS["is_limitup_touch"].astype(bool))
        )
        .astype(int)
        .sum()
    )

    mix_total = int(locked_total + touched_total + bigmove10_ex_locked_total)

    return {
        "universe_total": int(len(dfS)),
        "totals": {
            "locked_total": int(locked_total),
            "touched_total": int(touched_total),
            "bigmove10_total": int(bigmove10_total),
            "bigmove10_ex_locked_total": int(bigmove10_ex_locked_total),
            "mix_total": int(mix_total),
        },
    }


# =============================================================================
# Aggregate
# =============================================================================
def aggregate(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(raw_payload or {})

    ymd = str(payload.get("ymd") or "")
    ymd_effective = str(payload.get("ymd_effective") or ymd)

    dfS = _df(payload.get("snapshot_main") or [], ymd=ymd_effective)

    if not ymd_effective and not dfS.empty and "ymd" in dfS.columns:
        ymd_effective = str(dfS["ymd"].dropna().astype(str).max())[:10]

    payload["ymd_effective"] = ymd_effective or str(payload.get("ymd") or "")
    payload["is_market_open"] = bool(not dfS.empty)

    # === events ===
    limitup_df = _build_limitup(dfS)
    payload["limitup"] = limitup_df.to_dict(orient="records") if not limitup_df.empty else []

    # ✅ sector summary: MUST use full universe dfS (proper sector_total denominator)
    payload["sector_summary"] = _sector_summary(dfS)

    # ✅ HARD GUARD (double insurance): make sure no row carries 'pct'
    if isinstance(payload.get("sector_summary"), list):
        for r in payload["sector_summary"]:
            if isinstance(r, dict):
                r.pop("pct", None)

    # === peers ===
    ret_min = _env_float("KR_PEERS_RET_MIN", 0.0)
    max_per_sector = _env_int("KR_PEERS_MAX_PER_SECTOR", 10)

    peers_by_sector = _build_peers_by_sector(
        dfS,
        limitup_df,
        ret_min=ret_min,
        max_peers_per_sector=max_per_sector,
    )
    payload["peers_by_sector"] = peers_by_sector
    payload["peers_not_limitup"] = _flatten_peers(peers_by_sector)

    # === filters / stats ===
    payload.setdefault("filters", {})
    payload["filters"]["kr_peers_ret_min"] = float(ret_min)
    payload["filters"]["kr_peers_max_per_sector"] = int(max_per_sector)
    payload["filters"]["kr_limit_rate"] = 0.30

    payload.setdefault("stats", {})
    new_listing_stats: Dict[str, Any] = {}
    if not dfS.empty and "is_new_listing" in dfS.columns:
        new_listing_stats["new_listing_count"] = int(dfS["is_new_listing"].sum())
        if not limitup_df.empty and "is_new_listing" in limitup_df.columns:
            new_listing_stats["new_listing_event_count"] = int(limitup_df["is_new_listing"].sum())

    payload["stats"].update(
        {
            "snapshot_main_count": int(len(dfS)),
            "snapshot_open_count": int(len(payload.get("snapshot_open") or [])),
            "limitup_count": int(len(payload.get("limitup") or [])),
            "peers_sectors": int(len(payload.get("peers_by_sector") or {})),
            "peers_flat_count": int(len(payload.get("peers_not_limitup") or [])),
            "is_market_open": 1 if payload["is_market_open"] else 0,
            **new_listing_stats,
        }
    )

    # ✅ debug: 신규상장 리스트(원하면)
    if not dfS.empty and "is_new_listing" in dfS.columns:
        new_listings = dfS[dfS["is_new_listing"]].copy()
        if not new_listings.empty:
            payload["new_listings_debug"] = new_listings[
                ["symbol", "name", "new_listing_date", "new_listing_days", "new_listing_reason"]
            ].to_dict(orient="records")

    # =============================================================================
    # ✅ JP-compatible meta fields for overview footer
    # =============================================================================
    payload.setdefault("meta", {})
    meta_patch = _build_meta_totals(dfS)
    payload["meta"]["universe_total"] = int(meta_patch.get("universe_total", 0))
    payload["meta"]["totals"] = dict(meta_patch.get("totals", {}))
    payload["meta"]["ymd_effective"] = payload.get("ymd_effective", "")

    return payload


# =============================================================================
# ✅ Debug CLI: run aggregator without downloader/main.py
# =============================================================================
if __name__ == "__main__":
    import argparse
    import json
    from pathlib import Path

    ap = argparse.ArgumentParser(description="KR aggregator debug runner (no download).")
    ap.add_argument("--in", dest="inp", required=True, help="입력 RAW payload JSON (cache 등).")
    ap.add_argument("--out", dest="out", required=True, help="출력 aggregated JSON 경로.")
    ap.add_argument("--pretty", action="store_true", help="Pretty JSON (indent=2).")
    args = ap.parse_args()

    in_path = Path(args.inp)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    agg = aggregate(raw)

    with out_path.open("w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(agg, f, ensure_ascii=False, indent=2)
        else:
            json.dump(agg, f, ensure_ascii=False)

    print(f"✅ aggregated written: {out_path}")
