# markets/kr/snapshot_builder.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

from .indicators_kr import compute_streak_maps, apply_maps

# ✅ unified meta.time builder (same schema as other markets)
try:
    from markets.common.time_builders import build_meta_time_asia  # type: ignore
except Exception:
    build_meta_time_asia = None  # type: ignore

from datetime import datetime, timezone


def _default_db_path() -> str:
    return os.getenv("KR_DB_PATH", os.path.join(os.path.dirname(__file__), "kr_stock_warehouse.db"))


def _th30() -> float:
    return float(os.getenv("KR_LIMITUP30_TH", "0.30"))


def _th10() -> float:
    return float(os.getenv("KR_BIGUP10_TH", "0.10"))


def _lookback_trading_days() -> int:
    # streak 계산용 (K+1 거래일 확보해서 prev_close 안정화)
    return int(os.getenv("KR_STREAK_LOOKBACK_TRADING_DAYS", "60"))


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _latest_k_trading_days(conn: sqlite3.Connection, k: int, *, leq_ymd: str) -> List[str]:
    rows = conn.execute(
        "SELECT DISTINCT date FROM stock_prices WHERE date <= ? ORDER BY date DESC LIMIT ?",
        (leq_ymd, int(k)),
    ).fetchall()
    dates = [r[0] for r in rows if r and r[0]]
    dates.sort()
    return dates


def _load_day_snapshot(conn: sqlite3.Connection, ymd_effective: str) -> pd.DataFrame:
    sql = """
    WITH p AS (
      SELECT
        symbol,
        date,
        open, high, low, close, volume,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
      FROM stock_prices
    )
    SELECT
      p.symbol,
      p.date AS ymd,
      p.open, p.high, p.low, p.close, p.volume,
      p.last_close,
      i.name,
      i.sector,
      i.market,
      i.market_detail
    FROM p
    LEFT JOIN stock_info i ON i.symbol = p.symbol
    WHERE p.date = ?
    """
    return pd.read_sql_query(sql, conn, params=(ymd_effective,))


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    out["name"] = out["name"].fillna("Unknown")
    out["sector"] = out["sector"].fillna("미분류")
    out.loc[out["sector"].isin(["", "—", "-", "--", "－", "–"]), "sector"] = "미분류"

    for c in ["open", "high", "low", "close", "last_close", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["ret"] = 0.0
    m = out["last_close"].notna() & (out["last_close"] > 0) & out["close"].notna()
    out.loc[m, "ret"] = (out.loc[m, "close"] / out.loc[m, "last_close"]) - 1.0

    return out


def _add_flags(df: pd.DataFrame, *, th30: float, th10: float) -> pd.DataFrame:
    """
    ✅ 핵심 수정:
    - ret_high(고가 수익률)을 항상 계산해 snapshot_main 에 포함시키기
      (aggregator가 touch fallback 계산해도 정확도 유지)
    - is_limitup30_touch 는 'locked 제외' 규칙 유지
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    out["ret_high"] = pd.NA
    mh = out["last_close"].notna() & (out["last_close"] > 0) & out["high"].notna()
    out.loc[mh, "ret_high"] = (out.loc[mh, "high"] / out.loc[mh, "last_close"]) - 1.0

    out["is_limitup30_locked"] = out["ret"].notna() & (out["ret"] >= float(th30))

    # ✅ 터치 정의(중요): 고가 30% 도달 & 종가 30% 미만 (locked 제외)
    out["is_limitup30_touch"] = (
        out["ret_high"].notna()
        & (out["ret_high"] >= float(th30))
        & (out["ret"].notna())
        & (out["ret"] < float(th30))
    )

    out["is_bigup10"] = out["ret"].notna() & (out["ret"] >= float(th10))

    return out


def _load_daily_for_streak(conn: sqlite3.Connection, dates: List[str]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()

    ph = ",".join(["?"] * len(dates))
    sql = f"""
    SELECT symbol, date AS ymd, high, close
    FROM stock_prices
    WHERE date IN ({ph})
    """
    df = pd.read_sql_query(sql, conn, params=tuple(dates))
    if df is None or df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["ymd"] = df["ymd"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")

    df = df.sort_values(["symbol", "ymd"], kind="mergesort")
    df["prev_close"] = df.groupby("symbol")["close"].shift(1)
    return df


def _status_lines(r: Dict[str, Any]) -> Dict[str, str]:
    """
    status_line1/status_line2 는 렌더러에서 그대로 노출되므로
    ✅ 여기서 '언어'가 최종 결정됩니다.
    """
    locked = bool(r.get("is_limitup30_locked"))
    touch = bool(r.get("is_limitup30_touch"))
    big10 = bool(r.get("is_bigup10"))
    ret = float(r.get("ret") or 0.0)

    # line1: today (KR)
    if locked:
        line1 = "상한가"
    elif touch:
        line1 = "터치"
    elif big10:
        # 20% 이상이면 '급등', 아니면 '강세' (원하면 컷 변경 가능)
        line1 = "급등" if ret >= 0.20 else "강세"
    else:
        line1 = ""

    # line2: yesterday (KR) - priority: prev limitup(locked/touch) > prev 10%+ > none
    prev_limit = bool(r.get("prev_was_limitup30_locked")) or bool(r.get("prev_was_limitup30_touch"))
    if prev_limit:
        n = int(r.get("streak30_prev") or 0)
        line2 = "전일 상한가(30%) 없음" if n <= 0 else f"전일 상한가 {n}연속(30%)"
    else:
        n10 = int(r.get("streak10_prev") or 0)
        line2 = "전일 10%+ 없음" if n10 <= 0 else f"전일 10%+ {n10}일"

    return {"status_line1": line1, "status_line2": line2}


def _build_meta_time_kr() -> Dict[str, Any]:
    """
    Build meta.time in unified schema.
    - Prefer ZoneInfo via build_meta_time_asia
    - Fallback: empty dict (renderers should tolerate)
    """
    if build_meta_time_asia is None:
        return {}

    try:
        dt_utc = datetime.now(timezone.utc)
        return build_meta_time_asia(
            dt_utc,
            tz_name="Asia/Seoul",
            fallback_offset="+09:00",
        )
    except Exception:
        return {}


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    db_path = _default_db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"KR DB not found: {db_path} (set KR_DB_PATH to override)")

    th30 = _th30()
    th10 = _th10()
    K = max(20, _lookback_trading_days())

    # ✅ meta.time (KR)
    meta_time = _build_meta_time_kr()

    conn = sqlite3.connect(db_path)
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")
        log(f"📦 KR DB = {db_path} | th30={th30} th10={th10} | K={K}")

        # day snapshot
        df = _load_day_snapshot(conn, ymd_effective)
        df = _normalize(df)
        df = _add_flags(df, th30=th30, th10=th10)

        if df is None or df.empty:
            snapshot_main: List[Dict[str, Any]] = []
        else:
            # ✅ 핵심: ret_high 를 snapshot_main 에 포함
            snapshot_main = df[
                [
                    "symbol",
                    "name",
                    "sector",
                    "ymd",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "last_close",
                    "ret",
                    "ret_high",
                    "is_limitup30_locked",
                    "is_limitup30_touch",
                    "is_bigup10",
                    "market",
                    "market_detail",
                ]
            ].to_dict(orient="records")

        # streak maps
        dates = _latest_k_trading_days(conn, K + 1, leq_ymd=ymd_effective)
        daily_df = _load_daily_for_streak(conn, dates)
        maps = compute_streak_maps(daily_df, ymd_effective=ymd_effective, th30=th30, th10=th10)
        snapshot_main = apply_maps(snapshot_main, maps)

        # status lines (언어/문구는 여기서 결정)
        snapshot_main = [dict(r, **_status_lines(r)) for r in snapshot_main]

        raw_payload: Dict[str, Any] = {
            "market": "kr",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "snapshot_main": snapshot_main,
            "snapshot_open": [],
            "stats": {
                "snapshot_main_count": int(len(snapshot_main)),
                "snapshot_open_count": 0,
            },
            "filters": {
                "kr_limitup30_th": float(th30),
                "kr_bigup10_th": float(th10),
                "kr_streak_lookback_trading_days": int(K),
                "kr_streak30_mode": os.getenv("KR_STREAK30_MODE", "touch"),
            },
            "meta": {
                "db_path": db_path,
                "ymd_effective": ymd_effective,
                "time": meta_time,  # ✅ unified meta.time
            },
        }
        return raw_payload
    finally:
        conn.close()