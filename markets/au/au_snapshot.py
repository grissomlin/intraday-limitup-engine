# markets/au/au_snapshot.py
# -*- coding: utf-8 -*-
"""
AU Snapshot Builder (DB-based)

Goal:
- Read AU stock DB (stock_prices + stock_info)
- Pick latest trading day <= ymd (ymd_effective)
- For each symbol, get:
  - today's OHLCV (ymd_effective)
  - prev_close from previous trading day (< ymd_effective)
- Compute:
  - ret = close/prev_close - 1
  - touch_ret = high/prev_close - 1
  - touched_only = (touch_ret>=TH) & ~(ret>=TH)
  - status_text: "10%+ mover" / "Touched 10%+"
- Build snapshot_open rows (FULL universe, not filtered)

✅ Fix in this revision:
- Also compute from DB:
  - hit_prev (whether previous trading day had close ret >= TH)
  - streak / streak_prev (consecutive days with close ret >= TH)
So sector/peer pages can display "Prev >= 10%" properly.

✅ NEW in this revision:
- Fill meta.time with DST-aware Australia/Sydney timezone info for overview subtitle:
  meta.time: {
    market_tz, market_tz_offset, market_utc_offset,
    market_finished_at, market_finished_hm,
    market_finished_at_iso, market_finished_at_utc
  }
- generated_at is UTC (Z) to avoid local machine timezone leakage.

✅ IMPORTANT FIX:
- Windows / missing tzdb may cause ZoneInfo('Australia/Sydney') to fail.
  Fallback is DST-aware (rule-of-thumb) so +11 in summer, +10 in winter.
- Provide market_utc_offset alias and a market_finished_at_iso (with offset)
  so renderers can use a single reliable field without key-order bugs.

✅ IMPORTANT FIX (this revision):
- Compute stats.is_market_open properly (was previously hard-coded to 0).
  This matters for runners that gate payload/cache writing on market-open status.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone, time as dtime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore


def _db_path() -> str:
    return os.getenv("AU_DB_PATH", os.path.join(os.path.dirname(__file__), "au_stock_warehouse.db"))


def _infer_is_reit(sector: Any) -> bool:
    s = str(sector or "").strip().lower()
    if not s:
        return False
    return ("reit" in s) or ("real estate investment trust" in s)


def _get_effective_dates(conn: sqlite3.Connection, ymd: str) -> Tuple[Optional[str], Optional[str]]:
    """
    return (ymd_effective, prev_ymd)
    """
    ymd = str(ymd)[:10]
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    ymd_eff = row[0] if row else None
    if not ymd_eff:
        return None, None

    row2 = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date < ?", (ymd_eff,)).fetchone()
    prev = row2[0] if row2 else None
    return ymd_eff, prev


def _load_today_rows(conn: sqlite3.Connection, ymd_eff: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT symbol, date, open, high, low, close, volume
        FROM stock_prices
        WHERE date = ?
        """,
        conn,
        params=(ymd_eff,),
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
    return df


def _load_prev_close(conn: sqlite3.Connection, prev_ymd: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT symbol, close AS prev_close
        FROM stock_prices
        WHERE date = ?
        """,
        conn,
        params=(prev_ymd,),
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "prev_close"])
    return df


def _load_info(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT symbol, name, sector, market_detail
        FROM stock_info
        WHERE market = 'AU'
        """,
        conn,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "name", "sector", "market_detail"])
    return df


def _ret_threshold() -> float:
    """
    Threshold used for:
      - touched_only definition
      - status_text messaging
    Priority:
      1) AU_RET_TH
      2) AU_OPEN_WATCHLIST_RET_TH (keeps consistent with builders_au)
      3) default 0.10
    """
    v = (os.getenv("AU_RET_TH") or "").strip()
    if v:
        try:
            return float(v)
        except Exception:
            pass
    v2 = (os.getenv("AU_OPEN_WATCHLIST_RET_TH") or "").strip()
    if v2:
        try:
            return float(v2)
        except Exception:
            pass
    return 0.10


def _build_status_text(ret: float, touched_only: bool, th: float) -> str:
    p = int(round(th * 100))
    if ret >= th:
        return f"{p}%+ mover"
    if touched_only:
        return f"Touched {p}%+"
    return ""


def _load_prev_flags_for_day(
    conn: sqlite3.Connection,
    *,
    ymd_eff: str,
    th: float,
) -> pd.DataFrame:
    """
    Compute (for date=ymd_eff):
      - prev_close via LAG(close)
      - ret via close/prev_close - 1
      - hit_prev via LAG(hit)
      - streak / streak_prev for consecutive hit days (hit = ret>=th)

    IMPORTANT:
    - This returns only rows where ret is computable (needs prev_close > 0).
    - We'll LEFT-MERGE this back to df_today to keep FULL universe.
    """
    sql = """
    WITH p AS (
      SELECT
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
      FROM stock_prices
      WHERE date <= ?
    ),
    rets AS (
      SELECT
        symbol,
        date,
        close,
        prev_close,
        CASE
          WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
          THEN (close / prev_close) - 1.0
          ELSE NULL
        END AS ret,
        CASE
          WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
               AND (close / prev_close) - 1.0 >= ?
          THEN 1 ELSE 0
        END AS hit
      FROM p
    ),
    grp AS (
      SELECT
        *,
        SUM(CASE WHEN hit = 0 THEN 1 ELSE 0 END)
          OVER (PARTITION BY symbol ORDER BY date ROWS UNBOUNDED PRECEDING) AS g
      FROM rets
      WHERE ret IS NOT NULL
    ),
    streaked AS (
      SELECT
        *,
        CASE
          WHEN hit = 1 THEN
            SUM(hit) OVER (
              PARTITION BY symbol, g
              ORDER BY date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
          ELSE 0
        END AS streak
      FROM grp
    ),
    final AS (
      SELECT
        s.*,
        COALESCE(LAG(s.hit) OVER (PARTITION BY s.symbol ORDER BY s.date), 0) AS hit_prev,
        COALESCE(LAG(s.streak) OVER (PARTITION BY s.symbol ORDER BY s.date), 0) AS streak_prev
      FROM streaked s
    )
    SELECT
      symbol,
      prev_close AS prev_close_sql,
      ret       AS ret_sql,
      hit_prev,
      streak,
      streak_prev
    FROM final
    WHERE date = ?
    """
    df = pd.read_sql_query(sql, conn, params=(ymd_eff, th, ymd_eff))
    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "prev_close_sql", "ret_sql", "hit_prev", "streak", "streak_prev"])
    return df


# -----------------------------------------------------------------------------
# meta.time builder (DST-aware, with robust fallback)
# -----------------------------------------------------------------------------
def _parse_utc_offset(s: str) -> Optional[timedelta]:
    """
    Parse "+11:00" / "+10" / "-05:30" -> timedelta.
    """
    ss = str(s or "").strip()
    if not ss:
        return None
    try:
        sign = 1
        if ss.startswith("-"):
            sign = -1
            ss = ss[1:]
        elif ss.startswith("+"):
            ss = ss[1:]

        if ":" in ss:
            hh, mm = ss.split(":", 1)
        else:
            hh, mm = ss, "0"

        h = int(hh)
        m = int(mm)
        return sign * timedelta(hours=h, minutes=m)
    except Exception:
        return None


def _format_offset(td: timedelta) -> str:
    """
    "+11:00" style.
    """
    total = int(td.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hh = total // 3600
    mm = (total % 3600) // 60
    return f"{sign}{hh:02d}:{mm:02d}"


def _first_sunday(year: int, month: int) -> datetime:
    """
    First Sunday of (year, month) in UTC timeline (heuristic is enough).
    """
    d = datetime(year, month, 1, tzinfo=timezone.utc)
    shift = (6 - d.weekday()) % 7  # Sunday=6
    return d + timedelta(days=shift)


def _sydney_offset_fallback(dt_utc: datetime) -> timedelta:
    """
    DST-aware fallback when ZoneInfo('Australia/Sydney') is unavailable.

    Rule-of-thumb for Australia/Sydney:
      - DST starts: first Sunday in October
      - DST ends  : first Sunday in April
    Offset: +11 during DST, else +10.
    """
    base_local = dt_utc.astimezone(timezone(timedelta(hours=10)))
    y = base_local.year

    dst_end = _first_sunday(y, 4)
    if base_local.month <= 3:
        dst_start = _first_sunday(y - 1, 10)
    else:
        dst_start = _first_sunday(y, 10)

    in_dst = (dt_utc >= dst_start) and (dt_utc < dst_end)
    return timedelta(hours=11 if in_dst else 10)


def _build_meta_time_au(dt_utc: datetime) -> Dict[str, Any]:
    """
    Build meta.time for AU overview subtitle.

    Priority:
    1) ZoneInfo(AU_MARKET_TZ) (DST correct)
    2) AU_MARKET_TZ_OFFSET if explicitly provided
    3) DST-aware heuristic for Sydney (+11 in summer, +10 in winter)

    Also output:
      - market_finished_at_iso (includes offset) so renderers don't need to guess.
      - market_utc_offset alias (same as market_tz_offset) to avoid key-order bugs.
    """
    market_tz = (os.getenv("AU_MARKET_TZ") or "").strip() or "Australia/Sydney"

    market_finished_at = ""
    market_finished_hm = ""
    market_tz_offset = ""
    market_finished_at_iso = ""

    # 1) Best: real ZoneInfo
    if ZoneInfo is not None:
        try:
            tzinfo = ZoneInfo(market_tz)
            dt_local = dt_utc.astimezone(tzinfo)
            market_finished_at = dt_local.strftime("%Y-%m-%d %H:%M")
            market_finished_hm = dt_local.strftime("%H:%M")
            off = dt_local.utcoffset()
            if off is not None:
                market_tz_offset = _format_offset(off)
            # ISO with offset (very important for robust parsing)
            market_finished_at_iso = dt_local.isoformat(timespec="minutes")
        except Exception:
            pass

    # 2) Fallback: env override or DST-aware heuristic
    if not market_finished_hm:
        off_str = (os.getenv("AU_MARKET_TZ_OFFSET") or "").strip()
        td = _parse_utc_offset(off_str) if off_str else None
        if td is None:
            td = _sydney_offset_fallback(dt_utc)

        tzinfo = timezone(td)
        dt_local = dt_utc.astimezone(tzinfo)
        market_finished_at = dt_local.strftime("%Y-%m-%d %H:%M")
        market_finished_hm = dt_local.strftime("%H:%M")
        market_tz_offset = _format_offset(td)
        market_finished_at_iso = dt_local.isoformat(timespec="minutes")

    return {
        "market_tz": market_tz,
        "market_tz_offset": market_tz_offset,
        "market_utc_offset": market_tz_offset,  # ✅ alias, MUST match
        "market_finished_at": market_finished_at,
        "market_finished_hm": market_finished_hm,
        "market_finished_at_iso": market_finished_at_iso,  # ✅ includes offset
        "market_finished_at_utc": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


# -----------------------------------------------------------------------------
# Market-open detection (AU)
# -----------------------------------------------------------------------------
def _sydney_tzinfo(dt_utc: datetime) -> timezone | Any:
    """
    Return tzinfo for Australia/Sydney.
    Prefer ZoneInfo (DST aware); fallback to DST-aware fixed offset.
    """
    market_tz = (os.getenv("AU_MARKET_TZ") or "").strip() or "Australia/Sydney"
    if ZoneInfo is not None:
        try:
            return ZoneInfo(market_tz)
        except Exception:
            pass
    # fallback offset (DST aware)
    return timezone(_sydney_offset_fallback(dt_utc))


def _parse_hhmm(asof: str) -> Optional[dtime]:
    """
    Parse "HH:MM" -> datetime.time
    """
    s = str(asof or "").strip()
    if not s:
        return None
    try:
        hh, mm = s.split(":", 1)
        return dtime(int(hh), int(mm))
    except Exception:
        return None


def _is_market_open_au(*, dt_utc: datetime, asof: str, has_today_rows: bool) -> int:
    """
    Heuristic:
    - If DB has no today rows, treat as closed.
    - Otherwise determine Sydney local time for (ymd + asof), and check if in session.
      ASX (cash equity) normal session ~10:00-16:00 Sydney time.

    Note:
    - This is a simple gate signal for runners; it doesn't need to be perfect to the minute.
    """
    if not has_today_rows:
        return 0

    t = _parse_hhmm(asof)
    tzinfo = _sydney_tzinfo(dt_utc)
    try:
        now_local = dt_utc.astimezone(tzinfo)
    except Exception:
        now_local = dt_utc  # fallback (won't happen often)

    # Prefer the passed "asof" time if it parses; else use now_local time.
    local_time = t or now_local.timetz().replace(tzinfo=None)

    open_t = dtime(10, 0)
    close_t = dtime(16, 0)
    return 1 if (open_t <= local_time <= close_t) else 0


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    """
    Build AU intraday payload with snapshot_open (FULL universe).
    """
    db_path = _db_path()

    # ✅ always build a UTC timestamp once (avoid local machine timezone leakage)
    dt_utc = datetime.now(timezone.utc)

    if not os.path.exists(db_path):
        return {
            "market": "au",
            "ymd": str(ymd)[:10],
            "ymd_effective": "",
            "slot": slot,
            "asof": asof,
            "generated_at": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "snapshot_open": [],
            "stats": {"snapshot_open_count": 0, "is_market_open": 0},
            "meta": {
                "db_path": db_path,
                "note": "DB not found. Run au_prices.run_sync first.",
                "time": _build_meta_time_au(dt_utc),
            },
        }

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        ymd_eff, prev_ymd = _get_effective_dates(conn, ymd)
        if not ymd_eff or not prev_ymd:
            return {
                "market": "au",
                "ymd": str(ymd)[:10],
                "ymd_effective": ymd_eff or "",
                "slot": slot,
                "asof": asof,
                "generated_at": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "snapshot_open": [],
                "stats": {"snapshot_open_count": 0, "is_market_open": 0},
                "meta": {
                    "db_path": db_path,
                    "note": "Insufficient trading days in DB (need at least 2 dates).",
                    "ymd_effective": ymd_eff,
                    "prev_ymd": prev_ymd,
                    "time": _build_meta_time_au(dt_utc),
                },
            }

        th = float(_ret_threshold())

        df_today = _load_today_rows(conn, ymd_eff)
        df_prev = _load_prev_close(conn, prev_ymd)
        df_info = _load_info(conn)

        # Merge: today + prev_close + info  (FULL universe stays here)
        df = df_today.merge(df_prev, on="symbol", how="left").merge(df_info, on="symbol", how="left")

        # NEW: compute hit_prev / streak from DB, then left-merge back
        df_flags = _load_prev_flags_for_day(conn, ymd_eff=ymd_eff, th=th)
        if df_flags is not None and not df_flags.empty:
            df = df.merge(df_flags, on="symbol", how="left")
        else:
            df["hit_prev"] = 0
            df["streak"] = 0
            df["streak_prev"] = 0
            df["ret_sql"] = pd.NA
            df["prev_close_sql"] = pd.NA

        # Clean defaults
        for c, dv in [
            ("name", "Unknown"),
            ("sector", "Unknown"),
            ("market_detail", "ASX"),
            ("open", 0.0),
            ("high", 0.0),
            ("low", 0.0),
            ("close", 0.0),
            ("volume", 0),
            ("prev_close", 0.0),
            ("hit_prev", 0),
            ("streak", 0),
            ("streak_prev", 0),
        ]:
            if c not in df.columns:
                df[c] = dv
            df[c] = df[c].fillna(dv)

        # numeric normalize
        for col in ["open", "high", "low", "close", "prev_close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)

        df["hit_prev"] = pd.to_numeric(df.get("hit_prev", 0), errors="coerce").fillna(0).astype(int)
        df["streak"] = pd.to_numeric(df.get("streak", 0), errors="coerce").fillna(0).astype(int)
        df["streak_prev"] = pd.to_numeric(df.get("streak_prev", 0), errors="coerce").fillna(0).astype(int)

        # denom (avoid div by 0)
        denom = df["prev_close"].where(df["prev_close"] > 0, pd.NA)

        # ret: prefer SQL ret if available, else compute
        if "ret_sql" in df.columns:
            df["ret_sql"] = pd.to_numeric(df["ret_sql"], errors="coerce")
        df["ret"] = df.get("ret_sql")
        need_calc = df["ret"].isna()
        df.loc[need_calc, "ret"] = (df.loc[need_calc, "close"] / denom.loc[need_calc]) - 1.0
        df["ret"] = pd.to_numeric(df["ret"], errors="coerce").fillna(0.0)

        # touch_ret: high vs prev close
        df["touch_ret"] = (df["high"] / denom) - 1.0
        df["touch_ret"] = pd.to_numeric(df["touch_ret"], errors="coerce").fillna(0.0)

        # touched_only: hit threshold intraday but didn't close above threshold
        df["touched_only"] = (df["touch_ret"] >= th) & ~(df["ret"] >= th)
        df["touched_only"] = df["touched_only"].fillna(False).astype(bool)

        # if prev_close missing, ret/touch_ret are 0; ensure touched_only false
        df.loc[df["prev_close"] <= 0, "touched_only"] = False

        # is_reit
        df["is_reit"] = df.get("sector", "").apply(_infer_is_reit)

        # move fields
        if "move_band" not in df.columns:
            df["move_band"] = -1
        if "move_key" not in df.columns:
            df["move_key"] = ""

        df["market_label"] = "AU"
        df["bar_date"] = str(ymd_eff)[:10]

        df["status_text"] = [
            _build_status_text(float(r), bool(t), th)
            for r, t in zip(df["ret"].tolist(), df["touched_only"].tolist())
        ]

        df = df.sort_values("ret", ascending=False).reset_index(drop=True)

        rows: List[Dict[str, Any]] = []
        for r in df.itertuples(index=False):
            touch_ret_val = float(getattr(r, "touch_ret") or 0.0)
            touched_only_val = bool(getattr(r, "touched_only"))
            ret_val = float(getattr(r, "ret") or 0.0)

            rows.append(
                {
                    "symbol": str(getattr(r, "symbol")),
                    "name": str(getattr(r, "name") or "Unknown"),
                    "sector": str(getattr(r, "sector") or "Unknown"),
                    "is_reit": bool(getattr(r, "is_reit")),
                    "market_detail": str(getattr(r, "market_detail") or "ASX"),
                    "market_label": str(getattr(r, "market_label") or "AU"),
                    "bar_date": str(getattr(r, "bar_date") or str(ymd_eff)[:10]),
                    "prev_close": float(getattr(r, "prev_close") or 0.0),
                    "open": float(getattr(r, "open") or 0.0),
                    "high": float(getattr(r, "high") or 0.0),
                    "low": float(getattr(r, "low") or 0.0),
                    "close": float(getattr(r, "close") or 0.0),
                    "volume": int(getattr(r, "volume") or 0),
                    "ret": ret_val,
                    "touch_ret": touch_ret_val,
                    "touched_only": touched_only_val,
                    "streak": int(getattr(r, "streak") or 0),
                    "streak_prev": int(getattr(r, "streak_prev") or 0),
                    "hit_prev": int(getattr(r, "hit_prev") or 0),
                    "badge_text": "",
                    "badge_level": 0,
                    "status_text": str(getattr(r, "status_text") or _build_status_text(ret_val, touched_only_val, th)),
                    "limit_type": "open_limit",
                    "is_limitup_touch": False,
                    "is_limitup_locked": False,
                    "move_band": int(getattr(r, "move_band") if getattr(r, "move_band") is not None else -1),
                    "move_key": str(getattr(r, "move_key") or ""),
                }
            )

        # ✅ FIX: compute is_market_open (was always 0)
        is_open = _is_market_open_au(
            dt_utc=dt_utc,
            asof=asof,
            has_today_rows=(df_today is not None and not df_today.empty),
        )

        payload: Dict[str, Any] = {
            "market": "au",
            "ymd": str(ymd)[:10],
            "ymd_effective": str(ymd_eff)[:10],
            "slot": slot,
            "asof": asof,
            "generated_at": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "snapshot_open": rows,
            "stats": {
                "snapshot_open_count": int(len(rows)),
                "is_market_open": int(is_open),
            },
            "meta": {
                "db_path": db_path,
                "prev_ymd": str(prev_ymd)[:10],
                "ret_th": th,
                "time": _build_meta_time_au(dt_utc),
            },
        }
        return payload
    finally:
        conn.close()


if __name__ == "__main__":
    print(run_intraday(slot="close", asof="16:10", ymd=datetime.now().strftime("%Y-%m-%d"))["stats"])
