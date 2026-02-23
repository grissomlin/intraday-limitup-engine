# markets/uk/uk_snapshot.py
# -*- coding: utf-8 -*-
"""DB -> raw snapshot builder for UK (adapted from markets/us/us_snapshot.py).

✅ Fix (2026-02-23):
- UK DB may contain mixed price scales (pence vs pounds) even within the SAME symbol.
- DO NOT compute ret/hit/streak in SQL; compute in pandas AFTER scale normalization.
- Snapshot output remains compatible (move_band/move_key + badge_text fallback).
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# -----------------------------------------------------------------------------
# Optional imports (if you haven't created uk_config.py yet, this file still runs)
# -----------------------------------------------------------------------------
try:
    from .uk_config import log, _db_path  # type: ignore
except Exception:

    def log(msg: str) -> None:
        print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

    def _db_path() -> Path:
        return Path(os.getenv("UK_DB_PATH", os.path.join(os.path.dirname(__file__), "uk_stock_warehouse.db")))


# -----------------------------------------------------------------------------
# ZoneInfo (DST-aware for Europe/London)
# -----------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# Shared helpers
try:
    from scripts.render_images_common.move_bands import move_badge  # (band, key)
except Exception:

    def move_badge(ret: float) -> Tuple[int, str]:
        # minimal fallback: same bands
        try:
            r = float(ret)
        except Exception:
            return -1, ""
        if r >= 1.00:
            return 5, "move_band_5"
        if r >= 0.50:
            return 4, "move_band_4"
        if r >= 0.40:
            return 3, "move_band_3"
        if r >= 0.30:
            return 2, "move_band_2"
        if r >= 0.20:
            return 1, "move_band_1"
        if r >= 0.10:
            return 0, "move_band_0"
        return -1, ""


try:
    from scripts.render_images_common.i18n import t as _t  # type: ignore
except Exception:

    def _t(lang: str, key: str, default: str = "", **kwargs: Any) -> str:
        try:
            return (default or key).format(**kwargs)
        except Exception:
            return default or key


# =============================================================================
# Tunables (env)
# =============================================================================
UK_RET_TH = float(os.getenv("UK_RET_TH", "0.10"))  # mover threshold (close ret)
UK_TOUCH_TH = float(os.getenv("UK_TOUCH_TH", "0.10"))  # touched threshold (high ret)
UK_ROWS_PER_BOX = int(os.getenv("UK_ROWS_PER_BOX", "6"))
UK_PEER_EXTRA_PAGES = int(os.getenv("UK_PEER_EXTRA_PAGES", "1"))

# How many recent rows per symbol to load for streak calculation
UK_STREAK_LOOKBACK_ROWS = int(os.getenv("UK_STREAK_LOOKBACK_ROWS", "90"))

# Optional: what language to use for badge_text fallback (render should translate anyway)
UK_BADGE_FALLBACK_LANG = (os.getenv("UK_BADGE_FALLBACK_LANG", "en") or "en").strip().lower()

# scale normalize thresholds (safe guards)
UK_SCALE_UPPER_RATIO = float(os.getenv("UK_SCALE_UPPER_RATIO", "20.0"))  # close/prev_close >= 20 => divide by 100
UK_SCALE_LOWER_RATIO = float(os.getenv("UK_SCALE_LOWER_RATIO", "0.05"))  # close/prev_close <= 0.05 => multiply by 100
UK_SCALE_FACTOR = float(os.getenv("UK_SCALE_FACTOR", "100.0"))


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


def _build_time_meta_uk(*, ymd_effective: str, asof: str) -> Dict[str, Any]:
    """
    Build meta.time (DST-aware) for overview subtitle.
    """
    if ZoneInfo is None:
        return {}

    try:
        tz = ZoneInfo("Europe/London")

        ymd2 = str(ymd_effective)[:10]
        hm2 = str(asof)[:5] if asof else ""
        if not ymd2 or len(ymd2) < 10 or not hm2 or len(hm2) < 4:
            return {}

        dt_local = datetime.fromisoformat(f"{ymd2} {hm2}").replace(tzinfo=tz)

        off = dt_local.utcoffset()
        if off is None:
            return {}

        total_min = int(off.total_seconds() // 60)
        sign = "+" if total_min >= 0 else "-"
        hh = abs(total_min) // 60
        mm = abs(total_min) % 60

        return {
            "market_tz": "Europe/London",
            "market_tz_offset": f"{sign}{hh:02d}:{mm:02d}",
            "market_finished_hm": hm2,
            "market_finished_at": dt_local.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M"),
        }
    except Exception:
        return {}


def _normalize_scale_inplace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize OHLC scale (pence vs pounds) using close/prev_close ratio.

    If close / prev_close >= UK_SCALE_UPPER_RATIO => divide today's OHLC by UK_SCALE_FACTOR
    If close / prev_close <= UK_SCALE_LOWER_RATIO => multiply today's OHLC by UK_SCALE_FACTOR

    NOTE:
    - prev_close is *not* modified (it belongs to previous day).
    - We normalize today's open/high/low/close only.
    """
    if df.empty:
        return df

    eps = 1e-12
    valid = df["prev_close"].notna() & (df["prev_close"] > eps) & df["close"].notna() & (df["close"] > eps)
    if valid.sum() == 0:
        return df

    ratio = (df.loc[valid, "close"] / df.loc[valid, "prev_close"]).astype(float)

    mask_down = ratio >= float(UK_SCALE_UPPER_RATIO)
    mask_up = ratio <= float(UK_SCALE_LOWER_RATIO)

    # Apply in dataframe index space
    idx_down = ratio.index[mask_down]
    idx_up = ratio.index[mask_up]

    if len(idx_down) > 0:
        for c in ("open", "high", "low", "close"):
            if c in df.columns:
                df.loc[idx_down, c] = df.loc[idx_down, c] / float(UK_SCALE_FACTOR)
        df.loc[idx_down, "scale_note"] = "scaled_down(/100)"

    if len(idx_up) > 0:
        for c in ("open", "high", "low", "close"):
            if c in df.columns:
                df.loc[idx_up, c] = df.loc[idx_up, c] * float(UK_SCALE_FACTOR)
        df.loc[idx_up, "scale_note"] = "scaled_up(*100)"

    return df


def _compute_streaks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute hit_10_close, streak, hit_prev, streak_prev per symbol.

    streak = consecutive count of hit_10_close ending at each day (reset to 0 on non-hit days).
    """
    if df.empty:
        return df

    df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)

    # hit_10_close already computed; ensure boolean
    df["hit_10_close"] = df["hit_10_close"].fillna(False).astype(bool)

    # group key resets when hit is False
    # g = cumulative count of non-hit rows -> partitions into hit-runs
    df["g"] = (~df["hit_10_close"]).groupby(df["symbol"]).cumsum()

    # streak within (symbol, g) for hit rows
    df["streak"] = 0
    hit_rows = df["hit_10_close"]
    df.loc[hit_rows, "streak"] = (
        df.loc[hit_rows, "hit_10_close"].astype(int).groupby([df.loc[hit_rows, "symbol"], df.loc[hit_rows, "g"]]).cumsum()
    )

    # previous day info
    df["hit_prev"] = df.groupby("symbol")["hit_10_close"].shift(1).fillna(False).astype(int)
    df["streak_prev"] = df.groupby("symbol")["streak"].shift(1).fillna(0).astype(int)

    return df.drop(columns=["g"], errors="ignore")


def run_intraday(slot: str, asof: str, ymd: str, db_path: Optional[Path] = None) -> Dict[str, Any]:
    db_path = db_path or _db_path()
    if isinstance(db_path, str):
        db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"UK DB not found: {db_path} (set UK_DB_PATH to override)")

    conn = sqlite3.connect(str(db_path))
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        # Load recent rows per symbol (for streak), with prev_close via LAG(close)
        sql = """
        WITH base AS (
          SELECT
            sp.symbol,
            sp.date AS ymd,
            sp.open, sp.high, sp.low, sp.close, sp.volume,
            LAG(sp.close) OVER (PARTITION BY sp.symbol ORDER BY sp.date) AS prev_close,
            i.name,
            i.sector,
            i.market_detail,
            ROW_NUMBER() OVER (PARTITION BY sp.symbol ORDER BY sp.date DESC) AS rn
          FROM stock_prices sp
          JOIN stock_info i ON i.symbol = sp.symbol
          WHERE i.market='UK' AND sp.date <= ?
        )
        SELECT
          symbol, ymd, open, high, low, close, volume, prev_close,
          name, sector, market_detail
        FROM base
        WHERE rn <= ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, int(UK_STREAK_LOOKBACK_ROWS)))
    finally:
        conn.close()

    # ---- build meta.time (DST-aware) early so even empty payload has it ----
    time_meta = _build_time_meta_uk(ymd_effective=str(ymd_effective)[:10], asof=str(asof))

    if df.empty:
        return {
            "market": "uk",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "UK treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": UK_RET_TH,
                "touch_th": UK_TOUCH_TH,
                "rows_per_box": UK_ROWS_PER_BOX,
                "peer_extra_pages": UK_PEER_EXTRA_PAGES,
                "streak_lookback_rows": UK_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective"}],
            "meta": {
                "db_path": str(db_path),
                "ymd_effective": ymd_effective,
                "time": time_meta,
            },
        }

    # normalize fields
    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].fillna("Unknown").replace("", "Unknown")
    df["market_detail"] = df["market_detail"].fillna("Unknown")
    df["scale_note"] = ""

    for col in ("prev_close", "open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0).astype(int)

    # Drop rows with no prev_close or no close (cannot compute ret)
    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    skipped_no_prev = int((~m).sum())
    df = df[m].copy()
    if df.empty:
        return {
            "market": "uk",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "UK treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": UK_RET_TH,
                "touch_th": UK_TOUCH_TH,
                "rows_per_box": UK_ROWS_PER_BOX,
                "peer_extra_pages": UK_PEER_EXTRA_PAGES,
                "streak_lookback_rows": UK_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0, "snapshot_open_skipped_no_prev": skipped_no_prev},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "all_rows_missing_prev_close_or_close"}],
            "meta": {"db_path": str(db_path), "ymd_effective": ymd_effective, "time": time_meta},
        }

    # -------- SCALE NORMALIZE (UK pence/£ mixed) --------
    df = _normalize_scale_inplace(df)

    # -------- recompute ret / touch_ret after normalization --------
    df["ret"] = (df["close"] / df["prev_close"]) - 1.0
    df["touch_ret"] = (df["high"] / df["prev_close"]) - 1.0

    # touched / hit flags
    df["touched_10"] = df["touch_ret"].notna() & (df["touch_ret"] >= UK_TOUCH_TH)
    df["hit_10_close"] = df["ret"].notna() & (df["ret"] >= UK_RET_TH)
    df["touched_only"] = df["touched_10"] & (~df["hit_10_close"])

    # -------- recompute streak / prev flags --------
    df = _compute_streaks(df)

    # move band / key (no text here; render should translate)
    badges = df["ret"].fillna(0.0).apply(lambda x: move_badge(float(x)))
    df["move_band"] = badges.apply(lambda t: int(t[0]) if t and len(t) >= 1 else -1)
    df["move_key"] = badges.apply(lambda t: str(t[1]) if t and len(t) >= 2 else "")

    # backward compatible fields
    df["badge_level"] = df["move_band"].where(df["move_band"] >= 0, 0).astype(int)
    df["badge_text"] = df["move_key"].apply(lambda k: _t(UK_BADGE_FALLBACK_LANG, k, default="") if k else "")

    # -------- select day rows --------
    df_day = df[df["ymd"].astype(str) == str(ymd_effective)].copy()
    if df_day.empty:
        # We had history but not the day; return empty snapshot.
        return {
            "market": "uk",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "UK treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": UK_RET_TH,
                "touch_th": UK_TOUCH_TH,
                "rows_per_box": UK_ROWS_PER_BOX,
                "peer_extra_pages": UK_PEER_EXTRA_PAGES,
                "streak_lookback_rows": UK_STREAK_LOOKBACK_ROWS,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0, "snapshot_open_skipped_no_prev": skipped_no_prev},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective_after_history_load"}],
            "meta": {"db_path": str(db_path), "ymd_effective": ymd_effective, "time": time_meta},
        }

    # -------- build snapshot_open rows --------
    snapshot_open: List[Dict[str, Any]] = []
    for _, r in df_day.iterrows():
        prev_close = float(r["prev_close"] or 0.0)
        close = float(r["close"] or 0.0)
        ret = float(r.get("ret") or 0.0)

        streak = int(r.get("streak") or 0)
        streak_prev = int(r.get("streak_prev") or 0)
        hit_prev = int(r.get("hit_prev") or 0)

        touched_only = bool(r.get("touched_only") or False)
        hit_today = bool(r.get("hit_10_close") or False)

        parts: List[str] = []
        if touched_only:
            parts.append(f"touched ≥{int(UK_TOUCH_TH * 100)}% (close < {int(UK_RET_TH * 100)}%)")
            parts.append(f"prev close < {int(UK_RET_TH * 100)}%")
        elif hit_today and hit_prev == 1:
            parts.append(f"{int(UK_RET_TH * 100)}%+ streak: {streak}")
            parts.append(f"prev streak: {streak_prev}")
        elif hit_today and hit_prev == 0:
            parts.append(f"close ≥{int(UK_RET_TH * 100)}%")
            parts.append("prev not hit")

        status_text = " | ".join(parts)

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "UK",
                "market_detail": str(r.get("market_detail") or "Unknown"),
                "market_label": str(r.get("market_detail") or "Unknown"),
                "bar_date": str(r["ymd"]),
                "prev_close": prev_close,
                "open": float(r.get("open") or 0.0),
                "high": float(r.get("high") or 0.0),
                "low": float(r.get("low") or 0.0),
                "close": close,
                "volume": int(r.get("volume") or 0),
                "ret": ret,
                "touch_ret": float(r.get("touch_ret") or 0.0),
                "touched_only": bool(touched_only),
                "streak": int(streak),
                "streak_prev": int(streak_prev),
                "hit_prev": int(hit_prev),
                # ✅ new (preferred)
                "move_band": int(r.get("move_band") if r.get("move_band") is not None else -1),
                "move_key": str(r.get("move_key") or ""),
                # ✅ backward compatible
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "limit_type": "open_limit",
                "status_text": status_text,
                # (debug hint; harmless if ignored by render)
                # "scale_note": str(r.get("scale_note") or ""),
            }
        )

    # -------- peers (same logic as US open-limit style) --------
    df_sort = df_day.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_10_close"]].groupby("sector").size().to_dict()

    df_peers = df_sort[~df_sort["hit_10_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, UK_ROWS_PER_BOX))
        peer_cap = UK_ROWS_PER_BOX * (mover_pages + max(0, UK_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, rr in g2.iterrows():
            rows.append(
                {
                    "symbol": str(rr["symbol"]),
                    "name": str(rr["name"]),
                    "sector": str(rr["sector"]),
                    "market": "UK",
                    "market_detail": str(rr.get("market_detail") or "Unknown"),
                    "market_label": str(rr.get("market_detail") or "Unknown"),
                    "bar_date": str(rr["ymd"]),
                    "prev_close": float(rr["prev_close"] or 0.0),
                    "open": float(rr.get("open") or 0.0),
                    "high": float(rr.get("high") or 0.0),
                    "low": float(rr.get("low") or 0.0),
                    "close": float(rr.get("close") or 0.0),
                    "volume": int(rr.get("volume") or 0),
                    "ret": float(rr.get("ret") or 0.0),
                    "touch_ret": float(rr.get("touch_ret") or 0.0),
                    "touched_only": bool(rr.get("touched_only") or False),
                    "streak": int(rr.get("streak") or 0),
                    "streak_prev": int(rr.get("streak_prev") or 0),
                    "hit_prev": int(rr.get("hit_prev") or 0),
                    # ✅ new
                    "move_band": int(rr.get("move_band") if rr.get("move_band") is not None else -1),
                    "move_key": str(rr.get("move_key") or ""),
                    # ✅ backward compatible
                    "badge_text": str(rr.get("badge_text") or ""),
                    "badge_level": int(rr.get("badge_level") or 0),
                    "limit_type": "open_limit",
                    "status_text": "",
                }
            )

        peers_by_sector[str(sector)] = rows

    peers_not_limitup: List[Dict[str, Any]] = []
    for _, rows in peers_by_sector.items():
        peers_not_limitup.extend(rows)

    return {
        "market": "uk",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "filters": {
            "enable_open_watchlist": True,
            "note": "UK treated as open_limit (no daily limit). Built from local DB.",
            "ret_th": UK_RET_TH,
            "touch_th": UK_TOUCH_TH,
            "rows_per_box": UK_ROWS_PER_BOX,
            "peer_extra_pages": UK_PEER_EXTRA_PAGES,
            "badge_fallback_lang": UK_BADGE_FALLBACK_LANG,
            "streak_lookback_rows": int(UK_STREAK_LOOKBACK_ROWS),
            "scale_upper_ratio": float(UK_SCALE_UPPER_RATIO),
            "scale_lower_ratio": float(UK_SCALE_LOWER_RATIO),
            "scale_factor": float(UK_SCALE_FACTOR),
        },
        "stats": {
            "snapshot_main_count": 0,
            "snapshot_open_count": len(snapshot_open),
            "snapshot_open_skipped_no_prev": skipped_no_prev,
            "peers_sectors": int(len(peers_by_sector)),
            "peers_flat_count": int(len(peers_not_limitup)),
        },
        "snapshot_main": [],
        "snapshot_open": snapshot_open,
        "peers_by_sector": peers_by_sector,
        "peers_not_limitup": peers_not_limitup,
        "errors": [],
        "meta": {
            "db_path": str(db_path),
            "ymd_effective": ymd_effective,
            "time": time_meta,
        },
    }


if __name__ == "__main__":
    res = run_intraday(
        slot="test",
        asof=datetime.now().strftime("%H:%M"),
        ymd=datetime.now().strftime("%Y-%m-%d"),
    )
    print("snapshot_open =", len(res.get("snapshot_open") or []))
    print("meta.time =", (res.get("meta") or {}).get("time"))
