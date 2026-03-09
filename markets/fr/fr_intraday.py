# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

from .fr_config import (
    FR_BADGE_FALLBACK_LANG,
    FR_PEER_EXTRA_PAGES,
    FR_RET_TH,
    FR_ROWS_PER_BOX,
    FR_STREAK_LOOKBACK_ROWS,
    FR_TOUCH_TH,
    db_path,
    log,
)
from .fr_db import pick_latest_leq


try:
    from scripts.render_images_common.move_bands import move_badge  # (band, key)
except Exception:
    def move_badge(ret: float) -> Tuple[int, str]:
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


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


def _clean_text(x: Any, default: str = "Unknown") -> str:
    s = ("" if x is None else str(x)).strip()
    if (not s) or s.lower() in {"nan", "none", "null", "-", "—", "--", "n/a", "na"}:
        return default
    return s


def _clean_sector_text(x: Any) -> str:
    return _clean_text(x, default="Unknown")


def _paris_offset_colon_for_date(ymd: Optional[str] = None) -> str:
    """
    Return Europe/Paris UTC offset for the given date, with colon:
      +01:00
      +02:00

    If ymd is missing or parsing fails, fallback to current Paris offset.
    """
    try:
        if ZoneInfo is not None:
            tz = ZoneInfo("Europe/Paris")

            if ymd:
                # noon avoids edge-case ambiguity around DST switch near midnight
                dt_paris = datetime.fromisoformat(f"{str(ymd)[:10]}T12:00:00").replace(tzinfo=tz)
            else:
                dt_paris = datetime.now(tz)

            off = dt_paris.strftime("%z")  # +0100 / +0200
            if off and len(off) == 5:
                return f"{off[:3]}:{off[3:]}"
    except Exception:
        pass
    return "+01:00"


def _now_paris_iso() -> str:
    """
    Return timezone-aware ISO datetime in Europe/Paris, e.g.
      2026-03-09T12:46:00+01:00
    """
    try:
        if ZoneInfo is not None:
            return datetime.now(ZoneInfo("Europe/Paris")).isoformat(timespec="seconds")
    except Exception:
        pass
    return datetime.now().isoformat(timespec="seconds")


def _compute_streaks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)
    df["hit_10_close"] = df["hit_10_close"].fillna(False).astype(bool)

    df["g"] = (~df["hit_10_close"]).groupby(df["symbol"]).cumsum()

    df["streak"] = 0
    hit_rows = df["hit_10_close"]
    df.loc[hit_rows, "streak"] = (
        df.loc[hit_rows, "hit_10_close"]
        .astype(int)
        .groupby([df.loc[hit_rows, "symbol"], df.loc[hit_rows, "g"]])
        .cumsum()
    )

    df["hit_prev"] = df.groupby("symbol")["hit_10_close"].shift(1).fillna(False).astype(int)
    df["streak_prev"] = df.groupby("symbol")["streak"].shift(1).fillna(0).astype(int)

    return df.drop(columns=["g"], errors="ignore")


def _base_payload(
    *,
    slot: str,
    asof: str,
    ymd: str,
    ymd_effective: str,
    dbp: Path,
    time_meta: Dict[str, Any],
    snapshot_open_count: int = 0,
    skipped_no_prev: int = 0,
    peers_sectors: int = 0,
    peers_flat_count: int = 0,
    errors: Optional[List[Dict[str, Any]]] = None,
    snapshot_open: Optional[List[Dict[str, Any]]] = None,
    peers_by_sector: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    peers_not_limitup: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "market": "fr",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": _now_paris_iso(),
        "filters": {
            "enable_open_watchlist": True,
            "note": "FR treated as open_limit (no daily limit). Built from local DB.",
            "ret_th": FR_RET_TH,
            "touch_th": FR_TOUCH_TH,
            "rows_per_box": FR_ROWS_PER_BOX,
            "peer_extra_pages": FR_PEER_EXTRA_PAGES,
            "badge_fallback_lang": FR_BADGE_FALLBACK_LANG,
            "streak_lookback_rows": int(FR_STREAK_LOOKBACK_ROWS),
        },
        "stats": {
            "snapshot_main_count": 0,
            "snapshot_open_count": int(snapshot_open_count),
            "snapshot_open_skipped_no_prev": int(skipped_no_prev),
            "peers_sectors": int(peers_sectors),
            "peers_flat_count": int(peers_flat_count),
        },
        "snapshot_main": [],
        "snapshot_open": snapshot_open or [],
        "peers_by_sector": peers_by_sector or {},
        "peers_not_limitup": peers_not_limitup or [],
        "errors": errors or [],
        "meta": {
            "db_path": str(dbp),
            "ymd_effective": ymd_effective,
            "time": time_meta,
        },
    }


def run_intraday(slot: str, asof: str, ymd: str, db_path_override: Optional[Path] = None) -> Dict[str, Any]:
    dbp = db_path_override or Path(db_path())
    if isinstance(dbp, str):
        dbp = Path(dbp)

    if not dbp.exists():
        raise FileNotFoundError(f"FR DB not found: {dbp} (set FR_DB_PATH to override)")

    conn = sqlite3.connect(str(dbp))
    try:
        ymd_effective = pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        time_meta: Dict[str, Any] = {
            "market_tz": "Europe/Paris",
            "market_tz_offset": _paris_offset_colon_for_date(ymd_effective),
        }

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
          WHERE i.market='FR' AND sp.date <= ?
        )
        SELECT
          symbol, ymd, open, high, low, close, volume, prev_close,
          name, sector, market_detail
        FROM base
        WHERE rn <= ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, int(FR_STREAK_LOOKBACK_ROWS)))
    finally:
        conn.close()

    if df.empty:
        return _base_payload(
            slot=slot,
            asof=asof,
            ymd=ymd,
            ymd_effective=ymd_effective,
            dbp=dbp,
            time_meta=time_meta,
            errors=[{"reason": "no_rows_for_ymd_effective"}],
        )

    df["name"] = (
        df["name"]
        .fillna("Unknown")
        .astype(str)
        .str.strip()
        .replace({"": "Unknown", "nan": "Unknown", "None": "Unknown", "none": "Unknown"})
    )
    df["sector"] = df["sector"].apply(_clean_sector_text)
    df["market_detail"] = (
        df["market_detail"]
        .fillna("Unknown")
        .astype(str)
        .str.strip()
        .replace({"": "Unknown", "nan": "Unknown", "None": "Unknown", "none": "Unknown"})
    )

    for col in ("prev_close", "open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0).astype(int)

    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    skipped_no_prev = int((~m).sum())
    df = df[m].copy()

    if df.empty:
        return _base_payload(
            slot=slot,
            asof=asof,
            ymd=ymd,
            ymd_effective=ymd_effective,
            dbp=dbp,
            time_meta=time_meta,
            skipped_no_prev=skipped_no_prev,
            errors=[{"reason": "all_rows_missing_prev_close_or_close"}],
        )

    df["ret"] = (df["close"] / df["prev_close"]) - 1.0
    df["touch_ret"] = (df["high"] / df["prev_close"]) - 1.0

    df["touched_10"] = df["touch_ret"].notna() & (df["touch_ret"] >= FR_TOUCH_TH)
    df["hit_10_close"] = df["ret"].notna() & (df["ret"] >= FR_RET_TH)
    df["touched_only"] = df["touched_10"] & (~df["hit_10_close"])

    df = _compute_streaks(df)

    badges = df["ret"].fillna(0.0).apply(lambda x: move_badge(float(x)))
    df["move_band"] = badges.apply(lambda t: int(t[0]) if t and len(t) >= 1 else -1)
    df["move_key"] = badges.apply(lambda t: str(t[1]) if t and len(t) >= 2 else "")

    df["badge_level"] = df["move_band"].where(df["move_band"] >= 0, 0).astype(int)
    df["badge_text"] = df["move_key"].apply(lambda k: _t(FR_BADGE_FALLBACK_LANG, k, default="") if k else "")

    df_day = df[df["ymd"].astype(str) == str(ymd_effective)].copy()
    if df_day.empty:
        return _base_payload(
            slot=slot,
            asof=asof,
            ymd=ymd,
            ymd_effective=ymd_effective,
            dbp=dbp,
            time_meta=time_meta,
            skipped_no_prev=skipped_no_prev,
            errors=[{"reason": "no_rows_for_ymd_effective_after_history_load"}],
        )

    # helpful meta time fields for renderers
    try:
        hm = str(asof).strip()
        if len(hm) >= 5 and ":" in hm:
            time_meta["market_finished_hm"] = hm[:5]
    except Exception:
        pass

    # write timezone-aware Paris timestamp into payload
    time_meta["market_finished_at"] = _now_paris_iso()

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
            parts.append(f"touched ≥{int(FR_TOUCH_TH * 100)}% (close < {int(FR_RET_TH * 100)}%)")
            parts.append(f"prev close < {int(FR_RET_TH * 100)}%")
        elif hit_today and hit_prev == 1:
            parts.append(f"{int(FR_RET_TH * 100)}%+ streak: {streak}")
            parts.append(f"prev streak: {streak_prev}")
        elif hit_today and hit_prev == 0:
            parts.append(f"close ≥{int(FR_RET_TH * 100)}%")
            parts.append("prev not hit")

        status_text = " | ".join(parts)

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "FR",
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
                "move_band": int(r.get("move_band") if r.get("move_band") is not None else -1),
                "move_key": str(r.get("move_key") or ""),
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "limit_type": "open_limit",
                "status_text": status_text,
            }
        )

    df_sort = df_day.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_10_close"]].groupby("sector").size().to_dict()

    df_peers = df_sort[~df_sort["hit_10_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, FR_ROWS_PER_BOX))
        peer_cap = FR_ROWS_PER_BOX * (mover_pages + max(0, FR_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, rr in g2.iterrows():
            rows.append(
                {
                    "symbol": str(rr["symbol"]),
                    "name": str(rr["name"]),
                    "sector": str(rr["sector"]),
                    "market": "FR",
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
                    "move_band": int(rr.get("move_band") if rr.get("move_band") is not None else -1),
                    "move_key": str(rr.get("move_key") or ""),
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

    return _base_payload(
        slot=slot,
        asof=asof,
        ymd=ymd,
        ymd_effective=ymd_effective,
        dbp=dbp,
        time_meta=time_meta,
        snapshot_open_count=len(snapshot_open),
        skipped_no_prev=skipped_no_prev,
        peers_sectors=len(peers_by_sector),
        peers_flat_count=len(peers_not_limitup),
        snapshot_open=snapshot_open,
        peers_by_sector=peers_by_sector,
        peers_not_limitup=peers_not_limitup,
    )
