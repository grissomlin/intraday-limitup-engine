# scripts/render_images/status_report.py
# -*- coding: utf-8 -*-
"""
把 TW payload 轉成可貼 X 的文字報告，並寫入 out_dir / tw_status.txt

設計目標：
- 短、清楚、可 debug
- 不塞進 payload（避免 token / json 變胖）
"""

from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path


def _hhmm(asof: str) -> str:
    s = (asof or "").strip()
    if not s:
        return ""
    if "T" in s:
        return s.split("T", 1)[1][:5]
    return s[:5]


def _pick_sector(row: Dict[str, Any]) -> str:
    return (row.get("sector") or "未分類").strip() or "未分類"


def _pick_name(row: Dict[str, Any]) -> str:
    return (row.get("name") or "").strip()


def _pick_symbol(row: Dict[str, Any]) -> str:
    return (row.get("symbol") or "").strip()


def _sym_no_suffix(sym: str) -> str:
    s = (sym or "").strip()
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _pct(x) -> str:
    try:
        return f"{float(x) * 100.0:+.2f}%"
    except Exception:
        return ""


def _locked_cnt(row: Dict[str, Any]) -> int:
    for k in ("locked_cnt", "limitup_locked", "locked", "lock_cnt"):
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except Exception:
                pass
    return 0


def build_tw_status_text(
    payload: Dict[str, Any],
    *,
    top_sectors: int = 15,
    top_each_bucket: int = 20,
    include_peers: bool = False,
    top_peers: int = 15,
    promo_line: str = "",
) -> str:
    """
    產出一段純文字，可貼 X。
    """
    payload = dict(payload or {})
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()
    asof = str(payload.get("asof") or payload.get("generated_at") or "").strip()
    hhmm = _hhmm(asof)

    limitup: List[Dict[str, Any]] = list(payload.get("limitup") or [])
    sector_summary: List[Dict[str, Any]] = list(payload.get("sector_summary") or [])
    universe = payload.get("universe") or {}
    uni_total = int(universe.get("total") or 0)

    def _st(r) -> str:
        return str(r.get("limitup_status", "")).lower()

    locked = [r for r in limitup if _st(r) == "locked"]
    touch = [r for r in limitup if _st(r) == "touch_only"]
    theme = [r for r in limitup if _st(r) == "no_limit_theme"]

    locked_sectors = [s for s in sector_summary if _locked_cnt(s) > 0]
    sector_cnt = len(locked_sectors)

    lines: List[str] = []

    title = f"台股盤中漲停快照 {ymd}" + (f" 截至{hhmm}" if hhmm else "")
    lines.append(title)
    lines.append("-" * len(title))

    lines.append(
        f"漲停/觸及：{len(limitup)}（鎖{len(locked)}｜觸{len(touch)}｜題{len(theme)}）｜漲停產業：{sector_cnt}｜Universe：{uni_total}"
    )

    # sector ranking（只列 locked_cnt>0；與 overview 一致）
    if locked_sectors:
        locked_sectors = sorted(locked_sectors, key=_locked_cnt, reverse=True)[: max(1, int(top_sectors))]
        lines.append("")
        lines.append("【漲停產業 Top】")
        for i, srow in enumerate(locked_sectors, start=1):
            sec = (srow.get("sector") or "未分類").strip() or "未分類"
            lc = int(srow.get("locked_cnt") or 0)
            tc = int(srow.get("touch_cnt") or 0)  # 你的定義：touch_cnt 含 locked
            nc = int(srow.get("no_limit_cnt") or 0)
            lines.append(f"{i:02d}. {sec}｜鎖{lc} 觸{tc} 題{nc}")

    def fmt_row(r: Dict[str, Any]) -> str:
        name = _pick_name(r)
        sym = _sym_no_suffix(_pick_symbol(r))
        st = (r.get("status_text") or "").strip()
        ret = _pct(r.get("ret"))
        extra = []
        if st:
            extra.append(st)
        if ret:
            extra.append(ret)
        extra_s = ("｜" + " ".join(extra)) if extra else ""
        if name and sym:
            return f"{name}({sym}){extra_s}"
        return f"{name or sym}{extra_s}"

    def add_bucket(title2: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        lines.append("")
        lines.append(f"")
        for r in rows[: max(1, int(top_each_bucket))]:
            lines.append("- " + fmt_row(r))

    add_bucket("鎖漲停", locked)
    add_bucket("觸及未鎖", touch)
    add_bucket("題材/無漲跌幅", theme)

    if include_peers:
        peers = list(payload.get("peers_not_limitup") or [])
        if peers:

            def _ret_val(x):
                try:
                    return float(x.get("ret", -999) or -999)
                except Exception:
                    return -999.0

            peers = sorted(peers, key=_ret_val, reverse=True)[: max(1, int(top_peers))]
            lines.append("")
            lines.append("【同產業未漲停 Top】")
            for r in peers:
                name = _pick_name(r)
                sym = _sym_no_suffix(_pick_symbol(r))
                ret = _pct(r.get("ret"))
                sec = _pick_sector(r)
                if name and sym:
                    lines.append(f"- {name}({sym}) {ret}｜{sec}")
                else:
                    lines.append(f"- {name or sym} {ret}｜{sec}")

    if promo_line.strip():
        lines.append("")
        lines.append(promo_line.strip())

    return "\n".join(lines).strip() + "\n"


def write_tw_status_txt(
    payload: Dict[str, Any],
    out_dir: Path,
    *,
    filename: str = "tw_status.txt",
    **kwargs,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    text = build_tw_status_text(payload, **kwargs)
    path.write_text(text, encoding="utf-8")
    return path
