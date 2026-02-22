# scripts/render_images_tw/sector_blocks/_tags.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Any, Dict, Tuple


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def pick_board_tag_style(board_kind: str) -> Tuple[str, Tuple[float, float, float]]:
    """
    Board tag chip:
      - ROTC / emerging / open_limit => purple chip with white text
    """
    k = (board_kind or "").strip().lower()
    if k in ("rotc", "emerging", "open_limit"):
        return "興櫃", (0.55, 0.35, 0.85)
    return str(board_kind or ""), (0.35, 0.35, 0.40)


_INLINE_EMG_RE = re.compile(r"[\s]*[\[\(（【〔［]\s*(興櫃|兴柜)\s*[\]\)）】〕］][\s]*")


def strip_inline_emerging_tag(s: str) -> str:
    """
    Remove inline bracket tag from line1: (興櫃) / 【興櫃】 / 〔興櫃〕 etc.
    """
    s = _safe_str(s)
    if not s:
        return ""
    s = _INLINE_EMG_RE.sub(" ", s)
    s = " ".join(s.split())
    return s


def is_emerging_row(r: Dict[str, Any]) -> bool:
    """
    Decide whether we should draw the purple 興櫃 pill.
    """
    board_kind = _safe_str(r.get("board_kind") or "").lower()
    market_detail = _safe_str(r.get("market_detail") or "").lower()
    board_tag = _safe_str(r.get("board_tag") or "")

    if board_kind in ("rotc", "emerging", "open_limit"):
        return True
    if market_detail in ("rotc", "emerging", "open_limit"):
        return True
    if "興櫃" in board_tag or "兴柜" in board_tag:
        return True

    try:
        if bool(r.get("is_emerging", False)) or bool(r.get("is_otc", False)):
            # keep is_otc only if your pipeline maps OTC->emerging
            pass
    except Exception:
        pass
    return False
