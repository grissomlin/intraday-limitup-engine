# scripts/render_images_common/overview/adapters.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict


def cn_row_for_mix(row: Dict[str, Any], market: str, metric: str) -> Dict[str, Any]:
    """
    CN 需求：
      - overview_metric = mix
      - 但 mix 計算要排除 ST 封板污染：
          mix_ex_st = locked_non_st + touched + big10_ex

    做法（不改 metrics.py）：
      - CN + metric==mix 時，把 row 映射成「value/mix_cnt/mix_pct」= mix_ex_st_*（若存在）
      - compute_value() 會優先吃 row["value"]
      - compute_pct() (mix) 會吃 row["mix_pct"]
      - badge_text() 仍用原本 metrics.badge_text()（因為 row 已被映射）

    ⚠️ DO NOT affect other markets.
    """
    m = (market or "").upper()
    mm = (metric or "").strip().lower()
    if m != "CN":
        return row
    if mm not in ("mix", "all", "bigmove10+locked+touched"):
        return row

    if not isinstance(row, dict):
        return row

    if "mix_ex_st_cnt" not in row and "mix_ex_st_pct" not in row:
        return row

    rr = dict(row)
    ex_cnt = rr.get("mix_ex_st_cnt", None)
    ex_pct = rr.get("mix_ex_st_pct", None)

    # value: make compute_value() deterministic
    if ex_cnt is not None:
        rr["mix_cnt"] = ex_cnt  # for readability/debug
        rr["value"] = ex_cnt

    # pct: make compute_pct(mix) deterministic
    if ex_pct is not None:
        rr["mix_pct"] = ex_pct
        rr["value_pct"] = ex_pct

    return rr