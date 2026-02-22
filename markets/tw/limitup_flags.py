# markets/tw/limitup_flags.py
# -*- coding: utf-8 -*-
"""
TW limitup flags (tick-based)
-----------------------------
只處理「standard」制度的漲停判斷（touch / locked），並支援：
- 先把 close/high/open/low round_to_tick（只對 standard），避免浮點誤差
- 若標成 standard 但 high 明顯超過「漲停價 + N ticks」且 ret 足夠大 -> 轉 no_limit（避免新掛牌/特殊制度被誤判）
- 可選：強制 ret>=10% 當漲停（不建議，僅 debug）

注意：
- open_limit（例如興櫃）不在此檔處理，應走 watchlist/theme 邏輯
"""

from __future__ import annotations

from typing import List

import pandas as pd

from .config import (
    FORCE_RET_GE_10_AS_LIMITUP,
    AUTO_INFER_NO_LIMIT_FROM_PRICE,
    AUTO_NO_LIMIT_EXCEED_TICKS,
    AUTO_NO_LIMIT_MIN_RET,
)

# 這裡用相對 import，避免 markets.tw.* 拆包後路徑不一致
from .rules import (
    calc_limitup_price,
    is_limitup_touch,
    is_limitup_locked,
    round_to_tick,
    get_tick_size,
)


# =============================================================================
# Helpers
# =============================================================================
def round_cols_to_tick_for_standard(dfS: pd.DataFrame, cols: List[str]) -> None:
    """
    把 close/high/open/low 等價格先 round 到 tick，避免浮點誤差導致「明明=漲停價卻判錯」。
    只處理 limit_type == 'standard'
    """
    if dfS is None or dfS.empty:
        return
    if "limit_type" not in dfS.columns:
        return

    lt = dfS["limit_type"].fillna("standard").astype(str).str.strip()
    m_std = lt.eq("standard")
    if not m_std.any():
        return

    for c in cols:
        if c not in dfS.columns:
            continue

        s = pd.to_numeric(dfS[c], errors="coerce")
        m = m_std & s.notna()
        if not m.any():
            continue

        dfS.loc[m, c] = s[m].apply(lambda x: float(round_to_tick(float(x))))


def auto_infer_no_limit_from_price(dfS: pd.DataFrame) -> None:
    """
    如果某檔被歸類為 standard，但 high 明顯超過「漲停價 + N ticks」，且 ret >= 門檻，
    代表它很可能不是 10% 漲跌幅制度（新掛牌/特殊制度/資料來源制度不同）。
    -> 轉成 no_limit（讓它走題材邏輯，不要出現 touch_only 的怪狀態）

    會同步清掉：
    - is_limitup_touch / is_limitup_locked
    - limit_up_price
    並在 status_text 為空時補 '題材'
    """
    if (not AUTO_INFER_NO_LIMIT_FROM_PRICE) or dfS is None or dfS.empty:
        return

    need = {"prev_close", "high", "limit_type"}
    if not need.issubset(set(dfS.columns)):
        return

    lt = dfS["limit_type"].fillna("standard").astype(str).str.strip()
    m_std = lt.eq("standard")
    if not m_std.any():
        return

    prev_close = pd.to_numeric(dfS["prev_close"], errors="coerce")
    high = pd.to_numeric(dfS["high"], errors="coerce")

    # ret 門檻（避免誤判）
    if "ret" in dfS.columns:
        ret = pd.to_numeric(dfS["ret"], errors="coerce").fillna(0.0)
    else:
        ret = pd.Series(0.0, index=dfS.index)

    # 算漲停價（只算 standard）
    lup = prev_close.apply(
        lambda x: calc_limitup_price(float(x)) if pd.notna(x) and float(x) > 0 else None
    )

    def exceed_many_ticks(h, lu) -> bool:
        if h is None or lu is None:
            return False
        try:
            h = float(h)
            lu = float(lu)
            tick = float(get_tick_size(lu))
            thr = lu + tick * max(1, int(AUTO_NO_LIMIT_EXCEED_TICKS))
            return h > thr
        except Exception:
            return False

    m_suspect = pd.Series(False, index=dfS.index)

    # 只檢查 standard 的 index，避免 open_limit / no_limit 被誤傷
    for i in dfS.index[m_std].tolist():
        lu = lup.at[i]
        h = high.at[i]
        if exceed_many_ticks(h, lu) and float(ret.at[i]) >= float(AUTO_NO_LIMIT_MIN_RET):
            m_suspect.at[i] = True

    if not m_suspect.any():
        return

    # 轉成 no_limit
    dfS.loc[m_suspect, "limit_type"] = "no_limit"

    # 清掉 standard 漲停資訊（避免混淆）
    if "is_limitup_touch" in dfS.columns:
        dfS.loc[m_suspect, "is_limitup_touch"] = False
    if "is_limitup_locked" in dfS.columns:
        dfS.loc[m_suspect, "is_limitup_locked"] = False
    if "limit_up_price" in dfS.columns:
        dfS.loc[m_suspect, "limit_up_price"] = None

    # status_text 為空才補
    if "status_text" in dfS.columns:
        m_empty = dfS["status_text"].fillna("").astype(str).str.strip().eq("")
        dfS.loc[m_suspect & m_empty, "status_text"] = "題材"


def infer_limitup_flags_from_price(dfS: pd.DataFrame) -> None:
    """
    用 tick 漲停價判斷（只針對 standard）
    - limit_up_price: calc_limitup_price(prev_close)  (floor_to_tick)
    - touch : high >= limit_up_price
    - locked: is_limitup_locked(close, limit_up_price) (round_to_tick + tick/2)
    """
    if dfS is None or dfS.empty:
        return

    # ensure columns exist
    if "is_limitup_touch" not in dfS.columns:
        dfS["is_limitup_touch"] = False
    if "is_limitup_locked" not in dfS.columns:
        dfS["is_limitup_locked"] = False
    if "limit_up_price" not in dfS.columns:
        dfS["limit_up_price"] = None
    if "limit_type" not in dfS.columns:
        dfS["limit_type"] = "standard"

    need = {"prev_close", "close", "high", "limit_type"}
    if not need.issubset(set(dfS.columns)):
        return

    # normalize types
    dfS["limit_type"] = dfS["limit_type"].fillna("standard").astype(str).str.strip()

    # ✅ 先把 close/high/open/low round 到 tick（只對 standard）
    round_cols_to_tick_for_standard(dfS, cols=["close", "high", "open", "low"])

    # ✅ 再用價格異常偵測，把「明顯不是 10% 制度」的標準股轉 no_limit（避免 7795）
    auto_infer_no_limit_from_price(dfS)

    m_std = dfS["limit_type"].eq("standard")
    if not m_std.any():
        return

    prev_close = pd.to_numeric(dfS["prev_close"], errors="coerce")
    close = pd.to_numeric(dfS["close"], errors="coerce")
    high = pd.to_numeric(dfS["high"], errors="coerce")

    # 1) compute limit_up_price for standard rows
    lup = prev_close.apply(
        lambda x: calc_limitup_price(float(x)) if pd.notna(x) and float(x) > 0 else None
    )
    dfS.loc[m_std, "limit_up_price"] = lup[m_std]

    # 2) touch: high >= lup
    touched = pd.Series(False, index=dfS.index)
    touched.loc[m_std] = [
        is_limitup_touch(high_price=h, limitup_price=lu)
        for h, lu in zip(
            high[m_std].tolist(),
            dfS.loc[m_std, "limit_up_price"].tolist(),
        )
    ]

    # 3) locked: use rules (tick-based)
    locked = pd.Series(False, index=dfS.index)
    locked.loc[m_std] = [
        is_limitup_locked(last_price=lp, limitup_price=lu)
        for lp, lu in zip(
            close[m_std].tolist(),
            dfS.loc[m_std, "limit_up_price"].tolist(),
        )
    ]

    # 只補 True（避免覆蓋既有）
    cur_touch = dfS["is_limitup_touch"].fillna(False).astype(bool)
    cur_lock = dfS["is_limitup_locked"].fillna(False).astype(bool)

    dfS.loc[m_std & (~cur_touch) & touched, "is_limitup_touch"] = True
    dfS.loc[m_std & (~cur_lock) & locked, "is_limitup_locked"] = True

    # 可選：你堅持「>=10% 當漲停」（不建議，僅 debug）
    if FORCE_RET_GE_10_AS_LIMITUP and "ret" in dfS.columns:
        ret = pd.to_numeric(dfS["ret"], errors="coerce").fillna(0.0)
        dfS.loc[m_std & (ret >= 0.10), "is_limitup_touch"] = True
        dfS.loc[m_std & (ret >= 0.10), "is_limitup_locked"] = True
