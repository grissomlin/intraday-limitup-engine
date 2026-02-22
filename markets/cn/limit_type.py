# markets/tw/limit_type.py
# -*- coding: utf-8 -*-
"""
TW limit_type inference (制度分類)
----------------------------------
把制度分類集中處理，讓後續 builders / flags 只看 limit_type 做決策。

建議的通用分類（跨市場可共用概念）：
- standard   : 有明確漲跌幅限制（可算 limit_up_price，用 touch/locked 判斷）
- no_limit   : 無漲跌幅限制（新上市無漲跌幅、特殊制度、手動清單）
- open_limit : 無固定漲停價判斷（例如興櫃：只做題材門檻，不做 touch/locked）
"""

from __future__ import annotations

import pandas as pd

from .config import NO_LIMIT_SYMBOLS


def infer_limit_type(dfS: pd.DataFrame) -> None:
    """
    統一在 aggregator 做制度分類：
    - market_detail == 'emerging' => open_limit   (興櫃)
    - symbol in TW_NO_LIMIT_SYMBOLS => no_limit   (新上市無漲跌幅期間/特殊案例)
    - else => standard

    另外：
    - 若外部已帶入 limit_type，會先做正規化（把 emerging_no_limit -> open_limit）
      但最終仍以此函數規則為準（避免半套狀態）。
    """
    if dfS is None or dfS.empty:
        return

    # columns
    if "limit_type" not in dfS.columns:
        dfS["limit_type"] = "standard"
    if "market_detail" not in dfS.columns:
        dfS["market_detail"] = ""
    if "symbol" not in dfS.columns:
        dfS["symbol"] = ""

    # normalize strings
    dfS["symbol"] = dfS["symbol"].fillna("").astype(str).str.strip()
    md = dfS["market_detail"].fillna("").astype(str).str.strip()

    # normalize existing limit_type values (backward compatibility)
    lt = dfS["limit_type"].fillna("standard").astype(str).str.strip().str.lower()
    lt = lt.replace(
        {
            "emerging_no_limit": "open_limit",  # 舊命名向下相容
            "emerging": "open_limit",
            "unlimited": "no_limit",
            "no_limit_theme": "no_limit",
        }
    )
    dfS["limit_type"] = lt

    # 1) emerging => open_limit
    m_open = md.eq("emerging")
    dfS.loc[m_open, "limit_type"] = "open_limit"

    # 2) explicit no-limit symbols (new listing / special cases)
    #    不覆蓋 emerging（興櫃仍走 open_limit）
    if NO_LIMIT_SYMBOLS:
        m_nl = dfS["symbol"].isin(NO_LIMIT_SYMBOLS) & (~m_open)
        dfS.loc[m_nl, "limit_type"] = "no_limit"

    # 3) default => standard (fill blanks / unknown)
    dfS["limit_type"] = dfS["limit_type"].fillna("").astype(str).str.strip()
    m_blank = dfS["limit_type"].eq("")
    dfS.loc[m_blank, "limit_type"] = "standard"

    # 保證只落在允許集合（防止外部亂塞值）
    allowed = {"standard", "no_limit", "open_limit"}
    dfS.loc[~dfS["limit_type"].isin(allowed), "limit_type"] = "standard"
