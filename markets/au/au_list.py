# markets/au/au_list.py
# -*- coding: utf-8 -*-
"""
AU List Builder (ASX)

Goal:
- Download official ASX listed companies CSV
- Build clean universe list for Big Movers pipeline
- Sector uses ASX "GICS industry group" 原文字串
- REITs can be inferred from sector text:
    e.g. "Equity Real Estate Investment Trusts (REITs)"

Output:
- data/au/lists/AU_list.csv

Env (optional):
- AU_LIST_URL: override ASX list url
- AU_LIST_CSV_PATH: if provided, read local csv instead of downloading
- AU_INCLUDE_REITS: 1/0 (default 0)  -> include REITs in universe or not
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd


ASX_LIST_URL_DEFAULT = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"


# =============================================================================
# Filters (exclude non-common-stock instruments)  ※用 sector 字串做保守排除
# =============================================================================
EXCLUDE_SECTOR_KEYWORDS = [
    # exchange traded products / funds
    "ETF",
    "ETP",
    "Fund",
    "Structured",
    "Warrant",
    "Option",
    "Note",
    "Bond",
    "Debenture",
    # trusts / REITs（可用 AU_INCLUDE_REITS=1 開啟）
    "Trust",
    "REIT",
    "Mortgage",
    # misc / not-classified
    "Closed-End",
    "Closed End",
    "Not Applic",
    "Not Applicable",
    "Class Pend",
]


def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _read_asx_list_csv(url: str, local_path: Optional[str] = None) -> pd.DataFrame:
    """
    ASXListedCompanies.csv 前面常有 2 行說明文字：
      line0: "ASX listed companies as at ..."
      line1: empty
      line2: header row
    所以要用「自動探測」避免 pandas 誤判成單欄。
    """
    src = local_path.strip() if (local_path or "").strip() else url

    # 先嘗試「正常讀」，如果欄數不對，再 fallback skiprows
    try:
        df0 = pd.read_csv(src)
        if df0.shape[1] >= 3:
            return df0
    except Exception:
        pass

    # 常見情況：skip 2 行
    for skip in (2, 1, 3, 0, 4, 5):
        try:
            df = pd.read_csv(src, skiprows=skip)
            if df.shape[1] >= 3:
                return df
        except Exception:
            continue

    # 最後：用 python engine + 自動分隔
    df = pd.read_csv(src, engine="python")
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    盡量兼容不同欄位名：
      - Company name / Company Name / Company
      - ASX code / ASX Code / Code
      - GICS industry group / GICS Industry Group / Sector
    """
    cols = [str(c).strip() for c in df.columns]
    df = df.copy()
    df.columns = cols

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c in df.columns:
                return c
        # case-insensitive fallback
        lower_map = {c.lower(): c for c in df.columns}
        for c in cands:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return None

    c_company = pick("Company name", "Company Name", "Company")
    c_code = pick("ASX code", "ASX Code", "Code")
    c_sector = pick("GICS industry group", "GICS Industry Group", "Sector")

    if not (c_company and c_code and c_sector):
        # 如果欄位名稱很怪，就用前三欄硬兜底
        if df.shape[1] >= 3:
            df = df.iloc[:, :3].copy()
            df.columns = ["Company", "Code", "Sector"]
            return df
        raise ValueError(f"ASX list csv columns not recognized: {df.columns.tolist()}")

    out = df[[c_company, c_code, c_sector]].copy()
    out.columns = ["Company", "Code", "Sector"]
    return out


def build_asx_universe() -> pd.DataFrame:
    url = (os.getenv("AU_LIST_URL") or "").strip() or ASX_LIST_URL_DEFAULT
    local_path = (os.getenv("AU_LIST_CSV_PATH") or "").strip() or None
    include_reits = _bool_env("AU_INCLUDE_REITS", False)

    print("📥 Downloading official ASX listed companies...")
    if local_path:
        print(f"   Using local csv: {local_path}")
    else:
        print(f"   Using URL: {url}")

    df_raw = _read_asx_list_csv(url=url, local_path=local_path)
    df = _normalize_columns(df_raw)

    # clean
    df["Company"] = df["Company"].astype(str).str.strip()
    df["Code"] = df["Code"].astype(str).str.strip()
    df["Sector"] = df["Sector"].astype(str).str.strip()

    # drop empty code
    df = df[df["Code"].astype(str).str.len() > 0].copy()

    print("✅ Raw ASX rows:", len(df))
    print(df.head())

    # REIT flag (先算出來，之後你要排除或保留都好控制)
    df["is_reit"] = df["Sector"].str.contains(r"\bREIT\b", case=False, na=False)

    # -----------------------------
    # Filter out non-common stocks
    # -----------------------------
    print("\n🧹 Filtering non-common-stock instruments...")

    patt = "|".join([pd.regex.escape(k) if hasattr(pd, "regex") else k for k in EXCLUDE_SECTOR_KEYWORDS])
    # 上面那行為了兼容；實際上用簡單 join 也可以，但這樣更保守

    # 不能用 pd.regex.escape（pandas 沒這個），所以我們自己簡化：逐一 contains OR
    # 這裡用最穩的作法：逐一 keyword 做 OR
    mask_excl = pd.Series(False, index=df.index)
    for kw in EXCLUDE_SECTOR_KEYWORDS:
        mask_excl = mask_excl | df["Sector"].str.contains(str(kw), case=False, na=False)

    # 如果允許 REIT，就把 REIT 排除條件拿掉
    if include_reits:
        mask_excl = mask_excl & (~df["is_reit"])

    df2 = df[~mask_excl].copy()

    print("✅ After filtering rows:", len(df2))
    print(df2.head())

    # -----------------------------
    # Yahoo ticker symbol
    # -----------------------------
    df2["YahooSymbol"] = df2["Code"].astype(str).str.strip() + ".AX"

    # output columns（給後面 sector mapping / snapshot 用）
    keep = ["YahooSymbol", "Code", "Company", "Sector", "is_reit"]
    for c in keep:
        if c not in df2.columns:
            df2[c] = ""  # 兜底
    return df2[keep].reset_index(drop=True)


def main():
    out_dir = Path("data/au/lists")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = build_asx_universe()

    out_csv = out_dir / "AU_list.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print("\n==============================")
    print("🇦🇺 AU Universe Ready")
    print("==============================")
    print("Total tickers:", len(df))
    print("Saved:", out_csv)


if __name__ == "__main__":
    main()
