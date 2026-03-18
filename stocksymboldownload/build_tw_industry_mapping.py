# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

METADATA_PATH = ROOT / "stocksymboldownload" / "stock_metadata.csv"
MAPPING_PATH = ROOT / "stocksymboldownload" / "industry_mapping_final.csv"


def build_mapping():

    if not METADATA_PATH.exists():
        raise RuntimeError("找不到 stock_metadata.csv")

    meta = pd.read_csv(METADATA_PATH, dtype=str)

    # 如果 mapping 已存在
    if MAPPING_PATH.exists():
        mapping = pd.read_csv(MAPPING_PATH, dtype=str)
    else:
        mapping = pd.DataFrame(
            columns=[
                "stock_id",
                "stock_name",
                "industry_l1",
                "industry_l2",
                "tags",
                "source",
            ]
        )

    known_ids = set(mapping["stock_id"])

    new_rows = []

    for _, r in meta.iterrows():

        sid = r["stock_id"]

        if sid in known_ids:
            continue

        new_rows.append(
            {
                "stock_id": sid,
                "stock_name": r["stock_name"],
                "industry_l1": r["industry"],
                "industry_l2": r["industry"],  # 先等於大產業
                "tags": "",
                "source": "metadata",
            }
        )

    if new_rows:

        df_new = pd.DataFrame(new_rows)

        mapping = pd.concat([mapping, df_new], ignore_index=True)

        print(f"新增 {len(df_new)} 檔股票到 mapping")

    mapping = mapping.sort_values("stock_id").reset_index(drop=True)

    mapping.to_csv(MAPPING_PATH, index=False, encoding="utf-8-sig")

    print("✅ industry_mapping_final.csv 更新完成")

    print(mapping.head(10).to_string())

    return mapping


def main():

    build_mapping()


if __name__ == "__main__":

    main()
