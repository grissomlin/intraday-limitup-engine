# dashboard/components/formatters.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def safe_format(template: str, mapping: dict) -> str:
    """
    1) 缺 key 不炸：format_map + SafeDict
    2) 把 '\\_' 還原成 '_'（避免 sector\\_summary\\_table 這種 key 對不起來）
    """
    if not isinstance(template, str):
        template = str(template)
    template = template.replace("\\_", "_")
    return template.format_map(SafeDict(mapping))


def df_to_md_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    """
    純 pandas → markdown table（不依賴 tabulate）
    """
    if df is None or df.empty:
        return "(empty)"

    df = df.head(max_rows).copy().fillna("").astype(str)
    headers = list(df.columns)
    sep = ["---"] * len(headers)

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(sep) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(row.tolist()) + " |")
    return "\n".join(lines)
