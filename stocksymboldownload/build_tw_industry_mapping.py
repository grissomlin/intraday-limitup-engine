# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]

METADATA_PATH = ROOT / "stocksymboldownload" / "stock_metadata.csv"
MAPPING_PATH = ROOT / "stocksymboldownload" / "industry_mapping_final.csv"
CANDIDATES_PATH = ROOT / "stocksymboldownload" / "industry_mapping_candidates.csv"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

# 只對這些大產業做細分；其他直接沿用 industry_l1
TARGET_INDUSTRIES = {
    "半導體業",
    "電子零組件業",
    "電腦及週邊設備業",
    "通信網路業",
    "光電業",
    "其他電子業",
    "電子通路業",
    "資訊服務業",
    "數位雲端",
}

# 各大產業可接受的子產業字典，盡量固定、可控
INDUSTRY_L2_OPTIONS = {
    "半導體業": [
        "晶圓代工", "IC設計", "封測", "矽晶圓", "記憶體",
        "記憶體模組", "快閃記憶體", "MCU", "電源管理IC",
        "砷化鎵", "光罩", "功率半導體", "半導體設備", "網通IC"
    ],
    "電子零組件業": [
        "PCB", "ABF載板", "連接器", "被動元件", "散熱",
        "鍵盤", "新型零組件", "零組件通路"
    ],
    "電腦及週邊設備業": [
        "伺服器ODM", "主機板", "筆電代工", "機殼",
        "系統整合", "網通設備", "散熱模組"
    ],
    "通信網路業": [
        "網通", "光通訊", "電信服務"
    ],
    "光電業": [
        "面板", "面板零組件", "偏光板", "鏡頭",
        "光學元件", "玻璃基板", "感測元件", "LED", "零組件"
    ],
    "其他電子業": [
        "半導體設備", "自動化設備", "設備工程", "設備零組件"
    ],
    "電子通路業": [
        "電子通路"
    ],
    "資訊服務業": [
        "資訊服務", "資安", "軟體服務"
    ],
    "數位雲端": [
        "雲端服務", "資訊服務", "資安", "軟體服務"
    ],
}


def load_metadata() -> pd.DataFrame:
    if not METADATA_PATH.exists():
        raise RuntimeError(f"找不到 {METADATA_PATH}")

    df = pd.read_csv(METADATA_PATH, dtype=str).fillna("")
    required = {"stock_id", "stock_name", "industry"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"stock_metadata.csv 缺少欄位: {missing}")

    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["stock_name"] = df["stock_name"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()
    df.loc[df["industry"].isin(["", "nan", "None"]), "industry"] = "未分類"
    return df


def load_mapping() -> pd.DataFrame:
    if MAPPING_PATH.exists():
        df = pd.read_csv(MAPPING_PATH, dtype=str).fillna("")
    else:
        df = pd.DataFrame(columns=[
            "stock_id", "stock_name", "industry_l1", "industry_l2",
            "tags", "source", "confidence", "reason", "last_update"
        ])

    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].astype(str).str.strip()
    return df


def should_use_gemini(industry_l1: str) -> bool:
    return industry_l1 in TARGET_INDUSTRIES


def default_row(stock_id: str, stock_name: str, industry_l1: str) -> dict[str, Any]:
    return {
        "stock_id": stock_id,
        "stock_name": stock_name,
        "industry_l1": industry_l1,
        "industry_l2": industry_l1,
        "tags": "",
        "source": "metadata",
        "confidence": "",
        "reason": "default_from_exchange_industry",
        "last_update": pd.Timestamp.now().strftime("%Y-%m-%d"),
    }


def build_prompt(stock_id: str, stock_name: str, industry_l1: str) -> str:
    options = INDUSTRY_L2_OPTIONS.get(industry_l1, [])
    options_text = "、".join(options) if options else "請保守判斷"

    return f"""
你是台股供應鏈分類助手。請根據股票名稱與交易所大產業，輸出唯一 JSON，不要加任何說明文字。

要求：
1. industry_l2 必須從這些候選中擇一：{options_text}
2. tags 可為 0~5 個，用陣列表示
3. confidence 為 0~1 浮點數
4. reason 簡短說明判斷依據
5. 如果無法高信心判斷，industry_l2 直接回 {industry_l1}

輸入：
stock_id: {stock_id}
stock_name: {stock_name}
industry_l1: {industry_l1}

輸出格式：
{{
  "industry_l2": "...",
  "tags": ["...", "..."],
  "confidence": 0.0,
  "reason": "..."
}}
""".strip()


def call_gemini(stock_id: str, stock_name: str, industry_l1: str) -> dict[str, Any]:
    if not GEMINI_API_KEY:
        return {
            "industry_l2": industry_l1,
            "tags": [],
            "confidence": "",
            "reason": "missing_GEMINI_API_KEY",
        }

    prompt = build_prompt(stock_id, stock_name, industry_l1)

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    )

    payload = {
        "contents": [
            {"parts": [{"text": prompt}]}
        ],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }

    try:
        r = requests.post(url, json=payload, timeout=45)
        r.raise_for_status()
        data = r.json()

        text = data["candidates"][0]["content"]["parts"][0]["text"]
        obj = json.loads(text)

        industry_l2 = str(obj.get("industry_l2", industry_l1)).strip() or industry_l1
        tags = obj.get("tags", [])
        confidence = obj.get("confidence", "")
        reason = str(obj.get("reason", "")).strip()

        allowed = set(INDUSTRY_L2_OPTIONS.get(industry_l1, []))
        if allowed and industry_l2 not in allowed:
            industry_l2 = industry_l1
            reason = f"fallback_invalid_option; {reason}"

        if not isinstance(tags, list):
            tags = []

        tags = [str(x).strip() for x in tags if str(x).strip()]

        return {
            "industry_l2": industry_l2,
            "tags": tags,
            "confidence": confidence,
            "reason": reason,
        }

    except Exception as e:
        return {
            "industry_l2": industry_l1,
            "tags": [],
            "confidence": "",
            "reason": f"gemini_error: {e}",
        }


def build_mapping() -> tuple[pd.DataFrame, pd.DataFrame]:
    meta = load_metadata()
    mapping = load_mapping()

    existing_ids = set(mapping["stock_id"]) if not mapping.empty else set()
    mapping_by_id = {row["stock_id"]: row for _, row in mapping.iterrows()} if not mapping.empty else {}

    final_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []

    for _, r in meta.iterrows():
        sid = r["stock_id"]
        stock_name = r["stock_name"]
        industry_l1 = r["industry"]

        # 已存在正式 mapping：直接沿用
        if sid in existing_ids:
            old = mapping_by_id[sid].copy()
            # 若名稱或大產業變了，順手同步
            old["stock_name"] = stock_name
            old["industry_l1"] = industry_l1
            final_rows.append(old)
            continue

        # 新增股票：先建立基本列
        row = default_row(sid, stock_name, industry_l1)

        # 只對科技相關大產業做 Gemini 細分
        if should_use_gemini(industry_l1):
            result = call_gemini(sid, stock_name, industry_l1)
            row["industry_l2"] = result["industry_l2"] or industry_l1
            row["tags"] = ";".join(result["tags"])
            row["source"] = "gemini" if result["reason"] != "missing_GEMINI_API_KEY" else "metadata"
            row["confidence"] = result["confidence"]
            row["reason"] = result["reason"]

            candidate_rows.append({
                "stock_id": sid,
                "stock_name": stock_name,
                "industry_l1": industry_l1,
                "suggested_industry_l2": row["industry_l2"],
                "suggested_tags": row["tags"],
                "confidence": row["confidence"],
                "reason": row["reason"],
            })

            # 避免打太快
            time.sleep(1.2)

        final_rows.append(row)

    final_df = pd.DataFrame(final_rows)
    final_df = final_df.sort_values("stock_id").reset_index(drop=True)

    candidates_df = pd.DataFrame(candidate_rows)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values("stock_id").reset_index(drop=True)

    final_df.to_csv(MAPPING_PATH, index=False, encoding="utf-8-sig")
    candidates_df.to_csv(CANDIDATES_PATH, index=False, encoding="utf-8-sig")

    print(f"✅ industry_mapping_final.csv 更新完成：{MAPPING_PATH}")
    print(f"✅ industry_mapping_candidates.csv 更新完成：{CANDIDATES_PATH}")
    print()
    print(final_df.head(10).to_string())

    return final_df, candidates_df


def main():
    build_mapping()


if __name__ == "__main__":
    main()
