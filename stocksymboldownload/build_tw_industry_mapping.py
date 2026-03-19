# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import io
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "stocksymboldownload"

METADATA_PATH = OUT_DIR / "stock_metadata.csv"
MAPPING_PATH = OUT_DIR / "industry_mapping_final.csv"
CANDIDATES_PATH = OUT_DIR / "industry_mapping_candidates.csv"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GDRIVE_TOKEN_B64 = os.environ.get("GDRIVE_TOKEN_B64", "").strip()
TW_STOCKLIST = os.environ.get("TW_STOCKLIST", "").strip()

# 第一次先只跑最重要兩類
TARGET_INDUSTRIES = {
    "半導體業",
    "電子零組件業",
}

# 一次送少一點，省額度也比較不容易 429
BATCH_SIZE = 10

# 每批之間休息久一點
BATCH_SLEEP_SEC = 8

# 429 重試參數
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 10

# confidence 門檻
AUTO_ACCEPT_THRESHOLD = 0.72
LOW_CONF_THRESHOLD = 0.55

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
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TWIndustryMappingBot/1.0)"
}

# =========================================================
# Keyword rules：先做規則分類，能省很多 token
# =========================================================
KEYWORD_RULES = {
    "半導體業": {
        "晶圓代工": [
            "台積電", "世界先進", "聯電", "力積電", "foundry", "wafer fab", "晶圓代工"
        ],
        "IC設計": [
            "瑞昱", "聯發科", "晶豪科", "世芯", "創意", "智原", "ASIC", "SoC", "IC設計", "design service"
        ],
        "封測": [
            "日月光", "矽格", "京元電子", "力成", "頎邦", "封測", "封裝", "測試", "OSAT"
        ],
        "矽晶圓": [
            "環球晶", "中美晶", "合晶", "嘉晶", "Silicon Wafer", "矽晶圓"
        ],
        "記憶體": [
            "南亞科", "華邦電", "旺宏", "DRAM", "NAND", "NOR", "記憶體"
        ],
        "記憶體模組": [
            "威剛", "十銓", "群聯", "memory module", "記憶體模組"
        ],
        "快閃記憶體": [
            "群聯", "慧榮", "flash", "快閃記憶體", "SSD controller"
        ],
        "MCU": [
            "新唐", "盛群", "凌陽", "義隆", "microcontroller", "MCU"
        ],
        "電源管理IC": [
            "致新", "矽力", "力智", "通嘉", "pmic", "power management"
        ],
        "砷化鎵": [
            "穩懋", "宏捷科", "全新", "砷化鎵", "GaAs"
        ],
        "光罩": [
            "光罩", "photomask", "mask"
        ],
        "功率半導體": [
            "朋程", "富鼎", "杰力", "漢磊", "MOSFET", "IGBT", "SiC", "功率半導體"
        ],
        "半導體設備": [
            "家登", "京鼎", "辛耘", "帆宣", "弘塑", "設備", "equipment"
        ],
        "網通IC": [
            "瑞昱", "聯傑", "網通IC", "ethernet", "wifi chip", "switch ic"
        ],
    },
    "電子零組件業": {
        "PCB": [
            "欣興", "健鼎", "金像電", "博智", "瀚宇博", "定穎", "台郡", "華通",
            "PCB", "印刷電路板", "circuit board"
        ],
        "ABF載板": [
            "南電", "景碩", "欣興", "ABF", "載板", "substrate"
        ],
        "連接器": [
            "貿聯", "嘉澤", "凡甲", "良維", "詮欣", "連接器", "connector", "cable"
        ],
        "被動元件": [
            "國巨", "華新科", "奇力新", "旺詮", "立隆電",
            "MLCC", "chip resistor", "電容", "電阻", "電感", "被動元件"
        ],
        "散熱": [
            "雙鴻", "奇鋐", "超眾", "建準", "散熱", "thermal", "heatsink", "fan"
        ],
        "鍵盤": [
            "群光", "精元", "達方", "鍵盤", "keyboard"
        ],
        "新型零組件": [
            "軟板", "FPC", "天線", "聲學", "模組", "鏡頭", "感測", "模切"
        ],
        "零組件通路": [
            "至上", "文曄", "大聯大", "益登", "威健", "通路", "代理", "distribution"
        ],
    },
}

# 有些子產業容易同時被多個規則命中，這裡給權重
RULE_PRIORITY = {
    "ABF載板": 120,
    "晶圓代工": 120,
    "封測": 110,
    "IC設計": 110,
    "矽晶圓": 110,
    "功率半導體": 110,
    "電源管理IC": 105,
    "網通IC": 105,
    "快閃記憶體": 105,
    "記憶體模組": 100,
    "記憶體": 95,
    "MCU": 95,
    "光罩": 95,
    "半導體設備": 95,
    "PCB": 110,
    "連接器": 100,
    "被動元件": 100,
    "散熱": 100,
    "鍵盤": 90,
    "零組件通路": 110,
    "新型零組件": 80,
}


# =========================================================
# Google Drive helpers
# =========================================================
def build_drive_service():
    if not GDRIVE_TOKEN_B64:
        raise RuntimeError("缺少 GDRIVE_TOKEN_B64")

    token_json = base64.b64decode(GDRIVE_TOKEN_B64).decode("utf-8")
    token_info = json.loads(token_json)

    creds = Credentials.from_authorized_user_info(token_info)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service


def find_drive_file(service, folder_id: str, file_name: str) -> str | None:
    query = (
        f"name = '{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed = false"
    )

    res = service.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name)",
        pageSize=10,
    ).execute()

    files = res.get("files", [])
    if not files:
        return None
    return files[0]["id"]


def download_drive_file_if_exists(service, folder_id: str, file_name: str, local_path: Path) -> bool:
    file_id = find_drive_file(service, folder_id, file_name)
    if not file_id:
        print(f"⚠️ Google Drive 上找不到 {file_name}，略過下載")
        return False

    request = service.files().get_media(fileId=file_id)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with io.FileIO(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    print(f"✅ 已從 Google Drive 下載：{file_name}")
    return True


def upload_or_update_drive_file(service, local_path: Path, folder_id: str):
    file_name = local_path.name
    file_id = find_drive_file(service, folder_id, file_name)

    with open(local_path, "rb") as f:
        media = MediaIoBaseUpload(
            io.BytesIO(f.read()),
            mimetype="text/csv",
            resumable=False,
        )

    if file_id:
        updated = service.files().update(
            fileId=file_id,
            media_body=media,
            fields="id,name",
        ).execute()
        print(f"✅ 已更新 Google Drive 檔案：{updated['name']} ({updated['id']})")
    else:
        created = service.files().create(
            body={"name": file_name, "parents": [folder_id]},
            media_body=media,
            fields="id,name",
        ).execute()
        print(f"✅ 已上傳 Google Drive 檔案：{created['name']} ({created['id']})")


# =========================================================
# Data loading
# =========================================================
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


def _ensure_mapping_columns(df: pd.DataFrame) -> pd.DataFrame:
    wanted_cols = [
        "stock_id",
        "stock_name",
        "industry_l1",
        "industry_l2",
        "tags",
        "source",
        "confidence",
        "review_needed",
        "reason",
        "last_update",
    ]
    for col in wanted_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[wanted_cols].copy()
    return df


def load_mapping() -> pd.DataFrame:
    if MAPPING_PATH.exists():
        df = pd.read_csv(MAPPING_PATH, dtype=str).fillna("")
    else:
        df = pd.DataFrame(columns=[
            "stock_id",
            "stock_name",
            "industry_l1",
            "industry_l2",
            "tags",
            "source",
            "confidence",
            "review_needed",
            "reason",
            "last_update",
        ])

    df = _ensure_mapping_columns(df)

    if "stock_id" in df.columns:
        df["stock_id"] = df["stock_id"].astype(str).str.strip()

    if "review_needed" in df.columns:
        df["review_needed"] = df["review_needed"].astype(str).str.strip()
        df.loc[df["review_needed"] == "", "review_needed"] = "Y"

    return df


# =========================================================
# Rule-based classification
# =========================================================
def normalize_text(text: str) -> str:
    text = str(text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def infer_by_keyword(industry_l1: str, stock_name: str) -> dict[str, Any] | None:
    rules = KEYWORD_RULES.get(industry_l1, {})
    if not rules:
        return None

    name_norm = normalize_text(stock_name)
    best_l2 = None
    best_score = -1
    matched_words: list[str] = []

    for l2, keywords in rules.items():
        local_hits = []
        score = RULE_PRIORITY.get(l2, 0)

        for kw in keywords:
            kw_norm = normalize_text(kw)
            if kw_norm and kw_norm in name_norm:
                local_hits.append(kw)
                score += 20

        if local_hits and score > best_score:
            best_l2 = l2
            best_score = score
            matched_words = local_hits

    if not best_l2:
        return None

    # 有命中規則就先自動接受
    confidence = 0.96 if len(matched_words) >= 2 else 0.90

    return {
        "industry_l2": best_l2,
        "tags": ";".join(matched_words[:5]),
        "confidence": confidence,
        "review_needed": "N",
        "reason": f"keyword_rule_match:{'|'.join(matched_words[:5])}",
        "source": "rule",
    }


def decide_review_needed(industry_l1: str, industry_l2: str, confidence: Any, source: str) -> str:
    conf = safe_float(confidence, 0.0)

    if source == "rule":
        return "N"

    # 如果 Gemini 最後還是只回大產業，幾乎等於沒判斷出來
    if not industry_l2 or industry_l2 == industry_l1:
        return "Y"

    if conf >= AUTO_ACCEPT_THRESHOLD:
        return "N"

    if conf < LOW_CONF_THRESHOLD:
        return "Y"

    # 中間灰區保守處理
    return "Y"


# =========================================================
# Gemini batch classification
# =========================================================
def chunk_list(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_batch_prompt(industry_l1: str, batch: list[dict[str, Any]]) -> str:
    options = INDUSTRY_L2_OPTIONS.get(industry_l1, [])
    options_text = "、".join(options)

    batch_text = "\n".join(
        [f'{x["stock_id"]}|{x["stock_name"]}' for x in batch]
    )

    return f"""
你是台股供應鏈分類助手。

任務：
我會給你同一個交易所大產業下的一批台股股票，請你為每一檔判斷主要子產業 industry_l2 與 tags。

規則：
1. industry_l2 必須從這些候選中擇一：{options_text}
2. tags 可為 0~5 個，用陣列表示
3. confidence 為 0~1 浮點數
4. reason 簡短
5. 若無法高信心判斷，industry_l2 回傳 {industry_l1}
6. 只能輸出 JSON 陣列，不要輸出其他文字
7. 若只是模糊推測，confidence 不要超過 0.70
8. 只有在非常明確時，confidence 才能 >= 0.85

輸入大產業：
{industry_l1}

股票清單：
{batch_text}

輸出格式：
[
  {{
    "stock_id": "2330",
    "industry_l2": "晶圓代工",
    "tags": ["AI", "先進製程"],
    "confidence": 0.95,
    "reason": "..."
  }}
]
""".strip()


def call_gemini_batch(industry_l1: str, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not GEMINI_API_KEY:
        print("⚠️ 缺少 GEMINI_API_KEY，Gemini 細分略過")
        return []

    prompt = build_batch_prompt(industry_l1, batch)

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }

    r = requests.post(url, headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    data = r.json()

    text = data["candidates"][0]["content"]["parts"][0]["text"]
    obj = json.loads(text)

    if not isinstance(obj, list):
        raise RuntimeError("Gemini 回傳不是 JSON array")

    allowed = set(INDUSTRY_L2_OPTIONS.get(industry_l1, []))
    cleaned: list[dict[str, Any]] = []

    for item in obj:
        sid = str(item.get("stock_id", "")).strip()
        industry_l2 = str(item.get("industry_l2", industry_l1)).strip() or industry_l1
        tags = item.get("tags", [])
        confidence = item.get("confidence", "")
        reason = str(item.get("reason", "")).strip()

        if industry_l2 not in allowed:
            industry_l2 = industry_l1
            reason = f"fallback_invalid_option; {reason}"

        if not isinstance(tags, list):
            tags = []

        tags = [str(x).strip() for x in tags if str(x).strip()]
        review_needed = decide_review_needed(
            industry_l1=industry_l1,
            industry_l2=industry_l2,
            confidence=confidence,
            source="gemini",
        )

        cleaned.append({
            "stock_id": sid,
            "industry_l2": industry_l2,
            "tags": ";".join(tags),
            "confidence": confidence,
            "review_needed": review_needed,
            "reason": reason,
        })

    return cleaned


def call_gemini_batch_with_retry(industry_l1: str, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    delay = INITIAL_RETRY_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return call_gemini_batch(industry_l1, batch)
        except requests.HTTPError as e:
            msg = str(e)
            if "429" in msg:
                print(f"   ⚠️ 429 限流，等待 {delay} 秒後重試 ({attempt}/{MAX_RETRIES})")
                time.sleep(delay)
                delay *= 2
                continue
            print(f"   ❌ HTTP 錯誤：{e}")
            return []
        except Exception as e:
            print(f"   ❌ Gemini 批次失敗：{e}")
            return []

    print("   ❌ 多次重試後仍失敗，略過此批")
    return []


# =========================================================
# Main mapping logic
# =========================================================
def default_row(stock_id: str, stock_name: str, industry_l1: str) -> dict[str, Any]:
    return {
        "stock_id": stock_id,
        "stock_name": stock_name,
        "industry_l1": industry_l1,
        "industry_l2": industry_l1,
        "tags": "",
        "source": "metadata",
        "confidence": "",
        "review_needed": "Y",
        "reason": "default_from_exchange_industry",
        "last_update": pd.Timestamp.now().strftime("%Y-%m-%d"),
    }


def apply_rule_based_mapping(final_df: pd.DataFrame) -> pd.DataFrame:
    updated = 0

    for idx, row in final_df.iterrows():
        industry_l1 = str(row["industry_l1"]).strip()
        stock_name = str(row["stock_name"]).strip()
        industry_l2 = str(row["industry_l2"]).strip()

        # 只處理還沒細分的
        if industry_l1 not in TARGET_INDUSTRIES:
            continue
        if industry_l2 and industry_l2 != industry_l1:
            continue

        res = infer_by_keyword(industry_l1, stock_name)
        if not res:
            continue

        final_df.at[idx, "industry_l2"] = res["industry_l2"]
        final_df.at[idx, "tags"] = res["tags"]
        final_df.at[idx, "source"] = res["source"]
        final_df.at[idx, "confidence"] = str(res["confidence"])
        final_df.at[idx, "review_needed"] = res["review_needed"]
        final_df.at[idx, "reason"] = res["reason"]
        final_df.at[idx, "last_update"] = pd.Timestamp.now().strftime("%Y-%m-%d")
        updated += 1

    print(f"✅ 規則分類完成：{updated} 筆")
    return final_df


def build_mapping() -> tuple[pd.DataFrame, pd.DataFrame]:
    meta = load_metadata()
    mapping = load_mapping()

    existing_ids = set(mapping["stock_id"]) if not mapping.empty else set()
    mapping_by_id = {row["stock_id"]: row.to_dict() for _, row in mapping.iterrows()} if not mapping.empty else {}

    final_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []

    for _, r in meta.iterrows():
        sid = r["stock_id"]
        stock_name = r["stock_name"]
        industry_l1 = r["industry"]

        if sid in existing_ids:
            row = mapping_by_id[sid].copy()
            row["stock_name"] = stock_name
            row["industry_l1"] = industry_l1

            if not row.get("industry_l2"):
                row["industry_l2"] = industry_l1

            if not row.get("review_needed"):
                row["review_needed"] = "Y"

            final_rows.append(row)
        else:
            final_rows.append(default_row(sid, stock_name, industry_l1))

    final_df = pd.DataFrame(final_rows)
    final_df = _ensure_mapping_columns(final_df)

    # 先跑規則，能大量減少 Gemini 數量
    final_df = apply_rule_based_mapping(final_df)

    need_df = final_df[
        final_df["industry_l1"].isin(TARGET_INDUSTRIES) &
        (final_df["industry_l2"] == final_df["industry_l1"])
    ].copy()

    if need_df.empty:
        print("✅ 沒有需要 Gemini 細分的股票")
    else:
        print(f"✅ 需要 Gemini 細分的股票數量：{len(need_df)}")

        for industry_l1 in sorted(need_df["industry_l1"].unique()):
            sub = need_df[need_df["industry_l1"] == industry_l1].copy()

            batch_items = [
                {
                    "stock_id": row["stock_id"],
                    "stock_name": row["stock_name"],
                }
                for _, row in sub.iterrows()
            ]

            batches = chunk_list(batch_items, BATCH_SIZE)
            print(f"🔹 {industry_l1}: {len(batch_items)} 檔，分 {len(batches)} 批")

            for i, batch in enumerate(batches, start=1):
                print(f"   ↳ 第 {i}/{len(batches)} 批，{len(batch)} 檔")

                results = call_gemini_batch_with_retry(industry_l1, batch)
                if not results:
                    continue

                result_map = {x["stock_id"]: x for x in results}

                for sid, res in result_map.items():
                    mask = final_df["stock_id"] == sid
                    if mask.any():
                        final_df.loc[mask, "industry_l2"] = res["industry_l2"]
                        final_df.loc[mask, "tags"] = res["tags"]
                        final_df.loc[mask, "source"] = "gemini"
                        final_df.loc[mask, "confidence"] = str(res["confidence"])
                        final_df.loc[mask, "review_needed"] = res["review_needed"]
                        final_df.loc[mask, "reason"] = res["reason"]
                        final_df.loc[mask, "last_update"] = pd.Timestamp.now().strftime("%Y-%m-%d")

                        row = final_df.loc[mask].iloc[0]
                        candidate_rows.append({
                            "stock_id": row["stock_id"],
                            "stock_name": row["stock_name"],
                            "industry_l1": row["industry_l1"],
                            "suggested_industry_l2": res["industry_l2"],
                            "suggested_tags": res["tags"],
                            "confidence": res["confidence"],
                            "review_needed": res["review_needed"],
                            "reason": res["reason"],
                        })

                time.sleep(BATCH_SLEEP_SEC)

    # 最後再統一修正 review_needed，避免舊資料缺漏
    final_df["review_needed"] = final_df.apply(
        lambda r: decide_review_needed(
            industry_l1=str(r["industry_l1"]),
            industry_l2=str(r["industry_l2"]),
            confidence=r["confidence"],
            source=str(r["source"]),
        ) if str(r["source"]) != "metadata" else (
            "Y" if str(r["industry_l2"]) == str(r["industry_l1"]) else "N"
        ),
        axis=1
    )

    final_df = final_df.sort_values("stock_id").reset_index(drop=True)
    candidates_df = pd.DataFrame(candidate_rows)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values("stock_id").reset_index(drop=True)

    final_df.to_csv(MAPPING_PATH, index=False, encoding="utf-8-sig")
    candidates_df.to_csv(CANDIDATES_PATH, index=False, encoding="utf-8-sig")

    print(f"✅ industry_mapping_final.csv 更新完成：{MAPPING_PATH}")
    print(f"✅ industry_mapping_candidates.csv 更新完成：{CANDIDATES_PATH}")

    if "review_needed" in final_df.columns:
        print()
        print("=== review_needed 統計 ===")
        print(final_df["review_needed"].value_counts(dropna=False).to_string())

    return final_df, candidates_df


def main():
    if TW_STOCKLIST:
        try:
            service = build_drive_service()
            download_drive_file_if_exists(service, TW_STOCKLIST, MAPPING_PATH.name, MAPPING_PATH)
        except Exception as e:
            print(f"⚠️ 從 Google Drive 下載舊 mapping 失敗：{e}")

    final_df, candidates_df = build_mapping()

    if TW_STOCKLIST:
        try:
            service = build_drive_service()
            upload_or_update_drive_file(service, MAPPING_PATH, TW_STOCKLIST)
            upload_or_update_drive_file(service, CANDIDATES_PATH, TW_STOCKLIST)
        except Exception as e:
            print(f"❌ 上傳 Google Drive 失敗：{e}")
            raise

    print()
    print(final_df.head(10).to_string())


if __name__ == "__main__":
    main()
