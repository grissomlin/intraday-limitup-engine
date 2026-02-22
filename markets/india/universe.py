# markets/india/universe.py
# -*- coding: utf-8 -*-
"""
India universe builder (NSE)

Inputs (daily refreshed):
- EQUITY_L.csv  (NSE equities master list; symbol + company name + ISIN ...)
- sec_list.csv  (NSE circuit filter band list; Symbol + Band + Remarks)

This module merges them into ONE universe dataframe for intraday pipeline:
- adds yf_symbol (default ".NS")
- band -> limit_pct (e.g. 20 -> 0.20, "No Band" -> None)

✅ Sector/Industry scaffolding (requested):
- Always output columns: sector, industry
- For now they are placeholders ("Unclassified") unless input files already have them.
- Later you can merge sector/industry from another source (e.g. your own mapping CSV),
  without changing downstream code (downloader/aggregator already see the cols).

Supports:
1) Google Drive folder (preferred for GitHub Actions)
2) Local file paths (VSCode debugging)

Env:
- GDRIVE_TOKEN_B64            : base64 encoded authorized_user token json
- IN_STOCKLIST                : Google Drive folder id containing the two CSVs
- IN_YF_SUFFIX                : default ".NS"

Optional local debug:
- IN_EQUITY_CSV_PATH          : local path to EQUITY_L.csv
- IN_SEC_CSV_PATH             : local path to sec_list.csv

Optional output:
- IN_WRITE_UNIVERSE           : "1" to write data/in_universe.csv when run as module
- IN_UNIVERSE_OUT_PATH        : override output path (default data/in_universe.csv)
"""

from __future__ import annotations

import base64
import io
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd


# =============================================================================
# Env helpers
# =============================================================================
def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _yf_suffix() -> str:
    return _env("IN_YF_SUFFIX", ".NS") or ".NS"


def _write_universe_enabled() -> bool:
    return _env("IN_WRITE_UNIVERSE", "0").lower() in ("1", "true", "yes", "y", "on")


def _out_path() -> str:
    return _env("IN_UNIVERSE_OUT_PATH", os.path.join("data", "in_universe.csv"))


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# =============================================================================
# Google Drive: download file-by-name from folder (in-memory)
# =============================================================================
def _get_drive_service():
    # import inside to keep local-dev lightweight
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request

    token_b64 = _env("GDRIVE_TOKEN_B64")
    if not token_b64:
        raise ValueError("Missing env: GDRIVE_TOKEN_B64")

    token_json = json.loads(base64.b64decode(token_b64).decode("utf-8"))
    creds = Credentials.from_authorized_user_info(token_json, scopes=["https://www.googleapis.com/auth/drive"])

    # GitHub Actions is stateless: refresh_token must exist if expired
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("drive", "v3", credentials=creds)


def _drive_find_file_id(service, *, folder_id: str, file_name: str) -> Optional[str]:
    # Prefer newest by modifiedTime
    q = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    res = (
        service.files()
        .list(
            q=q,
            fields="files(id,name,modifiedTime,size)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    items = res.get("files", []) or []
    if not items:
        return None
    return items[0].get("id")


def _drive_download_bytes(service, *, file_id: str) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload

    buf = io.BytesIO()
    req = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _status, done = dl.next_chunk()
    return buf.getvalue()


def _load_csv_from_drive(folder_id: str, file_name: str) -> pd.DataFrame:
    service = _get_drive_service()
    fid = _drive_find_file_id(service, folder_id=folder_id, file_name=file_name)
    if not fid:
        raise FileNotFoundError(f"Drive folder {folder_id} missing file: {file_name}")

    raw = _drive_download_bytes(service, file_id=fid)
    # NSE CSV is usually UTF-8; if it ever breaks, user can tweak here
    return pd.read_csv(io.BytesIO(raw))


# =============================================================================
# CSV loaders (local or drive)
# =============================================================================
def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _load_equity_df() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    EQUITY_L.csv
    """
    meta: Dict[str, Any] = {"source": None}

    local_path = _env("IN_EQUITY_CSV_PATH")
    if local_path and os.path.exists(local_path):
        df = pd.read_csv(local_path)
        meta["source"] = f"local:{local_path}"
        return _strip_columns(df), meta

    folder_id = _env("IN_STOCKLIST")
    if not folder_id:
        raise ValueError("Missing env: IN_STOCKLIST (Drive folder id) or set IN_EQUITY_CSV_PATH for local debug")

    df = _load_csv_from_drive(folder_id, "EQUITY_L.csv")
    meta["source"] = f"drive:{folder_id}/EQUITY_L.csv"
    return _strip_columns(df), meta


def _load_sec_df() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    sec_list.csv
    """
    meta: Dict[str, Any] = {"source": None}

    local_path = _env("IN_SEC_CSV_PATH")
    if local_path and os.path.exists(local_path):
        df = pd.read_csv(local_path)
        meta["source"] = f"local:{local_path}"
        return _strip_columns(df), meta

    folder_id = _env("IN_STOCKLIST")
    if not folder_id:
        raise ValueError("Missing env: IN_STOCKLIST (Drive folder id) or set IN_SEC_CSV_PATH for local debug")

    df = _load_csv_from_drive(folder_id, "sec_list.csv")
    meta["source"] = f"drive:{folder_id}/sec_list.csv"
    return _strip_columns(df), meta


# =============================================================================
# Merge logic
# =============================================================================
def _to_limit_pct(band: Any) -> Optional[float]:
    """
    band:
      - numeric string/int like 20, 10, 5, 2
      - "No Band"
    return:
      - 0.20, 0.10, 0.05, 0.02
      - None if No Band/blank
    """
    if band is None:
        return None
    s = str(band).strip()
    if not s:
        return None
    if s.lower() in ("no band", "noband", "none", "nan", "-"):
        return None
    try:
        v = float(s)
        if v <= 0:
            return None
        return v / 100.0
    except Exception:
        return None


def _make_yf_symbol(sym: Any) -> str:
    s = ("" if sym is None else str(sym)).strip().upper()
    if not s:
        return ""
    suf = _yf_suffix()
    if s.endswith(suf.upper()) or s.endswith(suf.lower()):
        return s
    return f"{s}{suf}"


def _pick_first_present(row: pd.Series, keys: list[str]) -> Optional[str]:
    for k in keys:
        if k in row.index:
            v = row.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s and s.lower() not in ("nan", "none", "-", "—"):
                return s
    return None


def _normalize_cat(x: Any, default: str = "Unclassified") -> str:
    s = ("" if x is None else str(x)).strip()
    if not s or s.lower() in ("nan", "none", "-", "—"):
        return default
    return s


def build_universe_df() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    eq, meta_eq = _load_equity_df()
    sec, meta_sec = _load_sec_df()

    # Normalize key col
    if "SYMBOL" not in eq.columns:
        raise ValueError(f"EQUITY_L.csv missing SYMBOL column. Columns={list(eq.columns)}")

    # Rename common fields (best-effort)
    eq = eq.rename(
        columns={
            "SYMBOL": "Symbol",
            "NAME OF COMPANY": "name",
            "SERIES": "series",
            "ISIN NUMBER": "isin",
            "FACE VALUE": "face_value",
            "PAID UP VALUE": "paid_up_value",
            "MARKET LOT": "market_lot",
            "DATE OF LISTING": "date_of_listing",
            # If someday the file contains these, we map them too
            "SECTOR": "sector",
            "INDUSTRY": "industry",
            "Sector": "sector",
            "Industry": "industry",
        }
    )

    # sec_list columns: Symbol, Series, Security Name, Band, Remarks
    if "Symbol" not in sec.columns:
        if "SYMBOL" in sec.columns:
            sec = sec.rename(columns={"SYMBOL": "Symbol"})
        else:
            raise ValueError(f"sec_list.csv missing Symbol column. Columns={list(sec.columns)}")

    sec = sec.rename(
        columns={
            "Security Name": "sec_name",
            "Band": "band",
            "Remarks": "remarks",
            "Series": "sec_series",
        }
    )

    # Deduplicate sec_list on Symbol: keep the first (usually fine)
    sec = sec.sort_values(["Symbol"]).drop_duplicates(subset=["Symbol"], keep="first")

    # Merge
    df = eq.merge(sec[["Symbol", "band", "remarks"]], on="Symbol", how="left")

    # -------------------------------------------------------------------------
    # ✅ Sector/Industry scaffolding
    # - If source already contains sector/industry use it
    # - Else set to "Unclassified"
    # -------------------------------------------------------------------------
    if "sector" not in df.columns:
        df["sector"] = None
    if "industry" not in df.columns:
        df["industry"] = None

    df["sector"] = df["sector"].apply(lambda x: _normalize_cat(x, "Unclassified"))
    df["industry"] = df["industry"].apply(lambda x: _normalize_cat(x, "Unclassified"))

    # Add computed fields
    df["yf_symbol"] = df["Symbol"].apply(_make_yf_symbol)
    df["band"] = df.get("band").astype("object") if "band" in df.columns else None
    df["limit_pct"] = df["band"].apply(_to_limit_pct) if "band" in df.columns else None

    # Keep a clean minimal schema (downstream stable)
    keep_cols = [
        "Symbol",
        "yf_symbol",
        "name",
        "sector",     # ✅ scaffolded
        "industry",   # ✅ scaffolded
        "series",
        "band",
        "limit_pct",
        "remarks",
        "isin",
        "face_value",
        "paid_up_value",
        "market_lot",
        "date_of_listing",
    ]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = None
    df = df[keep_cols].copy()

    # Clean blanks
    df["name"] = df["name"].fillna("Unknown").astype(str)
    df["remarks"] = df["remarks"].fillna("").astype(str)
    df["sector"] = df["sector"].fillna("Unclassified").astype(str)
    df["industry"] = df["industry"].fillna("Unclassified").astype(str)

    meta = {
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "yf_suffix": _yf_suffix(),
        "sources": {"equity": meta_eq.get("source"), "sec_list": meta_sec.get("source")},
        "counts": {"equity_rows": int(len(eq)), "sec_rows": int(len(sec)), "universe_rows": int(len(df))},
        "band_counts": df["band"].fillna("NA").astype(str).value_counts().to_dict(),
        "sector_counts": df["sector"].fillna("Unclassified").astype(str).value_counts().to_dict(),
        "industry_counts": df["industry"].fillna("Unclassified").astype(str).value_counts().to_dict(),
    }
    return df, meta


def load_universe_df() -> pd.DataFrame:
    """
    Public function used by downloader_in.py
    """
    df, _meta = build_universe_df()
    return df


# =============================================================================
# CLI
# =============================================================================
def main() -> int:
    df, meta = build_universe_df()

    log("✅ IN universe built.")
    log(f"   sources: {meta['sources']}")
    log(f"   rows: {meta['counts']}")
    log(f"   band_counts(top): {dict(list(meta['band_counts'].items())[:10])}")
    log(f"   sector_counts(top): {dict(list(meta['sector_counts'].items())[:10])}")
    log(f"   industry_counts(top): {dict(list(meta['industry_counts'].items())[:10])}")

    # sanity
    bad = df[df["yf_symbol"].astype(str).str.strip() == ""]
    if not bad.empty:
        log(f"⚠️ yf_symbol empty rows: {len(bad)} (will break downloader). Sample:")
        log(bad.head(5).to_string(index=False))

    if _write_universe_enabled():
        outp = _out_path()
        os.makedirs(os.path.dirname(outp) or ".", exist_ok=True)
        df.to_csv(outp, index=False, encoding="utf-8")
        log(f"💾 wrote: {outp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
