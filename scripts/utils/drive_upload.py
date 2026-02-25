# scripts/utils/drive_upload.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import mimetypes
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from .drive_fs import ensure_folder, list_files_in_folder


def _guess_mime(path: Path) -> str:
    mt, _ = mimetypes.guess_type(str(path))
    return mt or "application/octet-stream"


def _is_retryable_http_error(e: Exception) -> bool:
    if not isinstance(e, HttpError):
        return False
    status = getattr(e.resp, "status", None)
    return status in (429, 500, 502, 503, 504)


def _get_creds_from_service(service) -> Optional[Credentials]:
    http = getattr(service, "_http", None)
    return getattr(http, "credentials", None)


def _build_service_from_creds(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _upload_one_fast(
    base_service,
    folder_id: str,
    local_path: Path,
    *,
    existing_id: Optional[str] = None,
    overwrite: bool = False,
    verbose: bool = False,
    chunksize: int = 8 * 1024 * 1024,  # 8MB
    max_retries: int = 6,
) -> str:
    """
    多執行緒單檔上傳：
    - 每個 thread 用同 creds build 自己的 service（避免共用 http 物件）
    - overwrite=True 且 existing_id 有值 -> update；否則 create
    - 內建 429/5xx 退避重試
    """
    if not local_path.exists():
        raise FileNotFoundError(local_path)

    creds = _get_creds_from_service(base_service)
    service = base_service if creds is None else _build_service_from_creds(creds)

    mime_type = _guess_mime(local_path)
    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True, chunksize=chunksize)

    delay = 1.0
    for attempt in range(max_retries):
        try:
            if overwrite and existing_id:
                resp = (
                    service.files()
                    .update(
                        fileId=existing_id,
                        media_body=media,
                        fields="id,name,modifiedTime",
                        supportsAllDrives=True,
                    )
                    .execute()
                )
                if verbose:
                    print(f"♻️  updated: {local_path.name}", flush=True)
                return resp["id"]

            resp = (
                service.files()
                .create(
                    body={"name": local_path.name, "parents": [folder_id]},
                    media_body=media,
                    fields="id,name",
                    supportsAllDrives=True,
                )
                .execute()
            )
            if verbose:
                print(f"⬆️  uploaded: {local_path.name}", flush=True)
            return resp["id"]

        except Exception as e:
            if attempt == max_retries - 1 or not _is_retryable_http_error(e):
                raise
            time.sleep(delay + random.random() * 0.5)
            delay *= 2.0

    raise RuntimeError("unreachable")


def upload_dir(
    service,
    folder_id: str,
    local_dir: Path,
    *,
    pattern: str = "*.png",
    recursive: bool = False,
    overwrite: bool = True,
    verbose: bool = False,
    concurrent: bool = True,
    workers: int = 8,
    subfolder_name: Optional[str] = None,
) -> int:
    """
    上傳資料夾（加速版）
    - overwrite=True：只 list 一次建立 name->id，避免每張圖都查詢
    - concurrent=True：多執行緒上傳（100+ 張圖會快很多）
    - subfolder_name：若提供，會先在 folder_id 下建立/取得子資料夾並上傳到該子資料夾
    """
    local_dir = Path(local_dir)
    if not local_dir.exists():
        raise FileNotFoundError(local_dir)

    if subfolder_name:
        folder_id = ensure_folder(service, folder_id, subfolder_name)

    paths = sorted(local_dir.rglob(pattern) if recursive else local_dir.glob(pattern))
    paths = [p for p in paths if p.is_file()]
    if not paths:
        return 0

    # Non-concurrent path (still uses overwrite map)
    if not concurrent:
        existing_map: dict[str, str] = {}
        if overwrite:
            for f in list_files_in_folder(service, folder_id):
                name = str(f.get("name") or "")
                fid = str(f.get("id") or "")
                if name and fid:
                    existing_map[name] = fid

        n = 0
        for p in paths:
            _upload_one_fast(
                service,
                folder_id,
                p,
                existing_id=existing_map.get(p.name),
                overwrite=overwrite,
                verbose=verbose,
            )
            n += 1
        return n

    # Concurrent path
    existing_map: dict[str, str] = {}
    if overwrite:
        for f in list_files_in_folder(service, folder_id):
            name = str(f.get("name") or "")
            fid = str(f.get("id") or "")
            if name and fid:
                existing_map[name] = fid

    n = 0
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
        futs = [
            ex.submit(
                _upload_one_fast,
                service,
                folder_id,
                p,
                existing_id=existing_map.get(p.name),
                overwrite=overwrite,
                verbose=verbose,
            )
            for p in paths
        ]
        for fut in as_completed(futs):
            _ = fut.result()
            n += 1
    return n
