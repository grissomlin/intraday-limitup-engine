# scripts/utils/drive_fs.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional

from googleapiclient.errors import HttpError


def _escape_q(s: str) -> str:
    return (s or "").replace("'", "\\'")


def find_folder(service, parent_id: str, name: str) -> Optional[dict]:
    q = (
        f"'{parent_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"name = '{_escape_q(name)}' and trashed = false"
    )
    resp = (
        service.files()
        .list(
            q=q,
            fields="files(id,name,mimeType,modifiedTime)",
            pageSize=5,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = resp.get("files", []) or []
    return files[0] if files else None


def ensure_folder(service, parent_id: str, name: str) -> str:
    existing = find_folder(service, parent_id, name)
    if existing:
        return existing["id"]

    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    created = service.files().create(body=meta, fields="id,name", supportsAllDrives=True).execute()
    return created["id"]


def list_files_in_folder(service, folder_id: str, *, page_size: int = 1000) -> list[dict]:
    files: list[dict] = []
    page_token = None
    q = f"'{folder_id}' in parents and trashed = false"
    while True:
        resp = (
            service.files()
            .list(
                q=q,
                fields="nextPageToken,files(id,name,mimeType,modifiedTime)",
                pageSize=page_size,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files.extend(resp.get("files", []) or [])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def _is_retryable_http_error(e: Exception) -> bool:
    if not isinstance(e, HttpError):
        return False
    status = getattr(e.resp, "status", None)
    return status in (429, 500, 502, 503, 504)
