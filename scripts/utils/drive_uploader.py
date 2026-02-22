# scripts/utils/drive_uploader.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import json
import mimetypes
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

DEFAULT_SCOPES = ["https://www.googleapis.com/auth/drive"]

# default token cache path (repo-root/data/secrets/gdrive_token.json)
_THIS = Path(__file__).resolve()
_REPO_ROOT = _THIS.parents[2]
DEFAULT_TOKEN_FILE = _REPO_ROOT / "data" / "secrets" / "gdrive_token.json"


# =============================================================================
# dotenv loader (auto)
# =============================================================================
def _auto_load_dotenv() -> None:
    """
    Load .env if present.
    - Try CWD/.env
    - Try repo_root/.env (scripts/utils/.. -> repo)
    NOTE: override=False, so real env vars will win.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return

    cwd_env = Path.cwd() / ".env"
    if cwd_env.exists():
        load_dotenv(dotenv_path=str(cwd_env), override=False)

    try:
        root_env = _REPO_ROOT / ".env"
        if root_env.exists():
            load_dotenv(dotenv_path=str(root_env), override=False)
    except Exception:
        pass


_auto_load_dotenv()


# =============================================================================
# env / debug / CI
# =============================================================================
def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _debug(msg: str) -> None:
    if _env_on("GDRIVE_DEBUG"):
        print(msg, flush=True)


def _is_ci() -> bool:
    return _env_on("GITHUB_ACTIONS") or _env_on("CI")


def _allow_interactive() -> bool:
    """
    CI 預設禁止互動登入（避免卡住）
    本機若要跳出授權流程：GDRIVE_ALLOW_INTERACTIVE=1
    """
    if _env_on("GDRIVE_ALLOW_INTERACTIVE"):
        return True
    if _is_ci():
        return False
    return False


def _sha10(s: str) -> str:
    if not s:
        return ""
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return h[:10]


def _fingerprint_token_cfg(cfg: dict) -> str:
    """
    Return a non-sensitive fingerprint string for debugging.
    """
    rt = str(cfg.get("refresh_token") or "")
    cid = str(cfg.get("client_id") or "")
    exp = str(cfg.get("expiry") or "")
    return f"rt_sha10={_sha10(rt)} cid_sha10={_sha10(cid)} expiry={exp}"


def _load_env_json(name: str) -> Optional[dict]:
    """
    支援四種來源（依序）：
    1) 直接 JSON：NAME={"a":1}
    2) 標準 B64：NAME_B64=base64(json)
    3) 你目前在用的：NAME_JSON_B64（例如 GDRIVE_TOKEN_JSON_B64）
    4) legacy shortcuts：
       - GDRIVE_TOKEN_B64
       - GDRIVE_CLIENT_SECRET_B64
    """
    # 1) plain JSON
    s = (os.getenv(name) or "").strip()
    if s:
        try:
            return json.loads(s)
        except Exception as e:
            raise RuntimeError(f"Env {name} is not valid JSON: {e}")

    # 2) NAME_B64
    s_b64 = (os.getenv(f"{name}_B64") or "").strip()
    if s_b64:
        try:
            decoded = base64.b64decode(s_b64).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"Env {name}_B64 is not valid base64 JSON: {e}")

    # 3) NAME_JSON_B64 (your current convention)
    s_json_b64 = (os.getenv(f"{name}_JSON_B64") or "").strip()
    if s_json_b64:
        try:
            decoded = base64.b64decode(s_json_b64).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"Env {name}_JSON_B64 is not valid base64 JSON: {e}")

    # 4) legacy shortcuts
    legacy_map = {
        "GDRIVE_CLIENT_SECRET_JSON": "GDRIVE_CLIENT_SECRET_B64",
        "GDRIVE_TOKEN_JSON": "GDRIVE_TOKEN_B64",
    }
    legacy_key = legacy_map.get(name)
    if legacy_key:
        s_b64 = (os.getenv(legacy_key) or "").strip()
        if s_b64:
            try:
                decoded = base64.b64decode(s_b64).decode("utf-8")
                return json.loads(decoded)
            except Exception as e:
                raise RuntimeError(f"Env {legacy_key} is not valid base64 JSON: {e}")

    return None


def _ensure_refresh_token(token_cfg: dict) -> None:
    if not (token_cfg or {}).get("refresh_token"):
        raise RuntimeError(
            "GDRIVE_TOKEN_JSON missing refresh_token.\n"
            "You must generate token ONCE locally (offline access) and store it as a GitHub Secret."
        )


def _read_json_file(path: Path) -> Optional[dict]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def _write_json_file(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_creds_anywhere(creds: Credentials, token_file: Optional[str | Path]) -> None:
    """
    Always save refreshed creds to:
      1) token_file if provided
      2) DEFAULT_TOKEN_FILE
    """
    j = json.loads(creds.to_json())
    # Keep `token_uri/client_id/client_secret/scopes/refresh_token` etc.
    p_list: list[Path] = []
    if token_file:
        p_list.append(Path(token_file))
    if DEFAULT_TOKEN_FILE not in p_list:
        p_list.append(DEFAULT_TOKEN_FILE)

    for p in p_list:
        try:
            _write_json_file(p, j)
            _debug(f"[drive_uploader] token saved to {p}")
        except Exception as e:
            _debug(f"[drive_uploader] token save failed to {p}: {e}")

    if _env_on("GDRIVE_PRINT_TOKEN_B64"):
        try:
            b64 = base64.b64encode(json.dumps(j).encode("utf-8")).decode("utf-8")
            print("\n========== COPY THIS TO GITHUB SECRET ==========\n")
            print(b64)
            print("\n===============================================\n")
        except Exception:
            pass


# =============================================================================
# Auth
# =============================================================================
def get_drive_service(
    *,
    scopes: Optional[list[str]] = None,
    client_secret_file: Optional[str] = None,
    token_file: Optional[str] = None,
    env_client_secret_key: str = "GDRIVE_CLIENT_SECRET_JSON",
    env_token_key: str = "GDRIVE_TOKEN_JSON",
):
    """
    ✅ 優先讀 env/.env 的 JSON（或 B64）：
      - secret: GDRIVE_CLIENT_SECRET_JSON / _B64 / _JSON_B64 / legacy _B64
      - token : GDRIVE_TOKEN_JSON / _B64 / _JSON_B64 / legacy _B64

    ✅ 預設 token_file：data/secrets/gdrive_token.json（會自動回寫 refresh 後 token）

    ✅ CI/GitHub Actions：禁止互動登入（不會跳出 Please visit this URL...）
       - 必須提供 refresh_token 才能 headless refresh

    本機若要互動登入（只做一次拿 refresh_token）：GDRIVE_ALLOW_INTERACTIVE=1
    """
    scopes = scopes or DEFAULT_SCOPES
    _auto_load_dotenv()

    token_path = Path(token_file) if token_file else DEFAULT_TOKEN_FILE

    secret_cfg = _load_env_json(env_client_secret_key)
    token_cfg_env = _load_env_json(env_token_key)
    token_cfg_file = _read_json_file(token_path)

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _debug(
        "[drive_uploader] "
        f"CI={_is_ci()} allow_interactive={_allow_interactive()} "
        f"env_secret={bool(secret_cfg)} env_token={bool(token_cfg_env)} "
        f"token_path={token_path} now_utc={now_utc}"
    )

    if token_cfg_env:
        _debug(f"[drive_uploader] env_token_fp: {_fingerprint_token_cfg(token_cfg_env)}")
    if token_cfg_file:
        _debug(f"[drive_uploader] file_token_fp: {_fingerprint_token_cfg(token_cfg_file)}")

    creds: Optional[Credentials] = None
    token_source = ""

    # 1) token: env first, then file
    if token_cfg_env:
        creds = Credentials.from_authorized_user_info(token_cfg_env, scopes=scopes)
        token_source = "env"
    elif token_cfg_file:
        creds = Credentials.from_authorized_user_info(token_cfg_file, scopes=scopes)
        token_source = "file"

    # 2) refresh if needed
    def _try_refresh(c: Credentials, *, source: str) -> Optional[Credentials]:
        if c.valid:
            _debug(f"[drive_uploader] creds already valid (source={source})")
            return c
        # only refresh when expired + has refresh_token
        if c.expired and c.refresh_token:
            try:
                c.refresh(Request())
                _debug(f"[drive_uploader] token refreshed (source={source})")
                _save_creds_anywhere(c, token_path)
                return c
            except Exception as e:
                _debug(f"[drive_uploader] refresh failed (source={source}): {e}")
                return None
        _debug(f"[drive_uploader] creds invalid but not refreshable (source={source})")
        return None

    if creds:
        refreshed = _try_refresh(creds, source=token_source)
        creds = refreshed if refreshed else None

    # 2.5) If env path failed, fallback to file token (very common in your case)
    if (not creds or not creds.valid) and token_cfg_file and token_source != "file":
        creds2 = Credentials.from_authorized_user_info(token_cfg_file, scopes=scopes)
        creds2 = _try_refresh(creds2, source="file") or None
        if creds2 and creds2.valid:
            creds = creds2

    # 3) If still invalid: CI must fail fast; local may interactive (opt-in)
    if not creds or not creds.valid:
        if _is_ci() or not _allow_interactive():
            # if env token existed, ensure refresh_token exists (more useful error)
            if token_cfg_env:
                _ensure_refresh_token(token_cfg_env)
            raise RuntimeError(
                "No valid Google Drive credentials for headless run.\n"
                "Provide BOTH:\n"
                "  - GDRIVE_CLIENT_SECRET_JSON (or *_B64 / *_JSON_B64)\n"
                "  - GDRIVE_TOKEN_JSON (or *_B64 / *_JSON_B64) (must include refresh_token)\n"
                "If your refresh_token is expired/revoked, re-authorize locally with:\n"
                "  GDRIVE_ALLOW_INTERACTIVE=1\n"
            )

        # local interactive login (opt-in)
        if not secret_cfg and not (client_secret_file and os.path.exists(client_secret_file)):
            raise RuntimeError(
                "Missing OAuth client secret.\n"
                f"- Provide env {env_client_secret_key} (JSON/B64) OR\n"
                "- Provide --drive-client-secret <client_secret.json>"
            )

        flow = (
            InstalledAppFlow.from_client_config(secret_cfg, scopes)
            if secret_cfg
            else InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
        )
        creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

        # always save for future headless runs
        _save_creds_anywhere(creds, token_path)

    return build("drive", "v3", credentials=creds, cache_discovery=False)


# =============================================================================
# Folder helpers
# =============================================================================
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


# =============================================================================
# Clear & list
# =============================================================================
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


def clear_folder(service, folder_id: str, *, workers: int = 8, verbose: bool = False) -> int:
    """
    ⚠️ Drive 刪檔一定慢（N 檔 = N 次 API）
    這裡提供「併發刪除」版，仍然會比不上「不刪、改用子資料夾」快。
    """
    files = list_files_in_folder(service, folder_id)
    if not files:
        return 0

    def _del(fid: str, name: str):
        delay = 1.0
        for _ in range(6):
            try:
                service.files().delete(fileId=fid, supportsAllDrives=True).execute()
                if verbose:
                    print(f"🗑️ deleted: {name}", flush=True)
                return True
            except Exception as e:
                if isinstance(e, HttpError) and getattr(e.resp, "status", None) in (429, 500, 502, 503, 504):
                    time.sleep(delay + random.random() * 0.5)
                    delay *= 2.0
                    continue
                return False
        return False

    n = 0
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
        futs = [ex.submit(_del, f["id"], f.get("name", "")) for f in files if f.get("id")]
        for fut in as_completed(futs):
            if fut.result():
                n += 1
    return n


# =============================================================================
# Upload
# =============================================================================
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