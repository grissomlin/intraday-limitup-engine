# scripts/utils/drive_auth.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ✅ Prefer minimal scope to reduce invalid_scope when refreshing existing tokens
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

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


def _debug_env_presence(env_client_secret_key: str, env_token_key: str) -> None:
    """
    Safe debug (no secret content):
    - prints whether each env exists and its length
    - prints first 2 chars to detect empty/"***" masking weirdness
    Enabled only when GDRIVE_DEBUG=1
    """
    if not _env_on("GDRIVE_DEBUG"):
        return

    def _p(k: str) -> str:
        v = (os.getenv(k) or "")
        vv = v.strip()
        return f"{k}: present={bool(vv)} len={len(vv)} head2={vv[:2]!r}"

    keys = [
        env_client_secret_key,
        f"{env_client_secret_key}_B64",
        f"{env_client_secret_key}_JSON_B64",
        # legacy shortcuts used in workflows
        "GDRIVE_CLIENT_SECRET_B64",
        env_token_key,
        f"{env_token_key}_B64",
        f"{env_token_key}_JSON_B64",
        # legacy shortcuts used in workflows
        "GDRIVE_TOKEN_B64",
    ]
    _debug("[drive_auth][env_presence] " + " | ".join(_p(k) for k in keys))


def _debug_decode_json_fields(label: str, raw: str, *, required: list[str]) -> None:
    """
    Safe debug:
    - try base64 decode + json loads, then report which required fields exist
    - does NOT print token values
    Enabled only when GDRIVE_DEBUG=1
    """
    if not _env_on("GDRIVE_DEBUG"):
        return

    if not raw:
        _debug(f"[drive_auth][{label}] raw empty")
        return

    # try plain JSON
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            _debug(
                f"[drive_auth][{label}] parsed as plain JSON; has_fields="
                + str({k: (k in obj and bool(obj.get(k))) for k in required})
            )
            # also safe-print scopes length if present
            sc = obj.get("scopes")
            if isinstance(sc, list):
                _debug(f"[drive_auth][{label}] scopes_count={len(sc)}")
            return
    except Exception:
        pass

    # try base64(JSON)
    try:
        dec = base64.b64decode(raw).decode("utf-8", errors="replace")
    except Exception as e:
        _debug(f"[drive_auth][{label}] base64 decode failed: {type(e).__name__}: {str(e)[:120]}")
        return

    try:
        obj = json.loads(dec)
        if not isinstance(obj, dict):
            _debug(f"[drive_auth][{label}] decoded JSON is not dict")
            return
        _debug(
            f"[drive_auth][{label}] parsed as base64(JSON); has_fields="
            + str({k: (k in obj and bool(obj.get(k))) for k in required})
        )
        sc = obj.get("scopes")
        if isinstance(sc, list):
            _debug(f"[drive_auth][{label}] scopes_count={len(sc)}")
    except Exception as e:
        _debug(f"[drive_auth][{label}] json load failed after base64: {type(e).__name__}: {str(e)[:120]}")
        # Safe hint: first char only
        _debug(f"[drive_auth][{label}] decoded_head1={dec[:1]!r}")


def _load_env_json(name: str) -> Optional[dict]:
    """
    支援多種來源（依序）：
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

    # 3) NAME_JSON_B64
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
    p_list: list[Path] = []
    if token_file:
        p_list.append(Path(token_file))
    if DEFAULT_TOKEN_FILE not in p_list:
        p_list.append(DEFAULT_TOKEN_FILE)

    for p in p_list:
        try:
            _write_json_file(p, j)
            _debug(f"[drive_auth] token saved to {p}")
        except Exception as e:
            _debug(f"[drive_auth] token save failed to {p}: {e}")

    if _env_on("GDRIVE_PRINT_TOKEN_B64"):
        try:
            b64 = base64.b64encode(json.dumps(j).encode("utf-8")).decode("utf-8")
            print("\n========== COPY THIS TO GITHUB SECRET ==========\n")
            print(b64)
            print("\n===============================================\n")
        except Exception:
            pass


def _token_scopes(cfg: Optional[dict]) -> Optional[list[str]]:
    """
    If token json contains scopes, prefer it to avoid invalid_scope on refresh.
    """
    if not cfg:
        return None
    s = cfg.get("scopes")
    if isinstance(s, list) and s and all(isinstance(x, str) for x in s):
        out = [x.strip() for x in s if x.strip()]
        return out or None
    return None


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

    # Safe env presence debug
    _debug_env_presence(env_client_secret_key, env_token_key)

    # Load configs from env (supports plain/b64/json_b64/legacy)
    secret_cfg = _load_env_json(env_client_secret_key)
    token_cfg_env = _load_env_json(env_token_key)
    token_cfg_file = _read_json_file(token_path)

    # Safe decode checks (only when GDRIVE_DEBUG=1)
    raw_token = (
        (os.getenv(env_token_key) or "").strip()
        or (os.getenv(f"{env_token_key}_B64") or "").strip()
        or (os.getenv(f"{env_token_key}_JSON_B64") or "").strip()
        or (os.getenv("GDRIVE_TOKEN_B64") or "").strip()
    )
    raw_secret = (
        (os.getenv(env_client_secret_key) or "").strip()
        or (os.getenv(f"{env_client_secret_key}_B64") or "").strip()
        or (os.getenv(f"{env_client_secret_key}_JSON_B64") or "").strip()
        or (os.getenv("GDRIVE_CLIENT_SECRET_B64") or "").strip()
    )
    _debug_decode_json_fields(
        "TOKEN_ENV",
        raw_token,
        required=["client_id", "client_secret", "refresh_token", "token_uri"],
    )
    _debug_decode_json_fields(
        "CLIENT_SECRET_ENV",
        raw_secret,
        required=["installed", "web"],
    )

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _debug(
        "[drive_auth] "
        f"CI={_is_ci()} allow_interactive={_allow_interactive()} "
        f"env_secret={bool(secret_cfg)} env_token={bool(token_cfg_env)} "
        f"token_path={token_path} now_utc={now_utc}"
    )

    if token_cfg_env:
        _debug(f"[drive_auth] env_token_fp: {_fingerprint_token_cfg(token_cfg_env)}")
    if token_cfg_file:
        _debug(f"[drive_auth] file_token_fp: {_fingerprint_token_cfg(token_cfg_file)}")

    creds: Optional[Credentials] = None
    token_source = ""

    # 1) token: env first, then file
    if token_cfg_env:
        eff_scopes = _token_scopes(token_cfg_env) or scopes
        _debug(f"[drive_auth] effective_scopes(env)={eff_scopes}")
        creds = Credentials.from_authorized_user_info(token_cfg_env, scopes=eff_scopes)
        token_source = "env"
    elif token_cfg_file:
        eff_scopes = _token_scopes(token_cfg_file) or scopes
        _debug(f"[drive_auth] effective_scopes(file)={eff_scopes}")
        creds = Credentials.from_authorized_user_info(token_cfg_file, scopes=eff_scopes)
        token_source = "file"

    # 2) refresh if needed
    def _try_refresh(c: Credentials, *, source: str) -> Optional[Credentials]:
        if c.valid:
            _debug(f"[drive_auth] creds already valid (source={source})")
            return c
        if c.expired and c.refresh_token:
            try:
                c.refresh(Request())
                _debug(f"[drive_auth] token refreshed (source={source})")
                _save_creds_anywhere(c, token_path)
                return c
            except Exception as e:
                # ✅ Provide clearer hint for invalid_scope
                msg = str(e)
                _debug(f"[drive_auth] refresh failed (source={source}): {e}")
                if "invalid_scope" in msg:
                    _debug(
                        "[drive_auth] HINT: invalid_scope usually means your refresh_token was "
                        "issued for DIFFERENT scopes than this run. "
                        "Fix by either:\n"
                        "  (A) Make token scopes match (prefer token's own scopes), or\n"
                        "  (B) Re-authorize locally for Drive with scope drive.file and store new token."
                    )
                return None
        _debug(f"[drive_auth] creds invalid but not refreshable (source={source})")
        return None

    if creds:
        creds = _try_refresh(creds, source=token_source) or None

    # 2.5) If env path failed, fallback to file token
    if (not creds or not creds.valid) and token_cfg_file and token_source != "file":
        eff_scopes = _token_scopes(token_cfg_file) or scopes
        _debug(f"[drive_auth] effective_scopes(fallback_file)={eff_scopes}")
        creds2 = Credentials.from_authorized_user_info(token_cfg_file, scopes=eff_scopes)
        creds2 = _try_refresh(creds2, source="file") or None
        if creds2 and creds2.valid:
            creds = creds2

    # 3) If still invalid: CI must fail fast; local may interactive (opt-in)
    if not creds or not creds.valid:
        if _is_ci() or not _allow_interactive():
            if token_cfg_env:
                _ensure_refresh_token(token_cfg_env)
            raise RuntimeError(
                "No valid Google Drive credentials for headless run.\n"
                "Provide BOTH:\n"
                "  - GDRIVE_CLIENT_SECRET_JSON (or *_B64 / *_JSON_B64)\n"
                "  - GDRIVE_TOKEN_JSON (or *_B64 / *_JSON_B64) (must include refresh_token)\n"
                "If your refresh_token is expired/revoked OR scope mismatch (invalid_scope), re-authorize locally with:\n"
                "  GDRIVE_ALLOW_INTERACTIVE=1\n"
            )

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
        _save_creds_anywhere(creds, token_path)

    return build("drive", "v3", credentials=creds, cache_discovery=False)
