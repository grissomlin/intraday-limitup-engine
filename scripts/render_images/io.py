# scripts/render_images/io.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List, Optional


# =============================================================================
# Filesystem helpers
# =============================================================================
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Payload loading
# =============================================================================
def load_payload(payload_path: Path) -> Dict[str, Any]:
    if not payload_path.exists():
        raise FileNotFoundError(f"payload not found: {payload_path}")
    with payload_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def auto_find_latest_payload(
    repo_root: Path,
    *,
    market: str = "tw",
    slot: str = "midday",
) -> Optional[Path]:
    base = repo_root / "data" / "cache" / market
    if not base.exists():
        return None
    cand = sorted(base.glob(f"*/{slot}.payload.json"), key=lambda p: p.parent.name)
    return cand[-1] if cand else None


# =============================================================================
# Output paths
# =============================================================================
def resolve_output_dir(
    repo_root: Path,
    payload: Dict[str, Any],
    *,
    out_arg: Optional[str] = None,
) -> Path:
    if out_arg:
        out = Path(out_arg)
        if not out.is_absolute():
            out = (repo_root / out).resolve()
        ensure_dir(out)
        return out

    ymd = payload.get("ymd") or "unknown_ymd"
    slot = payload.get("slot") or "unknown_slot"
    out = repo_root / "media" / "images" / ymd / slot
    ensure_dir(out)
    return out


# =============================================================================
# list.txt (video pipeline)
# =============================================================================
def write_list_txt(out_dir: Path, image_paths: List[Path]) -> None:
    if not image_paths:
        return
    lines = []
    for p in image_paths:
        try:
            lines.append(str(p.relative_to(out_dir)).replace("\\", "/"))
        except ValueError:
            lines.append(str(p))
    (out_dir / "list.txt").write_text("\n".join(lines), encoding="utf-8")


# =============================================================================
# Display helpers
# =============================================================================
def parse_cutoff_text(payload: Dict[str, Any]) -> str:
    ymd = str(payload.get("ymd") or "").strip()
    asof = str(payload.get("asof") or "").strip()
    gen = str(payload.get("generated_at") or "").strip()

    src = asof or gen
    hhmm = ""

    if "T" in src:
        try:
            hhmm = src.split("T", 1)[1][:5]
        except Exception:
            hhmm = ""
    elif len(src) >= 5 and ":" in src:
        hhmm = src[:5]

    if ymd and hhmm:
        return f"{ymd} ｜ 截至 {hhmm}"
    if ymd:
        return ymd
    if hhmm:
        return f"截至 {hhmm}"
    return ""
