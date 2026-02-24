# scripts/render_images_common/sector_order.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _s(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def normalize_sector_key(s: Any) -> str:
    """
    Normalize sector names for matching overview order:
    - strip
    - collapse whitespace
    - lowercase
    """
    ss = _s(s)
    ss = re.sub(r"\s+", " ", ss).strip().lower()
    return ss


def extract_overview_sector_order(payload: Dict[str, Any]) -> List[str]:
    """
    Read payload["_overview_sector_order"] exported by overview renderer,
    normalize + de-dup keep order.
    """
    raw = payload.get("_overview_sector_order", []) or []
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for x in raw:
        k = normalize_sector_key(x)
        if k:
            out.append(k)
    seen = set()
    out2: List[str] = []
    for k in out:
        if k not in seen:
            out2.append(k)
            seen.add(k)
    return out2


def reorder_keys_by_overview(
    *,
    existing_keys: Iterable[str],
    overview_keys: List[str],
) -> List[str]:
    """
    Reorder existing keys by overview order; append remaining in original order.
    Inputs should already be normalized keys (e.g. normalize_sector_key()).
    """
    existing = list(existing_keys)
    existing_set = set(existing)

    out: List[str] = []
    seen = set()

    for k in (overview_keys or []):
        if k in existing_set and k not in seen:
            out.append(k)
            seen.add(k)

    for k in existing:
        if k not in seen:
            out.append(k)
            seen.add(k)

    return out


def write_list_txt_ordered(outdir: Path, ordered_paths: List[Path], filename: str = "list.txt") -> Path:
    """
    Write list.txt in the exact order you want.
    """
    outdir = Path(outdir).resolve()
    seen = set()
    final: List[Path] = []
    for p in ordered_paths:
        try:
            pp = Path(p).resolve()
            if pp.is_file():
                key = pp.name
                if key not in seen:
                    final.append(pp)
                    seen.add(key)
        except Exception:
            continue

    list_path = outdir / filename
    rel_lines = [p.relative_to(outdir).as_posix() for p in final]
    list_path.write_text("\n".join(rel_lines) + ("\n" if rel_lines else ""), encoding="utf-8")
    return list_path


def _collect_overview_images(outdir: Path, overview_prefix: str = "overview_sectors_") -> List[Path]:
    outdir = Path(outdir).resolve()
    pref = str(overview_prefix or "overview_sectors_").strip()
    paged = sorted(outdir.glob(f"{pref}*_p*.png"), key=lambda p: p.name)
    if paged:
        return [p for p in paged if p.is_file()]
    any_overview = sorted(outdir.glob(f"{pref}*.png"), key=lambda p: p.name)
    return [p for p in any_overview if p.is_file()]


def _infer_sector_key_from_filename(
    filename: str,
    *,
    prefix: str,
    suffix_re: str = r"_p\d+\.png$",
) -> str:
    """
    Extract sector part from filenames like:
      "{prefix}{sector}_p1.png"
    Then normalize it.
    """
    name = filename
    if not name.lower().endswith(".png"):
        return ""
    if not name.startswith(prefix):
        return ""
    core = name[len(prefix) :]
    core = re.sub(suffix_re, "", core, flags=re.IGNORECASE)
    # undo common safe_filename rules: underscores -> spaces
    core = core.replace("_", " ")
    return normalize_sector_key(core)


def write_list_txt_from_overview_order(
    *,
    outdir: Path,
    overview_prefix: str,
    sector_page_glob: str,
    overview_sector_keys: List[str],
    list_filename: str = "list.txt",
    # filename parse control
    sector_file_prefix: Optional[str] = None,
) -> Path:
    """
    Build list.txt:
      1) overview images first (overview_prefix)
      2) sector pages ordered by overview_sector_keys
      3) remaining pngs (not already included) by name

    This function assumes sector page filenames embed sector name, default:
      "{sector_file_prefix}{sector}_pN.png"
    where sector_file_prefix defaults to "{market_lower}_".
    """
    outdir = Path(outdir).resolve()

    ordered_paths: List[Path] = []

    # 1) overview first
    overview_imgs = _collect_overview_images(outdir, overview_prefix=overview_prefix)
    ordered_paths.extend(overview_imgs)

    # 2) sector pages
    sector_pages = sorted([p for p in outdir.glob(sector_page_glob) if p.is_file()], key=lambda p: p.name)

    # Determine prefix from glob if not provided:
    # e.g. "au_*_p*.png" -> "au_"
    if sector_file_prefix is None:
        m = re.match(r"^([a-z]{2})_", sector_page_glob.strip().lower())
        sector_file_prefix = f"{m.group(1)}_" if m else ""

    # Group pages by sector key (normalized)
    sec_to_pages: Dict[str, List[Path]] = {}
    for p in sector_pages:
        k = _infer_sector_key_from_filename(p.name, prefix=sector_file_prefix)
        if not k:
            continue
        sec_to_pages.setdefault(k, []).append(p)

    # sort pages inside each sector by name
    for k in sec_to_pages:
        sec_to_pages[k] = sorted(sec_to_pages[k], key=lambda p: p.name)

    # apply overview order
    ordered_keys = reorder_keys_by_overview(existing_keys=sec_to_pages.keys(), overview_keys=overview_sector_keys)

    for k in ordered_keys:
        ordered_paths.extend(sec_to_pages.get(k, []))

    # 3) append remaining pngs not yet included (stable)
    already = {p.name for p in ordered_paths}
    all_pngs = sorted([p for p in outdir.glob("*.png") if p.is_file()], key=lambda p: p.name)
    for p in all_pngs:
        if p.name not in already:
            ordered_paths.append(p)
            already.add(p.name)

    return write_list_txt_ordered(outdir, ordered_paths, filename=list_filename)
