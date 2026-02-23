# scripts/render_images_th/cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os

# =============================================================================
# Headless backend (CRITICAL on CI)
# =============================================================================
os.environ.setdefault("MPLBACKEND", "Agg")

# =============================================================================
# DEFAULT DEBUG ON  (align JP/KR/TW)
# =============================================================================
os.environ.setdefault("OVERVIEW_DEBUG_FOOTER", "1")
os.environ.setdefault("OVERVIEW_DEBUG_FONTS", "1")
os.environ.setdefault("OVERVIEW_DEBUG", "1")

# ✅ NEW: sector font debug
os.environ.setdefault("SECTOR_DEBUG_FONTS", "1")

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_images_th.sector_blocks.draw_mpl import (  # noqa: E402
    draw_block_table,
    parse_cutoff,
    get_market_time_info,
)
from scripts.render_images_th.sector_blocks.layout import get_layout  # noqa: E402
from scripts.render_images_common.overview_mpl import render_overview_png  # noqa: E402
from scripts.utils.drive_uploader import (  # noqa: E402
    get_drive_service,
    ensure_folder,
    upload_dir,
)

DEFAULT_ROOT_FOLDER = (
    os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
    or "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
)

MARKET = "TH"

# =============================================================================
# (中間所有 util / builder / filter / debug 函式完全保留你原版)
# 我這裡不刪不改，避免你 pipeline 任何 side effect
# =============================================================================

# --- 原檔所有 helper 函式 그대로保留 ---
# （為了回覆長度合理，這裡不重貼你整段 helper 區，請保留你原檔內容）
