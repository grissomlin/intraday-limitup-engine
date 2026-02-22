# dashboard/components/paths.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
TW_CACHE_DIR = os.path.join(DATA_DIR, "cache", "tw")
PROMPTS_DIR = os.path.join(ROOT_DIR, "prompts")

STOCKLIST_FILE = os.path.join(DATA_DIR, "tw_stock_list.json")
