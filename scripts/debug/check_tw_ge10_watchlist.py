# scripts/debug/check_tw_ge10_watchlist.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

EPS = 1e-9


def _s(x: Any) -> str:
    return str(x or "").strip()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
