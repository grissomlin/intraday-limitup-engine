# scripts/render_images/__init__.py
# -*- coding: utf-8 -*-

"""
scripts.render_images package

Matplotlib-based rendering pipeline (2026-01):

Active entry points:
- cli.py               -> CLI entry
- overview_mpl.py      -> Overview images (auto-paginated)
- sector_blocks_mpl.py -> Sector tables (limitup + peers)

⚠️ This package intentionally keeps __init__.py minimal to avoid
import errors from legacy / removed modules (render_table, adapters, etc.).
"""

from .cli import main as cli_main

__all__ = ["cli_main"]
