# Multi-Market Automated Stock Shorts System

A fully automated, CI-driven pipeline that monitors global stock markets, generates multilingual visual assets, builds vertical videos, and uploads them to YouTube — without manual intervention.

## Overview

This system monitors 9 global markets:

US, TW, JP, KR, TH, CN, CA, UK, AU

For each market, it:

- Detects abnormal price movers
- Generates sector-based visual summaries
- Handles multilingual font rendering (CJK / Thai safe)
- Builds vertical short-form videos via FFmpeg
- Uploads videos automatically using the YouTube Data API
- Assigns region-specific playlists
- Archives artifacts to Google Drive
- Runs entirely on GitHub Actions (matrix workflow per market)

The entire pipeline is headless, timezone-aware, and trading-day aware.

## Tech Stack

- Python (Pandas, Matplotlib)
- FFmpeg
- YouTube Data API
- Google Drive API
- GitHub Actions (CI matrix per market)
- Noto CJK / Thai font handling
- Market-local timezone logic
- Trading calendar alignment

## Architecture (Simplified)

main.py  
→ render_images_<market>  
→ render_video  
→ youtube_pipeline_safe  
→ (optional) drive upload  

Each market runs independently inside a CI matrix job.

## Status

Built over two months of iterative development during evenings and weekends.  
Actively maintained and continuously improving filtering logic and data quality.
