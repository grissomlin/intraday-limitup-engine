🌍 Multi-Market Sector Momentum Engine

A fully automated, CI-driven system that monitors global stock markets, detects abnormal sector-level momentum, generates multilingual visual assets, builds vertical short-form videos, and uploads them to YouTube — without manual intervention.

🚀 What This Project Is

This is not a stock-picking bot.

It is a market structure visualization engine designed to transform intraday price data into standardized “Sector Pages” — visual maps of:

10%+ abnormal movers

Limit-up locked / touched behavior

Sector-level expansion

Momentum acceleration zones

Capital diffusion structure

The goal is to answer:

Is capital moving at the sector level?

Is strength isolated or expanding?

Is momentum just starting — or accelerating?

How does price-limit regulation affect short-term behavior?

🌍 Markets Covered

Currently monitoring 9 global markets:

US, TW, JP, KR, TH, CN, CA, UK, AU

Markets are grouped structurally by price-limit system:

Market Type	Examples	Structure
No limit	US / CA / UK / AU	Momentum expansion driven by volatility
Single 10% limit	TW	Binary lock / unlock structure
Multi-tier limit	CN	10% / 20% / ST 5% layered behavior
High ceiling limit	KR	30% acceleration dynamics
Tiered limit	TH	Mixed momentum structure

This project treats market regulation as a structural variable, not just a country label.

🎬 Live Output
📺 YouTube Shorts (Auto-Generated Daily)

All generated videos can be viewed here:

👉 https://www.youtube.com/@grissomlin643/shorts

Each market runs on its own CI job and uploads automatically.

📝 Structural Explanation (In-Depth)

I’ve explained the Sector Pages logic and visual structure in detail here:

👉 https://vocus.cc/salon/grissomlin/room/69708fe07d9dd97f474498f1

The article explains:

How to read Sector Pages

What 10%+ means in different markets

What limit-up touch vs locked implies

How sector diffusion confirms momentum

🧠 Core Features

Intraday abnormal mover detection

Sector-level aggregation engine

Limit-up / touch classification logic

Multilingual-safe font rendering (CJK / Thai)

Vertical video generation (FFmpeg)

YouTube Data API auto-upload

Playlist auto-assignment

Google Drive artifact backup

GitHub Actions CI matrix (per market)

Trading-day aware scheduling

Market-local timezone alignment

Headless rendering (CI-safe)

🛠 Tech Stack

Python (Pandas, Matplotlib)

FFmpeg

YouTube Data API

Google Drive API

GitHub Actions (CI matrix per market)

Noto CJK / Thai font handling

Market-local timezone logic

Trading calendar alignment

🏗 Architecture (Simplified)
main.py
  → render_images_<market>
      → render_video
          → youtube_pipeline_safe
              → (optional) drive upload

Each market runs independently inside a GitHub Actions matrix job.

The entire system is:

Headless

Fully automated

Timezone-aware

Trading-day aware

📊 What Makes This Different

Most market content focuses on:

Individual stocks

Narrative explanations

Subjective interpretation

This project focuses on:

Structural behavior

Sector-level movement

Regulation-driven momentum differences

Standardized cross-market comparison

It attempts to make intraday momentum measurable and comparable across different regulatory environments.

📦 How to Run (Example)
python scripts/run_shorts.py --market us --slot midday

Each market can be run independently.

📌 Project Status

Core functionality is complete and fully operational.

Currently focusing on:

Refining filtering logic

Improving edge-case handling

Enhancing data quality consistency across markets

Further stabilizing CI automation

Built over two months of iterative development during evenings and weekends.
Actively maintained and continuously improving.

⚠ Disclaimer

For research and educational purposes only.
Not investment advice.
