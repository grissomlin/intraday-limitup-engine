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

<img width="1080" height="1920" alt="us_Consumer_Discretionary_p1" src="https://github.com/user-attachments/assets/38ef2614-66ff-4709-931d-eaf45c4a2fbb" />
<img width="1080" height="1920" alt="overview_sectors_bigmove10_p1" src="https://github.com/user-attachments/assets/7b0a2801-bd14-43c2-b041-79c635ec9984" />
<img width="1080" height="1920" alt="tw_化學工業_p1" src="https://github.com/user-attachments/assets/66927bc5-fbbd-450b-a9b6-4d8813ab8121" />
<img width="1080" height="1920" alt="overview_sectors_mix_p1" src="https://github.com/user-attachments/assets/48d53cef-b4d1-4b72-a01d-d684ee505f41" />
<img width="1080" height="1920" alt="overview_sectors_mix_p1" src="https://github.com/user-attachments/assets/0336c85f-d76b-4345-9d16-4fb35e730e13" />
<img width="1080" height="1920" alt="cn_磨具磨料_p1" src="https://github.com/user-attachments/assets/ac97ff2c-dd6e-4c56-9400-f30623c4a3dc" />
<img width="1080" height="1920" alt="overview_sectors_mix_p1" src="https://github.com/user-attachments/assets/619e7953-8956-40e8-abf4-a92a6dcb1c9c" />
<img width="1080" height="1920" alt="kr_펄프_종이_및_판지_제조업_p1" src="https://github.com/user-attachments/assets/a31b6413-711a-41e0-a9df-9ad740cc253c" />
<img width="1080" height="1920" alt="th_Industrial_Materials___Machinery_p1" src="https://github.com/user-attachments/assets/a3e6933f-2379-490e-889c-150175110508" />
<img width="1080" height="1920" alt="overview_sectors_mix_p1" src="https://github.com/user-attachments/assets/c5b1ba96-5041-4610-95a9-42af604e1132" />
<img width="1080" height="1920" alt="overview_sectors_bigmove10_p1" src="https://github.com/user-attachments/assets/7b1f8d43-dcfa-4c3c-84ed-25463a712190" />
<img width="1080" height="1920" alt="ca_Technology_p1" src="https://github.com/user-attachments/assets/b95efc0a-d6d5-4b4e-8109-083bfab59368" />
<img width="1080" height="1920" alt="overview_sectors_mix_p1" src="https://github.com/user-attachments/assets/b3e40b9d-f28b-4d6a-9212-0c9a28861b89" />
<img width="1080" height="1920" alt="jp_Wholesale_Trade_p1" src="https://github.com/user-attachments/assets/ef016239-d77d-48a4-afa7-b24384e64f23" />
<img width="1080" height="1920" alt="overview_sectors_bigmove10_p1" src="https://github.com/user-attachments/assets/0cf0cfa2-91e7-4ace-b81b-d90d23feb0c3" />
<img width="1080" height="1920" alt="au_Utilities_p1" src="https://github.com/user-attachments/assets/d49b5219-d8e9-4d8b-a869-1073b7ea210c" />






For research and educational purposes only.
Not investment advice.

