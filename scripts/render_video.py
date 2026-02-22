# scripts/render_video.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import List, Optional


def _run(cmd: List[str]) -> None:
    print("[render_video] $", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _ensure_ffmpeg() -> None:
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True, text=True)
    except Exception as e:
        raise RuntimeError(
            "找不到 ffmpeg。請先安裝：\n"
            "  - PowerShell：winget install Gyan.FFmpeg\n"
            "  - 或自行安裝並將 ffmpeg 加入 PATH\n"
        ) from e


# =============================================================================
# Fallback 圖片蒐集（當 list.txt 不存在時）
# =============================================================================
def _collect_images_current_layout(
    images_dir: Path,
    *,
    prefer_top: int = 15,
    ext: str = "png",
) -> List[Path]:
    """
    Try to collect images in the current layout (fallback mode):
      1) overview_sectors_top{prefer_top}_p*.{ext} (sorted)
         fallback to overview_sectors_top{prefer_top}.{ext}
         fallback to top12 (paged then single)
      2) sectors/*.{ext} (sorted by name)
    Return ABSOLUTE paths.
    """
    images: List[Path] = []

    def add_overview(topn: int) -> bool:
        paged = sorted(images_dir.glob(f"overview_sectors_top{topn}_p*.{ext}"), key=lambda p: p.name)
        if paged:
            images.extend(paged)
            return True

        single = images_dir / f"overview_sectors_top{topn}.{ext}"
        if single.exists():
            images.append(single)
            return True

        return False

    if not add_overview(prefer_top):
        add_overview(12)

    sec_dir = images_dir / "sectors"
    if sec_dir.exists():
        sec_imgs = sorted(sec_dir.glob(f"*.{ext}"), key=lambda p: p.name)
        images.extend(sec_imgs)

    return images


def _write_concat_list(
    images_abs: List[Path],
    out_txt: Path,
    *,
    seconds_per_image: float = 2.0,
    make_relative_to: Optional[Path] = None,
) -> None:
    """
    Write ffmpeg concat demuxer list_video.txt

    - If make_relative_to is provided, file paths are written relative to that dir
      (recommended when list_video.txt is placed inside that dir).
    - Repeat the last file once to avoid duration being ignored.
    """
    if not images_abs:
        raise RuntimeError("沒有找到任何圖片可製作影片，請先執行 render_images_* 產圖。")

    base_abs: Optional[Path] = None
    if make_relative_to is not None:
        base_abs = make_relative_to.resolve()

    dur = max(0.001, float(seconds_per_image))

    lines: List[str] = []
    for p in images_abs:
        p_abs = p.resolve()
        if base_abs is not None:
            pp = p_abs.relative_to(base_abs)
        else:
            pp = p_abs

        lines.append(f"file '{pp.as_posix()}'")
        lines.append(f"duration {dur:.6f}")

    # 重複最後一張（ffmpeg concat demuxer 規則）
    last_abs = images_abs[-1].resolve()
    if base_abs is not None:
        last_rel = last_abs.relative_to(base_abs)
    else:
        last_rel = last_abs

    lines.append(f"file '{last_rel.as_posix()}'")

    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_txt.write_text("\n".join(lines), encoding="utf-8")


def build_video_from_images(
    *,
    images_dir: Path,
    out_mp4: Path,
    seconds_per_image: float = 2.0,
    fps: int = 30,
    crf: int = 18,
    force_scale: Optional[str] = None,  # e.g. "1080:1920"
    fade: bool = False,
    ext: str = "png",
    prefer_top: int = 15,
    use_existing_list: bool = True,
    # ✅ 手動強制總長度（優先級最高）
    target_seconds: Optional[float] = None,
    # ✅ 自動上限：只有「原本會超過上限」才壓縮
    auto_cap: bool = True,
    max_seconds: float = 178.0,
) -> None:
    """
    用 ffmpeg concat 把圖片串成 mp4。

    ✅ 優先使用 images_dir/list.txt（順序來源）
      - list.txt 一行一張圖（相對於 images_dir）
      - 我們會再生成一份含 duration 的 images_dir/list_video.txt 給 ffmpeg

    若找不到 list.txt（或 use_existing_list=False），才 fallback 掃描資料夾排序。

    ✅ 行為：
      - 若 target_seconds 提供：永遠固定總長度（覆蓋 seconds_per_image）
      - 否則若 auto_cap=True：只有當 (N * seconds_per_image) > max_seconds 才壓縮到 max_seconds
      - 否則：照 seconds_per_image 原樣輸出
    """
    _ensure_ffmpeg()
    out_mp4.parent.mkdir(parents=True, exist_ok=True)

    images_dir = images_dir.resolve()
    list_txt = images_dir / "list.txt"

    imgs_abs: List[Path] = []

    if not (use_existing_list and list_txt.exists()):
        imgs_abs = _collect_images_current_layout(images_dir, prefer_top=prefer_top, ext=ext)
    else:
        src_lines = list_txt.read_text(encoding="utf-8").splitlines()
        rel_paths = [ln.strip() for ln in src_lines if ln.strip() and not ln.strip().startswith("#")]
        for rp in rel_paths:
            p = (images_dir / rp).resolve()
            if p.exists():
                imgs_abs.append(p)

    if not imgs_abs:
        raise RuntimeError("沒有找到任何圖片可製作影片。")

    n = len(imgs_abs)
    base_seconds = float(seconds_per_image)
    est_total = n * base_seconds

    # ✅ 優先：手動固定總長度
    if target_seconds is not None:
        ts = float(target_seconds)
        if ts <= 0:
            raise RuntimeError("--target-seconds 必須大於 0")
        seconds_per_image = ts / max(1, n)
        print(
            f"[render_video] 強制總長度模式：target_seconds={ts:.3f}s, 圖片數={n}, "
            f"每張停留={seconds_per_image:.6f}s"
        )
    else:
        # ✅ 自動上限：只有超過才壓縮
        if auto_cap:
            ms = float(max_seconds)
            if ms <= 0:
                raise RuntimeError("--max-seconds 必須大於 0")

            if est_total > ms:
                seconds_per_image = ms / max(1, n)
                print(
                    f"[render_video] 自動上限觸發：max_seconds={ms:.3f}s, 圖片數={n}, "
                    f"原本預估={est_total:.3f}s（{base_seconds:.3f}s/張） -> "
                    f"壓縮後每張={seconds_per_image:.6f}s"
                )
            else:
                print(
                    f"[render_video] 未觸發上限：圖片數={n}, "
                    f"原本預估={est_total:.3f}s（{base_seconds:.3f}s/張） <= max_seconds={ms:.3f}s，保持原設定"
                )
        else:
            print(
                f"[render_video] auto_cap=OFF：圖片數={n}, "
                f"預估總長={est_total:.3f}s（{base_seconds:.3f}s/張），照原設定輸出"
            )

    using_txt = images_dir / "list_video.txt"
    _write_concat_list(
        images_abs=imgs_abs,
        out_txt=using_txt,
        seconds_per_image=seconds_per_image,
        make_relative_to=images_dir,
    )

    vf: List[str] = []

    if force_scale:
        w, h = force_scale.split(":")
        vf.append(f"scale={w}:{h}:force_original_aspect_ratio=decrease")
        vf.append(f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2")

    vf.append("format=yuv420p")

    if fade:
        total = max(1.0, n * float(seconds_per_image))
        vf.append("fade=t=in:st=0:d=0.6")
        vf.append(f"fade=t=out:st={max(0.0, total-0.6):.3f}:d=0.6")

    cmd = [
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(using_txt),
        "-fps_mode", "cfr",
        "-r", str(fps),
        "-c:v", "libx264",
        "-crf", str(crf),
        "-pix_fmt", "yuv420p",
        "-vf", ",".join(vf),
        str(out_mp4),
    ]

    _run(cmd)


def _resolve_images_dir(*, market: str, ymd: str, slot: str) -> Path:
    """
    New unified layout:
      media/images/<market>/<ymd>/<slot>

    Backward compatible fallback:
      media/images/<ymd>/<slot>
    """
    m = (market or "").strip().lower()
    y = (ymd or "").strip()
    s = (slot or "").strip()

    p_new = Path("media") / "images" / m / y / s
    if p_new.exists():
        return p_new

    p_old = Path("media") / "images" / y / s
    if p_old.exists():
        print(f"[render_video] ⚠️ 使用舊版資料夾結構：{p_old}")
        return p_old

    return p_new


def main():
    ap = argparse.ArgumentParser(description="將產生好的圖片串接成 Shorts 影片（ffmpeg concat）。")

    ap.add_argument("--market", default="tw", help="市場資料夾名稱，例如 jp/kr/us（預設 tw）")
    ap.add_argument("--ymd", required=True, help="交易日，例如 2026-02-20")
    ap.add_argument("--slot", default="midday", help="時段，例如 midday/close/open")

    ap.add_argument("--seconds", type=float, default=2.0, help="每張圖片停留秒數（預設 2 秒）")

    # ✅ 手動強制總長度（你需要時才用）
    ap.add_argument(
        "--target-seconds",
        type=float,
        default=None,
        help="強制固定影片總長度（秒），例如 178=2:58（優先級最高，會覆蓋 --seconds）",
    )

    # ✅ 自動上限（預設開啟）：只有超過才壓縮
    ap.add_argument(
        "--auto-cap",
        dest="auto_cap",
        action="store_true",
        help="啟用『超過上限才壓縮』模式（預設已開啟）",
    )
    ap.add_argument(
        "--no-auto-cap",
        dest="auto_cap",
        action="store_false",
        help="關閉 auto-cap（強制照 --seconds，不做總長度保護）",
    )
    ap.add_argument(
        "--max-seconds",
        type=float,
        default=178.0,
        help="auto-cap 的上限秒數（預設 178，約 2:58）",
    )

    ap.add_argument("--fps", type=int, default=30, help="輸出 FPS（預設 30）")
    ap.add_argument("--crf", type=int, default=18, help="x264 CRF（預設 18，越低畫質越好）")
    ap.add_argument("--scale", default="1080:1920", help="強制縮放尺寸，例如 1080:1920（留空可關閉）")
    ap.add_argument("--fade", action="store_true", help="加入整段淡入淡出效果")
    ap.add_argument("--out", default="", help="輸出 mp4 路徑（選填）")
    ap.add_argument("--ext", default="png", choices=["png", "jpg", "jpeg"], help="圖片副檔名（預設 png）")
    ap.add_argument("--prefer-top", type=int, default=15, help="overview 預設使用 top N（預設 15）")

    ap.add_argument("--use-existing-list", action="store_true")
    ap.add_argument("--no-use-existing-list", dest="use_existing_list", action="store_false")
    # ✅ 關鍵：auto_cap 預設 True（不用加參數也會「超過才壓縮」）
    ap.set_defaults(use_existing_list=True, auto_cap=True)

    args = ap.parse_args()

    images_dir = _resolve_images_dir(market=args.market, ymd=args.ymd, slot=args.slot)
    if not images_dir.exists():
        raise RuntimeError(f"找不到圖片資料夾：{images_dir}，請先執行 scripts/render_images_{args.market}/cli.py")

    if args.out:
        out_mp4 = Path(args.out)
    else:
        out_mp4 = Path("media") / "videos" / args.market.lower() / f"{args.ymd}_{args.slot}.mp4"

    build_video_from_images(
        images_dir=images_dir,
        out_mp4=out_mp4,
        seconds_per_image=args.seconds,
        target_seconds=args.target_seconds,
        auto_cap=args.auto_cap,
        max_seconds=args.max_seconds,
        fps=args.fps,
        crf=args.crf,
        force_scale=(args.scale.strip() if args.scale.strip() else None),
        fade=args.fade,
        ext=args.ext,
        prefer_top=args.prefer_top,
        use_existing_list=args.use_existing_list,
    )

    print(f"✅ 影片已輸出：{out_mp4}")


if __name__ == "__main__":
    main()