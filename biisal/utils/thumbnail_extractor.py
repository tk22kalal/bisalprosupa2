import os
import shutil
import asyncio
import secrets
import logging
from pathlib import Path


def _find_binary(name: str) -> str | None:
    return shutil.which(name)


def check_ffmpeg() -> tuple[bool, str]:
    ffmpeg_path = _find_binary("ffmpeg")
    ffprobe_path = _find_binary("ffprobe")

    if not ffmpeg_path and not ffprobe_path:
        return False, "ffmpeg and ffprobe are NOT installed or not in PATH."
    if not ffmpeg_path:
        return False, "ffmpeg is NOT installed or not in PATH."
    if not ffprobe_path:
        return False, "ffprobe is NOT installed or not in PATH."
    return True, f"ffmpeg: {ffmpeg_path}  |  ffprobe: {ffprobe_path}"


async def extract_video_thumbnail(video_path: str, output_path: str = None, seek_time: str = "00:00:10") -> str:
    ffmpeg_path = _find_binary("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError(
            "ffmpeg is not installed or not in PATH on this server."
        )

    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video file not found: {video_path}")

    if output_path is None:
        thumbnail_dir = Path("/tmp/thumbnails")
        thumbnail_dir.mkdir(exist_ok=True)
        random_name = secrets.token_hex(8)
        output_path = str(thumbnail_dir / f"thumb_{random_name}.jpg")

    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        ffmpeg_path,
        "-ss", seek_time,
        "-i", video_path,
        "-vf", "scale=1280:-1",
        "-frames:v", "1",
        "-q:v", "2",
        "-y",
        output_path
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        error_msg = stderr.decode('utf-8', errors='ignore')
        logging.error(f"ffmpeg error: {error_msg}")
        raise RuntimeError(f"ffmpeg failed (exit {process.returncode}): {error_msg[:500]}")

    if not os.path.exists(output_path):
        raise RuntimeError("Thumbnail extraction failed: ffmpeg ran but no output file was created")

    return output_path


async def get_video_duration(video_path: str) -> float:
    ffprobe_path = _find_binary("ffprobe")
    if not ffprobe_path:
        logging.warning("ffprobe not found, returning 0 for duration")
        return 0.0

    try:
        cmd = [
            ffprobe_path,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return float(stdout.decode('utf-8').strip())
        return 0.0

    except Exception as e:
        logging.error(f"Error getting video duration: {e}")
        return 0.0


async def extract_thumbnail_from_middle(video_path: str, output_path: str = None) -> str:
    try:
        return await extract_video_thumbnail(video_path, output_path, "00:00:10")
    except RuntimeError as e:
        err = str(e)
        if "not installed" in err or "not in PATH" in err:
            raise
        logging.error(f"Thumbnail at 10s failed, trying 2s: {e}")

    try:
        return await extract_video_thumbnail(video_path, output_path, "00:00:02")
    except Exception as e:
        logging.error(f"Thumbnail at 2s failed, trying 1s: {e}")

    return await extract_video_thumbnail(video_path, output_path, "00:00:01")

