"""WebP thumbnails for photos, video frames and avatars, stored in media/cache/ and reused forever."""

import logging
import os
import re
import subprocess
import tempfile
import threading
from collections import defaultdict

from PIL import Image, ImageOps

from .config import MEDIA_DIR

log = logging.getLogger("thumbs")

CACHE_DIR = os.path.join(MEDIA_DIR, "cache")
SIZES = {"s": 400, "m": 960}  # s: grids and avatars, m: feed (the viewer always shows the original)
QUALITY = 78
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".m4v"}
ANIM_FRAMES = 12  # video/gif grid thumbnails: frames sampled evenly from start to end, looped
ANIM_FRAME_MS = 120
ANIM_WIDTH = 320
ANIM_QUALITY = 55

_SAFE_NAME = re.compile(r"^(avatars/)?[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_locks: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()
_ffmpeg_missing_logged = False

os.makedirs(CACHE_DIR, exist_ok=True)


def cache_path(file: str, size: str) -> str:
    return os.path.join(CACHE_DIR, f"{file.replace('/', '__')}.{size}.webp")


def ensure(file: str, size: str) -> str | None:
    """Path of the cached thumbnail, generating it on first use. None if it can't be made."""
    if size not in SIZES or not _SAFE_NAME.match(file):
        return None
    out = cache_path(file, size)
    if os.path.exists(out):
        return out
    src = os.path.join(MEDIA_DIR, file)
    if not os.path.isfile(src):
        return None
    with _locks_guard:
        lock = _locks[out]
    with lock:
        if os.path.exists(out):
            return out
        try:
            width = SIZES[size]
            if os.path.splitext(src)[1].lower() in VIDEO_EXTS:
                # Grid size: animated preview of the whole clip. Feed size: one still frame (poster).
                animated = size == "s"
                if animated:
                    width = ANIM_WIDTH
                frames = _video_frames(src, width, ANIM_FRAMES if animated else 1)
                if not frames:
                    return None
                _save_frames(frames, width, out)
            else:
                _save_frames([_load_image(src, width)], width, out)
        except Exception as e:
            log.warning("could not make %s thumbnail for %s: %s", size, file, e)
            return None
    return out


def ensure_all(file: str, sizes: tuple[str, ...]) -> None:
    for size in sizes:
        ensure(file, size)


def _load_image(src: str, width: int) -> Image.Image:
    im = Image.open(src)
    im.draft("RGB", (width, 1))  # JPEG: decode straight at a reduced scale (fast)
    im = ImageOps.exif_transpose(im)
    im.load()
    return im


def _ffmpeg(args: list[str]) -> subprocess.CompletedProcess | None:
    global _ffmpeg_missing_logged
    try:
        return subprocess.run(args, capture_output=True, timeout=120)
    except FileNotFoundError:
        if not _ffmpeg_missing_logged:
            log.warning("ffmpeg not installed; video thumbnails disabled")
            _ffmpeg_missing_logged = True
        return None


def _duration(src: str) -> float | None:
    r = _ffmpeg(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", src])
    try:
        return float(r.stdout.strip()) if r and r.returncode == 0 else None
    except ValueError:
        return None


def _video_frames(src: str, width: int, count: int) -> list[Image.Image]:
    """`count` frames spread evenly from the start to the end of the clip (1 = a single still)."""
    scale = f"scale='min({width},iw)':-2:flags=lanczos"
    with tempfile.TemporaryDirectory() as tmp:
        pattern = os.path.join(tmp, "f%03d.png")
        if count == 1:
            dur = _duration(src) or 0
            seek = "0.5" if dur > 1.5 else "0"  # skip black lead-in frames when the clip is long enough
            args = ["ffmpeg", "-v", "error", "-ss", seek, "-i", src, "-frames:v", "1", "-vf", scale, pattern]
        else:
            dur = _duration(src)
            fps = f"{count / dur:.6f}" if dur and dur > 0 else "2"
            args = ["ffmpeg", "-v", "error", "-i", src, "-vf", f"fps={fps},{scale}", "-frames:v", str(count), pattern]
        r = _ffmpeg(args)
        if not r or r.returncode != 0:
            return []
        frames = []
        for name in sorted(os.listdir(tmp)):
            im = Image.open(os.path.join(tmp, name))
            im.load()
            frames.append(im)
        return frames


def _save_frames(frames: list[Image.Image], width: int, out: str) -> None:
    prepared = []
    for im in frames:
        has_alpha = im.mode in ("RGBA", "LA") or (im.mode in ("P", "PA") and "transparency" in im.info)
        im = im.convert("RGBA" if has_alpha else "RGB")
        if im.width > width:
            im = im.resize((width, max(1, round(im.height * width / im.width))), Image.LANCZOS)
        prepared.append(im)
    tmp = out + ".tmp"
    if len(prepared) == 1:
        # method=6: WebP's most efficient mode — smallest stills, still fast for one frame.
        prepared[0].save(tmp, "WEBP", quality=QUALITY, method=6)
    else:
        # method=4 for animations: nearly the same size as 6 but ~40x faster to encode.
        prepared[0].save(
            tmp, "WEBP", save_all=True, append_images=prepared[1:],
            duration=ANIM_FRAME_MS, loop=0, quality=ANIM_QUALITY, method=4,
        )
    os.replace(tmp, out)
