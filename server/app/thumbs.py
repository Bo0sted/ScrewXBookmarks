"""WebP thumbnails for photos, video frames and avatars, stored in media/cache/ and reused forever."""

import logging
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from collections import defaultdict
from contextlib import contextmanager

from PIL import Image, ImageOps

from . import db
from .config import AVATAR_DIR, MEDIA_DIR
from .logs import fmt_duration

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


# ---------- background worker ----------
# Thumbnails are made here, separately from downloads, so a slow video preview never holds up the
# download queue. The "queue" is just files missing thumbnails: new downloads are pushed in directly,
# and a periodic scan picks up anything older (e.g. after a restart).

SIZES_FOR = {"media": ("s", "m"), "avatars": ("s",)}
SCAN_EVERY = 600
REPORT_SECONDS = 60

_queue: queue.Queue = queue.Queue()
_busy = threading.Lock()
_gave_up: set[tuple[str, str]] = set()  # couldn't be made (e.g. corrupt video); not retried until restart


def enqueue(table: str, file: str) -> None:
    _queue.put((table, file))


@contextmanager
def paused():
    """Blocks the worker between thumbnails, so nothing is half-written while held."""
    with _busy:
        yield


def start() -> None:
    threading.Thread(target=_run, name="thumbnails", daemon=True).start()


def _name(table: str, file: str) -> str:
    return f"avatars/{file}" if table == "avatars" else file


def _missing(table: str, file: str) -> bool:
    return any(not os.path.exists(cache_path(_name(table, file), s)) for s in SIZES_FOR[table])


def _scan() -> int:
    with db.tx() as c:
        rows = [(t, r["file"]) for t in SIZES_FOR for r in c.execute(f"SELECT file FROM {t} WHERE status='done' ORDER BY rowid DESC")]
    todo = [(t, f) for t, f in rows if (t, f) not in _gave_up and _missing(t, f)]
    for item in todo:
        _queue.put(item)
    return len(todo)


def _run() -> None:
    found = _scan()
    if found:
        log.info("%s files need thumbnails", f"{found:,}")
    made, since, last_scan = 0, time.time(), time.time()
    while True:
        try:
            table, file = _queue.get(timeout=30)
        except queue.Empty:
            if made:
                log.info("+%d made in %s · all caught up", made, fmt_duration(time.time() - since))
                made, since = 0, time.time()
            if time.time() - last_scan >= SCAN_EVERY:
                _scan()
                last_scan = time.time()
            continue
        if not made:
            since = time.time()
        try:
            with _busy:
                if _missing(table, file) and os.path.isfile(os.path.join(AVATAR_DIR if table == "avatars" else MEDIA_DIR, file)):
                    ensure_all(_name(table, file), SIZES_FOR[table])
                    if _missing(table, file):
                        _gave_up.add((table, file))
                    else:
                        made += 1
        except Exception:
            log.exception("thumbnail worker failed on %s", file)
        if made and time.time() - since >= REPORT_SECONDS:
            log.info("+%d made in %s · %s waiting", made, fmt_duration(time.time() - since), f"{_queue.qsize():,}")
            made, since = 0, time.time()
