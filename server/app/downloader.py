"""Background worker that fetches pending media/avatars from X's public CDN, with backoff."""

import logging
import os
import random
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import httpx

from . import db, thumbs
from .config import AVATAR_DIR, MEDIA_DIR
from .logs import fmt_bytes, fmt_duration

log = logging.getLogger("downloads")

DIRS = {"avatars": AVATAR_DIR, "media": MEDIA_DIR}
MAX_ATTEMPTS = 8
UA = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"

_wake = threading.Event()
_busy = threading.Lock()


def wake() -> None:
    _wake.set()


@contextmanager
def paused():
    """Blocks the worker between jobs, so nothing is half-written while held."""
    with _busy:
        yield


def start() -> None:
    threading.Thread(target=_run, name="downloader", daemon=True).start()


def _next_job():
    with db.tx() as c:
        for table in DIRS:
            row = c.execute(
                f"SELECT file, url, attempts FROM {table} WHERE status='pending' AND next_try <= ? ORDER BY rowid LIMIT 1",
                (db.now(),),
            ).fetchone()
            if row:
                return table, row
    return None


def _fetch(client: httpx.Client, url: str, dest: str) -> int:
    tmp = dest + ".part"
    size = 0
    with client.stream("GET", url) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes(1 << 16):
                f.write(chunk)
                size += len(chunk)
    os.replace(tmp, dest)
    return size


def _succeeded(table: str, file: str) -> None:
    with db.tx() as c:
        c.execute(f"UPDATE {table} SET status='done', error=NULL WHERE file=?", (file,))
        if table == "avatars":
            # Only promote to current avatar if it's the most recently seen one for that author.
            c.execute(
                """UPDATE authors SET avatar_file=:f WHERE id=(SELECT author_id FROM avatars WHERE file=:f)
                   AND :f=(SELECT file FROM avatars a WHERE a.author_id=authors.id ORDER BY last_seen DESC LIMIT 1)""",
                {"f": file},
            )


def _failed(table: str, file: str, attempts: int, error: str, permanent: bool) -> None:
    attempts += 1
    give_up = permanent or attempts >= MAX_ATTEMPTS
    next_try = (datetime.now(timezone.utc) + timedelta(minutes=2 ** attempts)).isoformat(timespec="seconds")
    with db.tx() as c:
        c.execute(
            f"UPDATE {table} SET status=?, attempts=?, next_try=?, error=? WHERE file=?",
            ("failed" if give_up else "pending", attempts, next_try, error[:500], file),
        )
    if give_up:
        log.warning("gave up on %s: %s", file, error)
    else:
        log.warning("%s failed (%s) · retry %d/%d in %s", file, error, attempts, MAX_ATTEMPTS - 1, fmt_duration(60 * 2 ** attempts))


def _queue_counts() -> tuple[int, int]:
    with db.tx() as c:
        return c.execute(
            """SELECT
                 (SELECT COUNT(*) FROM media WHERE status='pending') + (SELECT COUNT(*) FROM avatars WHERE status='pending'),
                 (SELECT COUNT(*) FROM media WHERE status='failed') + (SELECT COUNT(*) FROM avatars WHERE status='failed')"""
        ).fetchone()


class _Progress:
    """Aggregates downloads into one summary line every REPORT_SECONDS instead of a line per file."""

    def __init__(self) -> None:
        self.burst_files = 0
        self.burst_start: float | None = None
        self._reset()

    def _reset(self) -> None:
        self.files = self.bytes = 0
        self.since = time.time()

    def add(self, size: int) -> None:
        if self.burst_start is None:
            self.burst_start = self.since
        self.files += 1
        self.burst_files += 1
        self.bytes += size
        if time.time() - self.since >= REPORT_SECONDS:
            self.report(idle=False)

    def report(self, idle: bool) -> None:
        if not self.files:
            return
        elapsed = time.time() - self.since
        pending, failed = _queue_counts()
        parts = [f"+{self.files} files ({fmt_bytes(self.bytes)}) in {fmt_duration(elapsed)}"]
        if idle:
            parts.append("queue empty" if not pending else f"{pending:,} waiting to retry")
            self.burst_files, self.burst_start = 0, None
        else:
            # Average over the whole burst so the estimate doesn't jump around.
            rate = self.burst_files / max(time.time() - (self.burst_start or self.since), 1)
            parts.append(f"{pending:,} left · ~{fmt_duration(pending / rate)} remaining")
        if failed:
            parts.append(f"{failed} failed")
        log.info(" · ".join(parts))
        self._reset()


REPORT_SECONDS = 60


def _run() -> None:
    timeout = httpx.Timeout(30, read=300)
    progress = _Progress()
    with httpx.Client(timeout=timeout, follow_redirects=True, headers={"User-Agent": UA}) as client:
        while True:
            _wake.clear()
            with _busy:
                try:
                    job = _next_job()
                except Exception:
                    log.exception("queue query failed")
                    job = None
                if job:
                    size = _process(client, *job)
                    if size is not None:
                        progress.add(size)
            if not job:
                progress.report(idle=True)
                _wake.wait(60)
                continue
            time.sleep(random.uniform(0.5, 3))


def _process(client: httpx.Client, table: str, row) -> int | None:
    try:
        size = _fetch(client, row["url"], os.path.join(DIRS[table], row["file"]))
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        _failed(table, row["file"], row["attempts"], f"HTTP {code}", permanent=code in (403, 404, 410))
    except Exception as e:
        _failed(table, row["file"], row["attempts"], repr(e), permanent=False)
    else:
        _succeeded(table, row["file"])
        thumbs.enqueue(table, row["file"])
        return size
    return None
