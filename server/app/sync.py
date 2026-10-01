"""Scheduler: decides when a run happens and what it does. Every run goes through, in order:

  1. archive  — if an uploaded archive has unfetched posts. Nothing else happens until it's finished.
  2. catch-up — read the timeline from the top until 20 already-saved reposts in a row.
  3. backfill — only if incomplete: continue into older history from the saved position.

A run makes a random number of requests (its budget), so big jobs are spread over many runs.
"""

import asyncio
import logging
import random
import time
from contextlib import suppress
from datetime import datetime, timedelta

from .config import TWS_DB_PATH, X_COOKIES, X_USERNAME  # noqa: I001  (before twscrape: disables telemetry)

from twscrape import API

from . import archive, backfill, catchup, db
from .common import (
    WINDOW_MAX_REQUESTS, Run, SyncError, api_usage, index_summary, next_run_at, page_pause,
    session_page_budget, timeline_client,
)
from .logs import fmt_duration

log = logging.getLogger("sync")

HEARTBEAT_SECONDS = 30 * 60

STOP_REASONS = {
    "budget": "request budget for this run used up",
    "caught_up": "caught up with your latest reposts",
    "end": "reached the end of the timeline",
    "done": "archive import finished",
}

__all__ = ["WINDOW_MAX_REQUESTS", "api_usage", "status_line", "syncer"]


def status_line() -> str:
    total, oldest = index_summary()
    with db.tx() as c:
        pending, failed = c.execute(
            """SELECT
                 (SELECT COUNT(*) FROM media WHERE status='pending') + (SELECT COUNT(*) FROM avatars WHERE status='pending'),
                 (SELECT COUNT(*) FROM media WHERE status='failed') + (SELECT COUNT(*) FROM avatars WHERE status='failed')"""
        ).fetchone()
    parts = [f"{total:,} reposts indexed"]
    if archive.pending():
        parts.append(f"archive {archive.progress_text()}")
    parts.append("backfill done" if backfill.is_done() else f"backfill in progress (oldest {oldest})")
    line = " · ".join(parts) + f" · {pending:,} downloads pending"
    if failed:
        line += f", {failed} failed"
    used, _ = api_usage()
    return line + f" · api {used}/{WINDOW_MAX_REQUESTS} in 15 min"


class Syncer:
    def __init__(self) -> None:
        self.api = API(TWS_DB_PATH)
        self.running = False
        self.progress = ""
        self._trigger = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._manual: asyncio.Task | None = None

    @property
    def configured(self) -> bool:
        return bool(X_USERNAME and X_COOKIES)

    async def start(self) -> None:
        if not self.configured:
            db.set_state(last_error="X_USERNAME / X_COOKIES not set; sync disabled", next_run=None)
            log.warning("X_USERNAME / X_COOKIES not set; sync disabled")
            return
        await self.api.pool.add_account_cookies(X_USERNAME, X_COOKIES)
        # Only this process uses the session, so any lock left by a crash/hard stop is stale. Without this,
        # the first run after such a restart waits silently for up to 15 min for the lock to expire.
        await self.api.pool.reset_locks()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        for task in (self._task, self._manual):
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        self._task = None

    def trigger(self) -> None:
        self._trigger.set()

    def _set_progress(self, text: str) -> None:
        self.progress = text

    async def _loop(self) -> None:
        delay = random.uniform(20, 90)
        while True:
            due = time.time() + delay
            next_run = datetime.now().astimezone() + timedelta(seconds=delay)
            db.set_state(next_run=next_run.isoformat(timespec="seconds"))
            log.info("next run at %s (in %s)", next_run.strftime("%H:%M"), fmt_duration(delay))
            while (remaining := due - time.time()) > 0:
                try:
                    await asyncio.wait_for(self._trigger.wait(), timeout=min(remaining, HEARTBEAT_SECONDS))
                    log.info("sync requested from the web UI")
                    break
                except TimeoutError:
                    if due - time.time() > 1:
                        log.info("idle · %s · next run in %s", status_line(), fmt_duration(due - time.time()))
            self._trigger.clear()
            await self.run_once()
            busy = archive.pending() or not backfill.is_done()
            delay = (next_run_at(busy) - datetime.now().astimezone()).total_seconds()

    async def catch_up_now(self) -> dict:
        """Web UI: a single catch-up pass right away (no archive, no backfill)."""
        if self.running:
            return {"ok": False, "message": "A sync is already running", "new": 0}
        log.info("catch-up requested from the web UI")
        self._manual = asyncio.create_task(self.run_once(self._catchup_only))
        try:
            return await self._manual
        except asyncio.CancelledError:
            return {"ok": False, "message": "Sync was interrupted", "new": 0}
        finally:
            self._manual = None

    async def run_once(self, phases=None) -> dict:
        """One run; `phases` defaults to archive → catch-up → backfill. Returns {ok, message, new}."""
        if self.running:
            return {"ok": False, "message": "A sync is already running", "new": 0}
        self.running = True
        started = time.time()
        r = Run(self.api, session_page_budget(), self._set_progress)
        db.set_state(last_start=db.now(), last_error=None)
        try:
            log.info("run started · up to %d requests this run · %s", r.budget, status_line())
            why = await (phases or self._phases)(r)
            db.set_state(last_success=db.now())
            log.info(
                "run finished in %s · %d request%s · +%d new reposts · %s",
                fmt_duration(time.time() - started), r.requests, "" if r.requests == 1 else "s",
                r.new, STOP_REASONS[why],
            )
            return {"ok": True, "message": f"+{r.new} new repost{'' if r.new == 1 else 's'} · {STOP_REASONS[why]}", "new": r.new}
        except asyncio.CancelledError:
            log.info("run interrupted after %d requests · progress is saved, it will resume next run", r.requests)
            raise
        except SyncError as e:
            log.error("run stopped after %d requests: %s", r.requests, e)
            db.set_state(last_error=str(e))
            return {"ok": False, "message": str(e), "new": r.new}
        except Exception as e:
            log.exception("run failed after %d requests", r.requests)
            db.set_state(last_error=f"{type(e).__name__}: {e}")
            return {"ok": False, "message": f"{type(e).__name__}: {e}", "new": r.new}
        finally:
            self.running = False
            self.progress = ""
            db.set_state(last_finish=db.now())

    async def _phases(self, r: Run) -> str:
        """Every run: unfinished archive first (nothing else until it's done) → catch-up → backfill if incomplete."""
        if archive.pending():
            log.info("archive import has priority · %s · catch-up and backfill wait until it's finished",
                     archive.progress_text())
            why = await archive.run(r)
            if why != "done" or r.budget <= 0:
                return why
            pause = page_pause()
            log.info("archive finished with budget left · catch-up in %s", fmt_duration(pause))
            await asyncio.sleep(pause)
        return await self._timeline(r)

    async def _sign_in(self, r: Run) -> str:
        """Looks up the account (one request) and returns its user id."""
        r.progress("looking up account")
        await r.throttle()
        me = await self.api.user_by_login(X_USERNAME)
        if me is None:
            raise SyncError(await r.account_problem() or f"could not look up @{X_USERNAME}")
        r.requests += 1
        my_id = str(me.id)
        db.set_state(user_id=my_id, statuses_count=me.statusesCount)
        pause = page_pause()
        log.info("signed in as @%s · X reports %s posts on the account · first page in %s",
                 X_USERNAME, f"{me.statusesCount:,}", fmt_duration(pause))
        r.progress(f"signed in · first page in {fmt_duration(pause)}")
        await asyncio.sleep(pause)
        return my_id

    async def _catchup_only(self, r: Run) -> str:
        my_id = await self._sign_in(r)
        async with timeline_client(self.api) as client:
            return await catchup.run(r, client, my_id)

    async def _timeline(self, r: Run) -> str:
        my_id = await self._sign_in(r)
        async with timeline_client(self.api) as client:
            if not backfill.is_done() and not backfill.started():
                # Fresh backfill starts at the top anyway, so it covers catch-up too.
                return await backfill.run(r, client, my_id)
            why = await catchup.run(r, client, my_id)
            if why == "caught_up" and not backfill.is_done() and r.budget > 0:
                pause = page_pause()
                log.info("caught up with new reposts · resuming history backfill in %s", fmt_duration(pause))
                await asyncio.sleep(pause)
                why = await backfill.run(r, client, my_id)
            return why


syncer = Syncer()
