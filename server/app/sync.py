"""Periodically walks my profile timeline (UserTweets, which includes reposts) via X's internal
GraphQL API using my own session cookies, with human-like irregular pacing.

Each run:
  1. catch-up: start at the top, stop once KNOWN_STREAK_STOP already-indexed reposts in a row are seen.
  2. backfill: if history isn't fully indexed yet, resume deeper from the saved cursor.
A run only fetches a random number of pages, so large backfills are spread over many runs.
"""

import asyncio
import logging
import math
import json
import random
import time
from contextlib import suppress
from datetime import datetime, timedelta

from .config import QUIET_HOURS, TWS_DB_PATH, X_COOKIES, X_USERNAME  # noqa: I001  (before twscrape: disables telemetry)

from twscrape import API
from twscrape.api import GQL_FEATURES, GQL_URL, OP_UserTweets
from twscrape.queue_client import QueueClient
from twscrape.utils import encode_params

from . import db, downloader, store
from .logs import fmt_duration
from .parse import ParseError, bottom_cursor, count_timeline_items, find_reposts, parse_tweet

log = logging.getLogger("sync")

KNOWN_STREAK_STOP = 20
EMPTY_PAGES_END = 3
BACKFILL_END_CONFIRMATIONS = 2

# Hard cap on authenticated X requests, independent of the random pacing. X's observed limit for
# timeline requests is ~50 per 15 min; staying far below it avoids looking like a bot.
WINDOW_SECONDS = 15 * 60
WINDOW_MAX_REQUESTS = 30

HEARTBEAT_SECONDS = 30 * 60

STOP_REASONS = {
    "budget": "page budget for this run used up",
    "caught_up": "caught up with your latest reposts",
    "end": "reached the end of the timeline",
}


def api_usage() -> tuple[int, float | None]:
    """(requests in the current 15-min window, unix time the cap lifts if currently capped)."""
    now = time.time()
    recent = [t for t in json.loads(db.get_state("api_requests", "[]")) if now - t < WINDOW_SECONDS]
    capped_until = recent[0] + WINDOW_SECONDS if len(recent) >= WINDOW_MAX_REQUESTS else None
    return len(recent), capped_until


def _index_summary() -> tuple[int, str]:
    with db.tx() as c:
        total, oldest = c.execute("SELECT COUNT(*), MIN(reposted_at) FROM posts").fetchone()
    return total, (oldest[:10] if oldest else "—")


def status_line() -> str:
    total, oldest = _index_summary()
    with db.tx() as c:
        pending, failed = c.execute(
            """SELECT
                 (SELECT COUNT(*) FROM media WHERE status='pending') + (SELECT COUNT(*) FROM avatars WHERE status='pending'),
                 (SELECT COUNT(*) FROM media WHERE status='failed') + (SELECT COUNT(*) FROM avatars WHERE status='failed')"""
        ).fetchone()
    backfill = "backfill done" if db.get_state("backfill_done") == "1" else f"backfill in progress (oldest {oldest})"
    used, _ = api_usage()
    line = f"{total:,} reposts indexed · {backfill} · {pending:,} downloads pending"
    if failed:
        line += f", {failed} failed"
    return line + f" · api {used}/{WINDOW_MAX_REQUESTS} in 15 min"


class SyncError(RuntimeError):
    pass


def page_pause() -> float:
    """Seconds to 'read' a page before scrolling on: median ~11s, usually 4-30s, sometimes minutes."""
    d = random.lognormvariate(math.log(11), 0.5)
    if random.random() < 0.06:
        d += random.uniform(45, 240)
    return min(d, 420)


def session_page_budget() -> int:
    return random.randint(20, 60)


def _in_quiet_hours(t: datetime) -> bool:
    try:
        a, b = (int(x) % 24 for x in QUIET_HOURS.split("-"))
    except ValueError:
        return False
    return a <= t.hour < b if a < b else (t.hour >= a or t.hour < b)


def next_run_at(backfilling: bool) -> datetime:
    hours = random.uniform(1.0, 2.5) if backfilling else random.uniform(2.0, 5.0)
    t = datetime.now().astimezone() + timedelta(hours=hours)
    while _in_quiet_hours(t):
        t += timedelta(minutes=random.uniform(20, 50))
    return t


class Syncer:
    def __init__(self) -> None:
        self.api = API(TWS_DB_PATH)
        self.running = False
        self.progress = ""
        self._run_pages = self._run_new = 0
        self._trigger = asyncio.Event()
        self._task: asyncio.Task | None = None

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
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def trigger(self) -> None:
        self._trigger.set()

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
            backfilling = db.get_state("backfill_done") != "1"
            delay = (next_run_at(backfilling) - datetime.now().astimezone()).total_seconds()

    async def run_once(self) -> None:
        if self.running:
            return
        self.running = True
        self._run_pages = self._run_new = 0
        started = time.time()
        db.set_state(last_start=db.now(), last_error=None)
        try:
            budget = session_page_budget()
            log.info("run started · up to %d pages this run · %s", budget, status_line())
            self.progress = "looking up account"
            await self._throttle()
            me = await self.api.user_by_login(X_USERNAME)
            if me is None:
                raise SyncError(await self._account_problem() or f"could not look up @{X_USERNAME}")
            my_id = str(me.id)
            db.set_state(user_id=my_id, statuses_count=me.statusesCount)
            pause = page_pause()
            log.info(
                "signed in as @%s · X reports %s posts on the account · first page in %s",
                X_USERNAME, f"{me.statusesCount:,}", fmt_duration(pause),
            )
            self.progress = f"signed in · first page in {fmt_duration(pause)}"
            await asyncio.sleep(pause)

            done = db.get_state("backfill_done") == "1"
            cursor = db.get_state("backfill_cursor")
            deep = not done and cursor is None  # first run (or restarted backfill): walk from the top, all the way
            async with QueueClient(self.api.pool, "UserTweets") as client:
                budget, why = await self._walk(client, my_id, None, deep, budget)
                if not done and not deep and why == "caught_up" and budget > 0:
                    pause = page_pause()
                    log.info("caught up with new reposts · resuming history backfill in %s", fmt_duration(pause))
                    await asyncio.sleep(pause)
                    budget, why = await self._walk(client, my_id, cursor, True, budget)
            db.set_state(last_success=db.now())
            log.info(
                "run finished in %s · %d page%s · +%d new reposts · %s",
                fmt_duration(time.time() - started), self._run_pages, "" if self._run_pages == 1 else "s",
                self._run_new, STOP_REASONS[why],
            )
        except asyncio.CancelledError:
            log.info("run interrupted after %d pages · progress is saved, it will resume next run", self._run_pages)
            raise
        except SyncError as e:
            log.error("run stopped after %d pages: %s", self._run_pages, e)
            db.set_state(last_error=str(e))
        except Exception as e:
            log.exception("run failed after %d pages", self._run_pages)
            db.set_state(last_error=f"{type(e).__name__}: {e}")
        finally:
            self.running = False
            self.progress = ""
            db.set_state(last_finish=db.now())

    async def _walk(self, client: QueueClient, my_id: str, cursor: str | None, deep: bool, budget: int):
        mode = "backfill" if deep else "catch-up"
        streak = empty = pages = 0
        while budget > 0:
            kv = {
                "userId": my_id,
                "count": 20,
                "includePromotedContent": False,
                "withQuickPromoteEligibilityTweetFields": False,
                "withVoice": True,
                "withV2Timeline": True,
            }
            if cursor:
                kv["cursor"] = cursor
            await self._throttle()
            rep = await client.get(f"{GQL_URL}/{OP_UserTweets}", params=encode_params({"variables": kv, "features": GQL_FEATURES}))
            if rep is None:
                raise SyncError(await self._account_problem() or "X aborted the request (auth, rate limit or API change)")
            page = rep.json()
            budget -= 1
            pages += 1
            self._run_pages += 1

            new = known = 0
            for r in find_reposts(page, my_id):
                if store.is_known(r["repost_id"]):
                    known += 1
                    streak += 1
                    continue
                streak = 0
                new += 1
                self._ingest(r)
            self._run_new += new
            if new:
                downloader.wake()

            nxt = bottom_cursor(page)
            items = count_timeline_items(page)
            empty = empty + 1 if items == 0 else 0
            if deep and nxt:
                db.set_state(backfill_cursor=nxt)

            end_reason = None
            if not nxt:
                end_reason = f"X sent no 'next page' cursor ({items} entries on the page)"
            elif nxt == cursor:
                end_reason = "X returned the same cursor again"
            elif empty >= EMPTY_PAGES_END:
                end_reason = f"{empty} empty pages in a row"

            if end_reason:
                stop = "end"
            elif not deep and streak >= KNOWN_STREAK_STOP:
                stop = "caught_up"
            elif budget <= 0:
                stop = "budget"
            else:
                stop = None
            pause = 0.0 if stop else page_pause()

            total, oldest = _index_summary()
            used, _ = api_usage()
            self.progress = (
                f"{mode} · page {self._run_pages} · +{self._run_new} new this run · "
                f"api {used}/{WINDOW_MAX_REQUESTS} in 15 min"
            )
            log.info(
                "%s p%d · +%d new, %d known · %s indexed · oldest %s · api %d/%d · %s",
                mode, self._run_pages, new, known, f"{total:,}", oldest, used, WINDOW_MAX_REQUESTS,
                "stopping" if stop else f"next page in {fmt_duration(pause)}",
            )

            if deep and stop != "end":
                db.set_state(backfill_end_checks=0)  # timeline continued past here, so any earlier "end" was false
            if stop:
                if stop == "end":
                    log.info("timeline ended: %s", end_reason)
                if stop == "end" and deep:
                    # X sometimes returns a short/empty page mid-timeline. Only trust "the end" once it
                    # repeats on a later run from the same position.
                    checks = int(db.get_state("backfill_end_checks", "0")) + 1
                    if checks >= BACKFILL_END_CONFIRMATIONS:
                        db.set_state(backfill_done=1, backfill_cursor=None, backfill_end_checks=0,
                                     backfill_finished_at=db.now())
                        log.info("backfill complete · end of timeline confirmed (oldest repost %s)", oldest)
                    else:
                        db.set_state(backfill_end_checks=checks, backfill_cursor=nxt or cursor)
                        log.info("backfill paused · will re-check from this point next run before calling it complete")
                return budget, stop
            cursor = nxt
            if pause > 60:
                self.progress += f" · taking a {fmt_duration(pause)} break"
                log.info("taking a %s break (human-like pause)", fmt_duration(pause))
            await asyncio.sleep(pause)
        return budget, "budget"

    async def _throttle(self) -> None:
        """Blocks until another X request fits in the rolling window, then records it.
        Timestamps live in the DB so a restart can't reset the window."""
        while True:
            now = time.time()
            recent = [t for t in json.loads(db.get_state("api_requests", "[]")) if now - t < WINDOW_SECONDS]
            if len(recent) < WINDOW_MAX_REQUESTS:
                recent.append(now)
                db.set_state(api_requests=json.dumps(recent))
                return
            wait = recent[0] + WINDOW_SECONDS - now + random.uniform(5, 60)
            resume = datetime.now().astimezone() + timedelta(seconds=wait)
            self.progress = (
                f"cooling down {fmt_duration(wait)} until {resume.strftime('%H:%M')} "
                f"(rate cap: {WINDOW_MAX_REQUESTS} requests / 15 min)"
            )
            log.info(
                "rate cap · %d/%d requests in the last 15 min · cooling down %s (resumes %s)",
                len(recent), WINDOW_MAX_REQUESTS, fmt_duration(wait), resume.strftime("%H:%M:%S"),
            )
            await asyncio.sleep(wait)

    def _ingest(self, r: dict) -> None:
        try:
            p = parse_tweet(r["original"])
        except (ParseError, KeyError, TypeError, ValueError) as e:
            store.save_missing(r["repost_id"], r["original"].get("rest_id"), r["reposted_at"], str(e), r["original"])
            log.warning("repost %s not stored: %s", r["repost_id"], e)
            return
        store.save(p, r["repost_id"], r["reposted_at"], r["original"])

    async def _account_problem(self) -> str | None:
        try:
            for a in await self.api.pool.accounts_info():
                if not a["active"]:
                    return f"X session inactive ({a.get('error_msg') or 'cookies rejected'}); update X_COOKIES and restart"
        except Exception:
            log.exception("could not read account state")
        return None


syncer = Syncer()


def restart_backfill() -> None:
    db.set_state(backfill_done=0, backfill_cursor=None, backfill_end_checks=0)
