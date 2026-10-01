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
from .parse import ParseError, bottom_cursor, count_timeline_items, find_reposts, parse_tweet

log = logging.getLogger("sync")

KNOWN_STREAK_STOP = 20
EMPTY_PAGES_END = 2

# Hard cap on authenticated X requests, independent of the random pacing. X's observed limit for
# timeline requests is ~50 per 15 min; staying far below it avoids looking like a bot.
WINDOW_SECONDS = 15 * 60
WINDOW_MAX_REQUESTS = 30


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
            db.set_state(next_run=(datetime.now().astimezone() + timedelta(seconds=delay)).isoformat(timespec="seconds"))
            try:
                await asyncio.wait_for(self._trigger.wait(), timeout=delay)
            except TimeoutError:
                pass
            self._trigger.clear()
            await self.run_once()
            backfilling = db.get_state("backfill_done") != "1"
            delay = (next_run_at(backfilling) - datetime.now().astimezone()).total_seconds()

    async def run_once(self) -> None:
        if self.running:
            return
        self.running = True
        db.set_state(last_start=db.now(), last_error=None)
        try:
            budget = session_page_budget()
            self.progress = "looking up account"
            await self._throttle()
            me = await self.api.user_by_login(X_USERNAME)
            if me is None:
                raise SyncError(await self._account_problem() or f"could not look up @{X_USERNAME}")
            my_id = str(me.id)
            db.set_state(user_id=my_id, statuses_count=me.statusesCount)
            await asyncio.sleep(page_pause())

            done = db.get_state("backfill_done") == "1"
            cursor = db.get_state("backfill_cursor")
            deep = not done and cursor is None  # first run (or restarted backfill): walk from the top, all the way
            async with QueueClient(self.api.pool, "UserTweets") as client:
                budget, why = await self._walk(client, my_id, None, deep, budget)
                if not done and not deep and why == "caught_up" and budget > 0:
                    await asyncio.sleep(page_pause())
                    await self._walk(client, my_id, cursor, True, budget)
            db.set_state(last_success=db.now())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("sync failed")
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

            new = known = 0
            for r in find_reposts(page, my_id):
                if store.is_known(r["repost_id"]):
                    known += 1
                    streak += 1
                    continue
                streak = 0
                new += 1
                self._ingest(r)
            if new:
                downloader.wake()

            nxt = bottom_cursor(page)
            empty = empty + 1 if count_timeline_items(page) == 0 else 0
            if deep and nxt:
                db.set_state(backfill_cursor=nxt)
            self.progress = f"{mode}: page {pages}, {new} new / {known} known on last page"
            log.info("%s page %d: %d new, %d known reposts", mode, pages, new, known)

            if not nxt or nxt == cursor or empty >= EMPTY_PAGES_END:
                if deep:
                    db.set_state(backfill_done=1, backfill_cursor=None, backfill_finished_at=db.now())
                    log.info("backfill reached the end of the timeline")
                return budget, "end"
            cursor = nxt
            if not deep and streak >= KNOWN_STREAK_STOP:
                return budget, "caught_up"
            await asyncio.sleep(page_pause())
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
            self.progress = f"pausing {int(wait // 60)}m{int(wait % 60):02d}s (rate cap: {WINDOW_MAX_REQUESTS} requests / 15 min)"
            log.info("rate cap reached, pausing %.0fs", wait)
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
    db.set_state(backfill_done=0, backfill_cursor=None)
