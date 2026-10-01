"""Pieces shared by the sync phases (catch-up, backfill, archive): pacing, the request cap, saving posts."""

import asyncio
import json
import logging
import math
import random
import re
import time
from datetime import datetime, timedelta

from .config import QUIET_HOURS, X_COOKIES  # noqa: I001  (before twscrape: disables telemetry)

from twscrape.api import GQL_FEATURES, GQL_URL, OP_UserTweets
from twscrape.queue_client import QueueClient
from twscrape.utils import encode_params, parse_cookies
from twscrape.xclid import _make_client as make_x_web_client
from twscrape.xclid import get_scripts_list, get_tw_page_text

from . import db, downloader, store
from .logs import fmt_duration
from .parse import ParseError, bottom_cursor, count_timeline_items, find_reposts, parse_tweet

log = logging.getLogger("sync")

# Hard cap on authenticated X requests, independent of the random pacing. X's observed limit for
# timeline requests is ~50 per 15 min; staying far below it avoids looking like a bot.
WINDOW_SECONDS = 15 * 60
WINDOW_MAX_REQUESTS = 30
# Writes (delete/restore a repost) also count above. X allows ~300 posts + reposts per 3 h; stay at half.
WRITE_WINDOW_SECONDS = 3 * 3600
WRITE_WINDOW_MAX = 150

KNOWN_STREAK_STOP = 20
EMPTY_PAGES_END = 3


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


def next_run_at(busy: bool) -> datetime:
    """busy = archive or backfill still has work: shorter gaps between runs."""
    hours = random.uniform(1.0, 2.5) if busy else random.uniform(2.0, 5.0)
    t = datetime.now().astimezone() + timedelta(hours=hours)
    while _in_quiet_hours(t):
        t += timedelta(minutes=random.uniform(20, 50))
    return t


def api_usage() -> tuple[int, float | None]:
    """(requests in the current 15-min window, unix time the cap lifts if currently capped)."""
    recent = _recent("api_requests", WINDOW_SECONDS)
    capped_until = recent[0] + WINDOW_SECONDS if len(recent) >= WINDOW_MAX_REQUESTS else None
    return len(recent), capped_until


def _recent(key: str, window: float) -> list[float]:
    now = time.time()
    return [t for t in json.loads(db.get_state(key, "[]")) if now - t < window]


def reserve_request(write: bool = False) -> float | None:
    """Records one X request if it fits under the caps and returns None. Otherwise records nothing and
    returns the unix time it would fit. Timestamps live in the DB so a restart can't reset the windows."""
    recent = _recent("api_requests", WINDOW_SECONDS)
    writes = _recent("api_writes", WRITE_WINDOW_SECONDS) if write else []
    until = recent[0] + WINDOW_SECONDS if len(recent) >= WINDOW_MAX_REQUESTS else None
    if len(writes) >= WRITE_WINDOW_MAX:
        until = max(until or 0, writes[0] + WRITE_WINDOW_SECONDS)
    if until:
        return until
    now = time.time()
    values = {"api_requests": json.dumps([*recent, now])}
    if write:
        values["api_writes"] = json.dumps([*writes, now])
    db.set_state(**values)
    return None


def index_summary() -> tuple[int, str]:
    with db.tx() as c:
        total, oldest = c.execute("SELECT COUNT(*), MIN(reposted_at) FROM posts").fetchone()
    return total, (oldest[:10] if oldest else "—")


class Run:
    """One sync run: its page/request budget, counters, and the pacing every phase goes through."""

    def __init__(self, api, budget: int, set_progress) -> None:
        self.api = api
        self.budget = budget
        self.requests = 0
        self.pages = 0
        self.new = 0
        self._set_progress = set_progress

    def progress(self, text: str) -> None:
        self._set_progress(text)

    async def throttle(self) -> None:
        """Blocks until another X request fits in the rolling window, then records it."""
        while True:
            until = reserve_request()
            if until is None:
                return
            wait = until - time.time() + random.uniform(5, 60)
            resume = datetime.now().astimezone() + timedelta(seconds=wait)
            self.progress(
                f"cooling down {fmt_duration(wait)} until {resume.strftime('%H:%M')} "
                f"(rate cap: {WINDOW_MAX_REQUESTS} requests / 15 min)"
            )
            log.info(
                "rate cap · %d/%d requests in the last 15 min · cooling down %s (resumes %s)",
                WINDOW_MAX_REQUESTS, WINDOW_MAX_REQUESTS, fmt_duration(wait), resume.strftime("%H:%M:%S"),
            )
            await asyncio.sleep(wait)

    async def request(self, client: QueueClient, url: str, params: dict) -> dict:
        """One paced, capped, budgeted request to X's API."""
        await self.throttle()
        rep = await client.get(url, params=encode_params(params))
        if rep is None:
            raise SyncError(await self.account_problem() or "X aborted the request (auth, rate limit or API change)")
        self.budget -= 1
        self.requests += 1
        return rep.json()

    async def pause(self, seconds: float) -> None:
        if seconds > 60:
            log.info("taking a %s break (human-like pause)", fmt_duration(seconds))
        await asyncio.sleep(seconds)

    async def account_problem(self) -> str | None:
        try:
            for a in await self.api.pool.accounts_info():
                if not a["active"]:
                    return f"X session inactive ({a.get('error_msg') or 'cookies rejected'}); update X_COOKIES and restart"
        except Exception:
            log.exception("could not read account state")
        return None

    def save(self, repost_id: str, reposted_at: str, original: dict) -> dict | None:
        """Stores a reposted post (or records it as unavailable). Returns the parsed post if saved."""
        try:
            p = parse_tweet(original)
        except (ParseError, KeyError, TypeError, ValueError) as e:
            store.save_missing(repost_id, original.get("rest_id"), reposted_at, str(e), original)
            log.warning("repost %s not stored: %s", repost_id, e)
            return None
        store.save(p, repost_id, reposted_at, original)
        self.new += 1
        downloader.wake()
        return p


# ---------- profile timeline (used by catch-up and backfill) ----------

async def fetch_timeline_page(run: Run, client: QueueClient, my_id: str, cursor: str | None) -> dict:
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
    run.pages += 1
    return await run.request(client, f"{GQL_URL}/{OP_UserTweets}", {"variables": kv, "features": GQL_FEATURES})


def timeline_client(api) -> QueueClient:
    return QueueClient(api.pool, "UserTweets")


def ingest_page(run: Run, page: dict, my_id: str) -> list[bool]:
    """Saves the page's new reposts. Returns one flag per repost in timeline order: True = new."""
    flags = []
    for r in find_reposts(page, my_id):
        if store.is_known(r["repost_id"]):
            flags.append(False)
        else:
            run.save(r["repost_id"], r["reposted_at"], r["original"])
            flags.append(True)
    return flags


def timeline_end(page: dict, cursor: str | None, empty_streak: int) -> tuple[str | None, str | None, int]:
    """(next cursor, reason the timeline ended or None, updated empty-page streak)."""
    nxt = bottom_cursor(page)
    items = count_timeline_items(page)
    empty_streak = empty_streak + 1 if items == 0 else 0
    if not nxt:
        return nxt, f"X sent no 'next page' cursor ({items} entries on the page)", empty_streak
    if nxt == cursor:
        return nxt, "X returned the same cursor again", empty_streak
    if empty_streak >= EMPTY_PAGES_END:
        return nxt, f"{empty_streak} empty pages in a row", empty_streak
    return nxt, None, empty_streak


def log_timeline_page(run: Run, mode: str, flags: list[bool], stopping: bool, pause: float) -> None:
    total, oldest = index_summary()
    used, _ = api_usage()
    new = sum(flags)
    run.progress(f"{mode} · page {run.pages} · +{run.new} new this run · api {used}/{WINDOW_MAX_REQUESTS} in 15 min")
    log.info(
        "%s p%d · +%d new, %d known · %s indexed · oldest %s · api %d/%d · %s",
        mode, run.pages, new, len(flags) - new, f"{total:,}", oldest, used, WINDOW_MAX_REQUESTS,
        "stopping" if stopping else f"next page in {fmt_duration(pause)}",
    )


# ---------- X web-app operations ----------
# GraphQL operations twscrape doesn't ship (batch lookups, repost/un-repost). Their query IDs aren't public
# and change with X's deploys, so they're read from X's own web-app scripts (what a browser loads anyway)
# and cached.

KNOWN_OPS = ("TweetResultsByRestIds", "CreateRetweet", "DeleteRetweet")
OP_TTL = 2 * 86400
MAX_SCRIPTS = 60
_FEATURES_RE = re.compile(r"featureSwitches:\s*\[([^\]]*)\]")


def forget_op(name: str) -> None:
    db.set_state(**{f"op_{name}": None})


async def discover_op(name: str) -> dict | None:
    """{qid, features, at} for one of KNOWN_OPS, cached or read from X's scripts (other known
    operations found along the way are cached too). None if it can't be found."""
    cached = json.loads(db.get_state(f"op_{name}") or "null")
    if cached and time.time() - cached["at"] < OP_TTL:
        return cached
    log.info("looking up X's %s query ID in its web-app scripts", name)
    try:
        async with make_x_web_client(cookies=parse_cookies(X_COOKIES)) as clt:
            html = await get_tw_page_text("https://x.com", clt)
            for url in get_scripts_list(html)[:MAX_SCRIPTS]:
                try:
                    text = (await clt.get(url)).text
                except Exception:
                    continue
                for op_name in KNOWN_OPS:
                    m = re.search(r'queryId:\s*"([\w-]+)"\s*,\s*operationName:\s*"' + op_name + '"', text)
                    if m:
                        fs = _FEATURES_RE.search(text[m.end():m.end() + 6000])
                        op = {"qid": m.group(1), "features": re.findall(r'"(\w+)"', fs.group(1)) if fs else [], "at": time.time()}
                        db.set_state(**{f"op_{op_name}": json.dumps(op)})
                        if op_name == name:
                            cached = op
                if cached and cached["at"] > time.time() - 60:
                    log.info("%s found (%s, %d feature flags)", name, cached["qid"], len(cached["features"]))
                    return cached
                await asyncio.sleep(random.uniform(0.2, 0.8))
    except Exception as e:
        log.warning("couldn't read X's web-app scripts: %r", e)
    return None
