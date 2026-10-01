"""Archive: import reposts listed in the X data archive (tweets.js) and fetch each one, paced like the sync.

Only IDs and dates are kept from the uploaded file. While any archive item is pending, runs process the
archive and nothing else.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime

from . import config  # noqa: F401, I001  (before twscrape: disables telemetry)

from twscrape.api import GQL_FEATURES, GQL_URL, OP_TweetDetail
from twscrape.queue_client import GqlFeaturesOutdatedError, QueueClient
from twscrape.utils import encode_params

from . import db, store
from .common import Run, discover_op, forget_op, page_pause
from .logs import fmt_duration
from .parse import ParseError, parse_tweet, unwrap, x_date

log = logging.getLogger("archive")

_PREFIX = re.compile(r"^\s*window\.YTD\.[\w.]+\s*=\s*")
_RT = re.compile(r"^RT @(\w{1,15}):")


class ArchiveError(ValueError):
    pass


# ---------- import ----------

def parse_archive(data: bytes) -> tuple[int, list[dict], int]:
    """(tweet count, reposts, malformed entries). Raises ArchiveError if the file isn't an X tweets archive."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ArchiveError("the file isn't text (expected tweets.js / tweets.json from the X archive)")
    text = _PREFIX.sub("", text, count=1)
    try:
        entries = json.loads(text)
    except json.JSONDecodeError as e:
        raise ArchiveError(f"not valid JSON (line {e.lineno}): expected tweets.js / tweets.json from the X archive")
    if not isinstance(entries, list):
        raise ArchiveError("expected a list of tweets at the top level")

    tweets, malformed, reposts = 0, 0, []
    for entry in entries:
        t = entry.get("tweet") if isinstance(entry, dict) else None
        if not isinstance(t, dict) or not t.get("id_str") or not isinstance(t.get("full_text"), str) or not t.get("created_at"):
            malformed += 1
            continue
        tweets += 1
        m = _RT.match(t["full_text"])
        if not m:
            continue
        original_id = None
        for media in ((t.get("extended_entities") or {}).get("media") or (t.get("entities") or {}).get("media") or []):
            original_id = media.get("source_status_id_str") or media.get("source_status_id")
            if original_id:
                break
        try:
            reposted_at = x_date(t["created_at"])
        except ValueError:
            malformed += 1
            continue
        reposts.append({"repost_id": t["id_str"], "original_id": original_id, "rt_handle": m.group(1), "reposted_at": reposted_at})

    if tweets == 0:
        raise ArchiveError("no tweets found — is this tweets.js / tweets.json from the X archive?")
    return tweets, reposts, malformed


def import_archive(data: bytes, filename: str) -> dict:
    tweets, reposts, malformed = parse_archive(data)
    now = db.now()
    with db.tx() as c:
        before = c.execute("SELECT COUNT(*) FROM archive_items").fetchone()[0]
        c.executemany(
            """INSERT OR IGNORE INTO archive_items(repost_id, original_id, rt_handle, reposted_at, updated_at)
               VALUES (:repost_id, :original_id, :rt_handle, :reposted_at, :now)""",
            [{**r, "now": now} for r in reposts],
        )
        added = c.execute("SELECT COUNT(*) FROM archive_items").fetchone()[0] - before
        # Anything the timeline sync already saved (or found unavailable) needs no request.
        already = c.execute(
            """UPDATE archive_items SET status='known', note='already saved by sync', updated_at=?
               WHERE status='pending' AND (
                 repost_id IN (SELECT repost_id FROM posts) OR repost_id IN (SELECT repost_id FROM missing)
                 OR (original_id IS NOT NULL AND original_id IN (SELECT id FROM posts)))""",
            (now,),
        ).rowcount
    summary = {
        "file": filename, "tweets": tweets, "reposts": len(reposts),
        "with_original_id": sum(1 for r in reposts if r["original_id"]),
        "added": added, "duplicates": len(reposts) - added, "already_saved": already, "malformed": malformed,
        "at": now,
    }
    db.set_state(archive_last_import=json.dumps(summary), archive_completed_at=None)
    log.info(
        "imported %s · %s tweets · %s reposts · %s new to the queue · %s already saved by sync",
        filename, f"{tweets:,}", f"{len(reposts):,}", f"{added:,}", f"{already:,}",
    )
    return summary


# ---------- progress ----------

def progress() -> dict:
    with db.tx() as c:
        counts = dict(c.execute("SELECT status, COUNT(*) FROM archive_items GROUP BY status").fetchall())
    total = sum(counts.values())
    done = total - counts.get("pending", 0)
    return {
        "total": total, "done": done, "pending": counts.get("pending", 0),
        "saved": counts.get("saved", 0), "known": counts.get("known", 0), "missing": counts.get("missing", 0),
        "percent": (100.0 * done / total) if total else 0.0,
    }


def pending() -> bool:
    with db.tx() as c:
        return c.execute("SELECT 1 FROM archive_items WHERE status='pending' LIMIT 1").fetchone() is not None


def cancel() -> int:
    """Drops items not fetched yet (already-saved posts stay). Returns how many were dropped."""
    with db.tx() as c:
        n = c.execute("DELETE FROM archive_items WHERE status='pending'").rowcount
    log.info("archive import cancelled · %d pending items dropped", n)
    return n


def progress_text(p: dict | None = None) -> str:
    p = p or progress()
    return f"{p['done']:,}/{p['total']:,} ({p['percent']:.1f}%)"


# ---------- batch lookups ----------
# X's web app fetches known posts in bulk with its TweetResultsByRestIds query (found by common.discover_op).
# If it can't be found or X rejects it, the archive falls back to one-by-one lookups.

BATCH_OP = "TweetResultsByRestIds"
BATCH_SIZE = 20
BATCH_RETRY_AFTER = 6 * 3600
BATCH_TIMEOUT = 120
BATCH_MISS = "batch-miss"  # note on items a batch didn't return; they're retried one by one

_NULL_FEATURES_RE = re.compile(r"cannot be null:\s*(.+)$")


def batch_available() -> bool:
    return time.time() >= float(db.get_state("batch_disabled_until", "0"))


def disable_batch(reason: str) -> None:
    until = time.time() + BATCH_RETRY_AFTER
    db.set_state(batch_disabled_until=until)
    forget_op(BATCH_OP)
    log.warning("batch lookups unavailable (%s) · using one-by-one lookups until %s",
                reason, datetime.fromtimestamp(until).astimezone().strftime("%-I:%M %p"))


def _next_batch() -> list:
    """Up to BATCH_SIZE pending items, skipping (and marking) ones the sync already saved."""
    while True:
        with db.tx() as c:
            rows = c.execute(
                f"""SELECT * FROM archive_items WHERE status='pending' AND (note IS NULL OR note != '{BATCH_MISS}')
                    ORDER BY reposted_at DESC LIMIT ?""",
                (BATCH_SIZE,),
            ).fetchall()
        fresh = []
        for item in rows:
            if _already_saved(item):
                _mark(item["repost_id"], "known", "already saved by sync")
            else:
                fresh.append(item)
        if fresh or not rows:
            return fresh


def _batch_features(op: dict) -> dict:
    extra = json.loads(db.get_state("batch_extra_features") or "{}")
    base = {f: GQL_FEATURES.get(f, False) for f in op["features"]} if op["features"] else dict(GQL_FEATURES)
    return {**base, **extra}


async def _run_batch(r: Run, client: QueueClient, op: dict, items: list) -> bool:
    """One batch request. Returns False if batching failed (caller falls back to one-by-one)."""
    ids = [it["original_id"] or it["repost_id"] for it in items]
    url = f"{GQL_URL}/{op['qid']}/{BATCH_OP}"
    page = None
    for _ in range(2):  # second try only after adding feature flags X said were missing
        params = {
            "variables": {"tweetIds": ids, "withCommunity": False, "includePromotedContent": False, "withVoice": False},
            "features": _batch_features(op),
        }
        await r.throttle()
        try:
            rep = await asyncio.wait_for(client.get(url, params=encode_params(params)), BATCH_TIMEOUT)
        except GqlFeaturesOutdatedError as e:
            m = _NULL_FEATURES_RE.search(str(e))
            if not m:
                break
            extra = json.loads(db.get_state("batch_extra_features") or "{}")
            extra.update({f.strip(): False for f in m.group(1).split(",") if f.strip()})
            db.set_state(batch_extra_features=json.dumps(extra))
            log.info("batch lookup needs %d more feature flags · retrying", len(m.group(1).split(",")))
            continue
        except TimeoutError:
            break
        if rep is not None:
            page = rep.json()
            r.budget -= 1
            r.requests += 1
        break

    found = list(_tweets(page)) if page else []
    if not found:
        errors = "; ".join(e.get("message", "?") for e in (page or {}).get("errors", []))[:200]
        disable_batch(errors or "X rejected or ignored the batch request")
        return False

    saved = retry = 0
    handles: list[str] = []
    for item in items:
        original = _find_original(page, item)
        p = r.save(item["repost_id"], item["reposted_at"], original) if original else None
        if p:
            _mark(item["repost_id"], "saved", None)
            saved += 1
            handles.append(p["author"]["handle"])
        else:
            _mark(item["repost_id"], "pending", BATCH_MISS)  # deleted, withheld, or just not in the batch: check alone
            retry += 1

    stop = r.budget <= 0
    pause = 0.0 if stop else page_pause()
    text = progress_text()
    outcome = f"batch of {len(items)} in 1 request · {saved} saved" + (f", {retry} to check one by one" if retry else "")
    r.progress(f"archive · {text} · {outcome}")
    log.info("%s · %s · %s", text, outcome, "stopping" if stop else f"next in {fmt_duration(pause)}")
    if not stop:
        await r.pause(pause)
    return True


# ---------- one-by-one lookups ----------

def _next_item():
    with db.tx() as c:
        return c.execute(
            "SELECT * FROM archive_items WHERE status='pending' ORDER BY reposted_at DESC LIMIT 1"
        ).fetchone()


def _mark(repost_id: str, status: str, note: str | None) -> None:
    with db.tx() as c:
        c.execute("UPDATE archive_items SET status=?, note=?, updated_at=? WHERE repost_id=?",
                  (status, note, db.now(), repost_id))


def _already_saved(item) -> bool:
    if store.is_known(item["repost_id"]):
        return True
    if item["original_id"]:
        with db.tx() as c:
            return c.execute(
                "SELECT 1 FROM posts WHERE id=? UNION ALL SELECT 1 FROM deleted WHERE post_id=? LIMIT 1",
                (item["original_id"], item["original_id"]),
            ).fetchone() is not None
    return False


def _tweets(node):
    """Every tweet object in a response, in document order."""
    if isinstance(node, list):
        for x in node:
            yield from _tweets(x)
    elif isinstance(node, dict):
        t = unwrap(node) if node.get("__typename") == "TweetWithVisibilityResults" else node
        if isinstance(t.get("legacy"), dict) and t.get("rest_id"):
            yield t
        for v in node.values():
            yield from _tweets(v)


def _find_original(page: dict, item) -> dict | None:
    tweets = list(_tweets(page))
    if item["original_id"]:
        for t in tweets:
            if t["rest_id"] == item["original_id"]:
                return t
        return None
    # Only the repost's own ID is known: its response should carry the original inside it.
    for t in tweets:
        rt = (t["legacy"].get("retweeted_status_result") or {}).get("result")
        if t["rest_id"] == item["repost_id"] and rt:
            return rt
    # Fallback: X may answer a repost ID with the original itself — match it by the "RT @handle" author.
    handle = (item["rt_handle"] or "").lower()
    for t in tweets:
        author = unwrap((((t.get("core") or {}).get("user_results")) or {}).get("result"))
        name = ((author.get("core") or {}).get("screen_name") or (author.get("legacy") or {}).get("screen_name") or "").lower()
        if handle and name == handle and not t["legacy"].get("retweeted_status_result"):
            return t
    return None


async def run(r: Run) -> str:
    """Processes pending archive items within the run's budget. Returns 'done' or 'budget'.
    Uses batch lookups (up to BATCH_SIZE posts per request) while they work; anything a batch can't
    resolve, or everything if batching is unavailable, goes through one-by-one lookups."""
    async with QueueClient(r.api.pool, "TweetDetail") as client, \
               QueueClient(r.api.pool, BATCH_OP) as batch_client:
        op = None
        while r.budget > 0:
            if batch_available():
                items = _next_batch()
                if items:
                    op = op or await discover_op(BATCH_OP)
                    if op is None:
                        disable_batch("couldn't find X's batch lookup in its web app")
                    else:
                        handled = await _run_batch(r, batch_client, op, items)
                        if handled:
                            continue
                        op = None

            item = _next_item()
            if item is None:
                p = progress()
                db.set_state(archive_completed_at=db.now())
                log.info(
                    "archive import complete · %s saved · %s already had · %s unavailable",
                    f"{p['saved']:,}", f"{p['known']:,}", f"{p['missing']:,}",
                )
                return "done"
            if _already_saved(item):
                _mark(item["repost_id"], "known", "already saved by sync")
                continue

            target = item["original_id"] or item["repost_id"]
            page = await r.request(client, f"{GQL_URL}/{OP_TweetDetail}", {
                "variables": {
                    "focalTweetId": target, "with_rux_injections": False, "includePromotedContent": False,
                    "withCommunity": True, "withQuickPromoteEligibilityTweetFields": False,
                    "withBirdwatchNotes": False, "withVoice": True, "withV2Timeline": True,
                },
                "features": GQL_FEATURES,
            })
            original = _find_original(page, item)
            if original is None:
                reason = "post not found (deleted, protected or withheld)"
                store.save_missing(item["repost_id"], item["original_id"], item["reposted_at"], reason, None)
                _mark(item["repost_id"], "missing", reason)
                outcome = f"unavailable ({target})"
            else:
                saved = r.save(item["repost_id"], item["reposted_at"], original)
                if saved:
                    _mark(item["repost_id"], "saved", None)
                    outcome = f"saved @{saved['author']['handle']} ({len(saved['media'])} media)"
                else:
                    _mark(item["repost_id"], "missing", "could not read the post")
                    outcome = f"unavailable ({target})"

            stop = r.budget <= 0
            pause = 0.0 if stop else page_pause()
            text = progress_text()
            r.progress(f"archive · {text} · {outcome}")
            log.info("%s · %s · %s", text, outcome, "stopping" if stop else f"next in {fmt_duration(pause)}")
            if stop:
                break
            await r.pause(pause)
    return "budget"
