"""Archive: import reposts listed in the X data archive (tweets.js) and fetch each one, paced like the sync.

Only IDs and dates are kept from the uploaded file. While any archive item is pending, runs process the
archive and nothing else.
"""

import json
import logging
import re

from . import config  # noqa: F401,I001  (before twscrape: disables telemetry)

from twscrape.api import GQL_FEATURES, GQL_URL, OP_TweetDetail
from twscrape.queue_client import QueueClient

from . import db, store
from .common import Run, page_pause
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


# ---------- fetching ----------

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
            return c.execute("SELECT 1 FROM posts WHERE id=?", (item["original_id"],)).fetchone() is not None
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
    """Processes pending archive items within the run's budget. Returns 'done' or 'budget'."""
    async with QueueClient(r.api.pool, "TweetDetail") as client:
        while r.budget > 0:
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
