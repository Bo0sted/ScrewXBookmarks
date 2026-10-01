"""Delete (removes your repost on X and wipes the post's files) and restore (reposts it on X again).

Both are single writes to X sent right away (no human-like pauses), but they count against the same
rate caps as the sync and fail instead of waiting when a cap is reached.
"""

import asyncio
import logging
import os
from contextlib import suppress
from datetime import datetime
from urllib.parse import urlparse

from .config import MEDIA_DIR, X_USERNAME  # noqa: I001  (before twscrape: disables telemetry)

from twscrape.api import GQL_URL
from twscrape.queue_client import XClIdGenStore

from . import db, downloader, thumbs
from .common import discover_op, forget_op, reserve_request

log = logging.getLogger("actions")

ERR_NOT_FOUND = 144  # "No status found with that ID"
ERR_ALREADY_REPOSTED = 327


class ActionError(RuntimeError):
    pass


async def _mutation(api, name: str, variables: dict) -> dict:
    """One write to X's GraphQL API. It's a POST with a JSON body, which twscrape's QueueClient can't
    send, so it goes through the account's own client with the same headers twscrape uses."""
    op = await discover_op(name)
    if op is None:
        raise ActionError(f"Couldn't find X's {name} operation in its web app; try again later")
    acc = await api.pool.get_account(X_USERNAME)
    if acc is None or not acc.active:
        raise ActionError("X session inactive; update X_COOKIES and restart")
    url = f"{GQL_URL}/{op['qid']}/{name}"
    clt = acc.make_client()
    try:
        for fresh in (False, True):  # a 404 usually means a stale x-client-transaction-id (as in twscrape)
            until = reserve_request(write=True)
            if until:
                at = datetime.fromtimestamp(until).astimezone().strftime("%-I:%M %p")
                raise ActionError(f"Rate cap reached; nothing was changed. Try again after {at}")
            gen = await XClIdGenStore.get(acc.username, cookies=acc.cookies, fresh=fresh)
            rep = await clt.request(
                "POST", url, json={"variables": variables, "queryId": op["qid"]},
                headers={"x-client-transaction-id": gen.calc("POST", urlparse(url).path)},
            )
            if rep.status_code != 404:
                break
    except ActionError:
        raise
    except Exception as e:
        raise ActionError(f"Couldn't reach X: {e}") from e
    finally:
        await clt.aclose()
    try:
        res = rep.json()
    except ValueError:
        res = None
    if not isinstance(res, dict) or not (res.get("data") or res.get("errors")):
        if rep.status_code == 404:
            forget_op(name)  # the cached query ID is probably outdated; look it up again next time
        raise ActionError(f"X answered HTTP {rep.status_code}")
    return res


def _errors(res: dict) -> list[tuple[int, str]]:
    return [(e.get("code", -1), e.get("message", "?")) for e in res.get("errors") or []]


async def delete_post(api, post_id: str) -> str:
    with db.tx() as c:
        p = c.execute(
            """SELECT p.id, p.repost_id, p.author_id, p.handle, p.posted_at, p.reposted_at, a.name
               FROM posts p JOIN authors a ON a.id = p.author_id WHERE p.id=?""",
            (post_id,),
        ).fetchone()
    if not p:
        raise ActionError("Post not found (already deleted?)")

    res = await _mutation(api, "DeleteRetweet", {"source_tweet_id": post_id, "dark_request": False})
    errors = _errors(res)
    gone = False
    if errors and not (res.get("data") or {}).get("unretweet"):
        if not any(code == ERR_NOT_FOUND for code, _ in errors):
            raise ActionError("X refused: " + "; ".join(m for _, m in errors))
        gone = True  # the post itself no longer exists on X: nothing to un-repost

    removed = await asyncio.to_thread(_wipe, p)
    log.info("deleted %s by @%s · %s · %d files removed",
             post_id, p["handle"], "post gone on X" if gone else "repost removed on X", removed)
    return f"Deleted · {'post no longer exists on X' if gone else 'repost removed on X'} · {removed} files wiped"


def _wipe(p) -> int:
    """Moves the post to the deleted list and removes its media and thumbnails."""
    with downloader.paused(), thumbs.paused():
        with db.tx() as c:
            files = [r["file"] for r in c.execute("SELECT file FROM media WHERE post_id=?", (p["id"],))]
            c.execute(
                """INSERT OR REPLACE INTO deleted(post_id, repost_id, author_id, handle, name, posted_at, reposted_at, deleted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (p["id"], p["repost_id"], p["author_id"], p["handle"], p["name"], p["posted_at"], p["reposted_at"], db.now()),
            )
            c.execute("DELETE FROM media WHERE post_id=?", (p["id"],))
            c.execute("DELETE FROM posts WHERE id=?", (p["id"],))
        removed = 0
        for f in files:
            src = os.path.join(MEDIA_DIR, f)
            for path in (src, src + ".part", *(thumbs.cache_path(f, s) for s in thumbs.SIZES)):
                with suppress(FileNotFoundError):
                    os.remove(path)
                    removed += 1
    return removed


async def restore_post(api, post_id: str) -> str:
    with db.tx() as c:
        d = c.execute("SELECT handle FROM deleted WHERE post_id=?", (post_id,)).fetchone()
    if not d:
        raise ActionError("Not in the deleted list")

    res = await _mutation(api, "CreateRetweet", {"tweet_id": post_id, "dark_request": False})
    errors = _errors(res)
    if errors and not (res.get("data") or {}).get("create_retweet"):
        if not any(code == ERR_ALREADY_REPOSTED for code, _ in errors):
            raise ActionError("X refused: " + "; ".join(m for _, m in errors))

    with db.tx() as c:
        c.execute("DELETE FROM deleted WHERE post_id=?", (post_id,))
    log.info("restored %s by @%s · reposted on X, comes back with the next sync", post_id, d["handle"])
    return "Post will re-surface after sync. Please either wait for next sync window or run a manual sync"
