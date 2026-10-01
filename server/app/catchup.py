"""Catch-up: read the profile timeline from the top until we hit reposts we already have."""

import logging

from .common import KNOWN_STREAK_STOP, Run, fetch_timeline_page, ingest_page, log_timeline_page, page_pause, timeline_end

log = logging.getLogger("sync")


async def run(r: Run, client, my_id: str) -> str:
    """Returns why it stopped: 'caught_up', 'end' or 'budget'."""
    cursor, streak, empty = None, 0, 0
    while r.budget > 0:
        page = await fetch_timeline_page(r, client, my_id, cursor)
        flags = ingest_page(r, page, my_id)
        for is_new in flags:
            streak = 0 if is_new else streak + 1
        nxt, end_reason, empty = timeline_end(page, cursor, empty)

        if end_reason:
            stop = "end"
        elif streak >= KNOWN_STREAK_STOP:
            stop = "caught_up"
        elif r.budget <= 0:
            stop = "budget"
        else:
            stop = None
        pause = 0.0 if stop else page_pause()
        log_timeline_page(r, "catch-up", flags, bool(stop), pause)
        if end_reason:
            log.info("timeline ended: %s", end_reason)
        if stop:
            return stop
        cursor = nxt
        await r.pause(pause)
    return "budget"
