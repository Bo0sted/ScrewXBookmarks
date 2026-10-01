"""Backfill: walk the profile timeline down into older history, resuming from a saved cursor."""

import logging

from . import db
from .common import Run, fetch_timeline_page, index_summary, ingest_page, log_timeline_page, page_pause, timeline_end

log = logging.getLogger("sync")

# X sometimes returns a short/empty page mid-timeline: only trust "the end" once it repeats on a later run.
END_CONFIRMATIONS = 2


def is_done() -> bool:
    return db.get_state("backfill_done") == "1"


def started() -> bool:
    return db.get_state("backfill_cursor") is not None


def restart() -> None:
    db.set_state(backfill_done=0, backfill_cursor=None, backfill_end_checks=0, backfill_finished_manually=None)


def finish() -> None:
    """Marks the backfill complete by hand (e.g. older history comes from the archive instead)."""
    db.set_state(backfill_done=1, backfill_cursor=None, backfill_end_checks=0,
                 backfill_finished_at=db.now(), backfill_finished_manually=1)
    log.info("backfill marked complete by hand")


async def run(r: Run, client, my_id: str) -> str:
    """Continues from the saved cursor (or the top if never started). Returns 'end' or 'budget'."""
    cursor = db.get_state("backfill_cursor")
    empty = 0
    while r.budget > 0:
        page = await fetch_timeline_page(r, client, my_id, cursor)
        flags = ingest_page(r, page, my_id)
        nxt, end_reason, empty = timeline_end(page, cursor, empty)
        if nxt:
            db.set_state(backfill_cursor=nxt)

        stop = "end" if end_reason else ("budget" if r.budget <= 0 else None)
        pause = 0.0 if stop else page_pause()
        log_timeline_page(r, "backfill", flags, bool(stop), pause)

        if stop != "end":
            db.set_state(backfill_end_checks=0)  # timeline continued past here, so any earlier "end" was false
        if stop == "end":
            log.info("timeline ended: %s", end_reason)
            _confirm_end(nxt or cursor)
        if stop:
            return stop
        cursor = nxt
        await r.pause(pause)
    return "budget"


def _confirm_end(position: str | None) -> None:
    checks = int(db.get_state("backfill_end_checks", "0")) + 1
    _, oldest = index_summary()
    if checks >= END_CONFIRMATIONS:
        db.set_state(backfill_done=1, backfill_cursor=None, backfill_end_checks=0, backfill_finished_at=db.now())
        log.info("backfill complete · end of timeline confirmed (oldest repost %s)", oldest)
    else:
        db.set_state(backfill_end_checks=checks, backfill_cursor=position)
        log.info("backfill paused · will re-check from this point next run before calling it complete")
