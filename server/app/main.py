import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from .config import MEDIA_DIR  # noqa: I001  (must import first: disables twscrape telemetry)

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db, downloader, logs
from .reset import wipe_everything
from .sync import WINDOW_MAX_REQUESTS, api_usage, restart_backfill, status_line, syncer

logs.setup()
log = logging.getLogger("web")

PAGE_SIZE = 50


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db.init()
    log.info("started · %s", status_line())
    downloader.start()
    await syncer.start()
    yield
    await syncer.stop()


app = FastAPI(title="ScrewXBookmarks", lifespan=lifespan)
app.mount("/media", StaticFiles(directory=MEDIA_DIR), name="media")

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def _fmt_date(iso: str | None, with_time: bool = False) -> str:
    if not iso:
        return "—"
    d = datetime.fromisoformat(iso).astimezone()
    return d.strftime("%Y-%m-%d %H:%M" if with_time else "%Y-%m-%d")


templates.env.filters["date"] = _fmt_date


def _stats(c) -> dict:
    q = lambda sql: c.execute(sql).fetchone()[0]
    return {
        "posts": q("SELECT COUNT(*) FROM posts"),
        "authors": q("SELECT COUNT(*) FROM authors"),
        "pending": q("SELECT (SELECT COUNT(*) FROM media WHERE status='pending') + (SELECT COUNT(*) FROM avatars WHERE status='pending')"),
        "failed": q("SELECT (SELECT COUNT(*) FROM media WHERE status='failed') + (SELECT COUNT(*) FROM avatars WHERE status='failed')"),
        "missing": q("SELECT COUNT(*) FROM missing"),
        "syncing": syncer.running,
    }


def _with_media(c, posts: list) -> list[dict]:
    posts = [dict(p) for p in posts]
    if not posts:
        return posts
    ids = [p["id"] for p in posts]
    rows = c.execute(
        f"SELECT post_id, file, type, status FROM media WHERE post_id IN ({','.join('?' * len(ids))}) ORDER BY idx",
        ids,
    ).fetchall()
    by_post: dict[str, list] = {}
    for r in rows:
        by_post.setdefault(r["post_id"], []).append(dict(r))
    for p in posts:
        p["media"] = by_post.get(p["id"], [])
    return posts


FOLDER_PREVIEW = 10


@app.get("/authors")
def authors(request: Request, q: str = ""):
    with db.tx() as c:
        sql = """SELECT a.id, a.handle, a.name, a.avatar_file, COUNT(p.id) AS n, MAX(p.reposted_at) AS last
                 FROM authors a JOIN posts p ON p.author_id = a.id"""
        args: list = []
        if q:
            sql += " WHERE a.handle LIKE ? OR a.name LIKE ?"
            args = [f"%{q}%", f"%{q}%"]
        sql += " GROUP BY a.id ORDER BY last DESC"
        rows = [dict(a) for a in c.execute(sql, args).fetchall()]
        previews: dict[str, list] = {}
        for m in c.execute(
            """SELECT author_id, file, type FROM (
                 SELECT p.author_id, m.file, m.type,
                        ROW_NUMBER() OVER (PARTITION BY p.author_id ORDER BY p.reposted_at DESC, m.idx) AS rn
                 FROM media m JOIN posts p ON p.id = m.post_id WHERE m.status = 'done'
               ) WHERE rn <= ? ORDER BY author_id, rn""",
            (FOLDER_PREVIEW,),
        ):
            previews.setdefault(m["author_id"], []).append(dict(m))
        for a in rows:
            a["preview"] = previews.get(a["id"], [])
        stats = _stats(c)
    return templates.TemplateResponse(request, "authors.html", {"authors": rows, "q": q, "stats": stats})


@app.get("/u/{author_id}")
def author(request: Request, author_id: str, page: int = Query(1, ge=1)):
    with db.tx() as c:
        a = c.execute("SELECT * FROM authors WHERE id=?", (author_id,)).fetchone()
        if not a:
            raise HTTPException(404, "unknown author")
        total = c.execute("SELECT COUNT(*) FROM posts WHERE author_id=?", (author_id,)).fetchone()[0]
        posts = c.execute(
            "SELECT * FROM posts WHERE author_id=? ORDER BY reposted_at DESC LIMIT ? OFFSET ?",
            (author_id, PAGE_SIZE, (page - 1) * PAGE_SIZE),
        ).fetchall()
        posts = _with_media(c, posts)
        stats = _stats(c)
    return templates.TemplateResponse(
        request, "author.html",
        {"a": a, "posts": posts, "total": total, "page": page, "pages": max(1, -(-total // PAGE_SIZE)), "stats": stats},
    )


@app.get("/")
def recent(request: Request, page: int = Query(1, ge=1)):
    with db.tx() as c:
        total = c.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        posts = c.execute(
            """SELECT p.*, a.name, a.avatar_file FROM posts p JOIN authors a ON a.id = p.author_id
               ORDER BY p.reposted_at DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, (page - 1) * PAGE_SIZE),
        ).fetchall()
        posts = _with_media(c, posts)
        stats = _stats(c)
    return templates.TemplateResponse(
        request, "recent.html",
        {"posts": posts, "total": total, "page": page, "pages": max(1, -(-total // PAGE_SIZE)), "stats": stats},
    )


@app.get("/sync")
def sync_status(request: Request):
    keys = ["last_start", "last_finish", "last_success", "last_error", "next_run", "backfill_done",
            "backfill_finished_at", "statuses_count", "user_id"]
    state = {k: db.get_state(k) for k in keys}
    with db.tx() as c:
        stats = _stats(c)
        oldest = c.execute("SELECT MIN(reposted_at) FROM posts").fetchone()[0]
        missing = c.execute("SELECT * FROM missing ORDER BY reposted_at DESC LIMIT 100").fetchall()
        failed = c.execute(
            "SELECT file, url, error FROM media WHERE status='failed' UNION ALL SELECT file, url, error FROM avatars WHERE status='failed' LIMIT 100"
        ).fetchall()
    api_used, capped_until = api_usage()
    return templates.TemplateResponse(
        request, "sync.html",
        {"stats": stats, "state": state, "oldest": oldest, "missing": missing, "failed": failed,
         "running": syncer.running, "progress": syncer.progress, "configured": syncer.configured,
         "api_used": api_used, "api_max": WINDOW_MAX_REQUESTS,
         "capped_until": datetime.fromtimestamp(capped_until).astimezone().strftime("%H:%M") if capped_until else None},
    )


@app.post("/sync/run")
def sync_run():
    syncer.trigger()
    return RedirectResponse("/sync", status_code=303)


@app.post("/sync/restart-backfill")
def sync_restart_backfill():
    restart_backfill()
    return RedirectResponse("/sync", status_code=303)


@app.post("/reset")
async def reset_all(confirm: str = ""):
    if confirm != "RESET":
        raise HTTPException(400, "confirmation missing")
    await syncer.stop()
    await asyncio.to_thread(wipe_everything)
    await syncer.start()
    return RedirectResponse("/", status_code=303)


@app.post("/sync/retry-failed")
def sync_retry_failed():
    with db.tx() as c:
        for table in ("media", "avatars"):
            c.execute(f"UPDATE {table} SET status='pending', attempts=0, next_try='' WHERE status='failed'")
    downloader.wake()
    return RedirectResponse("/sync", status_code=303)
