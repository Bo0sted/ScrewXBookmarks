import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from .config import MEDIA_DIR  # noqa: I001  (must import first: disables twscrape telemetry)

from fastapi import FastAPI, HTTPException, Query, Request
from urllib.parse import urlencode

from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db, downloader, logs, thumbs
from .reset import wipe_everything
from . import actions, archive, backfill
from .sync import WINDOW_MAX_REQUESTS, api_usage, status_line, syncer

logs.setup()
log = logging.getLogger("web")

PAGE_SIZE = 30


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db.init()
    log.info("started · %s", status_line())
    downloader.start()
    thumbs.start()
    await syncer.start()
    yield
    await syncer.stop()


app = FastAPI(title="ScrewXBookmarks", lifespan=lifespan)
STATIC_DIR = Path(__file__).parent / "static"
app.mount("/media", StaticFiles(directory=MEDIA_DIR), name="media")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
# Cache-buster so browsers pick up a new app.js after an update.
templates.env.globals["asset_v"] = int((STATIC_DIR / "app.js").stat().st_mtime)


def _fmt_date(iso: str | None, with_time: bool = False) -> str:
    if not iso:
        return "—"
    d = datetime.fromisoformat(iso).astimezone()
    return d.strftime("%Y-%m-%d %-I:%M %p" if with_time else "%Y-%m-%d")


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
AUTHORS_PAGE = 40


def _next_url(request: Request, page: int, has_more: bool) -> str | None:
    """URL of the next chunk, fetched by the page's infinite scroll (partial=1 returns just the items)."""
    if not has_more:
        return None
    params = dict(request.query_params)
    params.update(page=str(page + 1), partial="1")
    return f"{request.url.path}?{urlencode(params)}"


@app.get("/thumb/{size}/{file:path}")
def thumb(size: str, file: str):
    path = thumbs.ensure(file, size)
    if not path:
        raise HTTPException(404, "no thumbnail")
    return FileResponse(path, media_type="image/webp", headers={"Cache-Control": "public, max-age=31536000, immutable"})


@app.get("/authors")
def authors(request: Request, q: str = "", page: int = Query(1, ge=1), partial: int = 0):
    with db.tx() as c:
        where, args = "", []
        if q:
            where = "WHERE a.handle LIKE ? OR a.name LIKE ?"
            args = [f"%{q}%", f"%{q}%"]
        rows = c.execute(
            f"""SELECT a.id, a.handle, a.name, a.avatar_file, COUNT(p.id) AS n, MAX(p.reposted_at) AS last
                FROM authors a JOIN posts p ON p.author_id = a.id {where}
                GROUP BY a.id ORDER BY last DESC LIMIT ? OFFSET ?""",
            [*args, AUTHORS_PAGE + 1, (page - 1) * AUTHORS_PAGE],
        ).fetchall()
        has_more = len(rows) > AUTHORS_PAGE
        rows = [dict(r) for r in rows[:AUTHORS_PAGE]]
        previews: dict[str, list] = {}
        if rows:
            ids = [r["id"] for r in rows]
            for m in c.execute(
                f"""SELECT * FROM (
                      SELECT p.author_id, p.id AS post_id, p.handle, p.posted_at, m.file, m.type, m.idx,
                             (SELECT COUNT(*) FROM media m2 WHERE m2.post_id = p.id) AS count,
                             ROW_NUMBER() OVER (PARTITION BY p.author_id ORDER BY p.reposted_at DESC, m.idx) AS rn
                      FROM media m JOIN posts p ON p.id = m.post_id
                      WHERE m.status = 'done' AND p.author_id IN ({','.join('?' * len(ids))})
                    ) WHERE rn <= ? ORDER BY author_id, rn""",
                [*ids, FOLDER_PREVIEW],
            ):
                previews.setdefault(m["author_id"], []).append(dict(m))
        for a in rows:
            a["preview"] = previews.get(a["id"], [])
        ctx = {"authors": rows, "q": q, "next_url": _next_url(request, page, has_more)}
        if partial:
            return templates.TemplateResponse(request, "_folders.html", ctx)
        ctx["stats"] = _stats(c)
    return templates.TemplateResponse(request, "authors.html", ctx)


@app.get("/u/{author_id}")
def author(request: Request, author_id: str, page: int = Query(1, ge=1), partial: int = 0):
    with db.tx() as c:
        a = c.execute("SELECT * FROM authors WHERE id=?", (author_id,)).fetchone()
        if not a:
            raise HTTPException(404, "unknown author")
        posts = c.execute(
            """SELECT p.*, a.name, a.avatar_file FROM posts p JOIN authors a ON a.id = p.author_id
               WHERE p.author_id=? ORDER BY p.reposted_at DESC LIMIT ? OFFSET ?""",
            (author_id, PAGE_SIZE + 1, (page - 1) * PAGE_SIZE),
        ).fetchall()
        has_more = len(posts) > PAGE_SIZE
        posts = _with_media(c, posts[:PAGE_SIZE])
        ctx = {"posts": posts, "show_author": False, "next_url": _next_url(request, page, has_more)}
        if partial:
            return templates.TemplateResponse(request, "_feed.html", ctx)
        ctx["a"] = a
        ctx["total"] = c.execute("SELECT COUNT(*) FROM posts WHERE author_id=?", (author_id,)).fetchone()[0]
        ctx["stats"] = _stats(c)
    return templates.TemplateResponse(request, "author.html", ctx)


@app.get("/")
def recent(request: Request, page: int = Query(1, ge=1), partial: int = 0, sort: str = "latest"):
    order = "ASC" if sort == "oldest" else "DESC"
    with db.tx() as c:
        posts = c.execute(
            f"""SELECT p.*, a.name, a.avatar_file FROM posts p JOIN authors a ON a.id = p.author_id
               ORDER BY p.reposted_at {order} LIMIT ? OFFSET ?""",
            (PAGE_SIZE + 1, (page - 1) * PAGE_SIZE),
        ).fetchall()
        has_more = len(posts) > PAGE_SIZE
        posts = _with_media(c, posts[:PAGE_SIZE])
        ctx = {"posts": posts, "show_author": True, "next_url": _next_url(request, page, has_more)}
        if partial:
            return templates.TemplateResponse(request, "_feed.html", ctx)
        ctx["stats"] = _stats(c)
        ctx["sort"] = "oldest" if order == "ASC" else "latest"
    return templates.TemplateResponse(request, "recent.html", ctx)


@app.get("/sync")
def sync_status(request: Request):
    keys = ["last_start", "last_finish", "last_success", "last_error", "next_run", "backfill_done",
            "backfill_finished_at", "backfill_finished_manually", "statuses_count", "user_id",
            "archive_completed_at"]
    state = {k: db.get_state(k) for k in keys}
    last_import = json.loads(db.get_state("archive_last_import") or "null")
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
         "capped_until": datetime.fromtimestamp(capped_until).astimezone().strftime("%-I:%M %p") if capped_until else None,
         "archive": archive.progress(), "last_import": last_import,
         "batch_until": None if archive.batch_available() else
             datetime.fromtimestamp(float(db.get_state("batch_disabled_until"))).astimezone().strftime("%-I:%M %p")},
    )


@app.post("/archive/import")
async def archive_import(request: Request, filename: str = "tweets.js"):
    """Raw file upload (the page sends the file as the request body)."""
    data = await request.body()
    if not data:
        return JSONResponse({"error": "the file is empty"}, status_code=400)
    try:
        summary = await asyncio.to_thread(archive.import_archive, data, filename[:200])
    except archive.ArchiveError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return {"ok": True, **summary}


@app.post("/archive/cancel")
def archive_cancel():
    archive.cancel()
    return RedirectResponse("/sync", status_code=303)


@app.post("/sync/finish-backfill")
def sync_finish_backfill():
    backfill.finish()
    return RedirectResponse("/sync", status_code=303)


@app.post("/sync/run")
def sync_run():
    syncer.trigger()
    return RedirectResponse("/sync", status_code=303)


@app.post("/sync/restart-backfill")
def sync_restart_backfill():
    backfill.restart()
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


@app.get("/deleted")
def deleted(request: Request):
    with db.tx() as c:
        rows = c.execute("SELECT * FROM deleted ORDER BY deleted_at DESC").fetchall()
        stats = _stats(c)
    return templates.TemplateResponse(request, "deleted.html", {"rows": rows, "stats": stats})


async def _action(coro) -> JSONResponse:
    """Runs a delete/restore and reports {ok, message} for the page's toast."""
    if not syncer.configured:
        coro.close()
        return JSONResponse({"ok": False, "message": "X_USERNAME / X_COOKIES are not set"}, status_code=400)
    try:
        return JSONResponse({"ok": True, "message": await coro})
    except actions.ActionError as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=400)
    except Exception as e:
        log.exception("action failed")
        return JSONResponse({"ok": False, "message": f"{type(e).__name__}: {e}"}, status_code=500)


@app.post("/post/{post_id}/delete")
async def post_delete(post_id: str):
    return await _action(actions.delete_post(syncer.api, post_id))


@app.post("/deleted/{post_id}/restore")
async def post_restore(post_id: str):
    return await _action(actions.restore_post(syncer.api, post_id))


@app.post("/sync/catchup")
async def sync_catchup():
    if not syncer.configured:
        return JSONResponse({"ok": False, "message": "X_USERNAME / X_COOKIES are not set"}, status_code=400)
    res = await syncer.catch_up_now()
    return JSONResponse(res, status_code=200 if res["ok"] else 409)
