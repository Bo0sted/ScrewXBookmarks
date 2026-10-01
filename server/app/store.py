import hashlib
import json

from . import db


def _ext(url: str, default: str) -> str:
    last = url.split("?", 1)[0].rsplit("/", 1)[-1]
    return last.rsplit(".", 1)[-1].lower() if "." in last else default


def is_known(repost_id: str) -> bool:
    with db.tx() as c:
        return c.execute(
            """SELECT 1 FROM posts WHERE repost_id=? UNION ALL SELECT 1 FROM missing WHERE repost_id=?
               UNION ALL SELECT 1 FROM deleted WHERE repost_id=? LIMIT 1""",
            (repost_id, repost_id, repost_id),
        ).fetchone() is not None


def save(p: dict, repost_id: str, reposted_at: str, raw: dict) -> str:
    """Upserts a parsed tweet. Returns 'saved' for a new post, 'updated' if it already existed."""
    now = db.now()
    a = p["author"]
    with db.tx() as c:
        c.execute(
            """INSERT INTO authors(id, handle, name, first_seen, updated_at) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET handle=excluded.handle, name=excluded.name, updated_at=excluded.updated_at""",
            (a["id"], a["handle"], a["name"], now, now),
        )

        if a["avatar_url"]:
            digest = hashlib.sha1(a["avatar_url"].encode()).hexdigest()[:10]
            file = f"{a['id']}_{digest}.{_ext(a['avatar_url'], 'jpg')}"
            c.execute(
                """INSERT INTO avatars(file, author_id, url, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(file) DO UPDATE SET last_seen=excluded.last_seen""",
                (file, a["id"], a["avatar_url"], now, now),
            )
            if c.execute("SELECT status FROM avatars WHERE file=?", (file,)).fetchone()["status"] == "done":
                c.execute("UPDATE authors SET avatar_file=? WHERE id=?", (file, a["id"]))

        existed = c.execute("SELECT 1 FROM posts WHERE id=?", (p["id"],)).fetchone() is not None
        # Re-reposting an old post moves it back to the top, same as on X.
        c.execute(
            """INSERT INTO posts(id, repost_id, author_id, handle, text, posted_at, reposted_at, raw)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 repost_id=CASE WHEN excluded.reposted_at > posts.reposted_at THEN excluded.repost_id ELSE posts.repost_id END,
                 reposted_at=max(posts.reposted_at, excluded.reposted_at),
                 handle=excluded.handle, text=excluded.text, raw=excluded.raw""",
            (p["id"], repost_id, a["id"], a["handle"], p["text"], p["posted_at"], reposted_at, json.dumps(raw)),
        )
        for i, m in enumerate(p["media"]):
            c.execute(
                "INSERT OR IGNORE INTO media(file, post_id, idx, type, url) VALUES (?, ?, ?, ?, ?)",
                (f"{p['id']}_{i}.{m['ext']}", p["id"], i, m["type"], m["url"]),
            )
        c.execute("DELETE FROM missing WHERE repost_id=?", (repost_id,))
        c.execute("DELETE FROM deleted WHERE post_id=?", (p["id"],))  # reposted again on X
    return "updated" if existed else "saved"


def save_missing(repost_id: str, tweet_id: str | None, reposted_at: str, reason: str, raw: dict | None) -> None:
    with db.tx() as c:
        c.execute(
            """INSERT INTO missing(repost_id, tweet_id, reposted_at, reason, raw) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(repost_id) DO UPDATE SET reason=excluded.reason, raw=excluded.raw""",
            (repost_id, tweet_id, reposted_at, reason, json.dumps(raw) if raw is not None else None),
        )
