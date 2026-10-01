"""Tags on posts and authors.

Tagging an author tags each of their posts once, and every new post of theirs as it's saved (store.save).
A post can then be untagged on its own without affecting the author's tag. Untagging the author removes
the tag from all of their posts.
"""

import sqlite3

from . import db

MAX_NAME = 50
# URL kind -> (table of the tagged thing, link table, link column)
KINDS = {
    "posts": ("posts", "post_tags", "post_id"),
    "authors": ("authors", "author_tags", "author_id"),
}


class TagError(ValueError):
    pass


def _clean(name) -> str:
    name = " ".join(str(name or "").split())
    if not name:
        raise TagError("Tag name can't be empty")
    if len(name) > MAX_NAME:
        raise TagError(f"Tag names can be at most {MAX_NAME} characters")
    return name


def all_tags() -> list[dict]:
    with db.tx() as c:
        return [dict(r) for r in c.execute(
            """SELECT t.id, t.name,
                 (SELECT COUNT(*) FROM post_tags WHERE tag_id = t.id) AS posts,
                 (SELECT COUNT(*) FROM author_tags WHERE tag_id = t.id) AS authors
               FROM tags t ORDER BY t.name COLLATE NOCASE"""
        )]


def create(name) -> dict:
    name = _clean(name)
    try:
        with db.tx() as c:
            tag_id = c.execute("INSERT INTO tags(name, created_at) VALUES (?, ?)", (name, db.now())).lastrowid
    except sqlite3.IntegrityError:
        raise TagError(f"A tag named “{name}” already exists")
    return {"id": tag_id, "name": name}


def rename(tag_id: int, name) -> dict:
    name = _clean(name)
    try:
        with db.tx() as c:
            if not c.execute("UPDATE tags SET name=? WHERE id=?", (name, tag_id)).rowcount:
                raise TagError("Tag not found")
    except sqlite3.IntegrityError:
        raise TagError(f"A tag named “{name}” already exists")
    return {"id": tag_id, "name": name}


def delete(tag_id: int) -> None:
    with db.tx() as c:  # post_tags / author_tags rows go with it (ON DELETE CASCADE)
        if not c.execute("DELETE FROM tags WHERE id=?", (tag_id,)).rowcount:
            raise TagError("Tag not found")


def _check_target(c, kind: str, target_id: str) -> None:
    table = KINDS[kind][0]
    if not c.execute(f"SELECT 1 FROM {table} WHERE id=?", (target_id,)).fetchone():
        raise TagError("Post not found" if kind == "posts" else "User not found")


def for_target(kind: str, target_id: str) -> dict:
    """Every tag, plus the ids of the ones on this post/author."""
    _, link, col = KINDS[kind]
    with db.tx() as c:
        _check_target(c, kind, target_id)
        tags = [dict(r) for r in c.execute("SELECT id, name FROM tags ORDER BY name COLLATE NOCASE")]
        applied = [r[0] for r in c.execute(f"SELECT tag_id FROM {link} WHERE {col}=?", (target_id,))]
    return {"tags": tags, "applied": applied}


def add(kind: str, target_id: str, tag_id: int) -> None:
    _, link, col = KINDS[kind]
    with db.tx() as c:
        _check_target(c, kind, target_id)
        if not c.execute("SELECT 1 FROM tags WHERE id=?", (tag_id,)).fetchone():
            raise TagError("Tag not found")
        c.execute(f"INSERT OR IGNORE INTO {link}({col}, tag_id) VALUES (?, ?)", (target_id, tag_id))
        if kind == "authors":
            c.execute(
                "INSERT OR IGNORE INTO post_tags(post_id, tag_id) SELECT id, ? FROM posts WHERE author_id=?",
                (tag_id, target_id),
            )


def remove(kind: str, target_id: str, tag_id: int) -> None:
    _, link, col = KINDS[kind]
    with db.tx() as c:
        c.execute(f"DELETE FROM {link} WHERE {col}=? AND tag_id=?", (target_id, tag_id))
        if kind == "authors":
            c.execute(
                "DELETE FROM post_tags WHERE tag_id=? AND post_id IN (SELECT id FROM posts WHERE author_id=?)",
                (tag_id, target_id),
            )


def for_posts(c, post_ids: list[str]) -> dict[str, list[dict]]:
    """post id -> its tags (by name), for rendering feeds."""
    out: dict[str, list[dict]] = {}
    if post_ids:
        for r in c.execute(
            f"""SELECT pt.post_id, t.id, t.name FROM post_tags pt JOIN tags t ON t.id = pt.tag_id
                WHERE pt.post_id IN ({','.join('?' * len(post_ids))}) ORDER BY t.name COLLATE NOCASE""",
            post_ids,
        ):
            out.setdefault(r["post_id"], []).append({"id": r["id"], "name": r["name"]})
    return out
