import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS authors (
  id          TEXT PRIMARY KEY,
  handle      TEXT NOT NULL,
  name        TEXT NOT NULL,
  avatar_file TEXT,
  first_seen  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS avatars (
  file       TEXT PRIMARY KEY,
  author_id  TEXT NOT NULL REFERENCES authors(id),
  url        TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'pending',
  attempts   INTEGER NOT NULL DEFAULT 0,
  next_try   TEXT NOT NULL DEFAULT '',
  error      TEXT,
  first_seen TEXT NOT NULL,
  last_seen  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
  id          TEXT PRIMARY KEY,               -- original tweet id
  repost_id   TEXT NOT NULL UNIQUE,           -- id of my (latest) retweet of it
  author_id   TEXT NOT NULL REFERENCES authors(id),
  handle      TEXT NOT NULL,
  text        TEXT NOT NULL,
  posted_at   TEXT NOT NULL,
  reposted_at TEXT NOT NULL,
  raw         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS posts_by_author ON posts(author_id, reposted_at DESC);
CREATE INDEX IF NOT EXISTS posts_by_repost ON posts(reposted_at DESC);

CREATE TABLE IF NOT EXISTS media (
  file     TEXT PRIMARY KEY,
  post_id  TEXT NOT NULL REFERENCES posts(id),
  idx      INTEGER NOT NULL,
  type     TEXT NOT NULL,
  url      TEXT NOT NULL,
  status   TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_try TEXT NOT NULL DEFAULT '',
  error    TEXT
);
CREATE INDEX IF NOT EXISTS media_by_post ON media(post_id, idx);

-- Reposts seen on the timeline that couldn't be stored (deleted/withheld original, parse failure).
CREATE TABLE IF NOT EXISTS missing (
  repost_id   TEXT PRIMARY KEY,
  tweet_id    TEXT,
  reposted_at TEXT NOT NULL,
  reason      TEXT NOT NULL,
  raw         TEXT
);

-- Reposts listed in an uploaded X archive (IDs only), fetched one by one by the archive phase.
CREATE TABLE IF NOT EXISTS archive_items (
  repost_id   TEXT PRIMARY KEY,
  original_id TEXT,
  rt_handle   TEXT,
  reposted_at TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',  -- pending | saved | known (already had) | missing
  note        TEXT,
  updated_at  TEXT
);
CREATE INDEX IF NOT EXISTS archive_by_status ON archive_items(status, reposted_at DESC);

CREATE TABLE IF NOT EXISTS state (
  key   TEXT PRIMARY KEY,
  value TEXT
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def tx():
    conn = connect()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def get_state(key: str, default: str | None = None) -> str | None:
    with tx() as c:
        row = c.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row["value"] if row and row["value"] is not None else default


def set_state(**values) -> None:
    with tx() as c:
        c.executemany(
            "INSERT INTO state(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(k, None if v is None else str(v)) for k, v in values.items()],
        )


def init() -> None:
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()
