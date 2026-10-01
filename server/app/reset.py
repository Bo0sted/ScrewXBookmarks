import logging
import os

from . import db, downloader
from .config import AVATAR_DIR, MEDIA_DIR, TWS_DB_PATH
from .thumbs import CACHE_DIR

log = logging.getLogger("reset")

# Children before parents (foreign keys).
TABLES = ("media", "avatars", "posts", "authors", "missing", "archive_items", "state")


def wipe_everything() -> None:
    """Deletes all indexed data, downloaded files, thumbnails and the stored X session. Caller must stop sync first."""
    with downloader.paused():
        with db.tx() as c:
            for t in TABLES:
                c.execute(f"DELETE FROM {t}")
        conn = db.connect()
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()

        removed = 0
        for d in (AVATAR_DIR, MEDIA_DIR, CACHE_DIR):
            for name in os.listdir(d):
                path = os.path.join(d, name)
                if os.path.isfile(path):
                    os.remove(path)
                    removed += 1

        for suffix in ("", "-wal", "-shm", "-journal"):
            if os.path.exists(TWS_DB_PATH + suffix):
                os.remove(TWS_DB_PATH + suffix)
    log.warning("all data wiped · %d files removed", removed)
