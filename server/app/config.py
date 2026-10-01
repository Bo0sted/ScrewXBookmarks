import os

# twscrape ships PostHog telemetry; must be disabled before it is imported anywhere.
os.environ["TWS_TELEMETRY"] = "0"
os.environ["DO_NOT_TRACK"] = "1"

DATA_DIR = os.environ.get("DATA_DIR", "/data")
MEDIA_DIR = os.path.join(DATA_DIR, "media")
AVATAR_DIR = os.path.join(MEDIA_DIR, "avatars")
DB_PATH = os.path.join(DATA_DIR, "db.sqlite")
TWS_DB_PATH = os.path.join(DATA_DIR, "x_session.db")

X_USERNAME = os.environ.get("X_USERNAME", "").lstrip("@")
X_COOKIES = os.environ.get("X_COOKIES", "")

# Local hours (TZ env) during which scheduled syncs won't start, e.g. "1-8". Empty = none.
QUIET_HOURS = os.environ.get("QUIET_HOURS", "1-8")

os.makedirs(AVATAR_DIR, exist_ok=True)
