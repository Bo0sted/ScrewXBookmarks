# ScrewXBookmarks

Self-hosted archive of your reposts on X. A server periodically reads your own profile timeline
(the same internal API the X website uses, via your session cookies), indexes every repost, and
downloads all media and author avatars to disk. A small web UI browses them by author, newest first.

## Layout on disk

```
<DATA_PATH>/
├── media/            # <tweet_id>_<n>.<ext>, flat
│   └── avatars/      # <author_id>_<hash>.<ext>, flat; old avatars are kept
├── db.sqlite         # index: authors, posts, media, sync state
└── x_session.db      # twscrape's session store (contains your cookies)
```

## Setup

1. `cp .env.example .env` and fill it in. For `X_COOKIES`, open x.com logged in, go to devtools →
   Storage → Cookies → `https://x.com`, and copy `auth_token` and `ct0`:
   `X_COOKIES="auth_token=...; ct0=..."`. These are equivalent to your password; keep `.env` private.
2. Create `docker-compose.yml` next to `.env`:

   ```yaml
   services:
     screwxbookmarks:
       build: ./server
       container_name: screwxbookmarks
       restart: unless-stopped
       user: "${PUID:-1000}:${PGID:-1000}"
       ports:
         - "${PORT:-8080}:8080"
       environment:
         X_USERNAME: ${X_USERNAME:?set X_USERNAME in .env}
         X_COOKIES: ${X_COOKIES:?set X_COOKIES in .env}
         QUIET_HOURS: ${QUIET_HOURS:-1-8}
         TZ: ${TZ:-UTC}
       volumes:
         - ${DATA_PATH:?set DATA_PATH in .env}:/data
   ```

3. `docker compose up -d --build`
4. Open `http://<server>:8080/sync`. The first run starts within ~1 minute, or press **Sync now**.

If X logs you out (or cookies expire), the sync page shows the error; paste fresh cookies into
`.env` and `docker compose up -d`.

## How syncing behaves

- Each run fetches a random 20–60 timeline pages, pausing a random ~4–30 s between pages (sometimes
  a few minutes). Runs repeat every 1–2.5 h while backfilling, then every 2–5 h. No scheduled runs
  start during `QUIET_HOURS`.
- A run first checks the top of your timeline for new reposts and stops once it sees 20 it already
  has. If the history backfill isn't finished, it then continues from where the last run stopped.
- Backfill is complete when X returns the end of your timeline. **X may cap how far back a profile
  timeline goes** (historically ~3,200 posts including your own tweets). The sync page shows the
  oldest indexed repost; if it stops short of 2019, the data archive is the fallback.
- Reposts whose original is deleted/protected are listed as *unavailable* on the sync page.
- Media comes from X's public CDN; failed downloads retry with backoff and can be retried manually.

## Development

```
python -m venv .venv && .venv/bin/pip install -r server/requirements.txt
set -a && . ./.env && set +a
DATA_DIR=$PWD/data .venv/bin/uvicorn app.main:app --app-dir server --reload
```
