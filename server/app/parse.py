"""Turns a raw tweet object from X's internal GraphQL API into the fields we store."""

import html
import re
from datetime import datetime, timezone


class ParseError(ValueError):
    pass


def unwrap(obj: dict | None) -> dict:
    obj = obj or {}
    if obj.get("__typename") == "TweetWithVisibilityResults":
        return obj.get("tweet") or {}
    return obj


def find_reposts(page: dict, my_id: str) -> list[dict]:
    """Returns my retweets on a UserTweets page, in timeline order:
    [{repost_id, reposted_at, original}] where `original` is the raw retweeted tweet."""
    out, seen = [], set()

    def walk(n):
        if isinstance(n, list):
            for x in n:
                walk(x)
            return
        if not isinstance(n, dict):
            return
        t = unwrap(n) if n.get("__typename") == "TweetWithVisibilityResults" else n
        legacy = t.get("legacy")
        if isinstance(legacy, dict) and t.get("rest_id") and legacy.get("retweeted_status_result"):
            author = unwrap((((t.get("core") or {}).get("user_results")) or {}).get("result"))
            if str(author.get("rest_id")) == my_id and t["rest_id"] not in seen:
                seen.add(t["rest_id"])
                out.append({
                    "repost_id": t["rest_id"],
                    "reposted_at": x_date(legacy["created_at"]),
                    "original": (legacy["retweeted_status_result"] or {}).get("result") or {},
                })
            return
        for v in n.values():
            walk(v)

    walk(page)
    return out


def count_timeline_items(page: dict) -> int:
    """Number of non-cursor timeline entries on a page (0 means we've run off the end)."""
    n = 0

    def walk(x):
        nonlocal n
        if isinstance(x, list):
            for y in x:
                walk(y)
        elif isinstance(x, dict):
            eid = x.get("entryId")
            if isinstance(eid, str) and "content" in x:
                if not eid.startswith("cursor-"):
                    n += 1
                return
            for v in x.values():
                walk(v)

    walk(page)
    return n


def bottom_cursor(page: dict) -> str | None:
    found = None

    def walk(x):
        nonlocal found
        if found is not None:
            return
        if isinstance(x, list):
            for y in x:
                walk(y)
        elif isinstance(x, dict):
            if x.get("cursorType") == "Bottom" and isinstance(x.get("value"), str):
                found = x["value"]
                return
            for v in x.values():
                walk(v)

    walk(page)
    return found


def parse_user(u: dict) -> dict:
    legacy = u.get("legacy") or {}
    # X moved screen_name/name/avatar out of `legacy` in 2025; accept both layouts.
    core = u.get("core") or {}
    uid = u.get("rest_id") or legacy.get("id_str")
    handle = core.get("screen_name") or legacy.get("screen_name")
    name = core.get("name") or legacy.get("name") or handle
    avatar = (u.get("avatar") or {}).get("image_url") or legacy.get("profile_image_url_https")
    if not uid or not handle:
        raise ParseError("tweet has no usable author")
    if avatar:
        avatar = re.sub(r"_normal(\.\w+)$", r"\1", avatar)  # strip size suffix -> original resolution
    return {"id": uid, "handle": handle, "name": html.unescape(name), "avatar_url": avatar}


def _text(t: dict, legacy: dict) -> str:
    note = (((t.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result")) or {}
    if note.get("text"):
        text, entities = note["text"], note.get("entity_set") or {}
    else:
        text, entities = legacy.get("full_text", ""), legacy.get("entities") or {}
    for u in entities.get("urls") or []:
        if u.get("url") and u.get("expanded_url"):
            text = text.replace(u["url"], u["expanded_url"])
    for m in (legacy.get("entities") or {}).get("media") or []:
        if m.get("url"):
            text = text.replace(m["url"], "")
    return html.unescape(text).strip()


def _media(legacy: dict) -> list[dict]:
    items = (legacy.get("extended_entities") or {}).get("media") or (legacy.get("entities") or {}).get("media") or []
    out = []
    for m in items:
        kind = m.get("type")
        if kind == "photo" and m.get("media_url_https"):
            base = m["media_url_https"]
            ext = base.rsplit(".", 1)[-1].lower() if "." in base.rsplit("/", 1)[-1] else "jpg"
            out.append({"type": "photo", "url": f"{base}?name=orig", "ext": ext})
        elif kind in ("video", "animated_gif"):
            variants = [
                v for v in (m.get("video_info") or {}).get("variants") or []
                if v.get("content_type") == "video/mp4" and v.get("url")
            ]
            if variants:
                best = max(variants, key=lambda v: v.get("bitrate") or 0)
                out.append({"type": "gif" if kind == "animated_gif" else "video", "url": best["url"], "ext": "mp4"})
    return out


def x_date(s: str) -> str:
    return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_tweet(raw: dict) -> dict:
    t = unwrap(raw)
    if t.get("__typename") == "TweetTombstone":
        raise ParseError("original post is unavailable (deleted, protected or withheld)")
    legacy = t.get("legacy") or {}
    tid = t.get("rest_id") or legacy.get("id_str")
    if not tid or "created_at" not in legacy:
        raise ParseError("not a tweet object")
    user = unwrap((((t.get("core") or {}).get("user_results")) or {}).get("result"))
    return {
        "id": tid,
        "author": parse_user(user),
        "text": _text(t, legacy),
        "posted_at": x_date(legacy["created_at"]),
        "media": _media(legacy),
    }
