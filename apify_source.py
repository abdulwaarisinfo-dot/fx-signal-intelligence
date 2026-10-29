"""
apify_source.py
===============
Apify integration for Flintel. Runs an Apify Actor synchronously and returns
posts in the SAME dict shape the rest of the service already uses
(id, title, selftext, author, subreddit, post_url, created_utc, score,
num_comments), so they go through _save_signal() unchanged.

Env vars:
  APIFY_TOKEN              Apify API token (required)
  TWITTER_PROVIDER         "apify" to use Apify for Twitter (default: rapidapi)
  APIFY_TWITTER_ACTOR      default: apidojo~tweet-scraper
  APIFY_REDDIT_ACTOR       default: trudax~reddit-scraper-lite
  APIFY_TIMEOUT_SECONDS    default: 120
  APIFY_MAX_ITEMS          default: 50

Actor ids use "~" instead of "/" in the API URL (user~actor-name).
Never raises: any failure just returns [] (same contract as the other fetchers).
"""
import logging
import os
from datetime import datetime, timezone

import requests

log = logging.getLogger("flintel.apify")

APIFY_BASE = "https://api.apify.com/v2"


def apify_enabled() -> bool:
    return bool(os.getenv("APIFY_TOKEN", ""))


def _run_actor(actor_id: str, run_input: dict) -> list:
    """Run an actor and return its dataset items (run-sync-get-dataset-items)."""
    token = os.getenv("APIFY_TOKEN", "")
    if not token:
        return []
    timeout = int(os.getenv("APIFY_TIMEOUT_SECONDS", "120"))
    url = f"{APIFY_BASE}/acts/{actor_id}/run-sync-get-dataset-items"
    try:
        resp = requests.post(
            url,
            params={"timeout": timeout},
            headers={"Authorization": f"Bearer {token}"},
            json=run_input,
            timeout=timeout + 15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        log.warning(f"[APIFY] actor failed | actor={actor_id} | {exc}")
        return []


def _first(d: dict, keys, default=None):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return default


def _to_ts(value):
    """ISO string / Twitter-style string / epoch -> epoch seconds, else None."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value) / 1000 if value > 1e11 else float(value)
    s = str(value)
    for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt).timestamp()
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def fetch_twitter_search(keyword: str) -> list:
    actor = os.getenv("APIFY_TWITTER_ACTOR", "apidojo~tweet-scraper")
    max_items = int(os.getenv("APIFY_MAX_ITEMS", "50"))
    items = _run_actor(actor, {
        "searchTerms": [keyword],
        "maxItems": max_items,
        "sort": "Latest",
    })
    posts = []
    for t in items:
        if not isinstance(t, dict) or t.get("noResults"):
            continue
        tweet_id = str(_first(t, ["id", "id_str", "tweetId"], ""))
        if not tweet_id:
            continue
        author_obj = t.get("author") or t.get("user") or {}
        username = (
            _first(author_obj, ["userName", "screen_name", "username"])
            or _first(t, ["userName", "screen_name"])
            or f"user_{tweet_id}"
        )
        posts.append({
            "id":           tweet_id,
            "title":        _first(t, ["text", "fullText", "full_text"], ""),
            "selftext":     "",
            "author":       username,
            "subreddit":    "",
            "post_url":     _first(t, ["url", "twitterUrl"], f"https://twitter.com/{username}/status/{tweet_id}"),
            "created_utc":  _to_ts(_first(t, ["createdAt", "created_at"])),
            "score":        _first(t, ["likeCount", "favorite_count"], 0) or 0,
            "num_comments": _first(t, ["replyCount", "reply_count"], 0) or 0,
        })
    return posts


def fetch_reddit_search(keyword: str) -> list:
    actor = os.getenv("APIFY_REDDIT_ACTOR", "trudax~reddit-scraper-lite")
    max_items = int(os.getenv("APIFY_MAX_ITEMS", "50"))
    items = _run_actor(actor, {
        "searches": [keyword],
        "type": "posts",
        "sort": "new",
        "maxItems": max_items,
        "skipComments": True,
    })
    posts = []
    for r in items:
        if not isinstance(r, dict):
            continue
        pid = str(_first(r, ["parsedId", "id"], ""))
        if not pid:
            continue
        # Same id format as the RSS / RapidAPI paths ("t3_<postid>") so a post
        # already stored from RSS is recognised as a duplicate, not re-saved.
        if not pid.startswith("t3_"):
            pid = f"t3_{pid}"
        posts.append({
            "id":           pid,
            "title":        _first(r, ["title"], ""),
            "selftext":     _first(r, ["body", "selftext"], ""),
            "author":       _first(r, ["username", "author"], ""),
            "subreddit":    _first(r, ["parsedCommunityName", "communityName", "subreddit"], ""),
            "post_url":     _first(r, ["url", "link"], ""),
            "created_utc":  _to_ts(_first(r, ["createdAt", "created_utc"])),
            "score":        _first(r, ["upVotes", "score"], 0) or 0,
            "num_comments": _first(r, ["numberOfComments", "num_comments"], 0) or 0,
        })
    return posts
