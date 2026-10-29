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
  APIFY_TIMEOUT_SECONDS    default: 180 (max run time per actor call)
  APIFY_MAX_ITEMS          default: 50

Actor ids use "~" instead of "/" in the API URL (user~actor-name).
Never raises: any failure just returns [] (same contract as the other fetchers).
"""
import logging
import os
import time
from datetime import datetime, timezone

import requests

log = logging.getLogger("flintel.apify")

APIFY_BASE = "https://api.apify.com/v2"


def apify_enabled() -> bool:
    return bool(os.getenv("APIFY_TOKEN", ""))


_TERMINAL = {"SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"}


def _run_actor(actor_id: str, run_input: dict, expect_items: int = 0) -> list:
    """Starts the actor ASYNC, polls until it finishes (or our deadline hits),
    then reads the run's dataset. If expect_items > 0, the run is ABORTED as
    soon as the dataset holds that many items (some actors keep running after
    reaching maxItems and just burn compute until timeout). Unlike
    run-sync-get-dataset-items, a run that
    TIMES OUT still returns whatever results it already produced (those are
    billed anyway), and we are not limited by the sync endpoint's 300s cap."""
    token = os.getenv("APIFY_TOKEN", "")
    if not token:
        return []
    timeout = int(os.getenv("APIFY_TIMEOUT_SECONDS", "180"))
    headers = {"Authorization": f"Bearer {token}"}
    run_id = None
    t0 = time.time()
    try:
        r = requests.post(
            f"{APIFY_BASE}/acts/{actor_id}/runs",
            params={"timeout": timeout},
            headers=headers, json=run_input, timeout=30,
        )
        if r.status_code >= 400:
            log.warning(f"[APIFY] start failed | actor={actor_id} | HTTP {r.status_code} | body={r.text[:500]!r}")
            return []
        run = (r.json() or {}).get("data") or {}
        run_id = run.get("id")
        dataset_id = run.get("defaultDatasetId")
        if not run_id or not dataset_id:
            log.warning(f"[APIFY] unexpected start response | actor={actor_id} | {r.text[:300]!r}")
            return []

        # Poll (long-poll up to 30s per call) until finished or our deadline.
        deadline = time.time() + timeout + 20
        status = run.get("status")
        reached_target = False
        while status not in _TERMINAL and time.time() < deadline:
            g = requests.get(
                f"{APIFY_BASE}/actor-runs/{run_id}",
                params={"waitForFinish": 10},
                headers=headers, timeout=25,
            )
            if g.status_code >= 400:
                log.warning(f"[APIFY] poll failed | run={run_id} | HTTP {g.status_code} | body={g.text[:300]!r}")
                break
            status = ((g.json() or {}).get("data") or {}).get("status")
            if expect_items and status not in _TERMINAL:
                try:
                    di = requests.get(f"{APIFY_BASE}/datasets/{dataset_id}", headers=headers, timeout=15)
                    count = int(((di.json() or {}).get("data") or {}).get("itemCount") or 0)
                except Exception:
                    count = 0
                if count >= expect_items:
                    reached_target = True
                    break

        if status not in _TERMINAL:  # target reached or our deadline hit -> stop it so it stops billing
            try:
                requests.post(f"{APIFY_BASE}/actor-runs/{run_id}/abort", headers=headers, timeout=15)
            except Exception:
                pass
            status = "SUCCEEDED" if reached_target else "ABORTED"

        # Read whatever the run produced - even on TIMED-OUT / ABORTED / FAILED.
        d = requests.get(
            f"{APIFY_BASE}/datasets/{dataset_id}/items",
            params={"format": "json", "clean": "true"},
            headers=headers, timeout=60,
        )
        if d.status_code >= 400:
            log.warning(f"[APIFY] dataset read failed | run={run_id} | HTTP {d.status_code} | body={d.text[:300]!r}")
            return []
        items = d.json()
        items = items if isinstance(items, list) else []
        if status != "SUCCEEDED":
            log.warning(f"[APIFY] run ended {status} | actor={actor_id} | run={run_id} | partial_items={len(items)}")
        else:
            log.info(
                f"[APIFY] 200 OK | actor={actor_id} | run={run_id} | status={status} | "
                f"items={len(items)} | {time.time() - t0:.1f}s"
            )
        return items
    except Exception as exc:
        log.warning(f"[APIFY] actor failed | actor={actor_id} | run={run_id} | {exc}")
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
    }, expect_items=max_items)
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


def fetch_reddit_search_many(keywords: list) -> list:
    """ONE actor run for MANY keywords (the actor accepts a `searches` array).
    maxItems = APIFY_MAX_ITEMS per keyword, as a TOTAL cap for the run; the run
    is aborted early as soon as that many items exist. Never raises."""
    keywords = [str(k).strip() for k in keywords if str(k).strip()]
    if not keywords:
        return []
    actor = os.getenv("APIFY_REDDIT_ACTOR", "trudax~reddit-scraper-lite")
    per_kw = int(os.getenv("APIFY_MAX_ITEMS", "50"))
    max_items = per_kw * len(keywords)
    items = _run_actor(actor, {
        "searches": keywords,
        "searchPosts": True,
        "searchComments": False,
        "sort": "new",
        "maxItems": max_items,
        "skipUserPosts": True,
        "skipComments": True,
        "proxy": {"useApifyProxy": True},
    }, expect_items=max_items)
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


def fetch_reddit_search(keyword: str) -> list:
    return fetch_reddit_search_many([keyword])
