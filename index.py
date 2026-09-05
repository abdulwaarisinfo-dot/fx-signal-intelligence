"""
FLINTEL — SIMPLIFIED FETCH-ONLY BACKGROUND SERVICE
====================================================
Platforms: Reddit (always on, no credentials) + Twitter/X (via RapidAPI,
only active if RAPID_API_KEY is set).

WHAT THIS SERVICE DOES (and nothing else):
  1. Reads pending "search jobs" from MongoDB collection `flintel_search_jobs`.
     Each job is written by the WEB SERVICE — it contains a topic_key plus a
     dynamically generated `keywords` list (fuzzy-matched from the user's
     search prompt). This service never hardcodes keywords — it only reads
     whatever the web service has patched in.
  2. For every keyword in a job, it fetches matching posts SITE-WIDE from
     Reddit (public search.json) and, if RAPID_API_KEY is configured, from
     Twitter/X (RapidAPI's twitter-api45) — from the last LOOKBACK_DAYS
     (default 180 = ~6 months). No subreddit targeting needed on either
     platform; each saved message just records whichever platform/source
     it actually came from.
  3. Saves every matched post as a raw, unscored message into MongoDB
     collection `flintel_signals`, tagged with the job's topic_key and
     platform ("reddit" or "twitter").
  4. Marks the job "done" so the web service can pick the results up.

WHAT THIS SERVICE DELIBERATELY DOES NOT DO (removed on purpose):
  - No hardcoded KEYWORDS / TARGET_SUBREDDITS python lists.
  - No subreddit-restricted search — fetching is keyword-only, site-wide.
  - No batching / batch-timeout / batch-gap logic.
  - No Claude scoring, no system prompts, no intent_score/tier/routing.
  - No Slack alerts, no HubSpot sync.
  - No Telegram / Facebook / LinkedIn pollers yet.
  - No unused Mongo collections/indexes left over from the old scoring
    pipeline (flintel_pending_batch, flintel_batch_seconds,
    flintel_rescore_messages, flintel_queue_messages, etc. are all gone).

This file is intentionally simple: one worker loop, one fetch function per
platform, one shared save function.
"""

import os
import time
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

# ─────────────────────────────────────────────────────────────────────────────
# ENV / CONFIG
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "flintel_bot")

# How often the worker checks for a new pending job when idle.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "2"))

# How many jobs can be processed IN PARALLEL. With this at 1 (old
# behaviour), a second job (e.g. "adidas") sits at status="pending" and
# waits until the first job (e.g. "nike") fully finishes before it's even
# picked up. Raising this lets multiple pending jobs start immediately,
# each on its own worker thread, instead of queuing behind one another.
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "3"))

# How far back to pull posts from. Reddit's search "t" param doesn't offer
# an exact 6-month bucket, so we request t=year (closest wider bucket) and
# then filter precisely to this many days in Python.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "180"))

# Small politeness delay between keyword searches so we don't hammer
# Reddit's public endpoint.
KEYWORD_GAP_SECONDS   = float(os.getenv("KEYWORD_GAP_SECONDS", "1.0"))

# No longer used by the fetch loop (search is keyword-only, site-wide —
# no per-subreddit looping). Kept as a harmless read so existing .env
# files with this variable set don't need to change.
SUBREDDIT_GAP_SECONDS = float(os.getenv("SUBREDDIT_GAP_SECONDS", "1.0"))

REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT", "flintel-fetch-service/1.0 (contact: ops@example.com)"
)

REDDIT_RESULTS_PER_QUERY = int(os.getenv("REDDIT_RESULTS_PER_QUERY", "100"))
REDDIT_REQUEST_TIMEOUT   = int(os.getenv("REDDIT_REQUEST_TIMEOUT", "15"))

# ── TWITTER / X — same RapidAPI approach as the original system
# (twitter-api45.p.rapidapi.com). Only active if RAPID_API_KEY is set;
# if it's missing, Twitter fetching is silently skipped (Reddit keeps
# working on its own either way). ──
RAPID_API_KEY          = os.getenv("RAPID_API_KEY", "")
TWITTER_ENABLED        = bool(RAPID_API_KEY)
TWITTER_HOST           = "twitter-api45.p.rapidapi.com"
TWITTER_RESULTS_PER_QUERY = int(os.getenv("TWITTER_RESULTS_PER_QUERY", "50"))
TWITTER_REQUEST_TIMEOUT   = int(os.getenv("TWITTER_REQUEST_TIMEOUT", "15"))

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel-fetch")

# ─────────────────────────────────────────────────────────────────────────────
# MONGODB
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    """Connects to MongoDB and ensures only the indexes this simplified
    service actually needs. No leftover indexes from the old scoring
    pipeline."""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        # Jobs queue — web service inserts here, this worker consumes.
        db.flintel_search_jobs.create_index(
            [("status", ASCENDING), ("requested_at", ASCENDING)],
            name="jobs_status_requested_at",
        )
        db.flintel_search_jobs.create_index(
            [("topic_key", ASCENDING)], name="jobs_topic_key"
        )

        # Raw fetched messages — this worker writes, web service reads.
        db.flintel_signals.create_index(
            [("message_id", ASCENDING)], unique=True, name="signals_message_id_unique"
        )
        db.flintel_signals.create_index(
            [("topic_key", ASCENDING)], name="signals_topic_key"
        )
        db.flintel_signals.create_index(
            [("search_keyword", ASCENDING)], name="signals_search_keyword"
        )
        db.flintel_signals.create_index(
            [("created_utc", ASCENDING)], name="signals_created_utc"
        )

        log.info(f"MongoDB connected | db={MONGODB_DB}")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# JOB QUEUE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_next_pending_job():
    """Atomically claims the oldest pending job so multiple worker instances
    (if ever scaled horizontally) never process the same job twice."""
    return db.flintel_search_jobs.find_one_and_update(
        {"status": "pending"},
        {"$set": {"status": "processing", "started_at": datetime.now(timezone.utc)}},
        sort=[("requested_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def mark_job_done(job_id, matched_count: int):
    db.flintel_search_jobs.update_one(
        {"_id": job_id},
        {"$set": {
            "status": "done",
            "matched_count": matched_count,
            "completed_at": datetime.now(timezone.utc),
        }},
    )


def mark_job_error(job_id, error: str):
    db.flintel_search_jobs.update_one(
        {"_id": job_id},
        {"$set": {
            "status": "error",
            "error": error,
            "completed_at": datetime.now(timezone.utc),
        }},
    )


def enqueue_search_job(topic_key: str, keywords: list, targeting_platform: str = "all") -> str:
    """Helper the web service can call (directly, or you can wrap this in a
    tiny insert from your own web-service code) to queue a new fetch job.
    Re-queuing the same topic_key simply resets it to pending — no
    duplicate jobs pile up for the same topic.

    targeting_platform: "reddit" -> Reddit only, "x_twitter" -> Twitter
    only, "all" (default) -> both."""
    topic_key = topic_key.strip().lower()
    targeting_platform = (targeting_platform or "all").strip().lower()
    db.flintel_search_jobs.update_one(
        {"topic_key": topic_key},
        {"$set": {
            "topic_key": topic_key,
            "keywords": keywords,
            "targeting_platform": targeting_platform,
            "status": "pending",
            "requested_at": datetime.now(timezone.utc),
            "started_at": None,
            "completed_at": None,
            "matched_count": 0,
            "error": None,
        }},
        upsert=True,
    )
    log.info(
        f"Job queued | topic_key={topic_key} | keywords={len(keywords)} | "
        f"targeting_platform={targeting_platform}"
    )
    return topic_key


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT FETCH (public search.json — no OAuth required)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_reddit_search(keyword: str) -> list:
    """Searches ALL of Reddit (site-wide, not restricted to any subreddit)
    for ONE keyword using Reddit's public search endpoint. Returns raw post
    dicts, each carrying whichever subreddit it actually came from. Never
    raises — a failure here just means zero results for this keyword; it
    never blocks the rest of the job."""
    url = "https://www.reddit.com/search.json"
    params = {
        "q": keyword,
        "sort": "new",
        "limit": REDDIT_RESULTS_PER_QUERY,
        "t": "year",  # widest bucket Reddit offers; precise 6-month cutoff applied below
    }
    headers = {"User-Agent": REDDIT_USER_AGENT}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=REDDIT_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        children = data.get("data", {}).get("children", [])

        posts = []
        for child in children:
            p = child.get("data", {})
            post_id = p.get("id")
            if not post_id:
                continue
            permalink = p.get("permalink", "") or ""
            posts.append({
                "id":            post_id,
                "title":         p.get("title", "") or "",
                "selftext":      p.get("selftext", "") or "",
                "author":        p.get("author", "unknown") or "unknown",
                "subreddit":     p.get("subreddit", "unknown") or "unknown",
                "post_url":      f"https://reddit.com{permalink}" if permalink else "",
                "created_utc":   p.get("created_utc"),
                "score":         p.get("score", 0),
                "num_comments":  p.get("num_comments", 0),
            })
        return posts

    except Exception as exc:
        log.warning(f"[REDDIT] search failed | keyword='{keyword}' | {exc}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X FETCH — same twitter-api45.p.rapidapi.com approach as the
# original system. One keyword per call, no restriction beyond the query
# itself. Reads the SAME job keywords from MongoDB — nothing hardcoded.
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_twitter_search(keyword: str) -> list:
    """Searches Twitter/X for ONE keyword via RapidAPI's twitter-api45.
    Returns raw post dicts in the same shape _fetch_reddit_search uses, so
    both platforms can be saved through the same _save_signal() call.
    Never raises — a failure here just means zero results for this
    keyword; it never blocks the rest of the job."""
    if not TWITTER_ENABLED:
        return []

    url = f"https://{TWITTER_HOST}/search.php"
    params = {
        "query": keyword,
        "search_type": "Top",
    }
    headers = {
        "x-rapidapi-key":  RAPID_API_KEY,
        "x-rapidapi-host": TWITTER_HOST,
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=TWITTER_REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("timeline") or data.get("results") or data.get("tweets") or []

        posts = []
        for t in results[:TWITTER_RESULTS_PER_QUERY]:
            if not isinstance(t, dict):
                continue
            tweet_id = str(t.get("tweet_id") or t.get("id") or "")
            if not tweet_id:
                continue

            text = t.get("text") or t.get("full_text") or ""
            author_obj = t.get("author") or t.get("user") or {}
            username = (
                t.get("screen_name")
                or author_obj.get("screen_name")
                or author_obj.get("username")
                or f"user_{tweet_id}"
            )

            # Twitter's created_at looks like "Wed Oct 10 20:19:24 +0000 2018".
            # Try to parse it for the same 6-month cutoff filtering Reddit
            # posts go through; if it's missing/unparseable, leave it as
            # None — the job loop below simply won't filter that post out.
            created_utc = None
            raw_created = t.get("created_at")
            if raw_created:
                try:
                    parsed = datetime.strptime(raw_created, "%a %b %d %H:%M:%S %z %Y")
                    created_utc = parsed.timestamp()
                except Exception:
                    created_utc = None

            posts.append({
                "id":            tweet_id,
                "title":         text,     # tweets have no separate title — the tweet text IS the message
                "selftext":      "",
                "author":        username,
                "subreddit":     "",       # not applicable to Twitter
                "post_url":      f"https://twitter.com/{username}/status/{tweet_id}",
                "created_utc":   created_utc,
                "score":         t.get("favorites", 0) or t.get("favorite_count", 0) or 0,
                "num_comments":  t.get("replies", 0) or t.get("reply_count", 0) or 0,
            })
        return posts

    except Exception as exc:
        log.warning(f"[TWITTER] search failed | keyword='{keyword}' | {exc}")
        return []




# ─────────────────────────────────────────────────────────────────────────────
# SAVE FETCHED MESSAGES
# ─────────────────────────────────────────────────────────────────────────────

def _save_signal(topic_key: str, matched_keyword: str, platform: str, post: dict) -> bool:
    """Upserts a raw fetched post (Reddit or Twitter — same shape from
    either fetch function) into flintel_signals. No scoring, no Claude, no
    derived fields — just the raw message plus which topic/keyword found
    it. Duplicate posts (already fetched for this topic before) are
    silently skipped via the unique index."""
    created = post.get("created_utc")
    created_dt = (
        datetime.fromtimestamp(created, tz=timezone.utc) if created else datetime.now(timezone.utc)
    )

    text = post["title"]
    if post.get("selftext") and post["selftext"].strip().lower() != post["title"].strip().lower():
        text = f"{post['title']}\n\n{post['selftext']}"

    doc = {
        "message_id":      f"{platform}_{post['id']}",
        "topic_key":        topic_key,
        "search_keyword":   matched_keyword,
        "platform":         platform,
        "subreddit":        post.get("subreddit", ""),
        "username":         post["author"],
        "title":            post["title"],
        "text":             text,
        "post_url":         post.get("post_url", ""),
        "score":            post.get("score", 0),
        "num_comments":     post.get("num_comments", 0),
        "created_utc":      created_dt,
        "fetched_at":       datetime.now(timezone.utc),
    }

    try:
        db.flintel_signals.insert_one(doc)
        return True
    except DuplicateKeyError:
        # Already fetched this post before (possibly for a different
        # topic/keyword search that also matched it) — not an error.
        return False
    except Exception as exc:
        log.error(f"[MONGO] save_signal error | message_id={doc['message_id']} | {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# JOB PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def process_job(job: dict) -> int:
    topic_key = job["topic_key"]
    keywords  = job.get("keywords") or []

    # NEW: targeting_platform controls which platform(s) this job fetches
    # from. "reddit" -> Reddit only, no Twitter call at all. "x_twitter" ->
    # Twitter only, no Reddit call at all. "all" (or missing, for backward
    # compatibility with jobs queued before this field existed) -> both,
    # exactly as before.
    targeting_platform = (job.get("targeting_platform") or "all").strip().lower()
    fetch_reddit  = targeting_platform in ("reddit", "all")
    fetch_twitter = targeting_platform in ("x_twitter", "all") and TWITTER_ENABLED

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    log.info(
        f"[JOB] START | topic_key={topic_key} | {len(keywords)} keyword(s) | "
        f"site-wide search | targeting_platform={targeting_platform} | "
        f"reddit={'on' if fetch_reddit else 'off'} | twitter={'on' if fetch_twitter else 'off'}"
    )

    matched_count = 0

    for keyword in keywords:
        # ── Reddit (always on, no credentials required — unless this job
        #    is scoped to x_twitter only) ──
        if fetch_reddit:
            reddit_posts = _fetch_reddit_search(keyword)
            for post in reddit_posts:
                created = post.get("created_utc")
                if created is None:
                    continue
                post_dt = datetime.fromtimestamp(created, tz=timezone.utc)
                if post_dt < cutoff:
                    continue  # older than lookback window — skip

                saved = _save_signal(topic_key, keyword, "reddit", post)
                if saved:
                    matched_count += 1

        # ── Twitter/X (only if RAPID_API_KEY is configured AND this job's
        #    targeting_platform allows it) ──
        if fetch_twitter:
            twitter_posts = _fetch_twitter_search(keyword)
            for post in twitter_posts:
                created = post.get("created_utc")
                if created is not None:
                    post_dt = datetime.fromtimestamp(created, tz=timezone.utc)
                    if post_dt < cutoff:
                        continue  # older than lookback window — skip
                # created is None (couldn't parse Twitter's timestamp) —
                # keep the post rather than silently dropping it.

                saved = _save_signal(topic_key, keyword, "twitter", post)
                if saved:
                    matched_count += 1

        time.sleep(KEYWORD_GAP_SECONDS)

    log.info(f"[JOB] DONE | topic_key={topic_key} | new_messages_saved={matched_count}")
    return matched_count




# ─────────────────────────────────────────────────────────────────────────────
# WORKER LOOP
# ─────────────────────────────────────────────────────────────────────────────
#
# fetch_next_pending_job() uses find_one_and_update, which MongoDB performs
# atomically — so multiple threads calling it at the same time can NEVER
# both claim the same job. This makes it safe to run several of these
# loops in parallel (see start_worker_pool below): each thread independently
# grabs the next available pending job and works on it, so a second search
# (e.g. "adidas") is never stuck waiting behind a first one (e.g. "nike").
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(worker_id: int = 0):
    log.info(f"[WORKER-{worker_id}] started | poll_interval={POLL_INTERVAL_SECONDS}s | lookback_days={LOOKBACK_DAYS}")

    while True:
        try:
            job = fetch_next_pending_job()

            if not job:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            log.info(f"[WORKER-{worker_id}] picked up job | topic_key={job.get('topic_key')}")

            try:
                matched_count = process_job(job)
                mark_job_done(job["_id"], matched_count)
            except Exception as exc:
                log.error(f"[WORKER-{worker_id}] [JOB] error | topic_key={job.get('topic_key')} | {exc}")
                mark_job_error(job["_id"], str(exc))

        except Exception as exc:
            log.error(f"[WORKER-{worker_id}] unexpected error: {exc}")
            time.sleep(5)


def start_worker_pool():
    """Spawns WORKER_CONCURRENCY worker threads, all pulling from the same
    flintel_search_jobs queue. As soon as any thread is free, it picks up
    the next pending job — no job waits on another to finish."""
    threads = []
    for i in range(WORKER_CONCURRENCY):
        t = threading.Thread(target=run_worker, args=(i,), daemon=True, name=f"Worker-{i}")
        t.start()
        threads.append(t)

    log.info(f"Worker pool started | concurrency={WORKER_CONCURRENCY}")

    # Keep the main thread alive while the worker threads run.
    for t in threads:
        t.join()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL — SIMPLIFIED FETCH-ONLY BACKGROUND SERVICE")
    log.info("=" * 70)
    log.info("  Platform          : Reddit + Twitter/X (via RapidAPI)")
    log.info("  Fetch mode        : SITE-WIDE keyword search (no subreddit targeting needed)")
    log.info("  Keywords source   : MongoDB (flintel_search_jobs) — no hardcoded list")
    log.info(f"  Twitter/X         : {'ENABLED' if TWITTER_ENABLED else 'DISABLED (set RAPID_API_KEY to enable)'}")
    log.info(f"  Lookback window   : {LOOKBACK_DAYS} days (~6 months)")
    log.info("  Scoring           : NONE — raw messages only")
    log.info("  Slack / HubSpot   : REMOVED")
    log.info("  Batching          : REMOVED — fetch and save immediately")
    log.info(f"  Worker concurrency: {WORKER_CONCURRENCY} parallel jobs — a new search never waits on another")
    log.info(f"  MongoDB DB        : {MONGODB_DB}")
    log.info("=" * 70)

    start_worker_pool()
