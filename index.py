"""
FLINTEL — SIMPLIFIED FETCH-ONLY BACKGROUND SERVICE
====================================================
Platforms: Reddit (RSS, always on, no credentials) + Twitter/X (via
RapidAPI, only active if RAPID_API_KEY is set).

WHAT THIS SERVICE DOES (and nothing else):
  1. Reads pending "search jobs" from MongoDB collection `flintel_search_jobs`.
     Each job is written by the WEB SERVICE — it contains a topic_key plus a
     dynamically generated `keywords` list (fuzzy-matched from the user's
     search prompt). This service never hardcodes keywords — it only reads
     whatever the web service has patched in.

  2. Reddit — CONTINUOUS, ALWAYS-ON POLLER (this is the part that changed):
     Instead of doing ONE RSS fetch tied to a single job's lifecycle, a
     dedicated background thread (`run_reddit_poller`) loops FOREVER, on its
     own timer (REDDIT_POLL_INTERVAL_SECONDS), completely independent of any
     job's status. On every cycle it:
       a) Fetches the site-wide r/all "new posts" RSS feed ONCE.
       b) Re-reads flintel_search_jobs and builds a fresh, in-memory
          keyword list out of EVERY job in the collection (not just
          "pending" ones — a job's keywords are live/patched-in by the web
          service at any time, so this list is rebuilt every cycle).
       c) Matches every RSS entry against every job's keywords locally,
          and for every match saves the post into flintel_signals tagged
          with that job's topic_key + whichever keyword matched.
     This never waits for a job to be picked up / claimed / marked
     "pending" — it just keeps running, cycle after cycle, forever,
     accumulating matches over time. This is what lets the site-wide RSS
     firehose (which only ever shows a rolling few-minutes-to-hours window)
     build up real historical-style coverage: the more it stays alive, the
     more of that rolling window it has actually seen and saved.

  3. Twitter/X (if RAPID_API_KEY is set): UNCHANGED — still one RapidAPI
     search call per keyword, per job, done inside `process_job` exactly as
     before, since that API supports real query search and doesn't need a
     "keep polling forever" workaround.

  4. Saves every matched post as a raw, unscored message into MongoDB
     collection `flintel_signals`, tagged with the job's topic_key and
     platform ("reddit" or "twitter"). Duplicate posts are silently
     skipped via the unique index on message_id — so the continuous poller
     re-fetching the same RSS window over and over is harmless; it will
     just keep hitting DuplicateKeyError for posts it already saved and
     only insert genuinely new ones.

  5. Job status (`flintel_search_jobs.status`) is still driven by the
     worker pool (`process_job` / `run_worker`), exactly as before, for
     Twitter's sake and so the web service still has a "done" signal to
     watch. Reddit matches are NOT tied to a specific job's matched_count
     anymore, since Reddit is no longer fetched inside a single job run —
     it's continuous and shared across every job's keywords at once.

⚠️ IMPORTANT TRADE-OFF — READ THIS:
  RSS only ever shows Reddit's current "new posts" window — a rolling,
  very recent set (roughly the last few minutes to a couple of hours of
  site-wide activity, depending on how busy Reddit is), NOT a searchable
  6-month history. The old .json search endpoint could pull posts from up
  to a year back for an exact keyword; this RSS feed cannot — it has no
  concept of "search for X", only "here's what's newest right now".
  LOOKBACK_DAYS / cutoff filtering is kept for consistency but will
  almost always be a no-op here, since RSS entries are always fresh.
  This is exactly why Reddit fetching is now a continuous always-on
  poller instead of a single fetch-per-job: a poller that keeps running
  and saving every new entry it hasn't seen before actually accumulates
  matches as time passes, which a single on-demand fetch never could.
  This was switched deliberately (per explicit request) from the
  more powerful but frequently-blocked-from-cloud-IPs .json search
  endpoint back to RSS, trading search power for reliability.

WHAT THIS SERVICE DELIBERATELY DOES NOT DO (removed on purpose):
  - No hardcoded KEYWORDS / TARGET_SUBREDDITS python lists.
  - No subreddit-restricted fetching — r/all covers everything in one feed.
  - No batching / batch-timeout / batch-gap logic.
  - No Claude scoring, no system prompts, no intent_score/tier/routing.
  - No Slack alerts, no HubSpot sync.
  - No Telegram / Facebook / LinkedIn pollers yet.
  - No unused Mongo collections/indexes left over from the old scoring
    pipeline (flintel_pending_batch, flintel_batch_seconds,
    flintel_rescore_messages, flintel_queue_messages, etc. are all gone).

Requires the `feedparser` package (pip install feedparser) for RSS parsing.

This file is intentionally simple: one always-on Reddit poller loop, one
job-driven worker pool for Twitter + job status, one shared save function.
"""

import os
import re
import html
import time
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests
import feedparser
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

# ─────────────────────────────────────────────────────────────────────────────
# ENV / CONFIG
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    """Parses a True/False on-off switch from an env var. Accepts
    true/false/1/0/yes/no (case-insensitive). Falls back to `default` if
    the var isn't set."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "flintel_bot")

# How often the worker checks for a new pending job when idle.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "2"))

# How often the ALWAYS-ON Reddit poller re-fetches the r/all RSS feed and
# re-reads the current keyword list from flintel_search_jobs. This loop
# never stops and never waits on any job's "pending" status.
REDDIT_POLL_INTERVAL_SECONDS = int(os.getenv("REDDIT_POLL_INTERVAL_SECONDS", "30"))

# Master ON/OFF switch for Reddit fetching. REDDIT_ENABLED=True (default)
# -> the poller keeps fetching/matching every cycle, same as always.
# REDDIT_ENABLED=False -> the poller thread stays alive (heartbeat/status
# still updates) but it skips fetching + matching entirely every cycle —
# Reddit fetching is simply stopped until this is flipped back to True.
#
# IMPORTANT: this is checked LIVE every single cycle via
# _is_reddit_enabled() below (not read once at startup) — so editing
# REDDIT_ENABLED in .env while the service is already running takes
# effect on the very next cycle, no restart needed. Flip it back to
# True and the next cycle rebuilds the keyword list from scratch and
# starts matching again, exactly as if freshly started.
def _is_reddit_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("REDDIT_ENABLED", True)

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

# Site-wide RSS feed — r/all aggregates new posts across (most) subreddits.
# limit= is respected by Reddit's RSS the same way it is by its old JSON
# endpoint (up to 100).
REDDIT_RSS_URL = os.getenv("REDDIT_RSS_URL", "https://www.reddit.com/r/all/new.rss")

REDDIT_RESULTS_PER_QUERY = int(os.getenv("REDDIT_RESULTS_PER_QUERY", "100"))
REDDIT_REQUEST_TIMEOUT   = int(os.getenv("REDDIT_REQUEST_TIMEOUT", "15"))

# ── TWITTER / X — same RapidAPI approach as the original system
# (twitter-api45.p.rapidapi.com). Only active if RAPID_API_KEY is set AND
# TWITTER_ENABLED is True; if either condition fails, Twitter fetching is
# silently skipped (Reddit keeps working on its own either way). ──
RAPID_API_KEY          = os.getenv("RAPID_API_KEY", "")

# Master ON/OFF switch for Twitter fetching, same idea/shape as
# REDDIT_ENABLED above. TWITTER_ENABLED=True (default) -> Twitter is
# fetched per-job as long as RAPID_API_KEY is also set. TWITTER_ENABLED=False
# -> Twitter fetching is stopped completely, even if RAPID_API_KEY is set.
#
# IMPORTANT: also checked LIVE every time via _is_twitter_enabled() below
# (not cached at startup) — editing TWITTER_ENABLED in .env while the
# service is running takes effect on the very next job, no restart needed.
def _is_twitter_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("TWITTER_ENABLED", True) and bool(os.getenv("RAPID_API_KEY", ""))


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

        # Live status/heartbeat flags — lets anything outside this process
        # check whether the Reddit poller and the Twitter/job worker pool
        # are currently running (True) or stopped (False).
        db.flintel_service_status.create_index(
            [("service", ASCENDING)], unique=True, name="service_status_service_unique"
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
    only, "all" (default) -> both.

    NOTE: this still exists and still works exactly as before — the
    always-on Reddit poller reads keywords straight out of whatever docs
    exist in flintel_search_jobs every cycle, so a job created here shows
    up in the poller's keyword list on its very next cycle without needing
    to be "picked up" first."""
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


def _set_service_status(service_name: str, running: bool):
    """Upserts a simple True/False heartbeat flag into
    flintel_service_status — one doc per service (e.g. "reddit_poller",
    "twitter_worker"). running=True means it's actively looping right now;
    running=False means it has stopped/crashed. Same shape/behaviour used
    for both Reddit and Twitter so anything outside this process (web
    service, monitoring dashboard, etc.) can check either the same way:

        db.flintel_service_status.find_one({"service": "reddit_poller"})
        db.flintel_service_status.find_one({"service": "twitter_worker"})

    Never raises — a DB hiccup while updating status never crashes the
    actual poller/worker loop."""
    try:
        db.flintel_service_status.update_one(
            {"service": service_name},
            {"$set": {
                "service": service_name,
                "running": running,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.warning(f"[STATUS] failed to update status | service={service_name} | running={running} | {exc}")


def _load_all_jobs_keyword_map() -> list:
    """Builds a fresh list of every job currently sitting in
    flintel_search_jobs — REGARDLESS of status ("pending", "processing",
    "done", "error" — all of them). This is what makes the Reddit poller
    "always know" every keyword that's ever been patched in by the web
    service, without waiting for a job to be claimed.

    Each web-service write to a job's `keywords` field (even on an
    already-"done" job) is picked up automatically on the poller's very
    next cycle, since this re-reads the collection from scratch every
    time it's called.

    Returns a list of dicts: {topic_key, keywords, targeting_platform}.
    Jobs with an empty/missing keywords list are skipped (nothing to
    match against). Never raises — a DB hiccup here just means this
    cycle matches against zero jobs; it never crashes the poller."""
    jobs = []
    try:
        cursor = db.flintel_search_jobs.find(
            {}, {"topic_key": 1, "keywords": 1, "targeting_platform": 1}
        )
        for doc in cursor:
            keywords = doc.get("keywords") or []
            if not keywords:
                continue
            jobs.append({
                "topic_key": doc.get("topic_key", ""),
                "keywords": keywords,
                "targeting_platform": (doc.get("targeting_platform") or "all").strip().lower(),
            })
    except Exception as exc:
        log.warning(f"[REDDIT-POLLER] failed to load jobs for keyword map: {exc}")

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT FETCH — site-wide RSS (r/all), NOT the .json search endpoint.
# ─────────────────────────────────────────────────────────────────────────────

def _match_any_keyword(text: str, keywords: list):
    """Case-insensitive substring match against every keyword in the job.
    Returns the FIRST matching keyword (so it can be recorded as
    search_keyword), or None if nothing matched. Same simple approach the
    very first version of this service used for keyword pre-filtering."""
    t = (text or "").lower()
    for kw in keywords:
        if kw and kw.lower() in t:
            return kw
    return None


def _fetch_reddit_rss_feed() -> list:
    """Fetches the SITE-WIDE r/all "new posts" RSS feed — ONE request, no
    query/keyword parameter (plain RSS doesn't support one). Every job's
    keywords are matched against these entries locally afterwards. Each
    entry's real subreddit is parsed straight out of its own permalink, so
    no subreddit targeting is needed — r/all already spans (most)
    subreddits in one feed. Never raises — a failure here just means zero
    entries this round; it never blocks the poller's next cycle."""
    url = f"{REDDIT_RSS_URL}?limit={REDDIT_RESULTS_PER_QUERY}"
    headers = {"User-Agent": REDDIT_USER_AGENT}

    entries = []
    try:
        resp = requests.get(url, headers=headers, timeout=REDDIT_REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)

        if feed.bozo and not feed.entries:
            log.warning(f"[REDDIT-RSS] feed parse issue: {feed.bozo_exception}")
            return entries

        for entry in feed.entries:
            entry_id = entry.get("id", "") or entry.get("link", "")
            if not entry_id:
                continue

            link = entry.get("link", "") or ""
            subreddit_match = re.search(r"/r/([^/]+)/", link)
            subreddit = subreddit_match.group(1) if subreddit_match else "unknown"

            title = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(summary)).strip()
            selftext = summary_plain if summary_plain.lower() != title.lower() else ""

            author = entry.get("author", "unknown").lstrip("u/").strip() or "unknown"

            published_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            created_utc = time.mktime(published_struct) if published_struct else None

            entries.append({
                "id":            entry_id.split("/")[-1] or entry_id,
                "title":         title,
                "selftext":      selftext,
                "author":        author,
                "subreddit":     subreddit,
                "post_url":      link,
                "created_utc":   created_utc,
                "score":         0,
                "num_comments":  0,
            })

    except Exception as exc:
        log.warning(f"[REDDIT-RSS] fetch failed: {exc}")

    return entries


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
    if not _is_twitter_enabled():
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
    it. Duplicate posts (already fetched before, by this cycle or a past
    one) are silently skipped via the unique index — this is exactly what
    makes it safe for the Reddit poller to keep re-fetching the same
    rolling RSS window over and over: only genuinely new posts get
    inserted."""
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
# REDDIT — ALWAYS-ON POLLER (runs forever, independent of the job queue)
# ─────────────────────────────────────────────────────────────────────────────
#
# This is the piece that makes Reddit coverage continuous instead of a
# single fetch-per-job snapshot. It never checks job "status", never
# claims/marks jobs, and never stops. It just:
#   loop forever:
#     1. fetch r/all RSS once
#     2. rebuild the keyword list from EVERY job in flintel_search_jobs
#     3. match + save
#     4. sleep REDDIT_POLL_INTERVAL_SECONDS, repeat
#
# Because step 2 re-reads the jobs collection from scratch every cycle,
# any keywords the web service patches into any job (new or existing) are
# picked up automatically on the very next cycle — no restart needed, no
# waiting for a "pending" status.
# ─────────────────────────────────────────────────────────────────────────────

def run_reddit_poller():
    log.info(
        f"[REDDIT-POLLER] started | interval={REDDIT_POLL_INTERVAL_SECONDS}s | "
        f"lookback_days={LOOKBACK_DAYS} (mostly no-op on a live RSS feed)"
    )

    cutoff_days = timedelta(days=LOOKBACK_DAYS)

    try:
        while True:
            # Mark alive at the top of every cycle — as long as this loop
            # keeps turning, flintel_service_status.reddit_poller.running
            # stays True.
            _set_service_status("reddit_poller", True)

            cycle_start = time.time()
            try:
                if not _is_reddit_enabled():
                    # Master switch is off (checked LIVE, fresh from .env,
                    # every cycle) — thread stays alive (heartbeat keeps
                    # updating) but no fetching/matching happens at all
                    # until this flips back to True. As soon as it does,
                    # the very next cycle below runs _load_all_jobs_keyword_map()
                    # fresh and starts matching again from scratch.
                    log.info("[REDDIT-POLLER] REDDIT_ENABLED=False | fetching stopped this cycle")
                    time.sleep(REDDIT_POLL_INTERVAL_SECONDS)
                    continue

                jobs = _load_all_jobs_keyword_map()
                reddit_jobs = [j for j in jobs if j["targeting_platform"] in ("reddit", "all")]

                if not reddit_jobs:
                    # Nothing to match against yet — just wait for the next cycle.
                    time.sleep(REDDIT_POLL_INTERVAL_SECONDS)
                    continue

                entries = _fetch_reddit_rss_feed()
                cutoff = datetime.now(timezone.utc) - cutoff_days

                saved_this_cycle = 0
                for entry in entries:
                    created = entry.get("created_utc")
                    if created is not None:
                        post_dt = datetime.fromtimestamp(created, tz=timezone.utc)
                        if post_dt < cutoff:
                            continue  # older than lookback window — skip (rarely triggers on a live feed)

                    match_text = f"{entry.get('title', '')} {entry.get('selftext', '')}"

                    # Check this single RSS entry against EVERY job's keyword
                    # list — one entry can legitimately match several
                    # different topics at once, and each gets its own saved
                    # signal (message_id is unique per platform+post, but
                    # topic_key differs, so both are kept — same behaviour
                    # the old fetch-once-per-job version had).
                    for job in reddit_jobs:
                        matched_keyword = _match_any_keyword(match_text, job["keywords"])
                        if not matched_keyword:
                            continue
                        saved = _save_signal(job["topic_key"], matched_keyword, "reddit", entry)
                        if saved:
                            saved_this_cycle += 1

                log.info(
                    f"[REDDIT-POLLER] cycle done | jobs_checked={len(reddit_jobs)} | "
                    f"rss_entries={len(entries)} | new_messages_saved={saved_this_cycle}"
                )

            except Exception as exc:
                log.error(f"[REDDIT-POLLER] unexpected error: {exc}")

            elapsed = time.time() - cycle_start
            sleep_for = max(0.0, REDDIT_POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_for)
    finally:
        # Only reached if this loop truly exits (crash/shutdown) — flips
        # the flag to False so anything watching it knows Reddit stopped.
        _set_service_status("reddit_poller", False)


# ─────────────────────────────────────────────────────────────────────────────
# JOB PROCESSING — now Twitter-only. Reddit is handled entirely by the
# always-on poller above, independent of any single job's lifecycle.
# ─────────────────────────────────────────────────────────────────────────────

def process_job(job: dict) -> int:
    topic_key = job["topic_key"]
    keywords  = job.get("keywords") or []

    # targeting_platform controls which platform(s) this job fetches from.
    # "reddit" -> handled entirely by the always-on Reddit poller, nothing
    # left for this job to do here. "x_twitter" -> Twitter only, via this
    # job. "all" (or missing, for backward compatibility with jobs queued
    # before this field existed) -> Reddit keeps being covered by the
    # poller in the background, and this job additionally does the
    # Twitter search below.
    targeting_platform = (job.get("targeting_platform") or "all").strip().lower()
    fetch_twitter = targeting_platform in ("x_twitter", "all") and _is_twitter_enabled()

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    log.info(
        f"[JOB] START | topic_key={topic_key} | {len(keywords)} keyword(s) | "
        f"targeting_platform={targeting_platform} | "
        f"reddit=handled by always-on poller | twitter={'on' if fetch_twitter else 'off'}"
    )

    matched_count = 0

    # ── Twitter/X (only if RAPID_API_KEY is configured AND this job's
    #    targeting_platform allows it) — unchanged, one real search call
    #    per keyword since RapidAPI supports actual query search. ──
    if fetch_twitter:
        for keyword in keywords:
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

    log.info(f"[JOB] DONE | topic_key={topic_key} | new_messages_saved={matched_count} (Twitter only — Reddit accrues continuously via the poller)")
    return matched_count




# ─────────────────────────────────────────────────────────────────────────────
# WORKER LOOP (Twitter + job status only — Reddit no longer lives here)
# ─────────────────────────────────────────────────────────────────────────────
#
# fetch_next_pending_job() uses find_one_and_update, which MongoDB performs
# atomically — so multiple threads calling it at the same time can NEVER
# both claim the same job. This makes it safe to run several of these
# loops in parallel (see start_worker_pool below): each thread independently
# grabs the next available pending job and works on it, so a second search
# (e.g. "adidas") is never stuck waiting behind a first one (e.g. "nike").
#
# Reddit no longer depends on any of this — the always-on poller (see
# run_reddit_poller above) keeps accumulating Reddit matches for every job
# in the collection regardless of whether this worker pool has picked
# anything up yet.
# ─────────────────────────────────────────────────────────────────────────────

def run_worker(worker_id: int = 0):
    log.info(f"[WORKER-{worker_id}] started | poll_interval={POLL_INTERVAL_SECONDS}s | lookback_days={LOOKBACK_DAYS}")

    try:
        while True:
            # Mark alive on every iteration — same pattern/service-status
            # shape as the Reddit poller. As long as ANY worker thread in
            # the pool keeps turning, flintel_service_status.twitter_worker
            # stays True.
            _set_service_status("twitter_worker", True)

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
    finally:
        # Only reached if this specific worker thread truly exits. Since
        # other worker threads may still be alive and will keep setting
        # this back to True on their own next iteration, this simply
        # reflects "this thread stopped" rather than "the whole pool died".
        _set_service_status("twitter_worker", False)


def start_worker_pool():
    """Spawns WORKER_CONCURRENCY worker threads, all pulling from the same
    flintel_search_jobs queue (Twitter + job status only now). As soon as
    any thread is free, it picks up the next pending job — no job waits on
    another to finish."""
    threads = []
    for i in range(WORKER_CONCURRENCY):
        t = threading.Thread(target=run_worker, args=(i,), daemon=True, name=f"Worker-{i}")
        t.start()
        threads.append(t)

    log.info(f"Worker pool started | concurrency={WORKER_CONCURRENCY}")
    return threads


def start_all():
    """Starts the always-on Reddit poller (one thread, runs forever,
    independent of the job queue) AND the Twitter/job-status worker pool,
    then keeps the main thread alive while all of them run."""
    threads = []

    reddit_thread = threading.Thread(target=run_reddit_poller, daemon=True, name="RedditPoller")
    reddit_thread.start()
    threads.append(reddit_thread)

    threads.extend(start_worker_pool())

    for t in threads:
        t.join()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL — SIMPLIFIED FETCH-ONLY BACKGROUND SERVICE")
    log.info("=" * 70)
    log.info("  Platform          : Reddit (site-wide RSS, ALWAYS-ON poller) + Twitter/X (via RapidAPI, job-driven)")
    log.info(f"  Reddit fetch mode : continuous poller, every {REDDIT_POLL_INTERVAL_SECONDS}s, keyword list rebuilt from ALL jobs each cycle")
    log.info("  Keywords source   : MongoDB (flintel_search_jobs) — no hardcoded list, re-read live every cycle")
    log.info(f"  Reddit fetching   : {'ENABLED' if _is_reddit_enabled() else 'DISABLED (REDDIT_ENABLED=False — poller alive but not fetching)'} (checked live from .env every cycle, no restart needed to change)")
    log.info(f"  Twitter/X         : {'ENABLED' if _is_twitter_enabled() else 'DISABLED (set RAPID_API_KEY + TWITTER_ENABLED=True to enable)'} (checked live from .env every job, no restart needed to change)")
    log.info(f"  Lookback window   : {LOOKBACK_DAYS} days (mostly no-op on a live RSS feed)")
    log.info("  Scoring           : NONE — raw messages only")
    log.info("  Slack / HubSpot   : REMOVED")
    log.info("  Batching          : REMOVED — fetch and save immediately")
    log.info(f"  Worker concurrency: {WORKER_CONCURRENCY} parallel jobs (Twitter + job status) — a new search never waits on another")
    log.info(f"  MongoDB DB        : {MONGODB_DB}")
    log.info("=" * 70)

    start_all()
