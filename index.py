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
       a) Fetches the site-wide r/all "new posts" RSS feed ONCE, AND the
          site-wide r/all "new comments" RSS feed ONCE.
       b) Re-reads flintel_search_jobs and builds a fresh, in-memory
          keyword list out of EVERY job in the collection (not just
          "pending" ones — a job's keywords are live/patched-in by the web
          service at any time, so this list is rebuilt every cycle).
       c) Matches every RSS entry (posts AND comments) against every job's
          keywords locally, and for every match saves the entry into
          flintel_signals tagged with that job's topic_key + whichever
          keyword matched.
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

  4. Saves every matched post/comment as a raw, unscored message into
     MongoDB collection `flintel_signals`, tagged with the job's topic_key
     and platform ("reddit", "reddit_comment", or "twitter"). Duplicate
     entries are silently skipped via the unique index on message_id — so
     the continuous poller re-fetching the same RSS window over and over is
     harmless; it will just keep hitting DuplicateKeyError for entries it
     already saved and only insert genuinely new ones.

  4b. NEW — EMBEDDING LAYER (this is the one addition in this version):
     The moment a post/comment's raw text is about to be saved into
     `flintel_signals` (i.e. right before the very first insert of that
     document — duplicates never re-run this), this service generates ONE
     vector embedding from that document's own `text` field and stores it
     on the SAME document under the `embedding` field. Nothing else about
     the save path changed. See the "EMBEDDINGS" section below for full
     details, config, and the one-time backfill helper for historical docs
     that already have text but no embedding yet.

  5. Job status (`flintel_search_jobs.status`) is still driven by the
     worker pool (`process_job` / `run_worker`), exactly as before, for
     Twitter's sake and so the web service still has a "done" signal to
     watch. Reddit matches (posts + comments) are NOT tied to a specific
     job's matched_count anymore, since Reddit is no longer fetched inside
     a single job run — it's continuous and shared across every job's
     keywords at once.

⚠️ IMPORTANT TRADE-OFF — READ THIS:
  RSS only ever shows Reddit's current "new posts" / "new comments" window
  — a rolling, very recent set (roughly the last few minutes to a couple
  of hours of site-wide activity, depending on how busy Reddit is), NOT a
  searchable 6-month history. The old .json search endpoint could pull
  posts from up to a year back for an exact keyword; this RSS feed cannot
  — it has no concept of "search for X", only "here's what's newest right
  now". LOOKBACK_DAYS / cutoff filtering is kept for consistency but will
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
  - No query embeddings, no vector search, no retrieval/ranking logic, no
    Claude-generated search phrases, no intent classification. This
    version ONLY generates and stores the per-document embedding at save
    time (and via the optional one-time backfill helper) — nothing else
    in the pipeline changed.

Requires the `feedparser` package (pip install feedparser) for RSS parsing,
and the `openai` package (pip install openai) for embedding generation.

This file is intentionally simple: one always-on Reddit poller loop (now
covering both posts AND comments), one job-driven worker pool for Twitter
+ job status, one shared save function (now also generating+storing one
embedding per document on first save).

──────────────────────────────────────────────────────────────────────────
DUAL-MONGODB NOTE (unchanged from the previous version):
  - `flintel_signals` (raw fetched messages, now also carrying each
    document's own `embedding` field) still lives on the ORIGINAL MongoDB
    connection (`MONGODB_URI` / `MONGODB_DB`) — exactly as before.
  - EVERYTHING ELSE that touches Mongo — `flintel_search_jobs` (the job
    queue) and `flintel_service_status` (the poller/worker heartbeat) —
    still lives on the SECOND MongoDB connection (`MONGODB1_URI` /
    `MONGODB1_DB`).
  - Nothing else about the logic, structure, matching, saving, or field
    names changed, other than the new `embedding` field described below.
──────────────────────────────────────────────────────────────────────────

──────────────────────────────────────────────────────────────────────────
EMBEDDINGS — WHAT WAS ADDED IN THIS VERSION (and nothing else):
  - One embedding is generated from a document's own `text` field, once,
    at the moment that document is first saved into `flintel_signals`
    (inside `_save_signal`, right before `insert_one`). It is stored on
    that same document under `embedding` (a plain list of floats).
  - Embeddings are NEVER shared between documents — each document's
    embedding comes only from that document's own `text`.
  - Duplicates (an entry already saved before — same unique message_id)
    never reach the embedding call at all, because `_save_signal` already
    skips the whole insert via `DuplicateKeyError` before an embedding
    would ever be generated for it again. So an already-stored, unchanged
    post never gets re-embedded.
  - If embedding generation fails or is disabled (`EMBEDDING_ENABLED` =
    False, or no API key configured), the document is still saved exactly
    as before — `embedding` is simply set to `None` on that document
    rather than blocking the save. Nothing about the existing fetch/match/
    save pipeline is allowed to break because of this.
  - `backfill_missing_embeddings()` is a one-time, on-demand helper (run
    manually via `python flintel_service.py --backfill-embeddings`) that
    scans EXISTING documents in `flintel_signals` that already have a
    `text` field but no `embedding` (or `embedding: None`), and generates
    an embedding for each straight from that already-stored `text` — it
    never re-fetches anything from Reddit/Twitter. This does not run
    automatically on every startup; it only runs when explicitly invoked,
    so it never interferes with the normal always-on poller / worker
    pool behaviour.
  - Nothing else — no query embeddings, no vector index creation, no
    vector search, no ranking/retrieval changes.
──────────────────────────────────────────────────────────────────────────
"""

import os
import re
import sys
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


# ── Original MongoDB — used ONLY for `flintel_signals` now. ──
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "flintel_bot")

# ── Second MongoDB — used for EVERYTHING ELSE: `flintel_search_jobs`
# (the job queue) and `flintel_service_status` (poller/worker heartbeat).
# This is the one addition from the previous version. ──
MONGODB1_URI = os.getenv("MONGODB1_URI")
MONGODB1_DB  = os.getenv("MONGODB1_DB", "flintel_bot")

# How often the worker checks for a new pending job when idle.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "2"))

# How often the ALWAYS-ON Reddit poller re-fetches the r/all RSS feeds and
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

# Site-wide COMMENTS RSS feed — r/all's new-comments stream, same shape /
# same rolling-window behaviour as REDDIT_RSS_URL above, just comments
# instead of posts. Added purely additively — everything else in the file
# is untouched.
REDDIT_COMMENTS_RSS_URL = os.getenv(
    "REDDIT_COMMENTS_RSS_URL", "https://www.reddit.com/r/all/comments/.rss"
)

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

# ── EMBEDDINGS — the one new piece of config in this version. Everything
# here is additive; none of the settings above were touched. ──
#
# Master ON/OFF switch, same live-checked pattern as REDDIT_ENABLED /
# TWITTER_ENABLED above. EMBEDDING_ENABLED=True (default) -> every newly
# saved document gets an embedding generated from its own text.
# EMBEDDING_ENABLED=False -> _save_signal still saves documents exactly as
# before, just with embedding=None — fetching/matching/saving never stops
# or breaks because of this switch.
def _is_embedding_enabled() -> bool:
    load_dotenv(override=True)
    return _env_bool("EMBEDDING_ENABLED", True) and bool(os.getenv("OPENAI_API_KEY", ""))


EMBEDDING_PROVIDER   = os.getenv("EMBEDDING_PROVIDER", "openai")
EMBEDDING_MODEL      = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
EMBEDDING_TIMEOUT    = int(os.getenv("EMBEDDING_TIMEOUT", "20"))
# Max characters of a document's text sent to the embedding model per call
# (keeps a single unusually long post/comment from blowing past the
# model's token limit). Purely a safety truncation, does not change what
# gets stored as `text` on the document itself.
EMBEDDING_MAX_CHARS  = int(os.getenv("EMBEDDING_MAX_CHARS", "8000"))
# How many documents backfill_missing_embeddings() updates per DB batch.
EMBEDDING_BACKFILL_BATCH_SIZE = int(os.getenv("EMBEDDING_BACKFILL_BATCH_SIZE", "100"))
# Politeness delay between individual embedding calls during backfill, so
# a large historical backlog doesn't hammer the embedding API all at once.
EMBEDDING_BACKFILL_GAP_SECONDS = float(os.getenv("EMBEDDING_BACKFILL_GAP_SECONDS", "0.2"))

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
    """Connects to BOTH MongoDB instances and ensures only the indexes
    this simplified service actually needs, on whichever instance each
    collection now lives on:

      - `db`  (MONGODB_URI / MONGODB_DB)   -> `flintel_signals` ONLY.
      - `db1` (MONGODB1_URI / MONGODB1_DB) -> `flintel_search_jobs` and
                                               `flintel_service_status`.

    No leftover indexes from the old scoring pipeline, on either side."""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

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

        log.info(f"MongoDB (signals) connected | db={MONGODB_DB}")

        client1 = MongoClient(MONGODB1_URI, serverSelectionTimeoutMS=5000)
        client1.server_info()
        db1 = client1[MONGODB1_DB]

        # Jobs queue — web service inserts here, this worker consumes.
        db1.flintel_search_jobs.create_index(
            [("status", ASCENDING), ("requested_at", ASCENDING)],
            name="jobs_status_requested_at",
        )
        db1.flintel_search_jobs.create_index(
            [("topic_key", ASCENDING)], name="jobs_topic_key"
        )

        # Live status/heartbeat flags — lets anything outside this process
        # check whether the Reddit poller and the Twitter/job worker pool
        # are currently running (True) or stopped (False).
        db1.flintel_service_status.create_index(
            [("service", ASCENDING)], unique=True, name="service_status_service_unique"
        )

        log.info(f"MongoDB (jobs/status) connected | db1={MONGODB1_DB}")

        return db, db1
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db, db1 = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# JOB QUEUE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_next_pending_job():
    """Atomically claims the oldest pending job so multiple worker instances
    (if ever scaled horizontally) never process the same job twice."""
    return db1.flintel_search_jobs.find_one_and_update(
        {"status": "pending"},
        {"$set": {"status": "processing", "started_at": datetime.now(timezone.utc)}},
        sort=[("requested_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def mark_job_done(job_id, matched_count: int):
    db1.flintel_search_jobs.update_one(
        {"_id": job_id},
        {"$set": {
            "status": "done",
            "matched_count": matched_count,
            "completed_at": datetime.now(timezone.utc),
        }},
    )


def mark_job_error(job_id, error: str):
    db1.flintel_search_jobs.update_one(
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
    db1.flintel_search_jobs.update_one(
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

        db1.flintel_service_status.find_one({"service": "reddit_poller"})
        db1.flintel_service_status.find_one({"service": "twitter_worker"})

    Never raises — a DB hiccup while updating status never crashes the
    actual poller/worker loop."""
    try:
        db1.flintel_service_status.update_one(
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
        cursor = db1.flintel_search_jobs.find(
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


def _fetch_reddit_comments_rss_feed() -> list:
    """Fetches the SITE-WIDE r/all "new comments" RSS feed — ONE request,
    exact same approach/shape as _fetch_reddit_rss_feed() above, just
    pointed at REDDIT_COMMENTS_RSS_URL instead of REDDIT_RSS_URL. Added
    purely additively so the existing posts fetch function above is left
    completely untouched.

    A comment RSS entry's "title" field from Reddit is usually something
    like "Comment by u/someone on some post title" — not the comment body
    — so here `title` is built as the comment's own text (from the entry
    summary/content) so keyword-matching works against what the user
    actually wrote, same as it does for post title+selftext. Never raises
    — a failure here just means zero comment entries this round; it never
    blocks the poller's next cycle or the posts fetch."""
    url = f"{REDDIT_COMMENTS_RSS_URL}?limit={REDDIT_RESULTS_PER_QUERY}"
    headers = {"User-Agent": REDDIT_USER_AGENT}

    entries = []
    try:
        resp = requests.get(url, headers=headers, timeout=REDDIT_REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)

        if feed.bozo and not feed.entries:
            log.warning(f"[REDDIT-COMMENTS-RSS] feed parse issue: {feed.bozo_exception}")
            return entries

        for entry in feed.entries:
            entry_id = entry.get("id", "") or entry.get("link", "")
            if not entry_id:
                continue

            link = entry.get("link", "") or ""
            subreddit_match = re.search(r"/r/([^/]+)/", link)
            subreddit = subreddit_match.group(1) if subreddit_match else "unknown"

            # Prefer the full HTML content block if feedparser exposes it
            # (comment RSS usually puts the actual comment body there);
            # fall back to summary otherwise.
            raw_body = ""
            if entry.get("content"):
                try:
                    raw_body = entry["content"][0].get("value", "") or ""
                except Exception:
                    raw_body = ""
            if not raw_body:
                raw_body = entry.get("summary", "").strip()

            body_plain = re.sub(r"<[^>]+>", " ", html.unescape(raw_body)).strip()

            author = entry.get("author", "unknown").lstrip("u/").strip() or "unknown"

            published_struct = entry.get("published_parsed") or entry.get("updated_parsed")
            created_utc = time.mktime(published_struct) if published_struct else None

            entries.append({
                "id":            entry_id.split("/")[-1] or entry_id,
                "title":         body_plain,
                "selftext":      "",
                "author":        author,
                "subreddit":     subreddit,
                "post_url":      link,
                "created_utc":   created_utc,
                "score":         0,
                "num_comments":  0,
            })

    except Exception as exc:
        log.warning(f"[REDDIT-COMMENTS-RSS] fetch failed: {exc}")

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
# EMBEDDINGS — NEW SECTION IN THIS VERSION. Everything below is additive:
# one function that turns a document's own text into one vector, called
# from exactly one place (_save_signal, right before insert), plus one
# manually-triggered backfill helper for historical documents. Nothing
# else in the file calls these, and these never touch fetching/matching.
# ─────────────────────────────────────────────────────────────────────────────

_openai_client = None


def _get_openai_client():
    """Lazily creates (once) and reuses a single OpenAI client for the
    lifetime of the process. Returns None (never raises) if the `openai`
    package isn't installed or OPENAI_API_KEY isn't set — callers treat
    that as "embeddings unavailable right now" and just store
    embedding=None rather than failing the save."""
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    if not OPENAI_API_KEY:
        return None

    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY, timeout=EMBEDDING_TIMEOUT)
        return _openai_client
    except Exception as exc:
        log.warning(f"[EMBEDDING] could not initialise OpenAI client: {exc}")
        return None


def generate_embedding(text: str):
    """Generates ONE embedding vector from ONE piece of text, using the
    configured embedding model (EMBEDDING_MODEL, default
    "text-embedding-3-small"). This is the ONLY function in the whole
    service that talks to the embedding API.

    - One call in, one embedding out — this function is never given more
      than one document's text at a time, and never mixes text from more
      than one document into a single embedding call, so embeddings are
      never shared across posts.
    - Returns a plain list[float] on success, or None on any failure
      (missing/invalid key, network error, empty text, provider outage,
      etc.) — it NEVER raises, so a failed embedding call can never break
      or block the fetch/match/save pipeline that calls it.
    - Respects EMBEDDING_ENABLED (checked live by the caller via
      _is_embedding_enabled()) — this function itself doesn't check the
      switch again; callers gate on it first so this stays a pure
      "text in, vector out" helper.
    """
    if not text or not text.strip():
        return None

    client = _get_openai_client()
    if client is None:
        return None

    # Simple safety truncation — keeps one unusually long document from
    # exceeding the embedding model's input limit. Does not affect what
    # is stored as the document's own `text` field, only what is sent to
    # the embedding call.
    payload_text = text.strip()[:EMBEDDING_MAX_CHARS]

    try:
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=payload_text,
        )
        return response.data[0].embedding
    except Exception as exc:
        log.warning(f"[EMBEDDING] generation failed | model={EMBEDDING_MODEL} | {exc}")
        return None


def backfill_missing_embeddings():
    """ONE-TIME / ON-DEMAND helper — NOT called automatically anywhere in
    the normal poller/worker startup path. Run it manually when you want
    to generate embeddings for documents that were saved to
    flintel_signals BEFORE this embedding layer existed (or that were
    saved while EMBEDDING_ENABLED was False):

        python flintel_service.py --backfill-embeddings

    What it does, and nothing more:
      1. Finds documents in flintel_signals that already have a `text`
         field but no usable `embedding` (missing OR None OR empty list).
      2. For each one, generates an embedding from that document's own
         ALREADY-STORED `text` — it never re-fetches anything from Reddit
         or Twitter, and never touches any other field on the document.
      3. Writes the embedding onto that same document.

    Documents that already have a real embedding are left completely
    untouched (never regenerated). Processes in batches
    (EMBEDDING_BACKFILL_BATCH_SIZE at a time) with a small politeness
    delay between embedding calls (EMBEDDING_BACKFILL_GAP_SECONDS) so a
    large backlog doesn't hammer the embedding API all at once. Safe to
    stop and re-run at any time — it always just picks up wherever
    documents are still missing an embedding."""
    if not _is_embedding_enabled():
        log.warning(
            "[EMBEDDING-BACKFILL] EMBEDDING_ENABLED is False or OPENAI_API_KEY is not "
            "set — nothing to do. Set both and re-run."
        )
        return

    query = {
        "text": {"$exists": True, "$ne": ""},
        "$or": [
            {"embedding": {"$exists": False}},
            {"embedding": None},
            {"embedding": []},
        ],
    }

    total_scanned = 0
    total_updated = 0
    total_failed = 0

    log.info("[EMBEDDING-BACKFILL] starting one-time backfill of missing embeddings...")

    while True:
        batch = list(
            db.flintel_signals.find(query, {"_id": 1, "text": 1}).limit(EMBEDDING_BACKFILL_BATCH_SIZE)
        )
        if not batch:
            break

        for doc in batch:
            total_scanned += 1
            embedding = generate_embedding(doc.get("text", ""))

            if embedding is not None:
                try:
                    db.flintel_signals.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"embedding": embedding}},
                    )
                    total_updated += 1
                except Exception as exc:
                    total_failed += 1
                    log.error(f"[EMBEDDING-BACKFILL] update failed | _id={doc['_id']} | {exc}")
            else:
                total_failed += 1
                log.warning(f"[EMBEDDING-BACKFILL] embedding generation failed | _id={doc['_id']}")

            time.sleep(EMBEDDING_BACKFILL_GAP_SECONDS)

        log.info(
            f"[EMBEDDING-BACKFILL] progress | scanned={total_scanned} | "
            f"updated={total_updated} | failed={total_failed}"
        )

    log.info(
        f"[EMBEDDING-BACKFILL] done | scanned={total_scanned} | "
        f"updated={total_updated} | failed={total_failed}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# SAVE FETCHED MESSAGES
# ─────────────────────────────────────────────────────────────────────────────

def _save_signal(topic_key: str, matched_keyword: str, platform: str, post: dict) -> bool:
    """Upserts a raw fetched post/comment (Reddit post, Reddit comment, or
    Twitter — same shape from any fetch function) into flintel_signals. No
    scoring, no Claude, no derived fields — just the raw message plus which
    topic/keyword found it, plus (new in this version) one embedding
    generated from this document's own text. Duplicate entries (already
    fetched before, by this cycle or a past one) are silently skipped via
    the unique index — this is exactly what makes it safe for the Reddit
    poller to keep re-fetching the same rolling RSS window over and over:
    only genuinely new entries get inserted, and only genuinely new
    entries ever trigger an embedding call — an already-saved, unchanged
    post is never re-embedded."""
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

    # ── NEW: one embedding, generated from THIS document's own `text`
    # only, stored on this same document. Generated once, right here,
    # right before the first (and only, thanks to the unique index below)
    # insert of this document — never regenerated afterwards. If
    # embeddings are disabled or generation fails for any reason, this is
    # simply None and the save proceeds exactly as it always did. ──
    doc["embedding"] = generate_embedding(text) if _is_embedding_enabled() else None

    # Reddit POSTS carry a nested "reddit_comments" field — this is where
    # any matching comments for THIS post get pushed into (see
    # _attach_comment_to_post() below). Defaults to 0 (plain integer) when
    # no matching comment has been found yet; becomes a list of comment
    # texts the moment the first matching comment is attached. Twitter
    # docs don't get this field — comments are a Reddit-only concept here.
    if platform == "reddit":
        doc["reddit_comments"] = 0

    try:
        db.flintel_signals.insert_one(doc)
        return True
    except DuplicateKeyError:
        # Already fetched this post before (possibly for a different
        # topic/keyword search that also matched it) — not an error, and
        # no embedding call happens for it again.
        return False
    except Exception as exc:
        log.error(f"[MONGO] save_signal error | message_id={doc['message_id']} | {exc}")
        return False


def _extract_post_id_from_comment_link(link: str):
    """Pulls the parent POST's reddit id out of a comment's permalink.
    Reddit comment permalinks look like:
        https://www.reddit.com/r/subreddit/comments/POST_ID/slug/COMMENT_ID/
    so the id right after "/comments/" is always the parent post's id —
    same id the post itself was saved under as message_id
    f"reddit_{POST_ID}". Returns None if the link doesn't match the
    expected shape (never raises)."""
    if not link:
        return None
    m = re.search(r"/comments/([a-zA-Z0-9]+)/", link)
    return m.group(1) if m else None


def _attach_comment_to_post(topic_key: str, matched_keyword: str, comment_post_id: str, comment_text: str, comment_created_utc) -> bool:
    """Finds the PARENT POST's already-saved document in flintel_signals
    (matched on topic_key + message_id built from the comment's parent
    post id) and pushes this comment (text + its own date) into that
    document's "reddit_comments" field — turning it from the default 0
    into a list on the first match, and appending to that list on every
    match after.

    Each entry in "reddit_comments" is an object:
        {"text": "<comment text>", "created_utc": <comment's own date>}
    so the comment's date is preserved alongside its text, separate from
    the parent post's own created_utc.

    No separate comment document is created — comments live nested INSIDE
    their post's own document, exactly as requested. If the parent post
    was never itself saved (its own title/selftext never matched any
    job's keywords, so no post document exists to attach to), there is
    nowhere to nest this comment, so it is skipped — never raises.

    NOTE: this nested comment array does NOT get its own embedding — the
    embedding layer only applies to documents saved via _save_signal
    (i.e. the post's own text), exactly as scoped."""
    if not comment_post_id:
        return False

    comment_created_dt = (
        datetime.fromtimestamp(comment_created_utc, tz=timezone.utc)
        if comment_created_utc else datetime.now(timezone.utc)
    )

    post_message_id = f"reddit_{comment_post_id}"
    try:
        existing = db.flintel_signals.find_one(
            {"topic_key": topic_key, "message_id": post_message_id}
        )
        if not existing:
            # Parent post itself never matched/was never saved — nothing
            # to attach this comment to.
            return False

        current = existing.get("reddit_comments", 0)
        if not isinstance(current, list):
            current = []

        # De-dupe on comment text (poller re-fetching the same rolling
        # RSS window shouldn't create repeat entries).
        if any(c.get("text") == comment_text for c in current if isinstance(c, dict)):
            return False

        current.append({
            "text":         comment_text,
            "created_utc":  comment_created_dt,
        })

        db.flintel_signals.update_one(
            {"_id": existing["_id"]},
            {"$set": {"reddit_comments": current}},
        )
        return True

    except Exception as exc:
        log.error(f"[MONGO] attach_comment error | post_message_id={post_message_id} | {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — ALWAYS-ON POLLER (runs forever, independent of the job queue)
# ─────────────────────────────────────────────────────────────────────────────
#
# This is the piece that makes Reddit coverage continuous instead of a
# single fetch-per-job snapshot. It never checks job "status", never
# claims/marks jobs, and never stops. It just:
#   loop forever:
#     1. fetch r/all posts RSS once, AND r/all comments RSS once
#     2. rebuild the keyword list from EVERY job in flintel_search_jobs
#     3. match + save (posts and comments both go into flintel_signals,
#        same as before — comments just carry platform="reddit_comment"
#        so they can be told apart from posts later if needed; every
#        newly saved post also gets its own embedding via _save_signal)
#     4. sleep REDDIT_POLL_INTERVAL_SECONDS, repeat
#
# Because step 2 re-reads the jobs collection from scratch every cycle,
# any keywords the web service patches into any job (new or existing) are
# picked up automatically on the very next cycle — no restart needed, no
# waiting for a "pending" status.
# ─────────────────────────────────────────────────────────────────────────────

def _match_and_save_entries(entries: list, reddit_jobs: list, cutoff: datetime, platform: str) -> int:
    """Shared matching+saving loop used for BOTH the posts feed and the
    comments feed — exact same logic that used to live inline in
    run_reddit_poller() for posts, just pulled out so it can be reused for
    comments too without duplicating it. Behaviour is identical to before:
    every entry is checked against EVERY job's keyword list, and each
    match is saved individually via _save_signal()."""
    saved = 0
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
            was_saved = _save_signal(job["topic_key"], matched_keyword, platform, entry)
            if was_saved:
                saved += 1

    return saved


def _match_and_attach_comments(comment_entries: list, reddit_jobs: list, cutoff: datetime) -> int:
    """Same keyword-matching pass as _match_and_save_entries() above, but
    for COMMENTS specifically: instead of inserting a new standalone
    document, a match gets nested into its PARENT POST's own document
    (flintel_signals.<post doc>.reddit_comments) via
    _attach_comment_to_post(). If the parent post was never saved (its
    own text never matched any keyword), the comment has nowhere to nest
    and is skipped — same "only matching ones, attached under their
    post" behaviour requested."""
    attached = 0
    for entry in comment_entries:
        created = entry.get("created_utc")
        if created is not None:
            post_dt = datetime.fromtimestamp(created, tz=timezone.utc)
            if post_dt < cutoff:
                continue  # older than lookback window — skip (rarely triggers on a live feed)

        comment_text = entry.get("title", "")
        comment_created_utc = entry.get("created_utc")
        comment_post_id = _extract_post_id_from_comment_link(entry.get("post_url", ""))

        for job in reddit_jobs:
            matched_keyword = _match_any_keyword(comment_text, job["keywords"])
            if not matched_keyword:
                continue
            was_attached = _attach_comment_to_post(
                job["topic_key"], matched_keyword, comment_post_id, comment_text, comment_created_utc
            )
            if was_attached:
                attached += 1

    return attached


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

                cutoff = datetime.now(timezone.utc) - cutoff_days

                # ── Posts (unchanged) ──
                entries = _fetch_reddit_rss_feed()
                saved_posts = _match_and_save_entries(entries, reddit_jobs, cutoff, "reddit")

                # ── Comments — matched the same way as posts, but instead
                # of becoming their own document, matches get nested into
                # their PARENT POST's own document under "reddit_comments"
                # (0 by default, becomes a list of matching comment texts
                # once at least one is attached). No separate comment
                # document is created. ──
                comment_entries = _fetch_reddit_comments_rss_feed()
                attached_comments = _match_and_attach_comments(comment_entries, reddit_jobs, cutoff)

                saved_this_cycle = saved_posts

                log.info(
                    f"[REDDIT-POLLER] cycle done | jobs_checked={len(reddit_jobs)} | "
                    f"rss_entries={len(entries)} | comment_entries={len(comment_entries)} | "
                    f"new_posts_saved={saved_posts} | comments_attached_to_posts={attached_comments}"
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
    #    per keyword since RapidAPI supports actual query search. Every
    #    newly saved tweet also gets its own embedding via _save_signal,
    #    same as Reddit posts. ──
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
    # NEW: optional one-time backfill mode. Running with this flag does
    # NOT start the poller/worker pool — it only generates embeddings for
    # existing flintel_signals documents that have text but no embedding
    # yet, then exits. Normal `python flintel_service.py` (no flag) starts
    # everything exactly as before.
    if "--backfill-embeddings" in sys.argv:
        log.info("=" * 70)
        log.info("  FLINTEL — ONE-TIME EMBEDDING BACKFILL (historical documents only)")
        log.info("=" * 70)
        backfill_missing_embeddings()
        sys.exit(0)

    log.info("=" * 70)
    log.info("  FLINTEL — SIMPLIFIED FETCH-ONLY BACKGROUND SERVICE")
    log.info("=" * 70)
    log.info("  Platform          : Reddit (site-wide RSS — posts + comments, ALWAYS-ON poller) + Twitter/X (via RapidAPI, job-driven)")
    log.info(f"  Reddit fetch mode : continuous poller, every {REDDIT_POLL_INTERVAL_SECONDS}s, keyword list rebuilt from ALL jobs each cycle, posts AND comments both fetched")
    log.info("  Keywords source   : MongoDB1 (flintel_search_jobs) — no hardcoded list, re-read live every cycle")
    log.info(f"  Reddit fetching   : {'ENABLED' if _is_reddit_enabled() else 'DISABLED (REDDIT_ENABLED=False — poller alive but not fetching)'} (checked live from .env every cycle, no restart needed to change)")
    log.info(f"  Twitter/X         : {'ENABLED' if _is_twitter_enabled() else 'DISABLED (set RAPID_API_KEY + TWITTER_ENABLED=True to enable)'} (checked live from .env every job, no restart needed to change)")
    log.info(f"  Embeddings        : {'ENABLED — model=' + EMBEDDING_MODEL if _is_embedding_enabled() else 'DISABLED (set OPENAI_API_KEY + EMBEDDING_ENABLED=True to enable)'} (checked live from .env, one embedding per newly saved document)")
    log.info(f"  Lookback window   : {LOOKBACK_DAYS} days (mostly no-op on a live RSS feed)")
    log.info("  Scoring           : NONE — raw messages only")
    log.info("  Slack / HubSpot   : REMOVED")
    log.info("  Batching          : REMOVED — fetch and save immediately")
    log.info(f"  Worker concurrency: {WORKER_CONCURRENCY} parallel jobs (Twitter + job status) — a new search never waits on another")
    log.info(f"  MongoDB (signals) : {MONGODB_DB}")
    log.info(f"  MongoDB1 (jobs/status): {MONGODB1_DB}")
    log.info("  Embedding backfill: run with --backfill-embeddings for historical docs missing an embedding")
    log.info("=" * 70)

    start_all()
