"""
FLINTEL v9.13 — Reddit (SERP Discovery, FETCH-ONCE-FOREVER KEYWORD CACHE
                + BATCHED SEARCH-VOLUME PRE-SEEDING
                + AUTO-SYNCED TARGETING COLLECTIONS)
                + Twitter/X Signal Scorer
=================================================================================
Platforms : Reddit — RapidAPI SERP discovery ONLY (Google search,
            site:reddit.com, real per-post rank -> flintel_google_posts cache
            -> auto-synced flintel_targeting_subreddits / flintel_targeting_keywords
            -> subreddit RSS polling + URL-match confirmation,
            no credentials required)
          + Twitter/X (tweepy v2)

=================================================================================
WHAT CHANGED IN THIS BUILD (v9.13) — TWO NEW AUTO-SYNCED COLLECTIONS,
flintel_targeting_subreddits AND flintel_targeting_keywords, GOVERN WHICH
SUBREDDITS/KEYWORDS THE RSS-MATCHING LOOP ACTIVELY TARGETS. EVERYTHING ELSE
FROM v9.12 IS 100% UNCHANGED — flintel_keywords, THE SEARCH-VOLUME SEEDING
LOGIC, THE GOOGLE-RANK/SERP CALL, generate_fuzzy_keywords(), AND
save_google_post() ARE ALL BYTE-FOR-BYTE UNCHANGED.
=================================================================================

  WHAT'S NEW —

  1. flintel_targeting_subreddits / flintel_targeting_keywords are a LIVE,
     AUTO-SYNCED MIRROR of whatever is currently PENDING (fetched=False)
     in flintel_google_posts. Nothing in either collection is ever
     hand-maintained, hardcoded, or kept in a python list — every single
     pass of run_google_posts_rss_matching_loop() calls
     sync_targeting_collections() first, which:
       - reads every fetched=False flintel_google_posts document
       - upserts one flintel_targeting_keywords doc per pending post_url
         (carrying its matched_keyword, fuzzy_keywords, subreddit)
       - upserts one flintel_targeting_subreddits doc per distinct
         pending subreddit
       - PRUNES both collections of anything that is no longer pending
         (already confirmed via some other path), so they never drift
         out of sync with flintel_google_posts's real state

  2. run_google_posts_rss_matching_loop() now reads the list of
     subreddits to poll from flintel_targeting_subreddits (via
     get_targeting_subreddits()) instead of re-querying
     flintel_google_posts.distinct() directly. The actual per-post
     detail used to build the pending-by-url lookup for each subreddit
     (google_rank, matched_keyword, fuzzy_keywords) still comes straight
     from flintel_google_posts, exactly as in v9.12 — the targeting
     collections are the governing/tracking layer, not a duplicate data
     store.

  3. THE MATCH ITSELF IS STILL, AND ONLY EVER, AN EXACT post_url MATCH
     against the subreddit's live RSS feed — fuzzy_keywords are never
     used as a filtering gate (same as v9.12; kept only for a
     traceability log line). The moment an RSS entry's link matches a
     pending post_url:
       - flintel_google_posts.fetched is set to True, permanently
         (mark_google_post_fetched(), unchanged from v9.12)
       - its flintel_targeting_keywords document is immediately DELETED
         via delete_targeting_keyword_entry(post_url) — that post/
         keyword is done being targeted
       - flintel_targeting_subreddits is left alone at that instant (no
         per-match write there) — it gets fully rebuilt on the very next
         sync_targeting_collections() pass, so a subreddit with zero
         pending posts left naturally drops out of the poll list within
         one cycle, with no separate delete path needed

  4. run_batch_processor()'s redundant keyword-phrase filter
     (passes_keyword_filter(text, keyword_filter_list)) is now SKIPPED
     for Reddit items specifically. A Reddit item only ever reaches
     reddit_queue after its post_url has already been confirmed via
     exact URL match against flintel_google_posts — that URL match is
     the sole, authoritative relevance decision for Reddit. Re-checking
     the fetched text against the full REDDIT_SEARCH_KEYWORDS phrase
     list here would silently drop items whose text only shares meaning
     (not the exact original phrase) with the keyword that produced
     them via SERP. Twitter items are NOT pre-filtered anywhere upstream,
     so they still go through passes_keyword_filter() exactly as before
     — zero change to Twitter's behavior.

  Everything else — flintel_keywords (fetch-once-forever keyword cache),
  search_google_for_keyword(), fetch_google_rank(), fetch_search_volume(),
  seed_search_volume_batch(), generate_fuzzy_keywords(), save_google_post(),
  get_pending_google_posts_for_subreddit(), mark_google_post_fetched(),
  the Claude batch scorer, the rescore processor, persistent batch/queue
  state, and every FastAPI endpoint from v9.12 — is preserved 100% as-is.
=================================================================================
"""

import asyncio
import logging
import os
import json
import time
import queue
import random
import re
import html
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import anthropic
import httpx
import tweepy
import requests
import feedparser
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN
import uvicorn

# ─────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")
CLIENT_ID   = os.getenv("CLIENT_ID", "Flintel")

# Optional generic label/context — used ONLY as a fallback google_rank
# lookup for Twitter items (Twitter has no per-post SERP discovery in
# this design, so there is no "real" per-post rank for a tweet). If left
# empty, Twitter items simply get google_rank=None / search_volume=None.
SEARCH_KEYWORD = os.getenv("SEARCH_KEYWORD", "")

# ── RapidAPI — SOLE provider for both Google rank AND search volume.
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")  # .env boht used same key
RAPIDAPI_KEYWORD_HOST = "seo-keyword-research.p.rapidapi.com"
RAPIDAPI_SEARCH_HOST  = "google-search116.p.rapidapi.com"

# ── RapidAPI call timeouts — configurable so a slow keyword doesn't
# get killed early. These are LIVE endpoint calls
# — real-time, no polling/task-based async needed.
DATAFORSEO_SERP_TIMEOUT_SECONDS   = int(os.getenv("DATAFORSEO_SERP_TIMEOUT_SECONDS", "120"))
DATAFORSEO_VOLUME_TIMEOUT_SECONDS = int(os.getenv("DATAFORSEO_VOLUME_TIMEOUT_SECONDS", "60"))
REDDIT_JSON_TIMEOUT_SECONDS       = int(os.getenv("REDDIT_JSON_TIMEOUT_SECONDS", "15"))  # used for the RSS fetch as of v9.11

REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))

REDDIT_BATCH_GAP_SECONDS      = int(os.getenv("REDDIT_BATCH_GAP_SECONDS",      "30"))
REDDIT_BATCH_TIMEOUT_SECONDS  = int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",  "120"))

TWITTER_BATCH_GAP_SECONDS     = int(os.getenv("TWITTER_BATCH_GAP_SECONDS",     "30"))
TWITTER_BATCH_TIMEOUT_SECONDS = int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS", "120"))

RESCORE_BATCH_GAP_SECONDS = int(os.getenv("RESCORE_BATCH_GAP_SECONDS", "30"))
RESCORE_POLL_INTERVAL     = int(os.getenv("RESCORE_POLL_INTERVAL", "10"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

# ── SEARCH-VOLUME RANDOM FALLBACK CONFIG ────────────────────────────────────
# If a search-volume ("search/mo") API call fails for ANY reason — bad/
# exhausted RapidAPI credits, rate-limit, timeout, non-JSON body, no
# recognizable volume field, or RAPIDAPI_KEY not configured at all — we
# no longer leave search_volume as None. Instead we generate a random
# placeholder in this range so scoring/dashboards always have a plausible
# number instead of being dragged to the "no data" floor. This NEVER
# overwrites a real, provider-returned value — it only ever fills in for
# a genuine failure/absence, and every time it fires it is logged with a
# clearly-labelled "RANDOM FALLBACK" warning naming the exact value used
# and the reason, so it is always distinguishable from a real value in
# the logs. This is completely independent of, and never blocks or is
# blocked by, the separate Google-rank/SERP RapidAPI calls.
SEARCH_VOLUME_RANDOM_FALLBACK_MIN = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MIN", "300"))
SEARCH_VOLUME_RANDOM_FALLBACK_MAX = int(os.getenv("SEARCH_VOLUME_RANDOM_FALLBACK_MAX", "5000"))


def _random_search_volume_fallback() -> int:
    """Generates one random placeholder search_volume in the configured
    range. Pulled into its own tiny helper purely so every call site uses
    the exact same range/behavior."""
    return random.randint(SEARCH_VOLUME_RANDOM_FALLBACK_MIN, SEARCH_VOLUME_RANDOM_FALLBACK_MAX)


# ── REDDIT ENGAGEMENT (upvotes/comments) RANDOM FALLBACK CONFIG ────────────
# Reddit's public RSS feed (used for the per-post fetch — see module
# docstring) does NOT expose numeric upvote or comment counts — this is a
# genuine schema limitation of the RSS format itself, not a parsing bug.
# Since Component 3 (Engagement Signal) of the Claude scoring model needs
# a numeric value to score against, every Reddit post confirmed via RSS
# gets a random placeholder upvotes/comments value in this range instead
# of None/0, using the exact same "random fallback, always logged, never
# silently indistinguishable from a real value" pattern already used for
# search_volume above.
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN", "100"))
REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX = int(os.getenv("REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX", "3000"))


def _random_engagement_fallback() -> int:
    """Generates one random placeholder upvotes/comments value in the
    configured range. Separate helper (own range) from the search-volume
    one above, even though the pattern is identical, so the two ranges
    can be tuned independently."""
    return random.randint(REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN, REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX)


# ── SERP DISCOVERY CONFIG (Reddit's ONLY discovery mechanism now) ───────────
# Keywords now live DIRECTLY in this Python list — no .env / os.getenv
# involved. To add a new keyword, just add a new string to this list and
# restart (or, if hot-reload is set up, it gets picked up on the next
# sync pass). Everything downstream is unchanged:
#   - sync_keywords_to_db() inserts any keyword NOT already in
#     flintel_keywords with fetched=False, search_volume=None.
#   - get_keywords_missing_volume() + seed_search_volume_batch() fill in
#     search_volume for any keyword that doesn't have one yet, IN BATCHES
#     of up to 500 keywords per DataForSEO request (never one-by-one).
#     This looks at ALL of flintel_keywords, not just whatever happens to
#     still be in this python list right now.
#   - get_due_keywords() picks up only fetched=False keywords — looks at
#     ALL of flintel_keywords, not just this python list.
#   - mark_keyword_fetched() flips a keyword to fetched=True PERMANENTLY
#     right after its SERP results are all saved to flintel_google_posts
#     — it will never be re-fetched.
REDDIT_SEARCH_KEYWORDS = [
  
]

# ── PER-KEYWORD "FETCH ONCE, EVER" CACHE CONFIG ─────────────────────────────
# A keyword is fetched from DataForSEO exactly ONE time, ever. Once marked
# fetched=True, it is PERMANENTLY skipped — no 12h/24h/whatever re-fetch,
# no TTL expiry, nothing. This guarantees Claude/signals data is never
# disturbed by the same keyword being re-searched and re-processed later.
# The ONLY way a keyword gets processed again is if it is removed from
# flintel_keywords manually (or the collection is reset).
#
# KEYWORD_CHECK_INTERVAL_SECONDS -> how often the loop wakes up to ask
#                        "are there any NEW (never-fetched) keywords, or
#                        any keyword still missing a search_volume?"
#                        This is a cheap DB query, NOT a DataForSEO call
#                        by itself — the (batched) DataForSEO call only
#                        fires when there is actually something missing.
#
# "due" and "missing volume" are determined PURELY from flintel_keywords
# itself (fetched=False / search_volume=None on the stored document) —
# NOT from whether the keyword still happens to be present in the
# REDDIT_SEARCH_KEYWORDS python list above. The python list's only job is
# to tell sync_keywords_to_db() which brand-new keywords to INSERT
# (insert-only, via $setOnInsert — never overwrites an existing doc).
KEYWORD_CHECK_INTERVAL_SECONDS  = int(os.getenv("KEYWORD_CHECK_INTERVAL_SECONDS", "60"))

# ── KEYWORD RETRY COOLDOWN (kept, unchanged from v9.11.2) ───────────────────
# NOTE: as of v9.12, process_one_keyword() no longer fetches Reddit posts
# itself (that now happens in the fully separate
# run_google_posts_rss_matching_loop() below, driven off flintel_google_posts,
# not off a per-keyword failure). This cooldown mechanism and
# set_keyword_retry_cooldown() are kept 100% as-is for API compatibility
# and in case of future SERP-call-level failures, but process_one_keyword()
# no longer produces had_fetch_failure=True from a Reddit RSS failure —
# see process_one_keyword() below for what "had_fetch_failure" means now.
REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS = int(os.getenv("REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS", "1800"))

SERP_RESULTS_PER_KEYWORD = int(os.getenv("SERP_RESULTS_PER_KEYWORD", "20"))
SERP_MONTHS_BACK         = int(os.getenv("SERP_MONTHS_BACK", "6"))
SERP_FETCH_SLEEP_SECONDS = float(os.getenv("SERP_FETCH_SLEEP_SECONDS", "1.5"))

# ── SEARCH-VOLUME BATCH SEEDING CONFIG ──────────────────────────────────────
# search_volume/live bills PER REQUEST, not per keyword, and accepts up to
# 1000 keywords in a single call. We use 500 as a safe default chunk size.
SEARCH_VOLUME_BATCH_SIZE = int(os.getenv("SEARCH_VOLUME_BATCH_SIZE", "12"))

# ── FLINTEL_GOOGLE_POSTS / RSS-MATCHING CONFIG (from v9.12, unchanged) ──────
# GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS -> how often the independent
#   Reddit-RSS-matching loop wakes up to re-sync the targeting collections
#   and re-check flintel_google_posts for distinct subreddits that still
#   have fetched=False documents. This is a cheap DB query — the actual
#   per-subreddit RSS HTTP call only fires for subreddits that genuinely
#   have pending (fetched=False) documents.
#
# FUZZY_KEYWORDS_PER_POST -> how many auto-generated fuzzy keyword variants
#   generate_fuzzy_keywords() produces per discovered Google-SERP post
#   (6-7 by default, smart word-combination based off the matched Google
#   search keyword — see generate_fuzzy_keywords()).
GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS = int(os.getenv("GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS", "45"))
FUZZY_KEYWORDS_PER_POST = int(os.getenv("FUZZY_KEYWORDS_PER_POST", "7"))
GOOGLE_POSTS_RSS_ENTRY_LIMIT = int(os.getenv("GOOGLE_POSTS_RSS_ENTRY_LIMIT", "40"))

# ── TWITTER SEARCH KEYWORDS — independent from Reddit's list, can differ ──
TWITTER_SEARCH_KEYWORDS = [
    kw.strip() for kw in os.getenv(
        "TWITTER_SEARCH_KEYWORDS",
        "Wise blocked,bank blocked my transfer,Payoneer blocked,"
        "cross border payment,CRM is a nightmare,recommend a CRM,"
        "we got hacked,ransomware attack,need incident response,"
        "Salesforce alternative,switching from HubSpot"
    ).split(",") if kw.strip()
]

# ── REDDIT "SMART FETCH" CONFIG — v9.6 retry logic, unchanged ──────────────
# Governs the retry/backoff/User-Agent behaviour of _reddit_get_with_retry()
# — used both for the per-subreddit RSS fetch (v9.12) — public,
# credential-free, no OAuth/PRAW. Does NOT change what data is extracted or
# where it goes — only how reliably we get a 200 instead of a 403 from
# Reddit's public RSS feeds.
REDDIT_FETCH_MAX_RETRIES     = int(os.getenv("REDDIT_FETCH_MAX_RETRIES", "3"))
REDDIT_FETCH_BACKOFF_BASE    = float(os.getenv("REDDIT_FETCH_BACKOFF_BASE", "2.0"))
REDDIT_FETCH_JITTER_MIN      = float(os.getenv("REDDIT_FETCH_JITTER_MIN", "0.4"))
REDDIT_FETCH_JITTER_MAX      = float(os.getenv("REDDIT_FETCH_JITTER_MAX", "1.6"))
# Reddit recommends: "<platform>:<app id>:<version> (by /u/<username>)"
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT",
    "python:flintel-signal-bot:v9.13 (by /u/flintel_signals)",
)

# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

API_KEY = os.getenv("API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key",    auto_error=False)


async def verify_api_key(
    key_header: str = Security(api_key_header),
    key_query:  str = Security(api_key_query),
):
    if not API_KEY:
        return
    if key_header == API_KEY or key_query == API_KEY:
        return
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid or missing API key.")


# ─────────────────────────────────────────────────────────────────────────────
# PLATFORM ENABLE / DISABLE FLAGS
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED  = _bool_env("REDDIT_ENABLED",  True)
TWITTER_ENABLED = _bool_env("TWITTER_ENABLED", False)


def _working(flag: bool) -> str:
    return "✅ Working" if flag else "❌ Not Working"


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC JSON FIELD-EXTRACTION HELPERS — unchanged from v9.6.
#
# These exist because RapidAPI marketplace providers do NOT guarantee a
# fixed response schema the way DataForSEO's own API does. The old code
# assumed exact key names ("rank_absolute", "search_volume", "results")
# and silently returned None forever when the provider used a different
# name. _dig_value()/_dig_list() search across a list of candidate key
# names, at the top level and one level of nesting, so a provider's real
# field naming is found instead of guessed-and-missed.
# ─────────────────────────────────────────────────────────────────────────────

def _dig_value(obj, candidate_keys: list):
    """
    Searches `obj` (a dict, or a list of dicts) for the first present key
    from `candidate_keys`, checking the top level first, then one level
    of nested dict/list values. Returns the first match's value, or None
    if nothing matches. Purely additive/defensive — never raises.
    """
    if obj is None:
        return None

    def _try_dict(d):
        if not isinstance(d, dict):
            return None
        for key in candidate_keys:
            if key in d and d[key] is not None:
                return d[key]
        return None

    # top-level dict
    if isinstance(obj, dict):
        val = _try_dict(obj)
        if val is not None:
            return val
        # one level of nesting inside any dict/list value
        for v in obj.values():
            if isinstance(v, dict):
                val = _try_dict(v)
                if val is not None:
                    return val
            elif isinstance(v, list) and v:
                first = v[0]
                if isinstance(first, dict):
                    val = _try_dict(first)
                    if val is not None:
                        return val

    # top-level list of dicts (take the first element)
    elif isinstance(obj, list) and obj:
        first = obj[0]
        if isinstance(first, dict):
            val = _try_dict(first)
            if val is not None:
                return val

    return None


def _dig_list(obj, candidate_list_keys: list) -> list:
    """
    Searches a RapidAPI JSON response for the results/organic-results
    list, trying several common key names used across different
    providers ("results", "organic_results", "items", "data", "items",
    "organic", "response"). Falls back to: if `obj` itself is already a
    list, return it as-is. Returns [] if nothing usable is found —
    never raises.
    """
    if isinstance(obj, list):
        return obj
    if not isinstance(obj, dict):
        return []
    for key in candidate_list_keys:
        val = obj.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            # some providers nest one level deeper, e.g. {"data": {"results": [...]}}
            for inner_key in candidate_list_keys:
                inner_val = val.get(inner_key)
                if isinstance(inner_val, list):
                    return inner_val
    return []


# Candidate field names for a per-result Google rank/position.
RANK_FIELD_CANDIDATES = [
    "rank_absolute", "rank", "position", "google_rank",
    "serp_position", "rank_group", "index", "pos",
]

# Candidate field names for the result-list container.
RESULT_LIST_KEY_CANDIDATES = [
    "results", "organic_results", "organic", "items", "data", "response", "hits",
]

# Candidate field names for monthly search volume.
VOLUME_FIELD_CANDIDATES = [
    "search_volume", "searchVolume", "volume", "monthly_searches",
    "avg_monthly_searches", "monthlySearchVolume", "search_volume_monthly",
    "avg_search_volume",
]


# ─────────────────────────────────────────────────────────────────────────────
# SHARED QUEUES — platform-isolated, NEVER mixed.
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:  queue.Queue = queue.Queue()
twitter_queue: queue.Queue = queue.Queue()


def passes_keyword_filter(text: str, keywords: list) -> bool:
    """Generic keyword gate — takes an explicit keyword list so Reddit
    and Twitter can be filtered against their own independent lists.
    NOTE (v9.13): still used for Twitter exactly as before. For Reddit,
    run_batch_processor() below now SKIPS this call entirely — see that
    function's comments for why."""
    t = text.lower()
    for kw in keywords:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# FUZZY KEYWORD GENERATION (from v9.12, unchanged)
#
# Given the exact Google search keyword that produced a SERP result,
# deterministically generates 6-7 "fuzzy" keyword variants using smart
# word-combination logic — contiguous n-grams (bigrams/trigrams),
# stopword-stripped phrases, partial (head/tail-trimmed) phrases, and
# individually significant single words. No external NLP library is
# needed — this is a pure, dependency-free, reproducible heuristic.
#
# These fuzzy keywords are stored alongside each flintel_google_posts
# document (and mirrored onto its flintel_targeting_keywords document —
# see sync_targeting_collections() below) purely for traceability /
# secondary text confirmation — the AUTHORITATIVE match signal is always
# the exact post_url, never the fuzzy keywords alone.
# ─────────────────────────────────────────────────────────────────────────────

_FUZZY_STOPWORDS = {
    "a", "an", "the", "my", "our", "your", "their", "his", "her",
    "to", "for", "is", "are", "was", "were", "of", "on", "in", "it",
    "this", "that", "and", "or", "with", "at", "by", "from", "as",
    "be", "been", "has", "have", "had", "do", "does", "did", "not",
}


def generate_fuzzy_keywords(keyword: str, max_variants: int = FUZZY_KEYWORDS_PER_POST) -> list:
    """
    Deterministically generates up to `max_variants` fuzzy keyword
    strings from `keyword` (the exact Google search keyword that produced
    a given SERP result). Smart, dependency-free, word-combination based:

      - the full original phrase (lowercased)
      - the stopword-stripped content-word phrase
      - every contiguous bigram
      - every contiguous trigram (if the phrase has >= 3 words)
      - head-trimmed and tail-trimmed partial phrases
      - individually significant single words (len > 3, not a stopword)

    Variants are deduplicated, then sorted so longer/more-specific
    multi-word phrases are prioritized over single words, and finally
    capped at `max_variants` (default 7). Never raises — falls back to
    just the original phrase if `keyword` is empty/whitespace.
    """
    if not keyword or not keyword.strip():
        return []

    original = keyword.strip().lower()
    words = re.findall(r"[a-zA-Z0-9']+", original)
    if not words:
        return [original]

    content_words = [w for w in words if w not in _FUZZY_STOPWORDS]

    variants = set()
    variants.add(original)

    if content_words:
        variants.add(" ".join(content_words))

    # contiguous bigrams
    for i in range(len(words) - 1):
        variants.add(" ".join(words[i:i + 2]))

    # contiguous trigrams
    for i in range(len(words) - 2):
        variants.add(" ".join(words[i:i + 3]))

    # head/tail-trimmed partial phrases
    if len(words) > 1:
        variants.add(" ".join(words[:-1]))
        variants.add(" ".join(words[1:]))

    # individually significant single words
    for w in content_words:
        if len(w) > 3:
            variants.add(w)

    variants.discard("")

    result = list(variants)
    # prioritize longer, multi-word, more-specific phrases first
    result.sort(key=lambda v: (-len(v.split()), -len(v)))

    return result[:max_variants]


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY — built directly from TWITTER_SEARCH_KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    if not TWITTER_SEARCH_KEYWORDS:
        return (
            "(\"international transfer\" OR \"bank blocked\" OR \"we got hacked\""
            " OR \"CRM is a nightmare\") -is:retweet lang:en"
        )
    parts = [f'"{kw}"' if " " in kw else kw for kw in TWITTER_SEARCH_KEYWORDS]
    query = "(" + " OR ".join(parts) + ") -is:retweet lang:en"
    log.info(f"Twitter search query built | terms:{len(parts)} | len:{len(query)}")
    return query


TWITTER_SEARCH_QUERY = _build_twitter_search_query()


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE PROMPT — generic, niche-agnostic (unchanged schema)
# ─────────────────────────────────────────────────────────────────────────────

CLAUDE_SYSTEM_PROMPT = """
You are Flintel's signal intelligence analyst.

Your job is to read one social media post (Reddit or X), together with
its metadata and the industry it was matched against, and produce two
things:

1. An intent_score from 1 to 100, built from three weighted components
2. A short, human-written-style reply draft the end user can personalize
   and post themselves, in their own voice, from their own account

You score using the industry context you are given. You are never told
the specific company or product this is for — only the industry
category (e.g. "fintech_payments", "cybersecurity", "crm_sales_tools",
"logistics", "recruitment_hr", "accounting_software"). Two posts using
identical words ("hidden fees are killing us") can score very
differently depending on whether the industry context is fintech
billing versus logistics freight surcharges — use the industry field to
judge whether the post's actual subject matches that vertical's real
buyer pain, not just shared vocabulary.

═══════════════════════════════════════════════════════════════════════
INPUT YOU WILL RECEIVE, PER POST
═══════════════════════════════════════════════════════════════════════
- platform: "reddit" | "x"
- industry: one of the six category strings above
- search_keyword: the phrase this post was matched against
- post_text: the raw post content
- google_rank: integer, or null (X posts will almost always be null —
  see Component 2 below)
- search_volume: monthly search volume for search_keyword, or null
- upvotes / likes: integer, platform-appropriate
- comments: integer

═══════════════════════════════════════════════════════════════════════
SCORING MODEL — 100 POINTS, THREE COMPONENTS
═══════════════════════════════════════════════════════════════════════

── COMPONENT 1 — RELEVANCE MATCH (0-40 points) ──────────────────────
Does this post genuinely discuss the same problem or need as
search_keyword, interpreted through the lens of the given industry —
in meaning, not just in shared words?

  36-40  Unambiguously about exactly this problem, in this industry.
  25-35  Clearly related, but broader, tangential, or partial —
         e.g. discussing the general category without the specific pain.
  10-24  Matching words present, but the actual subject differs, OR the
         pain described belongs to a different industry than the one
         given (e.g. "hidden fees" post is about parking tickets, not
         payment processing).
  0-9    No genuine connection.

THIS COMPONENT IS A HARD GATE.
If relevance scores below 10: is_relevant = false, and intent_score
must not exceed 15 — regardless of how strong Component 2 or 3 look.
A top-ranked, highly-upvoted post about the wrong subject is still a
wrong-subject post.

── COMPONENT 2 — GOOGLE VISIBILITY (0-30 points) ─────────────────────
google_rank contribution (0-20):
  Rank 1        -> 20
  Rank 2-3      -> 16
  Rank 4-10     -> 11
  Rank 11-20    -> 6
  Not ranked/null -> 0

search_volume contribution (0-10):
  10,000+/mo    -> 10
  3,000-9,999   -> 7
  500-2,999     -> 4
  Under 500/null -> 1

X-SPECIFIC NOTE: X posts are not Google-indexed the way Reddit threads
are, so google_rank will almost always be null for platform == "x".
A null rank on an X post is EXPECTED and is not a quality signal one
way or the other — do not treat it as a penalty, and do not attempt to
infer or guess a rank that wasn't provided. Score the 0-point rank
contribution plainly and let Components 1 and 3 carry that post.

── COMPONENT 3 — ENGAGEMENT SIGNAL (0-30 points) ─────────────────────
Derived from upvotes/likes and comments, judged proportionally to
platform norms — the same raw number means different things on
different platforms.

Reference anchors (interpolate between these, don't treat as rigid
cutoffs):
  REDDIT   Strong: 50+ upvotes, 15+ comments      -> 22-30
           Moderate: 10-49 upvotes, 3-14 comments  -> 10-21
           Low: under 10 upvotes, under 3 comments -> 0-9
  X        Strong: 100+ likes, 10+ replies         -> 22-30
           Moderate: 20-99 likes, 2-9 replies       -> 10-21
           Low: under 20 likes, under 2 replies     -> 0-9
  No engagement data provided on either platform    -> 0

FINAL intent_score = Component 1 + Component 2 + Component 3, capped at 100.

═══════════════════════════════════════════════════════════════════════
WORKED EXAMPLES
═══════════════════════════════════════════════════════════════════════

Example A — high-scoring, correct industry match
  Input: platform=reddit, industry=fintech_payments,
  search_keyword="cross-border payment fees", google_rank=2,
  search_volume=4200, upvotes=87, comments=22,
  post_text="Does anyone have a solid alternative to [processor] for
  cross-border fees? We're getting killed on FX markups every month."
  Reasoning: Directly about cross-border payment fees in a fintech
  context (Component 1: 39). Rank 2 + volume 4,200/mo (Component 2:
  16+7=23). 87 upvotes/22 comments on Reddit is strong (Component 3: 26).
  Output: intent_score=88, is_relevant=true,
  reply_draft="Cross-border fees catch a lot of teams off guard —
  worth checking whether your provider discloses FX markup upfront or
  buries it in the settlement rate. Have you compared what you're
  actually losing per transaction?"

Example B — hard-gate failure despite strong surface signals
  Input: platform=reddit, industry=logistics,
  search_keyword="hidden fees", google_rank=1, search_volume=8000,
  upvotes=340, comments=95,
  post_text="Just found out my city adds a hidden fee to every parking
  ticket if you pay online. Total scam."
  Reasoning: Shares the words "hidden fees" but is about parking
  tickets, not logistics/freight pricing (Component 1: 4 — hard gate
  triggered). Rank and engagement are irrelevant once the gate fails.
  Output: intent_score=9, is_relevant=false, reply_draft=null

Example C — X post, no Google rank, still a real match
  Input: platform=x, industry=cybersecurity,
  search_keyword="EDR alert fatigue", google_rank=null,
  search_volume=1400, likes=64, comments=11,
  post_text="Our SOC ignored a real alert last week because we get 200
  false positives a day. Something has to change."
  Reasoning: Directly describes EDR alert fatigue (Component 1: 37).
  google_rank null is expected for X — score 0 for that piece, but
  volume 1,400 still contributes (Component 2: 0+4=4). 64 likes/11
  comments is strong for X (Component 3: 25).
  Output: intent_score=66, is_relevant=true,
  reply_draft="200 false positives a day would burn out any team, not
  just miss the real one. Sounds like the tuning problem is as much
  the issue as the tool itself — has your team looked at what's driving
  the noise ratio that high?"

═══════════════════════════════════════════════════════════════════════
REPLY DRAFT — RULES
═══════════════════════════════════════════════════════════════════════
Only generate reply_draft when is_relevant is true. Otherwise: null.

- Generic and honest — never invent a fake personal story, dollar
  amount, or timeline not present in the input.
- Acknowledge the poster's situation in one clause, then offer one
  genuinely useful angle — not a pitch.
- 2-3 sentences maximum. No links, no "DM me," no product/company name
  (the end user adds that themselves if relevant).
- End on warmth or a question, never a call-to-action.
- AVOID: "I totally understand," "This is so common," or any opener
  that could paste onto literally any post — anchor the first clause
  to a specific detail from post_text so it reads as actually read,
  not templated.

═══════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════════
Return ONLY valid JSON. No preamble, no markdown, no code fences.
Return one object per post, in a JSON array, same order as received.

[
  {
    "index": <1-based integer matching input order>,
    "intent_score": <integer 1-100>,
    "is_relevant": <true|false>,
    "reply_draft": "<string, 2-3 sentences, or null if is_relevant is false>"
  }
]

Score every post received. Return the same count as received. Never
omit an item. Never add commentary outside the JSON array.
"""


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB — signals collection + persistent batch-state collections +
# per-keyword fetch-once-forever cache collection (flintel_keywords) +
# flintel_google_posts (SERP-discovered post_url cache, decoupled from
# Reddit RSS fetching) + NEW (v9.13): flintel_targeting_subreddits /
# flintel_targeting_keywords — auto-synced mirror collections.
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        db.signals.create_index([("post_url", ASCENDING)], name="post_url_lookup")
        for field in ["intent_score", "created_at", "client_id", "platform", "is_relevant", "status"]:
            db.signals.create_index([(field, ASCENDING)])

        # persistent batch state — survives restarts, no in-flight batch lost
        db.flintel_pending_batch.create_index([("platform", ASCENDING)], unique=True, name="platform_unique")
        db.flintel_seen_ids.create_index([("platform", ASCENDING)], unique=True, name="seen_platform_unique")
        db.flintel_queue_messages.create_index(
            [("_platform_key", ASCENDING), ("message_id", ASCENDING)],
            unique=True, name="queue_platform_message_unique",
        )
        db.flintel_batch_seconds.create_index(
            [("platform", ASCENDING)], unique=True, name="batch_seconds_platform_unique"
        )

        # ── flintel_keywords — FETCH-ONCE-FOREVER cache. UNTOUCHED in v9.13.
        # This collection, its indexes, and every function that reads/writes
        # it (sync_keywords_to_db, get_due_keywords, get_keywords_missing_volume,
        # mark_keyword_fetched, set_keyword_retry_cooldown,
        # seed_search_volume_batch) are byte-for-byte identical to v9.11.1.
        db.flintel_keywords.create_index([("keyword", ASCENDING)], unique=True, name="keyword_unique")
        db.flintel_keywords.create_index([("fetched", ASCENDING)], name="keyword_fetched_idx")
        db.flintel_keywords.create_index([("search_volume", ASCENDING)], name="keyword_volume_idx")
        db.flintel_keywords.create_index([("next_retry_at", ASCENDING)], name="keyword_retry_cooldown_idx")

        # ── flintel_google_posts — from v9.12, UNTOUCHED. Stores every
        # Google-SERP-discovered Reddit post_url the instant SERP discovery
        # finds it — completely independent of whether/when that post's
        # actual Reddit RSS confirmation happens. One document per
        # discovered post_url:
        #   post_url        : the exact Reddit post URL Google SERP returned
        #   google_rank      : the real per-post rank from that SERP call
        #   matched_keyword  : the exact Google search keyword that produced it
        #   fuzzy_keywords   : 6-7 auto-generated fuzzy variants of matched_keyword
        #   subreddit        : subreddit name extracted from post_url
        #   fetched          : False until run_google_posts_rss_matching_loop()
        #                      confirms this exact post_url via subreddit RSS —
        #                      then True, PERMANENTLY (fetch-once-forever, same
        #                      spirit as flintel_keywords)
        #   created_at       : when this document was first saved
        db.flintel_google_posts.create_index(
            [("post_url", ASCENDING)], unique=True, name="google_post_url_unique"
        )
        db.flintel_google_posts.create_index([("fetched", ASCENDING)], name="google_post_fetched_idx")
        db.flintel_google_posts.create_index([("subreddit", ASCENDING)], name="google_post_subreddit_idx")
        db.flintel_google_posts.create_index(
            [("subreddit", ASCENDING), ("fetched", ASCENDING)], name="google_post_subreddit_fetched_idx"
        )

        # ── flintel_targeting_subreddits — NEW in v9.13. One document per
        # DISTINCT subreddit that currently has at least one PENDING
        # (fetched=False) flintel_google_posts document. Fully rebuilt
        # every pass of sync_targeting_collections() — this collection is
        # what run_google_posts_rss_matching_loop() actually reads to
        # decide which subreddits to poll this cycle.
        db.flintel_targeting_subreddits.create_index(
            [("subreddit", ASCENDING)], unique=True, name="targeting_subreddit_unique"
        )

        # ── flintel_targeting_keywords — NEW in v9.13. One document per
        # PENDING flintel_google_posts document, keyed by post_url,
        # carrying its matched_keyword + fuzzy_keywords + subreddit. This
        # is a live tracking mirror — the moment a post_url is CONFIRMED
        # (URL match against a subreddit's RSS feed), its document here is
        # deleted immediately. Also fully re-synced (stale entries pruned)
        # on every pass, so it never drifts from flintel_google_posts's
        # real pending state.
        db.flintel_targeting_keywords.create_index(
            [("post_url", ASCENDING)], unique=True, name="targeting_keyword_post_url_unique"
        )
        db.flintel_targeting_keywords.create_index(
            [("subreddit", ASCENDING)], name="targeting_keyword_subreddit_idx"
        )
        db.flintel_targeting_keywords.create_index(
            [("keyword", ASCENDING)], name="targeting_keyword_keyword_idx"
        )

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT — streaming
# ─────────────────────────────────────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(
    api_key=ANTHROPIC_API_KEY,
    http_client=httpx.Client(
        timeout=httpx.Timeout(connect=30.0, read=None, write=60.0, pool=30.0)
    ),
)


def retry_with_backoff(func, *args, retries=3, delay=2, label="op", **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            wait = delay * attempt
            log.error(f"[{label}] attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                log.info(f"[{label}] retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.critical(f"[{label}] all {retries} attempts failed.")
                return None


def log_operator_alert(title: str, detail: str, level: str = "ERROR"):
    log.log(
        logging.CRITICAL if level == "CRITICAL" else logging.ERROR,
        f"[OPERATOR ALERT] {title} — {detail}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENT BATCH STATE HELPERS — survives process restarts, so a
# half-filled batch never disappears.
# ─────────────────────────────────────────────────────────────────────────────

def load_pending_batch(platform: str) -> tuple:
    try:
        doc = db.flintel_pending_batch.find_one({"platform": platform})
        if not doc:
            return [], None
        items = doc.get("items", [])
        start_ts = doc.get("batch_start_time")
        start_time = start_ts.timestamp() if start_ts else None
        if items:
            log.warning(f"[{platform.upper()}] Resuming persisted batch | {len(items)} item(s) recovered.")
        return items, start_time
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_pending_batch error: {exc}")
        return [], None


def save_pending_batch(platform: str, items: list, batch_start_time):
    try:
        start_dt = datetime.fromtimestamp(batch_start_time, tz=timezone.utc) if batch_start_time else None
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "items": items, "batch_start_time": start_dt,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_pending_batch error: {exc}")


def clear_pending_batch(platform: str):
    try:
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "items": [], "batch_start_time": None,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_pending_batch error: {exc}")


def load_seen_ids(platform: str) -> set:
    try:
        doc = db.flintel_seen_ids.find_one({"platform": platform})
        return set(doc.get("ids", [])) if doc else set()
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_seen_ids error: {exc}")
        return set()


def save_seen_ids(platform: str, ids: set, cap: int = 200_000):
    try:
        id_list = list(ids)
        if len(id_list) > cap:
            id_list = id_list[-cap:]
        db.flintel_seen_ids.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "ids": id_list, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_seen_ids error: {exc}")


def save_queue_message(platform: str, item: dict):
    try:
        mid = item.get("message_id")
        if not mid:
            return
        doc = dict(item)
        doc["_platform_key"] = platform
        doc["message_id"] = mid
        doc["queued_at"] = datetime.now(timezone.utc)
        db.flintel_queue_messages.update_one(
            {"_platform_key": platform, "message_id": mid}, {"$set": doc}, upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_queue_message error: {exc}")


def remove_queue_message(platform: str, message_id: str):
    if not message_id:
        return
    try:
        db.flintel_queue_messages.delete_one({"_platform_key": platform, "message_id": message_id})
    except Exception as exc:
        log.error(f"[{platform.upper()}] remove_queue_message error: {exc}")


def load_queue_messages(platform: str) -> list:
    try:
        docs = list(db.flintel_queue_messages.find({"_platform_key": platform}))
        items = []
        for d in docs:
            d.pop("_id", None)
            d.pop("_platform_key", None)
            d.pop("queued_at", None)
            items.append(d)
        return items
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_queue_messages error: {exc}")
        return []


def save_batch_seconds(platform: str, batch_start_time):
    try:
        start_dt = datetime.fromtimestamp(batch_start_time, tz=timezone.utc) if batch_start_time else None
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "batch_start_time": start_dt,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_batch_seconds error: {exc}")


def clear_batch_seconds(platform: str):
    try:
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {"platform": platform, "batch_start_time": None,
                       "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_batch_seconds error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD CACHE — flintel_keywords collection. 100% UNCHANGED FROM v9.11.1.
# FETCH-ONCE-FOREVER design: each keyword gets fetched from DataForSEO
# exactly ONE time, ever. Once fetched=True, it is PERMANENTLY skipped by
# get_due_keywords() — no TTL, no re-due date, no 12h/24h re-fetch.
#
# NOTE (v9.12/v9.13): "fetched=True" here now means "this keyword's Google
# SERP results have all been saved to flintel_google_posts" — it no longer
# means "Reddit RSS was fetched for every result" (that dependency has
# been removed — see process_one_keyword() below). Nothing about the
# flintel_keywords collection itself, its schema, or any function in this
# section changed to make that true; it's a natural consequence of
# process_one_keyword() no longer calling into Reddit's RSS at all.
# ─────────────────────────────────────────────────────────────────────────────

def sync_keywords_to_db(keywords: list):
    """
    Ensures every keyword currently in REDDIT_SEARCH_KEYWORDS exists in
    flintel_keywords. Brand-new keywords are inserted with fetched=False
    and search_volume=None (both due immediately, real-time). Keywords
    that already exist are left completely untouched — $setOnInsert only
    writes on first-ever insert. Safe to call every loop pass and on
    every restart.

    This is INSERT-ONLY and additive — it never deletes or hides a
    keyword's existing document just because that keyword is no longer
    present in `keywords`.
    """
    now = datetime.now(timezone.utc)
    for kw in keywords:
        try:
            db.flintel_keywords.update_one(
                {"keyword": kw},
                {"$setOnInsert": {
                    "keyword":                  kw,
                    "fetched":                  False,
                    "search_volume":            None,
                    "search_volume_is_random":  False,
                    "last_fetched_at":          None,
                    "next_retry_at":            None,
                    "created_at":               now,
                }},
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[KEYWORD-CACHE] sync error for {kw!r}: {exc}")


def get_keywords_missing_volume(keywords: list = None) -> list:
    """
    Returns keyword strings whose flintel_keywords document has no
    search_volume stored yet (missing field or explicit None both match
    this query). Taken DIRECTLY against the full flintel_keywords
    collection — NOT restricted to "{'keyword': {'$in': keywords}}".
    """
    try:
        cursor = db.flintel_keywords.find(
            {"search_volume": None},
            {"keyword": 1},
        )
        return [d["keyword"] for d in cursor]
    except Exception as exc:
        log.error(f"[VOLUME-SEED] get_keywords_missing_volume error: {exc}")
        return []


def get_due_keywords() -> list:
    """
    Returns keyword docs that have NEVER been fetched yet (fetched=False).
    Once a keyword is marked fetched=True, it is PERMANENTLY excluded from
    this query. Taken DIRECTLY against the full flintel_keywords
    collection — NOT restricted to the current python list.

    A keyword whose Reddit RSS fetch failed also needs its "next_retry_at"
    cooldown to have passed before it's returned here — see
    REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS and set_keyword_retry_cooldown()
    below. A keyword with next_retry_at unset/None (brand new, never
    attempted) or already in the past is still due immediately.
    """
    try:
        now = datetime.now(timezone.utc)
        cursor = db.flintel_keywords.find({
            "fetched": False,
            "$or": [
                {"next_retry_at": None},
                {"next_retry_at": {"$exists": False}},
                {"next_retry_at": {"$lte": now}},
            ],
        })
        return list(cursor)
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] get_due_keywords error: {exc}")
        return []


def set_keyword_retry_cooldown(keyword: str, cooldown_seconds: int = REDDIT_KEYWORD_RETRY_COOLDOWN_SECONDS):
    """
    Kept 100% as-is from v9.11.2 for API compatibility. As of v9.12/v9.13,
    process_one_keyword() no longer produces a Reddit-RSS-driven
    had_fetch_failure (that logic moved to the fully separate
    run_google_posts_rss_matching_loop(), which operates on
    flintel_google_posts / the targeting collections, not on a
    per-keyword failure flag) — so this function is not currently invoked
    by the SERP discovery loop, but is left untouched in case any future
    SERP-call-level failure needs the same cooldown mechanism.
    """
    now = datetime.now(timezone.utc)
    next_retry = now + timedelta(seconds=cooldown_seconds)
    try:
        db.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {"next_retry_at": next_retry}},
        )
        log.info(
            f"[KEYWORD-CACHE] '{keyword}' cooldown set | next_retry_at:{next_retry.isoformat()} "
            f"({cooldown_seconds}s from now) — will not be re-attempted before then"
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] set_keyword_retry_cooldown error for {keyword!r}: {exc}")


def mark_keyword_fetched(keyword: str):
    """
    Flips a keyword to fetched=True — PERMANENTLY. There is no TTL and no
    next_due_at anymore: once true, this keyword will never be picked up
    by get_due_keywords() again, even after restarts, even after 12h,
    24h, or any amount of time. The only way to re-process a keyword is
    to manually reset/delete its document in flintel_keywords.
    """
    now = datetime.now(timezone.utc)
    try:
        db.flintel_keywords.update_one(
            {"keyword": keyword},
            {"$set": {
                "fetched":         True,
                "last_fetched_at": now,
            }},
        )
    except Exception as exc:
        log.error(f"[KEYWORD-CACHE] mark_keyword_fetched error for {keyword!r}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# flintel_google_posts HELPERS (from v9.12, unchanged)
#
# This collection is the sole source of truth for "which Google-SERP-
# discovered Reddit post_urls are still waiting to be confirmed via
# subreddit RSS?" — completely independent of flintel_keywords, which
# only tracks keyword-level SERP-discovery state.
# ─────────────────────────────────────────────────────────────────────────────

def save_google_post(post_url: str, google_rank, matched_keyword: str, subreddit: str) -> bool:
    """
    Saves ONE newly-discovered Google-SERP result into flintel_google_posts,
    auto-generating its fuzzy_keywords from matched_keyword. Insert-only
    per unique post_url (unique index on post_url) — if this exact
    post_url was already saved in a previous pass, this is a silent no-op
    (duplicate discovery of the same URL, e.g. from a different keyword's
    SERP results overlapping). Does NOT touch flintel_keywords. Does NOT
    wait on or call into any Reddit endpoint — this save is immediate and
    fully independent of Reddit's RSS reliability.
    """
    fuzzy = generate_fuzzy_keywords(matched_keyword, max_variants=FUZZY_KEYWORDS_PER_POST)
    doc = {
        "post_url":        post_url,
        "google_rank":     google_rank,
        "matched_keyword": matched_keyword,
        "fuzzy_keywords":  fuzzy,
        "subreddit":       subreddit,
        "fetched":         False,
        "created_at":      datetime.now(timezone.utc),
    }
    try:
        db.flintel_google_posts.insert_one(doc)
        log.info(
            f"[GOOGLE-POSTS] SAVED | post_url:{post_url} | rank:{google_rank} | "
            f"subreddit:r/{subreddit or '?'} | matched_keyword:{matched_keyword!r} | "
            f"fuzzy_keywords:{fuzzy}"
        )
        return True
    except DuplicateKeyError:
        log.debug(f"[GOOGLE-POSTS] Duplicate post_url skipped (already cached): {post_url}")
        return False
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] save_google_post error for {post_url}: {exc}")
        return False


def get_pending_google_posts_for_subreddit(subreddit: str) -> list:
    """
    Returns every fetched=False flintel_google_posts document for one
    subreddit — the exact set of post_urls run_google_posts_rss_matching_loop()
    is currently trying to confirm via that subreddit's RSS feed. UNCHANGED
    FROM v9.12 — still the source of full per-post detail (google_rank,
    matched_keyword, fuzzy_keywords) used when building each subreddit's
    pending-by-url lookup.
    """
    try:
        return list(db.flintel_google_posts.find({"subreddit": subreddit, "fetched": False}))
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_pending_google_posts_for_subreddit error for r/{subreddit}: {exc}")
        return []


def mark_google_post_fetched(post_url: str):
    """
    Flips a flintel_google_posts document to fetched=True — PERMANENTLY,
    same fetch-once-forever spirit as mark_keyword_fetched() above. Once
    true, this post_url will never be returned by
    get_pending_google_posts_for_subreddit() again.
    """
    now = datetime.now(timezone.utc)
    try:
        db.flintel_google_posts.update_one(
            {"post_url": post_url},
            {"$set": {"fetched": True, "fetched_at": now}},
        )
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] mark_google_post_fetched error for {post_url}: {exc}")


def get_cached_search_volume_for_keyword(keyword: str) -> tuple:
    """
    Read-only lookup straight off flintel_keywords for a single keyword's
    already-seeded search_volume + search_volume_is_random flag. NEVER
    triggers a new API call, NEVER writes to flintel_keywords — this is
    purely a cache read used by run_google_posts_rss_matching_loop() so
    that stage never re-queries the search-volume API itself.
    Returns (search_volume_or_None, is_random_bool).
    """
    try:
        doc = db.flintel_keywords.find_one(
            {"keyword": keyword}, {"search_volume": 1, "search_volume_is_random": 1}
        )
        if not doc:
            return None, False
        return doc.get("search_volume"), bool(doc.get("search_volume_is_random", False))
    except Exception as exc:
        log.error(f"[GOOGLE-POSTS] get_cached_search_volume_for_keyword error for {keyword!r}: {exc}")
        return None, False


# ─────────────────────────────────────────────────────────────────────────────
# flintel_targeting_subreddits / flintel_targeting_keywords HELPERS
# (NEW in v9.13)
#
# These two collections are a LIVE, AUTO-SYNCED MIRROR of whatever is
# currently PENDING (fetched=False) in flintel_google_posts. Nothing here
# is ever hand-maintained or kept in a python list — sync_targeting_
# collections() is called at the top of every
# run_google_posts_rss_matching_loop() pass and fully reconciles both
# collections against flintel_google_posts's live pending state:
#   - inserts a flintel_targeting_keywords doc for any newly-pending
#     post_url (carrying its matched_keyword, fuzzy_keywords, subreddit)
#   - inserts/refreshes a flintel_targeting_subreddits doc for any
#     subreddit that still has at least one pending post
#   - PRUNES both collections of anything no longer pending, so neither
#     one ever drifts out of sync
#
# run_google_posts_rss_matching_loop() reads its subreddit poll list from
# flintel_targeting_subreddits (get_targeting_subreddits()) instead of
# querying flintel_google_posts.distinct() directly. The moment a
# post_url is CONFIRMED via exact RSS-link match, its flintel_targeting_
# keywords document is deleted immediately (delete_targeting_keyword_entry())
# — that keyword/post is done being targeted. flintel_targeting_subreddits
# needs no per-match delete: it is fully rebuilt on the very next sync
# pass, so a subreddit with zero pending posts left naturally drops off
# the poll list within one cycle.
# ─────────────────────────────────────────────────────────────────────────────

def sync_targeting_collections():
    """
    Rebuilds flintel_targeting_subreddits and flintel_targeting_keywords
    from whatever is CURRENTLY pending (fetched=False) in
    flintel_google_posts. Called at the start of every
    run_google_posts_rss_matching_loop() pass so both collections always
    reflect live reality — nothing is ever stale beyond one pass, and
    nothing here is ever a hardcoded/hand-maintained python list.
    """
    try:
        pending_docs = list(
            db.flintel_google_posts.find(
                {"fetched": False},
                {"post_url": 1, "matched_keyword": 1, "fuzzy_keywords": 1, "subreddit": 1},
            )
        )
    except Exception as exc:
        log.error(f"[TARGETING-SYNC] failed to read pending flintel_google_posts: {exc}")
        return

    now = datetime.now(timezone.utc)

    # ── flintel_targeting_keywords — one doc per pending post_url ────────
    pending_urls = []
    for doc in pending_docs:
        post_url = doc.get("post_url")
        if not post_url:
            continue
        pending_urls.append(post_url)
        try:
            db.flintel_targeting_keywords.update_one(
                {"post_url": post_url},
                {"$setOnInsert": {
                    "post_url":        post_url,
                    "keyword":         doc.get("matched_keyword", ""),
                    "fuzzy_keywords":  doc.get("fuzzy_keywords", []),
                    "subreddit":       doc.get("subreddit", ""),
                    "created_at":      now,
                }},
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[TARGETING-SYNC] keyword upsert error for {post_url}: {exc}")

    # prune any flintel_targeting_keywords doc whose post_url is no longer
    # pending (already confirmed/fetched through some other path) — keeps
    # this collection an exact live mirror, never drifting from reality.
    try:
        db.flintel_targeting_keywords.delete_many({"post_url": {"$nin": pending_urls}})
    except Exception as exc:
        log.error(f"[TARGETING-SYNC] keyword prune error: {exc}")

    # ── flintel_targeting_subreddits — one doc per distinct pending subreddit ─
    pending_subreddits = sorted({doc.get("subreddit") for doc in pending_docs if doc.get("subreddit")})
    for sub in pending_subreddits:
        try:
            db.flintel_targeting_subreddits.update_one(
                {"subreddit": sub},
                {
                    "$set": {"subreddit": sub, "last_synced_at": now},
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        except Exception as exc:
            log.error(f"[TARGETING-SYNC] subreddit upsert error for r/{sub}: {exc}")

    try:
        db.flintel_targeting_subreddits.delete_many({"subreddit": {"$nin": pending_subreddits}})
    except Exception as exc:
        log.error(f"[TARGETING-SYNC] subreddit prune error: {exc}")

    log.info(
        f"[TARGETING-SYNC] synced | pending_posts:{len(pending_urls)} | "
        f"targeting_subreddits:{len(pending_subreddits)}"
    )


def get_targeting_subreddits() -> list:
    """
    Reads the distinct subreddit list DIRECTLY off
    flintel_targeting_subreddits — this is what
    run_google_posts_rss_matching_loop() actually polls this cycle,
    instead of re-querying flintel_google_posts.distinct() live every
    single pass. Always fresh, since sync_targeting_collections() runs
    immediately before this is called.
    """
    try:
        docs = db.flintel_targeting_subreddits.find({}, {"subreddit": 1})
        return [d["subreddit"] for d in docs if d.get("subreddit")]
    except Exception as exc:
        log.error(f"[TARGETING-SYNC] get_targeting_subreddits error: {exc}")
        return []


def delete_targeting_keyword_entry(post_url: str):
    """
    Deletes ONE flintel_targeting_keywords document by post_url — called
    the instant that post_url is CONFIRMED via exact RSS-link match
    inside run_google_posts_rss_matching_loop(). That keyword/post is
    done being targeted and is removed immediately, rather than waiting
    for the next sync_targeting_collections() prune pass.
    """
    try:
        db.flintel_targeting_keywords.delete_one({"post_url": post_url})
    except Exception as exc:
        log.error(f"[TARGETING-SYNC] delete_targeting_keyword_entry error for {post_url}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH-VOLUME BATCH SEEDING — 100% UNCHANGED FROM v9.11.1. chunks
# keywords, fetches volume for each one (single.php only accepts one
# keyword per call), writes results back onto each keyword's own
# flintel_keywords document.
# ─────────────────────────────────────────────────────────────────────────────

def seed_search_volume_batch(keywords_needing_volume: list, batch_size: int = SEARCH_VOLUME_BATCH_SIZE):
    """
    ONE-TIME (per keyword) BATCH search-volume seeding. Splits
    `keywords_needing_volume` into chunks of up to `batch_size` and
    fetches volume for every keyword in the chunk. Results are written
    back onto each keyword's own flintel_keywords document
    (search_volume field, plus search_volume_is_random).
    """
    if not keywords_needing_volume:
        return
    if not RAPIDAPI_KEY:
        log.warning(
            "[VOLUME-SEED] RapidAPI key not set — cannot call the search-volume API. "
            "Applying RANDOM FALLBACK values to all keywords in this pass so they are "
            "never left permanently at None."
        )

    for i in range(0, len(keywords_needing_volume), batch_size):
        chunk = keywords_needing_volume[i:i + batch_size]
        try:
            volume_map = {}
            random_map = {}

            for kw in chunk:
                if not RAPIDAPI_KEY:
                    vol = _random_search_volume_fallback()
                    volume_map[kw] = vol
                    random_map[kw] = True
                    log.warning(
                        f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | "
                        f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                        f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: RAPIDAPI_KEY not "
                        f"configured — call never made | this is NOT a real search volume."
                    )
                    continue

                url = "https://seo-keyword-research.p.rapidapi.com/single.php"

                querystring = {"keyword": kw, "country": "us"}

                headers = {
                    "x-rapidapi-key": RAPIDAPI_KEY, # .env
                    "x-rapidapi-host": RAPIDAPI_KEYWORD_HOST,
                    "Content-Type": "application/json"
                }

                try:
                    r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_VOLUME_TIMEOUT_SECONDS)
                    status_code = r.status_code
                    try:
                        row = r.json()
                    except ValueError:
                        log.error(f"[VOLUME-SEED] Non-JSON response for {kw!r} | status:{status_code}")
                        row = None
                except Exception as call_exc:
                    log.error(f"[VOLUME-SEED] request error for {kw!r}: {call_exc}")
                    status_code = None
                    row = None

                vol = _dig_value(row, VOLUME_FIELD_CANDIDATES)
                if vol is None:
                    api_message = row.get("message") if isinstance(row, dict) else None
                    log.warning(
                        f"[VOLUME-SEED] No search_volume for {kw!r} | status:{status_code} | "
                        f"api_message:{api_message!r} | tried_fields:{VOLUME_FIELD_CANDIDATES} | "
                        f"raw_keys:{list(row.keys()) if isinstance(row, dict) else type(row).__name__}"
                    )
                    vol = _random_search_volume_fallback()
                    random_map[kw] = True
                    log.warning(
                        f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | "
                        f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                        f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: no credits / bad key / "
                        f"rate-limited / no usable field (see api_message above) | this is NOT "
                        f"a real, provider-returned search volume."
                    )
                else:
                    random_map[kw] = False
                volume_map[kw] = vol

            for kw in chunk:
                vol = volume_map.get(kw)
                is_random = random_map.get(kw, False)
                db.flintel_keywords.update_one(
                    {"keyword": kw},
                    {"$set": {"search_volume": vol, "search_volume_is_random": is_random}},
                    upsert=True,
                )

            random_count = sum(1 for v in random_map.values() if v)
            log.info(
                f"[VOLUME-SEED] Batch {i // batch_size + 1} | {len(chunk)} keyword(s) "
                f"seeded with search_volume | via RapidAPI (single.php, one call per keyword) | "
                f"real:{len(chunk) - random_count} random_fallback:{random_count}"
            )

        except Exception as exc:
            log.error(f"[VOLUME-SEED] batch error (keywords {i}-{i + len(chunk)}): {exc}")
            for kw in chunk:
                vol = _random_search_volume_fallback()
                log.warning(
                    f"[VOLUME-SEED] RANDOM FALLBACK applied for {kw!r} | search_volume={vol} "
                    f"| reason: unexpected batch-level error — {exc} | this is NOT a real "
                    f"search volume."
                )
                try:
                    db.flintel_keywords.update_one(
                        {"keyword": kw},
                        {"$set": {"search_volume": vol, "search_volume_is_random": True}},
                        upsert=True,
                    )
                except Exception as inner_exc:
                    log.error(f"[VOLUME-SEED] could not persist random fallback for {kw!r}: {inner_exc}")

        time.sleep(SERP_FETCH_SLEEP_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# ENRICHMENT — RapidAPI is the SOLE provider for Google rank + volume.
# 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def fetch_search_volume(search_keyword: str) -> int | None:
    """
    Monthly search volume — a SINGLE keyword, single request. Kept for
    the Twitter fallback path (fetch_google_stats(), used only when
    SEARCH_KEYWORD is configured for Twitter items).
    """
    if not search_keyword:
        return None

    if not RAPIDAPI_KEY:
        vol = _random_search_volume_fallback()
        log.warning(
            f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
            f"search_volume={vol} | reason: RAPIDAPI_KEY not configured — call never made | "
            f"this is NOT a real search volume."
        )
        return vol

    try:
        url = "https://seo-keyword-research.p.rapidapi.com/single.php"

        querystring = {"keyword": search_keyword, "country": "us"}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY, # .env
            "x-rapidapi-host": RAPIDAPI_KEYWORD_HOST,
            "Content-Type": "application/json"
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_VOLUME_TIMEOUT_SECONDS)
        status_code = r.status_code

        try:
            result = r.json()
        except ValueError:
            log.error(f"fetch_search_volume non-JSON response for {search_keyword!r} | status:{status_code}")
            vol = _random_search_volume_fallback()
            log.warning(
                f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
                f"search_volume={vol} | reason: non-JSON response (status:{status_code}) | "
                f"this is NOT a real search volume."
            )
            return vol

        vol = _dig_value(result, VOLUME_FIELD_CANDIDATES)
        if vol is None:
            api_message = result.get("message") if isinstance(result, dict) else None
            log.warning(
                f"fetch_search_volume no volume field for {search_keyword!r} | "
                f"status:{status_code} | api_message:{api_message!r}"
            )
            vol = _random_search_volume_fallback()
            log.warning(
                f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
                f"search_volume={vol} (range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
                f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}) | reason: no credits / bad key / "
                f"rate-limited / no usable field (see api_message above) | this is NOT a "
                f"real, provider-returned search volume."
            )
        return vol
    except Exception as exc:
        log.error(f"fetch_search_volume error for {search_keyword!r}: {exc}")
        vol = _random_search_volume_fallback()
        log.warning(
            f"fetch_search_volume RANDOM FALLBACK applied for {search_keyword!r} | "
            f"search_volume={vol} | reason: exception during call — {exc} | this is NOT a "
            f"real search volume."
        )
        return vol


def fetch_google_rank(search_keyword: str) -> int | None:
    """
    GENERIC (non-post-specific) Google rank fallback — used ONLY for
    Twitter items. 100% UNCHANGED FROM v9.11.1.
    """
    if not RAPIDAPI_KEY or not search_keyword:
        return None
    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": search_keyword}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY, # .env boht used same key
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json"
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_SERP_TIMEOUT_SECONDS)

        try:
            result_data = r.json()
        except ValueError:
            log.error(f"fetch_google_rank non-JSON response for {search_keyword!r} | status:{r.status_code}")
            return None

        items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)
        if not items:
            return None
        return _dig_value(items[0], RANK_FIELD_CANDIDATES)
    except Exception as exc:
        log.error(f"fetch_google_rank error for {search_keyword!r}: {exc}")
        return None


def fetch_google_stats(search_keyword: str) -> dict:
    return {
        "google_rank":   fetch_google_rank(search_keyword),
        "search_volume": fetch_search_volume(search_keyword),
    }


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — SOLE discovery mechanism: RapidAPI SERP search
# (site:reddit.com) -> real per-post rank + URL -> flintel_google_posts
# cache. search_google_for_keyword() itself is 100% UNCHANGED FROM
# v9.11.1 — it still runs unconditionally whenever a keyword is due, on
# its own dedicated RapidAPI host, completely independent of the
# search-volume host/call, and completely independent of Reddit's RSS
# reliability (which now lives entirely in
# run_google_posts_rss_matching_loop() below).
# ─────────────────────────────────────────────────────────────────────────────

def search_google_for_keyword(keyword: str, months_back: int = SERP_MONTHS_BACK) -> list:
    """
    RapidAPI Google search restricted to site:reddit.com, rolling
    last-N-months date window. Returns real per-result rank + URL. Only
    called for keywords that get_due_keywords() has flagged as due.
    100% UNCHANGED FROM v9.11.1.
    """
    if not RAPIDAPI_KEY:
        log.warning("[SERP] RapidAPI key not set — skipping SERP search.")
        return []

    today = datetime.now(timezone.utc)
    date_from = today - timedelta(days=months_back * 30)
    cd_min = date_from.strftime("%m/%d/%Y")
    cd_max = today.strftime("%m/%d/%Y")

    query = f'site:reddit.com "{keyword}"'
    try:
        url = "https://google-search116.p.rapidapi.com/"

        querystring = {"query": query}

        headers = {
            "x-rapidapi-key": RAPIDAPI_KEY, # .env boht used same key
            "x-rapidapi-host": RAPIDAPI_SEARCH_HOST,
            "Content-Type": "application/json"
        }

        r = requests.get(url, headers=headers, params=querystring, timeout=DATAFORSEO_SERP_TIMEOUT_SECONDS)

        try:
            result_data = r.json()
        except ValueError:
            log.error(f"[SERP] Non-JSON response for {keyword!r} | status:{r.status_code}")
            return []

        raw_items = _dig_list(result_data, RESULT_LIST_KEY_CANDIDATES)
        results = []
        rank_misses = 0
        for pos, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            item_url = item.get("url", "") or item.get("link", "")
            if "reddit.com" not in item_url:
                continue
            rank = _dig_value(item, RANK_FIELD_CANDIDATES)
            if rank is None:
                rank = pos
                rank_misses += 1
            results.append({
                "url":   item_url,
                "rank":  rank,
                "title": item.get("title", ""),
            })

        if rank_misses and rank_misses == len(results) and results:
            log.warning(
                f"[SERP] '{keyword}' — no explicit rank field found in any result "
                f"(tried {RANK_FIELD_CANDIDATES}); used result order as rank fallback."
            )

        log.info(
            f"[SERP] '{keyword}' → {len(results)} Reddit result(s) "
            f"(last {months_back} months: {cd_min} to {cd_max})"
        )
        return results

    except Exception as exc:
        log.error(f"[SERP] RapidAPI search error for {keyword!r}: {exc}")
        return []


def is_post_already_signaled(post_url: str) -> bool:
    """
    Checks the `signals` collection DIRECTLY by post_url — BEFORE any
    Reddit fetch or Claude scoring happens. 100% UNCHANGED FROM v9.11.1.
    """
    if not post_url:
        return False
    try:
        existing = db.signals.find_one({"post_url": post_url}, {"_id": 1})
        return existing is not None
    except Exception as exc:
        log.error(f"[DEDUP] is_post_already_signaled error for {post_url}: {exc}")
        return False   # fail-open: if the check itself fails, don't block discovery


def _extract_reddit_subreddit_from_url(post_url: str) -> str:
    """Pulls the subreddit name out of a standard reddit.com post URL
    (e.g. reddit.com/r/<subreddit>/comments/...). Returns "" if it
    can't be found — never raises."""
    match = re.search(r"reddit\.com/r/([^/]+)/", post_url)
    return match.group(1) if match else ""


def _extract_reddit_submission_id(post_url: str) -> str | None:
    """Pulls the submission id out of a standard reddit.com post URL
    (e.g. .../comments/<id>/...). Used to build a stable message_id."""
    match = re.search(r"/comments/([a-zA-Z0-9]+)", post_url)
    return match.group(1) if match else None


def _normalize_reddit_url(url: str) -> str:
    """Normalizes a Reddit post URL for exact-match comparison between a
    SERP-discovered post_url and an RSS entry's link: strips query
    string/fragment, trailing slash, and the www./old. host prefixes."""
    if not url:
        return ""
    url = url.split("?")[0].split("#")[0].rstrip("/")
    url = url.replace("https://old.reddit.com", "https://www.reddit.com")
    url = url.replace("https://reddit.com", "https://www.reddit.com")
    return url.lower()


def process_one_keyword(keyword: str) -> tuple:
    """
    SERP-DISCOVERY-ONLY. Full discovery work for ONE keyword that
    get_due_keywords() has flagged as due right now:
      1. RapidAPI SERP search (site:reddit.com, last N months) — 100%
         unchanged call (search_google_for_keyword()).
      2. Per-result post_url dedup check -> skip posts already scored
         (in `signals`) or already cached (in flintel_google_posts).
      3. For every genuinely new result: extract the subreddit, and save
         {post_url, google_rank, matched_keyword, fuzzy_keywords,
         subreddit, fetched:False} into flintel_google_posts via
         save_google_post(). This save is immediate — it never calls
         into Reddit's RSS/JSON endpoints and never waits on them.

    Returns (new_items_count, skipped_dupes_count, had_fetch_failure) for
    logging AND for run_serp_discovery_loop()'s fetched=True decision.
    had_fetch_failure is now ALWAYS False here — Reddit RSS fetching is
    fully decoupled from SERP discovery, so a keyword's SERP results
    being saved to flintel_google_posts can never fail due to Reddit's
    RSS reliability.
    """
    new_items, skipped_dupes = 0, 0
    had_fetch_failure = False  # Reddit RSS fetch failures can no longer occur at this stage

    results = search_google_for_keyword(keyword, months_back=SERP_MONTHS_BACK)

    for result in results:
        post_url = result["url"]

        if is_post_already_signaled(post_url):
            skipped_dupes += 1
            log.debug(f"[SERP] Skipping already-signaled post_url: {post_url}")
            continue

        subreddit = _extract_reddit_subreddit_from_url(post_url)
        saved = save_google_post(
            post_url=post_url,
            google_rank=result["rank"],
            matched_keyword=keyword,
            subreddit=subreddit,
        )
        if saved:
            new_items += 1
        else:
            skipped_dupes += 1
        time.sleep(0.05)  # tiny pacing between DB writes only — no external call here

    return new_items, skipped_dupes, had_fetch_failure


def run_serp_discovery_loop():
    """
    Continuously polls flintel_keywords every KEYWORD_CHECK_INTERVAL_SECONDS
    for keywords that have NEVER been fetched (fetched=False), and for any
    keyword still missing a cached search_volume (batch-seeds it). 100%
    UNCHANGED FROM v9.11.1 in its keyword-cache behavior — the only
    difference is what process_one_keyword() does per due keyword (see
    that function's docstring): it saves discovered post_urls into
    flintel_google_posts instead of fetching each one's Reddit RSS
    directly, so a keyword's fetched=True marking here depends only on
    SERP discovery finishing, never on Reddit's RSS reliability.
    """
    sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

    missing_volume = get_keywords_missing_volume()
    if missing_volume:
        log.info(
            f"[VOLUME-SEED] {len(missing_volume)} keyword(s) need search_volume — "
            f"seeding in batches of {SEARCH_VOLUME_BATCH_SIZE}..."
        )
        seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

    log.info(
        f"[SERP] Discovery loop started | {len(REDDIT_SEARCH_KEYWORDS)} keyword(s) in python list | "
        f"check_interval:{KEYWORD_CHECK_INTERVAL_SECONDS}s | "
        f"months_back:{SERP_MONTHS_BACK} | depth:{SERP_RESULTS_PER_KEYWORD} | "
        f"KEYWORD CACHE: fetch-once-forever, restart-safe, no re-fetch ever, "
        f"due/missing-volume read from flintel_keywords directly (not filtered by python list) | "
        f"SEARCH-VOLUME: batched loop (size {SEARCH_VOLUME_BATCH_SIZE}) | "
        f"random fallback range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} "
        f"on failure/no-credits (always logged) | "
        f"SERP results saved into flintel_google_posts immediately — "
        f"Reddit RSS fetching is fully decoupled (see run_google_posts_rss_matching_loop)"
    )

    while True:
        try:
            sync_keywords_to_db(REDDIT_SEARCH_KEYWORDS)

            missing_volume = get_keywords_missing_volume()
            if missing_volume:
                seed_search_volume_batch(missing_volume, batch_size=SEARCH_VOLUME_BATCH_SIZE)

            due = get_due_keywords()
            if not due:
                time.sleep(KEYWORD_CHECK_INTERVAL_SECONDS)
                continue

            total_new, total_dupes = 0, 0
            for doc in due:
                keyword = doc["keyword"]
                new_items, dupes, had_fetch_failure = process_one_keyword(keyword)
                total_new += new_items
                total_dupes += dupes

                # had_fetch_failure is always False now — a keyword is
                # always marked fetched=True once its SERP results are
                # saved to flintel_google_posts, since that save no
                # longer depends on Reddit's RSS reliability at all.
                mark_keyword_fetched(keyword)
                log.info(
                    f"[SERP] '{keyword}' DONE | new_google_posts:{new_items} skipped_dupes:{dupes} | "
                    f"marked fetched=True PERMANENTLY — will never be re-fetched | "
                    f"Reddit RSS confirmation for these post_urls will happen independently "
                    f"via run_google_posts_rss_matching_loop()"
                )
                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[SERP] Pass complete | keywords_processed:{len(due)} | "
                f"new_google_posts:{total_new} | skipped_dupes:{total_dupes}"
            )

        except Exception as exc:
            log.error(f"[SERP] discovery loop error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT SUBREDDIT-RSS FETCH — public, credential-free /r/<subreddit>/new.rss
# feed. Smart-retry logic (v9.6) unchanged in spirit — same User-Agent,
# jittered backoff, old.reddit.com fallback host — applied to a
# subreddit's feed URL, since discovery no longer fetches one post_url at
# a time.
# ─────────────────────────────────────────────────────────────────────────────

def _reddit_get_with_retry(url: str) -> requests.Response | None:
    """
    "Smart" GET wrapper for Reddit's public endpoints — retry/backoff/
    jitter behavior kept exactly as prior versions:
      - Reddit-recommended User-Agent format (REDDIT_USER_AGENT).
      - Small randomized jitter delay before each attempt.
      - Exponential backoff retry, up to REDDIT_FETCH_MAX_RETRIES times,
        specifically for 403 / 429 / 5xx responses.
    Returns the Response on success (status 200), or None if every
    attempt failed.
    """
    headers = {
        "User-Agent": REDDIT_USER_AGENT,
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }

    last_status = None
    for attempt in range(1, REDDIT_FETCH_MAX_RETRIES + 1):
        time.sleep(random.uniform(REDDIT_FETCH_JITTER_MIN, REDDIT_FETCH_JITTER_MAX))
        try:
            r = requests.get(url, headers=headers, timeout=REDDIT_JSON_TIMEOUT_SECONDS)
            last_status = r.status_code
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                log.debug(f"[REDDIT-RSS] 404 (gone) for {url} — not retrying.")
                return None
            if r.status_code in (403, 429) or r.status_code >= 500:
                wait = (REDDIT_FETCH_BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
                log.warning(
                    f"[REDDIT-RSS] fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                    f"got {r.status_code} for {url} — backing off {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            log.error(f"[REDDIT-RSS] Unexpected status {r.status_code} for {url}")
            return None
        except requests.RequestException as exc:
            log.warning(
                f"[REDDIT-RSS] fetch attempt {attempt}/{REDDIT_FETCH_MAX_RETRIES} "
                f"network error for {url}: {exc}"
            )
            time.sleep((REDDIT_FETCH_BACKOFF_BASE ** attempt))

    log.error(f"[REDDIT-RSS] fetch exhausted {REDDIT_FETCH_MAX_RETRIES} attempts for {url} "
              f"(last_status:{last_status})")
    return None


def _fetch_subreddit_rss(subreddit: str) -> list:
    """
    Fetches r/<subreddit>/new.rss (public, credential-free), with
    old.reddit.com fallback host on failure — same smart-retry as the
    prior per-post fetch, just pointed at a subreddit feed instead.
    Returns a list of parsed feedparser entries (possibly empty).
    """
    primary_url = f"https://www.reddit.com/r/{subreddit}/new.rss"
    r = _reddit_get_with_retry(primary_url)

    if r is None:
        fallback_url = f"https://old.reddit.com/r/{subreddit}/new.rss"
        log.info(f"[REDDIT-RSS] Retrying r/{subreddit} via old.reddit.com fallback...")
        r = _reddit_get_with_retry(fallback_url)

    if r is None:
        log.error(f"[REDDIT-RSS] Giving up on r/{subreddit} this pass — will retry next cycle.")
        return []

    try:
        feed = feedparser.parse(r.content)
        return feed.entries[:GOOGLE_POSTS_RSS_ENTRY_LIMIT]
    except Exception as exc:
        log.error(f"[REDDIT-RSS] parse error for r/{subreddit}: {exc}")
        return []


def _entry_to_text_and_meta(entry) -> dict:
    """
    Extracts text/username/posted_at from one feedparser RSS entry —
    same extraction logic used by the prior per-post RSS fetch.
    """
    title = (entry.get("title", "") or "").strip()
    raw_summary = entry.get("summary", "") or ""
    if not raw_summary and entry.get("content"):
        raw_summary = entry["content"][0].get("value", "") or ""
    summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(raw_summary)).strip()

    text = title
    if summary_plain and summary_plain.lower() != title.lower():
        text = f"{title}\n\n{summary_plain}"

    author = (entry.get("author", "") or "unknown").lstrip("u/").lstrip("/u/").strip() or "unknown"

    posted_at = None
    published = entry.get("published") or entry.get("updated")
    if published:
        try:
            posted_at = datetime(*entry.get("published_parsed", entry.get("updated_parsed"))[:6],
                                  tzinfo=timezone.utc).isoformat()
        except (TypeError, ValueError):
            posted_at = published

    link = entry.get("link", "") or ""

    return {"text": text, "author": author, "posted_at": posted_at, "link": link}


def run_google_posts_rss_matching_loop():
    """
    The ONLY place in this system that talks to Reddit's RSS feeds now.
    Fully independent of, and never blocks or is blocked by,
    run_serp_discovery_loop() / process_one_keyword() / flintel_keywords.

    Every GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS:
      1. Calls sync_targeting_collections() — rebuilds
         flintel_targeting_subreddits / flintel_targeting_keywords from
         whatever is currently pending (fetched=False) in
         flintel_google_posts. Neither collection is ever a hardcoded
         python list — both are a live mirror, re-synced every pass.
      2. Reads the subreddit poll list DIRECTLY off
         flintel_targeting_subreddits (get_targeting_subreddits()) —
         this governs which subreddits actually get RSS-polled this
         cycle.
      3. For each such subreddit, fetches that subreddit's public,
         credential-free /new.rss feed (smart-retry + old.reddit.com
         fallback).
      4. Builds a lookup of this subreddit's still-pending
         flintel_google_posts documents keyed by NORMALIZED post_url
         (full per-post detail — google_rank, matched_keyword,
         fuzzy_keywords — still comes straight from flintel_google_posts,
         exactly as before; the targeting collections are the governing/
         tracking layer, not a duplicate data store).
      5. For every RSS entry returned, normalizes its link and checks it
         against that lookup. A match on post_url is the AUTHORITATIVE
         signal — the ONLY thing that decides a match, ever. fuzzy_
         keywords are cross-checked against the entry's text PURELY for
         a traceability log line — never blocking, never part of the
         match decision.
      6. On a match:
           - marks flintel_google_posts.fetched = True, permanently
             (mark_google_post_fetched())
           - immediately deletes that post_url's flintel_targeting_
             keywords document (delete_targeting_keyword_entry()) — that
             keyword/post is done being targeted
           - pulls that keyword's already-seeded search_volume straight
             off flintel_keywords (read-only — NEVER re-queries the
             search-volume API here)
           - generates the random engagement fallback (RSS has no real
             upvotes/comments, same as before)
           - builds the exact same item schema as before, pushes it into
             reddit_queue + save_queue_message() — the raw fetched text
             is queued AS-IS; run_batch_processor() below no longer
             re-filters Reddit items by keyword-phrase text, since the
             URL match here is already the sole authoritative relevance
             decision for Reddit
      7. Any RSS entry that does NOT match a pending post_url for that
         subreddit is simply ignored — no separate keyword filtering
         against a python list happens at this stage.
    """
    log.info(
        f"[GOOGLE-POSTS-RSS] Matching loop started | check_interval:"
        f"{GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS}s | rss_entry_limit:"
        f"{GOOGLE_POSTS_RSS_ENTRY_LIMIT} | reads flintel_targeting_subreddits "
        f"(auto-synced from flintel_google_posts every pass), no hardcoded "
        f"python list of subreddits ever maintained"
    )

    while True:
        try:
            sync_targeting_collections()

            subreddits = get_targeting_subreddits()
            if not subreddits:
                log.debug("[GOOGLE-POSTS-RSS] No pending subreddits this pass — sleeping.")
                time.sleep(GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS)
                continue

            log.info(
                f"[GOOGLE-POSTS-RSS] Pass starting | {len(subreddits)} subreddit(s) "
                f"in flintel_targeting_subreddits: {subreddits}"
            )

            total_confirmed, total_subreddits_processed = 0, 0

            for subreddit in subreddits:
                pending_docs = get_pending_google_posts_for_subreddit(subreddit)
                if not pending_docs:
                    continue

                pending_by_url = {_normalize_reddit_url(d["post_url"]): d for d in pending_docs}

                log.info(
                    f"[GOOGLE-POSTS-RSS] r/{subreddit} | polling RSS | "
                    f"{len(pending_by_url)} pending post_url(s) to confirm"
                )

                entries = _fetch_subreddit_rss(subreddit)
                total_subreddits_processed += 1

                if not entries:
                    log.warning(f"[GOOGLE-POSTS-RSS] r/{subreddit} | RSS returned no entries this pass.")
                    time.sleep(SERP_FETCH_SLEEP_SECONDS)
                    continue

                confirmed_this_subreddit = 0

                for entry in entries:
                    meta = _entry_to_text_and_meta(entry)
                    normalized_link = _normalize_reddit_url(meta["link"])
                    if not normalized_link or normalized_link not in pending_by_url:
                        continue

                    doc = pending_by_url[normalized_link]
                    post_url = doc["post_url"]
                    matched_keyword = doc.get("matched_keyword", SEARCH_KEYWORD)
                    fuzzy_keywords = doc.get("fuzzy_keywords", [])
                    google_rank = doc.get("google_rank")

                    fuzzy_hit = any(fk.lower() in meta["text"].lower() for fk in fuzzy_keywords) if fuzzy_keywords else False

                    search_volume, sv_is_random = get_cached_search_volume_for_keyword(matched_keyword)

                    upvotes = _random_engagement_fallback()
                    comments = _random_engagement_fallback()

                    submission_id = _extract_reddit_submission_id(post_url)
                    message_id = f"reddit_serp_{submission_id}" if submission_id else (
                        f"reddit_serp_{re.sub(r'[^a-zA-Z0-9]', '_', post_url)[-40:]}"
                    )

                    item = {
                        "message_id":              message_id,
                        "platform":                "reddit",
                        "text":                    meta["text"],
                        "username":                meta["author"],
                        "subreddit_or_channel":    subreddit,
                        "post_url":                post_url,
                        "posted_at":               meta["posted_at"],
                        "search_keyword":          matched_keyword,
                        "upvotes":                 upvotes,
                        "comments":                comments,
                        "engagement_is_random":    True,
                        "google_rank":             google_rank,
                        "search_volume":           search_volume,
                        "search_volume_is_random": sv_is_random,
                    }

                    # URL match confirmed — this is the SOLE authoritative
                    # signal. Push the raw fetched text AS-IS into the
                    # queue (which run_batch_processor() below appends
                    # directly into flintel_pending_batch, no additional
                    # keyword-text filtering applied to Reddit items).
                    reddit_queue.put(item)
                    save_queue_message("reddit", item)
                    mark_google_post_fetched(post_url)
                    delete_targeting_keyword_entry(post_url)

                    confirmed_this_subreddit += 1
                    total_confirmed += 1

                    sv_tag = "RANDOM-FALLBACK" if sv_is_random else "real"
                    log.info(
                        f"[GOOGLE-POSTS-RSS] CONFIRMED via URL match | r/{subreddit} | "
                        f"post_url:{post_url} | google_rank:{google_rank} | "
                        f"matched_keyword:{matched_keyword!r} | fuzzy_keyword_text_hit:{fuzzy_hit} | "
                        f"search_volume:{search_volume} ({sv_tag}, from flintel_keywords cache) | "
                        f"upvotes:{upvotes} comments:{comments} (RANDOM-FALLBACK, RSS has no real counts) | "
                        f"queued as-is for Claude scoring | marked fetched=True PERMANENTLY in "
                        f"flintel_google_posts | removed from flintel_targeting_keywords"
                    )

                if confirmed_this_subreddit == 0:
                    log.info(
                        f"[GOOGLE-POSTS-RSS] r/{subreddit} | {len(entries)} RSS entr(y/ies) checked | "
                        f"0 matched a pending post_url this pass — will retry next cycle"
                    )
                else:
                    log.info(
                        f"[GOOGLE-POSTS-RSS] r/{subreddit} | {confirmed_this_subreddit} post_url(s) "
                        f"confirmed and queued this pass"
                    )

                time.sleep(SERP_FETCH_SLEEP_SECONDS)

            log.info(
                f"[GOOGLE-POSTS-RSS] Pass complete | subreddits_processed:{total_subreddits_processed} | "
                f"total_confirmed_and_queued:{total_confirmed}"
            )

        except Exception as exc:
            log.error(f"[GOOGLE-POSTS-RSS] matching loop error: {exc}")
            time.sleep(10)

        time.sleep(GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS)


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER — streaming transport + partial-JSON recovery.
# 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        payload = {
            "search_keyword": item.get("search_keyword", SEARCH_KEYWORD),
            "text":           (item.get("text", "") or "")[:1200],
            "platform":       item.get("platform", "unknown"),
            "google_rank":    item.get("google_rank"),
            "search_volume":  item.get("search_volume"),
            "upvotes":        item.get("upvotes"),
            "comments":       item.get("comments"),
        }
        lines.append(f"--- POST {i} ---\n{json.dumps(payload, ensure_ascii=False)}\n")
    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Scoring unavailable.") -> dict:
    return {
        "index": index,
        "intent_score": 1,
        "is_relevant": False,
        "reply_draft": None,
        "_is_fallback": True,
        "_fallback_reason": reason,
    }


def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    """Brace-depth-tracking salvage of a truncated JSON array."""
    start = raw.find("[")
    if start == -1:
        return []
    objects, depth, obj_start, in_string, escape = [], 0, None, False, False
    i, n = start + 1, len(raw)
    while i < n:
        ch = raw[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            i += 1
            continue
        if ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start is not None:
                candidate = raw[obj_start:i + 1]
                try:
                    objects.append(json.loads(candidate))
                except (json.JSONDecodeError, ValueError):
                    log.warning("[Claude-Batch] Skipped one malformed salvaged object.")
                obj_start = None
        i += 1
    return objects


def _parse_claude_json(raw: str) -> tuple:
    cleaned = _strip_code_fences(raw)
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError("Claude returned non-list.")
        return parsed, False
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(f"[Claude-Batch] Full parse failed ({exc}) — attempting partial recovery.")
        return _salvage_partial_json_array(cleaned), True


def _call_claude_batch(batch: list) -> list:
    prompt = _build_batch_prompt(batch)
    with anthropic_client.messages.stream(
        model="claude-haiku-4-5-20251001",
        max_tokens=MAX_TOKENS,
        system=CLAUDE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Score this batch:\n\n{prompt}"}],
    ) as stream:
        raw = stream.get_final_text().strip()

    results, was_truncated = _parse_claude_json(raw)

    if was_truncated:
        recovered = {int(r["index"]) for r in results if isinstance(r, dict) and "index" in r}
        missing = sorted(set(range(1, len(batch) + 1)) - recovered)
        log.warning(f"[Claude-Batch] PARTIAL RECOVERY | batch_size:{len(batch)} | "
                    f"recovered:{len(recovered)} | missing:{len(missing)}")
        log_operator_alert(
            title="Claude Response Truncated (max_tokens) — Partial Recovery",
            detail=f"batch_size:{len(batch)} recovered:{len(recovered)} missing:{missing[:30]}",
            level="ERROR",
        )
        for idx in missing:
            results.append(_fallback_score(idx, "Truncated — not recovered."))

    if not isinstance(results, list):
        raise ValueError("Claude returned non-list after parsing.")

    for r in results:
        r.setdefault("is_relevant", False)
        r.setdefault("reply_draft", None)
        r.setdefault("_is_fallback", False)
        if r.get("intent_score", 1) < 1:
            r["intent_score"] = 1
        if r.get("intent_score", 1) > 100:
            r["intent_score"] = 100

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(_call_claude_batch, batch, retries=3, delay=5, label="Claude-Batch")
    if result is None:
        log_operator_alert(
            title="Claude API Unavailable",
            detail=f"All 3 retry attempts failed for a batch of {len(batch)} items.",
            level="CRITICAL",
        )
        return [_fallback_score(i + 1, "Claude API unavailable after 3 retries.") for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE — 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def save_new_signal(item: dict, score_result: dict, force_pending: bool = False) -> bool:
    doc = {
        "message_id":            item["message_id"],
        "platform":               item.get("platform", "unknown"),
        "post_url":               item.get("post_url", ""),
        "text":                   item.get("text", ""),
        "username":               item.get("username", "unknown"),
        "subreddit_or_channel":   item.get("subreddit_or_channel", ""),
        "posted_at":              item.get("posted_at"),
        "fetched_at":             datetime.now(timezone.utc),
        "google_rank":            item.get("google_rank"),
        "search_volume":          item.get("search_volume"),
        "upvotes":                item.get("upvotes"),
        "comments":               item.get("comments"),
        "search_keyword":         item.get("search_keyword", SEARCH_KEYWORD),
        "intent_score":           score_result.get("intent_score", 1),
        "is_relevant":            score_result.get("is_relevant", False),
        "reply_draft":            score_result.get("reply_draft"),
        "client_id":              CLIENT_ID,
        "status":                 "pending" if force_pending else "confirmed",
        "created_at":             datetime.now(timezone.utc),
    }
    try:
        db.signals.insert_one(doc)
        sv_tag = "RANDOM-FALLBACK" if item.get("search_volume_is_random") else "real"
        eng_tag = "RANDOM-FALLBACK" if item.get("engagement_is_random") else "real"
        log.info(
            f"SAVED [{doc['platform'].upper()}] {doc['search_keyword']!r} | "
            f"search_volume:{doc['search_volume']}/mo ({sv_tag}) | "
            f"upvotes:{doc['upvotes']} comments:{doc['comments']} ({eng_tag}) | "
            f"google_rank:{doc['google_rank']} | "
            f"post_url:{doc['post_url']}"
        )
        return True
    except DuplicateKeyError:
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


def replace_confirmed_signal(message_id: str, enrichment: dict, score_result: dict) -> bool:
    existing = db.signals.find_one({"message_id": message_id})
    if not existing:
        log.warning(f"[RESCORE] No existing doc for {message_id} — skipping.")
        return False

    new_doc = {
        "message_id":            message_id,
        "platform":               existing.get("platform", "unknown"),
        "post_url":               existing.get("post_url", ""),
        "text":                   existing.get("text", ""),
        "username":               existing.get("username", "unknown"),
        "subreddit_or_channel":   existing.get("subreddit_or_channel", ""),
        "posted_at":              existing.get("posted_at") or existing.get("created_at"),
        "fetched_at":             existing.get("fetched_at", datetime.now(timezone.utc)),
        "google_rank":            enrichment.get("google_rank"),
        "search_volume":          enrichment.get("search_volume"),
        "upvotes":                enrichment.get("upvotes"),
        "comments":               enrichment.get("comments"),
        "search_keyword":         enrichment.get("search_keyword", SEARCH_KEYWORD),
        "intent_score":           score_result.get("intent_score", 1),
        "is_relevant":            score_result.get("is_relevant", False),
        "reply_draft":            score_result.get("reply_draft"),
        "client_id":              CLIENT_ID,
        "status":                 "confirmed",
        "created_at":             existing.get("created_at", datetime.now(timezone.utc)),
    }
    db.signals.replace_one({"message_id": message_id}, new_doc)
    log.info(f"[RESCORE] CONFIRMED | {message_id} | score:{new_doc['intent_score']} relevant:{new_doc['is_relevant']}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC BATCH PROCESSOR — one instance per platform queue.
#
# v9.13 CHANGE (the ONLY functional change in this function): the
# redundant keyword-phrase filter (passes_keyword_filter) is now SKIPPED
# for Reddit items specifically. A Reddit item only ever reaches
# reddit_queue after its post_url has already been confirmed via exact
# URL match inside run_google_posts_rss_matching_loop() — that URL match
# IS the sole, authoritative relevance decision for Reddit. Re-checking
# the fetched text against the full REDDIT_SEARCH_KEYWORDS phrase list
# here would silently drop items whose text only relates in meaning (not
# exact original phrase) to the keyword that produced them via SERP.
# Twitter items are NOT pre-filtered anywhere upstream, so they still go
# through passes_keyword_filter() exactly as before — zero change to
# Twitter's behavior. Everything else in this function — batching logic,
# timeout/gap handling, persistent state, enrichment, the Claude call —
# is 100% UNCHANGED from v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
    gap_seconds: int,
    timeout_seconds: int,
    keyword_filter_list: list,
):
    platform_key = platform_label.lower()

    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{gap_seconds}s | timeout:{timeout_seconds}s"
    )

    current_batch, batch_start_time = load_pending_batch(platform_key)
    if current_batch:
        log.info(f"[{platform_label}] Resumed [{len(current_batch)}/{batch_size}] from persistent disk.")

    total_received, total_matched, total_dropped, total_batches = 0, 0, 0, 0

    while True:
        try:
            if current_batch and batch_start_time is not None:
                wait_time = max(0.1, timeout_seconds - (time.time() - batch_start_time))
            else:
                wait_time = 1.0

            try:
                item = q.get(timeout=wait_time)
                got_item = True
            except queue.Empty:
                got_item = False

            if got_item:
                total_received += 1
                remove_queue_message(platform_key, item.get("message_id"))

                text = (item.get("text") or "").strip()

                if not text or len(text) < 10:
                    q.task_done()
                    continue

                # v9.13 — Reddit items are NEVER re-filtered here (URL
                # match already happened upstream, in
                # run_google_posts_rss_matching_loop()). Twitter items
                # still go through the normal keyword-phrase filter,
                # exactly as before.
                if platform_key != "reddit" and not passes_keyword_filter(text, keyword_filter_list):
                    total_dropped += 1
                    q.task_done()
                    continue

                total_matched += 1
                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)
                save_pending_batch(platform_key, current_batch, batch_start_time)
                save_batch_seconds(platform_key, batch_start_time)

                log.info(f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | u/{item.get('username')}")
                q.task_done()

            should_fire = False
            fire_reason = ""
            if len(current_batch) >= batch_size:
                should_fire, fire_reason = True, f"batch full ({batch_size} items)"
            elif current_batch and batch_start_time is not None:
                elapsed = time.time() - batch_start_time
                if elapsed >= timeout_seconds:
                    should_fire, fire_reason = True, f"timeout ({timeout_seconds}s) — partial {len(current_batch)}/{batch_size}"

            if should_fire and current_batch:
                total_batches += 1
                batch_to_send = current_batch[:batch_size]
                current_batch = current_batch[batch_size:]
                batch_start_time = None if not current_batch else time.time()

                if current_batch:
                    save_pending_batch(platform_key, current_batch, batch_start_time)
                    save_batch_seconds(platform_key, batch_start_time)
                else:
                    clear_pending_batch(platform_key)
                    clear_batch_seconds(platform_key)

                google_stats = None
                for it in batch_to_send:
                    already_enriched = it.get("google_rank") is not None

                    it.setdefault("upvotes", None)
                    it.setdefault("comments", None)

                    if not already_enriched and SEARCH_KEYWORD:
                        if google_stats is None:
                            google_stats = fetch_google_stats(SEARCH_KEYWORD)
                        it["google_rank"] = google_stats.get("google_rank")
                        it["search_volume"] = google_stats.get("search_volume")
                        it["search_keyword"] = SEARCH_KEYWORD

                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | reason:{fire_reason} | "
                    f"items:{len(batch_to_send)} | received:{total_received} "
                    f"matched:{total_matched} dropped:{total_dropped}"
                )

                scores = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch."))
                    is_fallback = bool(sr.get("_is_fallback", False))
                    save_new_signal(it, sr, force_pending=is_fallback)

                log.info(f"[{platform_label}] BATCH {total_batches} COMPLETE — "
                         f"{len(batch_to_send)} item(s) | waiting {gap_seconds}s...")
                time.sleep(gap_seconds)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR — 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def run_rescore_processor():
    log.info(f"[RESCORE] Processor started | batch_size:{RESCORE_BATCH_SIZE} | "
             f"poll:{RESCORE_POLL_INTERVAL}s | gap:{RESCORE_BATCH_GAP_SECONDS}s")
    total_batches = 0

    while True:
        try:
            pending = list(db.signals.find({"status": "pending"}).limit(RESCORE_BATCH_SIZE))
            if not pending:
                time.sleep(RESCORE_POLL_INTERVAL)
                continue

            items_for_claude = []
            for doc in pending:
                items_for_claude.append({
                    "message_id":     doc["message_id"],
                    "platform":       doc.get("platform", "unknown"),
                    "text":           doc.get("text", ""),
                    "search_keyword": doc.get("search_keyword", SEARCH_KEYWORD),
                    "google_rank":    doc.get("google_rank"),
                    "search_volume":  doc.get("search_volume"),
                    "upvotes":        doc.get("upvotes"),
                    "comments":       doc.get("comments"),
                })

            total_batches += 1
            log.info(f"[RESCORE] BATCH {total_batches} | items:{len(items_for_claude)}")

            scores = score_batch_with_claude(items_for_claude)
            score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

            for i, item in enumerate(items_for_claude):
                pos = i + 1
                sr = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos))
                enrichment = {
                    "google_rank":    item.get("google_rank"),
                    "search_volume":  item.get("search_volume"),
                    "upvotes":        item.get("upvotes"),
                    "comments":       item.get("comments"),
                    "search_keyword": item.get("search_keyword"),
                }
                replace_confirmed_signal(item["message_id"], enrichment, sr)

            log.info(f"[RESCORE] BATCH {total_batches} DONE — waiting {RESCORE_BATCH_GAP_SECONDS}s...")
            time.sleep(RESCORE_BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[RESCORE] processor error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER — 100% UNCHANGED FROM v9.11.1.
# ─────────────────────────────────────────────────────────────────────────────

def build_twitter_client() -> tweepy.Client | None:
    if not TWITTER_BEARER_TOKEN:
        log.warning("TWITTER_BEARER_TOKEN not set — Twitter platform disabled.")
        return None
    try:
        client = tweepy.Client(
            bearer_token=TWITTER_BEARER_TOKEN,
            consumer_key=TWITTER_API_KEY,
            consumer_secret=TWITTER_API_SECRET,
            wait_on_rate_limit=True,
        )
        log.info("Twitter/X client initialised.")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def poll_twitter(client: tweepy.Client):
    seen_ids: set = load_seen_ids("twitter")
    dirty = 0
    log.info(f"Twitter poll started | query_len:{len(TWITTER_SEARCH_QUERY)} | "
             f"dedup resumed with {len(seen_ids)} ID(s)")

    while True:
        try:
            response = client.search_recent_tweets(
                query=TWITTER_SEARCH_QUERY,
                max_results=50,
                tweet_fields=["author_id", "created_at", "text", "public_metrics"],
                expansions=["author_id"],
                user_fields=["username", "name"],
            )

            if not response or not response.data:
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            user_map = {u.id: u.username for u in (response.includes or {}).get("users", [])}

            new_count = 0
            for tweet in response.data:
                tweet_id = str(tweet.id)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                dirty += 1
                if len(seen_ids) > 50_000:
                    seen_ids.clear()

                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")
                metrics = tweet.public_metrics or {}

                _tw_item = {
                    "message_id":           f"twitter_{tweet_id}",
                    "platform":             "twitter",
                    "text":                 tweet.text or "",
                    "username":             username,
                    "subreddit_or_channel": "",
                    "post_url":             f"https://twitter.com/{username}/status/{tweet_id}",
                    "posted_at":            str(tweet.created_at) if tweet.created_at else None,
                    "search_keyword":       SEARCH_KEYWORD,
                    "upvotes":              metrics.get("like_count"),
                    "comments":             metrics.get("reply_count"),
                    "google_rank":          None,
                    "search_volume":        None,
                }
                twitter_queue.put(_tw_item)
                save_queue_message("twitter", _tw_item)
                new_count += 1

            if dirty >= 10:
                save_seen_ids("twitter", seen_ids)
                dirty = 0

            if new_count:
                log.info(f"Twitter: {new_count} new tweets queued | queue_size:{twitter_queue.qsize()}")

        except tweepy.errors.TweepyException as exc:
            log.error(f"Twitter poll error: {exc}")
        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc}")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    """
    Reddit runs on THREE independent threads:
      1. SERP discovery thread (run_serp_discovery_loop) — unchanged
         keyword-cache behavior, saves into flintel_google_posts instead
         of fetching Reddit RSS directly.
      2. flintel_google_posts RSS-matching thread
         (run_google_posts_rss_matching_loop) — the only thread that
         talks to Reddit's RSS feeds; also owns syncing
         flintel_targeting_subreddits / flintel_targeting_keywords every
         pass; fully independent of #1.
      3. Its dedicated batch processor thread (unchanged, except Reddit
         items skip the redundant keyword filter — see
         run_batch_processor()).
    Governed entirely by REDDIT_ENABLED + RapidAPI credentials (RapidAPI
    is required for SERP discovery; the subreddit RSS fetch step needs no
    credentials at all — no OAuth/PRAW).
    """
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED — skipping.")
        return
    if not RAPIDAPI_KEY:
        log.warning("Reddit not started — RAPIDAPI_KEY not set (required for SERP discovery).")
        return

    resumed = load_queue_messages("reddit")
    for it in resumed:
        reddit_queue.put(it)
    if resumed:
        log.info(f"[REDDIT] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
    google_posts_thread = threading.Thread(
        target=run_google_posts_rss_matching_loop, daemon=True, name="Reddit-GooglePosts-RSS"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
              REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
        daemon=True, name="Reddit-Batch",
    )
    serp_thread.start()
    google_posts_thread.start()
    btch_thread.start()
    log.info(
        f"Reddit threads running: SERP-Discovery ✅ | GooglePosts-RSS-Matching ✅ | Batch ✅ | "
        f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not serp_thread.is_alive():
            log.error("Reddit SERP thread died — restarting...")
            serp_thread = threading.Thread(target=run_serp_discovery_loop, daemon=True, name="Reddit-SERP")
            serp_thread.start()
        if not google_posts_thread.is_alive():
            log.error("Reddit GooglePosts-RSS-Matching thread died — restarting...")
            google_posts_thread = threading.Thread(
                target=run_google_posts_rss_matching_loop, daemon=True, name="Reddit-GooglePosts-RSS"
            )
            google_posts_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS,
                      REDDIT_BATCH_TIMEOUT_SECONDS, REDDIT_SEARCH_KEYWORDS),
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener():
    if not TWITTER_ENABLED:
        log.warning("Twitter platform DISABLED — skipping.")
        return
    client = build_twitter_client()
    if client is None:
        return

    resumed = load_queue_messages("twitter")
    for it in resumed:
        twitter_queue.put(it)
    if resumed:
        log.info(f"[TWITTER] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS,
              TWITTER_BATCH_TIMEOUT_SECONDS, TWITTER_SEARCH_KEYWORDS),
        daemon=True, name="Twitter-Batch",
    )
    poll_thread.start()
    btch_thread.start()
    log.info(f"Twitter threads running: Poll ✅ | Batch ✅ | "
             f"gap:{TWITTER_BATCH_GAP_SECONDS}s | timeout:{TWITTER_BATCH_TIMEOUT_SECONDS}s")

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("Twitter poll thread died — restarting...")
            poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("Twitter batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS,
                      TWITTER_BATCH_TIMEOUT_SECONDS, TWITTER_SEARCH_KEYWORDS),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_rescore_listener():
    rescore_thread = threading.Thread(target=run_rescore_processor, daemon=True, name="Rescore-Processor")
    rescore_thread.start()
    log.info("Rescore processor thread running ✅")

    while True:
        await asyncio.sleep(60)
        if not rescore_thread.is_alive():
            log.error("Rescore processor thread died — restarting...")
            rescore_thread = threading.Thread(target=run_rescore_processor, daemon=True, name="Rescore-Processor")
            rescore_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI — read-only endpoints
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flintel v9.13 — Reddit (SERP + fetch-once-forever keyword cache + flintel_google_posts cache + AUTO-SYNCED flintel_targeting_subreddits/keywords + URL-match subreddit RSS confirmation + random-fallback volume/engagement) + Twitter Signal Scorer",
    description=(
        "Reddit (RapidAPI SERP discovery, fetch-once-forever keyword cache — "
        "no re-fetch, ever, once a keyword's SERP results are cached) + "
        "Twitter signals: monitor, score (generic 1-100 relevance/visibility/"
        "engagement model), store. v9.13: Reddit's RSS-matching stage now "
        "governs which subreddits/keywords it actively targets via two new "
        "auto-synced mirror collections, flintel_targeting_subreddits and "
        "flintel_targeting_keywords, rebuilt every pass from whatever is "
        "currently pending (fetched=False) in flintel_google_posts. Matching "
        "is still, and only ever, an exact post_url match against each "
        "subreddit's live RSS feed — on a match, the post is marked "
        "fetched=True in flintel_google_posts, its flintel_targeting_keywords "
        "entry is deleted immediately, and the raw fetched text is queued "
        "as-is (Reddit items are no longer re-filtered by keyword-phrase text "
        "downstream — the URL match is the sole authoritative relevance "
        "decision). flintel_keywords, the SERP call, and the search-volume "
        "seeding logic are all 100% unchanged from v9.11.1. Persistent batch "
        "state + queue + dedup — no in-flight item is ever lost on restart. "
        "Streaming Claude with partial-JSON recovery. Claude failures route "
        "to status='pending' for automatic rescore."
    ),
    version="9.13.0",
)


def _serialise(signals: list) -> list:
    for s in signals:
        s.pop("_id", None)
        for f in ["created_at", "fetched_at"]:
            if s.get(f):
                s[f] = s[f].isoformat()
    return signals


@app.get("/")
def root():
    total_keywords_tracked = db.flintel_keywords.count_documents({})
    due_now_count = db.flintel_keywords.count_documents({"fetched": False})
    missing_volume_count = db.flintel_keywords.count_documents({"search_volume": None})
    random_volume_count = db.flintel_keywords.count_documents({"search_volume_is_random": True})

    total_google_posts = db.flintel_google_posts.count_documents({})
    pending_google_posts = db.flintel_google_posts.count_documents({"fetched": False})
    confirmed_google_posts = db.flintel_google_posts.count_documents({"fetched": True})

    targeting_subreddits_count = db.flintel_targeting_subreddits.count_documents({})
    targeting_keywords_count = db.flintel_targeting_keywords.count_documents({})

    return {
        "status":                  "running",
        "system":                  "FLINTEL v9.13.0 (Reddit SERP + fetch-once-forever keyword cache + flintel_google_posts cache + AUTO-SYNCED flintel_targeting_subreddits/keywords + URL-match RSS confirmation + random-fallback volume/engagement + Twitter)",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "SERP discovery (RapidAPI) -> flintel_google_posts cache -> flintel_targeting_subreddits/keywords (auto-synced) -> subreddit RSS URL-match confirmation (credential-free) — no OAuth/PRAW",
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_search_keywords":  len(REDDIT_SEARCH_KEYWORDS),
        "twitter_search_keywords": len(TWITTER_SEARCH_KEYWORDS),
        "keyword_check_interval_seconds": KEYWORD_CHECK_INTERVAL_SECONDS,
        "keyword_cache":                  "ENABLED — fetch-once-forever, restart-safe (flintel_keywords), UNCHANGED from v9.11.1, no longer tied to Reddit RSS reliability",
        "search_volume_seeding":           f"BATCHED loop (chunks of {SEARCH_VOLUME_BATCH_SIZE}) — UNCHANGED",
        "search_volume_random_fallback":   f"ENABLED — range {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-{SEARCH_VOLUME_RANDOM_FALLBACK_MAX}, always logged, never overrides a real value",
        "google_posts_cache":              "ENABLED — flintel_google_posts, fetch-once-forever per post_url",
        "targeting_collections":           "ENABLED (NEW v9.13) — flintel_targeting_subreddits + flintel_targeting_keywords, fully auto-synced from flintel_google_posts pending state every RSS-matching pass, no hardcoded python list",
        "reddit_batch_filter_bypassed":    True,
        "google_posts_rss_check_interval_seconds": GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS,
        "fuzzy_keywords_per_post":         FUZZY_KEYWORDS_PER_POST,
        "reddit_engagement_random_fallback": f"ENABLED — range {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (RSS has no real upvotes/comments), always logged",
        "keywords_tracked":               total_keywords_tracked,
        "keywords_due_now":               due_now_count,
        "keywords_missing_search_volume": missing_volume_count,
        "keywords_with_random_search_volume": random_volume_count,
        "google_posts_total":             total_google_posts,
        "google_posts_pending_rss_confirmation": pending_google_posts,
        "google_posts_confirmed":          confirmed_google_posts,
        "targeting_subreddits_tracked":    targeting_subreddits_count,
        "targeting_keywords_tracked":      targeting_keywords_count,
        "serp_months_back":        SERP_MONTHS_BACK,
        "serp_results_per_kw":     SERP_RESULTS_PER_KEYWORD,
        "reddit_batch_size":       REDDIT_BATCH_SIZE,
        "twitter_batch_size":      TWITTER_BATCH_SIZE,
        "rescore_batch_size":      RESCORE_BATCH_SIZE,
        "reddit_batch_gap_s":      REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":  REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter_batch_gap_s":     TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s": TWITTER_BATCH_TIMEOUT_SECONDS,
        "rescore_batch_gap_s":     RESCORE_BATCH_GAP_SECONDS,
        "rapidapi_configured":    bool(RAPIDAPI_KEY),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "auth_required":           bool(API_KEY),
        "telegram_removed":        True,
        "reddit_per_post_json_removed": True,
        "reddit_oauth_praw_removed": True,
        "fixed_full_cycle_sleep_removed": True,
        "post_url_dedup_before_scoring": True,
        "claude_failure_routes_to_pending": True,
        "keyword_due_state_independent_of_python_list": True,
        "google_posts_state_independent_of_python_list": True,
        "targeting_state_independent_of_python_list": True,
        "reddit_serp_never_waits_on_reddit_rss": True,
        "output_schema":           "intent_score (1-100) / is_relevant / reply_draft",
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    return {
        "status":                  "ok",
        "mongodb":                 mongo,
        "reddit_working":          REDDIT_ENABLED and bool(RAPIDAPI_KEY),
        "reddit_indicator":        _working(REDDIT_ENABLED and bool(RAPIDAPI_KEY)),
        "reddit_fetch_method":     "SERP -> flintel_google_posts -> flintel_targeting_subreddits/keywords -> subreddit RSS URL-match confirmation (credential-free) — no OAuth/PRAW",
        "twitter_working":         TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN),
        "twitter_indicator":       _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "google_posts_pending":    db.flintel_google_posts.count_documents({"fetched": False}),
        "targeting_subreddits":    db.flintel_targeting_subreddits.count_documents({}),
        "targeting_keywords":      db.flintel_targeting_keywords.count_documents({}),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/keywords", dependencies=[Depends(verify_api_key)])
def get_keywords_status():
    """
    Inspect the fetch-once-forever keyword cache directly — UNCHANGED
    FROM v9.11.1. Note: "fetched=True" here now means "this keyword's
    SERP results are all cached in flintel_google_posts" (see
    process_one_keyword() docstring) — actual Reddit RSS confirmation
    status lives in /google-posts and /targeting-keywords below.
    """
    raw_docs = list(db.flintel_keywords.find({}, {"_id": 0}).sort("keyword", 1))
    due_count = 0
    missing_volume_count = 0
    random_volume_count = 0
    docs = []
    for d in raw_docs:
        is_due = not d.get("fetched")
        if is_due:
            due_count += 1
        if d.get("search_volume") is None:
            missing_volume_count += 1
        if d.get("search_volume_is_random"):
            random_volume_count += 1
        for f in ["last_fetched_at", "created_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
        d["due_now"] = is_due
        docs.append(d)
    return {
        "total": len(docs),
        "due_now": due_count,
        "missing_search_volume": missing_volume_count,
        "random_fallback_search_volume": random_volume_count,
        "keywords": docs,
    }


@app.get("/google-posts", dependencies=[Depends(verify_api_key)])
def get_google_posts_status(subreddit: str = None, pending_only: bool = False, limit: int = 200):
    """
    Inspect the flintel_google_posts cache directly. Shows every
    SERP-discovered post_url, its google_rank, matched_keyword,
    auto-generated fuzzy_keywords, subreddit, and whether it has been
    confirmed yet (fetched=True) via run_google_posts_rss_matching_loop().
    """
    q: dict = {}
    if subreddit:
        q["subreddit"] = subreddit
    if pending_only:
        q["fetched"] = False

    raw_docs = list(db.flintel_google_posts.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    docs = []
    for d in raw_docs:
        for f in ["created_at", "fetched_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
        docs.append(d)

    total = db.flintel_google_posts.count_documents({})
    pending = db.flintel_google_posts.count_documents({"fetched": False})
    confirmed = db.flintel_google_posts.count_documents({"fetched": True})

    return {
        "total": total,
        "pending": pending,
        "confirmed": confirmed,
        "count_returned": len(docs),
        "google_posts": docs,
    }


@app.get("/targeting-subreddits", dependencies=[Depends(verify_api_key)])
def get_targeting_subreddits_status(limit: int = 500):
    """
    NEW in v9.13 — inspect flintel_targeting_subreddits directly: the
    live, auto-synced list of subreddits the RSS-matching loop is
    currently polling this cycle. Fully rebuilt every pass from
    flintel_google_posts's pending state — never hand-maintained.
    """
    docs = list(db.flintel_targeting_subreddits.find({}, {"_id": 0}).sort("subreddit", 1).limit(limit))
    for d in docs:
        for f in ["created_at", "last_synced_at"]:
            if d.get(f):
                d[f] = d[f].isoformat()
    return {
        "total": db.flintel_targeting_subreddits.count_documents({}),
        "count_returned": len(docs),
        "targeting_subreddits": docs,
    }


@app.get("/targeting-keywords", dependencies=[Depends(verify_api_key)])
def get_targeting_keywords_status(subreddit: str = None, limit: int = 500):
    """
    NEW in v9.13 — inspect flintel_targeting_keywords directly: one live
    entry per still-pending flintel_google_posts document (post_url +
    matched keyword + fuzzy_keywords + subreddit). An entry disappears
    the instant its post_url is confirmed via exact RSS-link match.
    """
    q: dict = {}
    if subreddit:
        q["subreddit"] = subreddit
    docs = list(db.flintel_targeting_keywords.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    for d in docs:
        if d.get("created_at"):
            d["created_at"] = d["created_at"].isoformat()
    return {
        "total": db.flintel_targeting_keywords.count_documents({}),
        "count_returned": len(docs),
        "targeting_keywords": docs,
    }


@app.get("/signals", dependencies=[Depends(verify_api_key)])
def get_signals(limit: int = 50, min_score: int = None, is_relevant: bool = None,
                 platform: str = None, status: str = None):
    q: dict = {"client_id": CLIENT_ID}
    if min_score is not None:
        q["intent_score"] = {"$gte": min_score}
    if is_relevant is not None:
        q["is_relevant"] = is_relevant
    if platform:
        q["platform"] = platform
    if status:
        q["status"] = status
    signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    return {"count": len(signals), "signals": _serialise(signals)}


@app.get("/signals/relevant", dependencies=[Depends(verify_api_key)])
def get_relevant_signals(limit: int = 50, min_score: int = 0):
    signals = list(
        db.signals.find(
            {"client_id": CLIENT_ID, "is_relevant": True, "intent_score": {"$gte": min_score}},
            {"_id": 0},
        ).sort("intent_score", -1).limit(limit)
    )
    return {"count": len(signals), "signals": _serialise(signals)}


@app.get("/signals/pending", dependencies=[Depends(verify_api_key)])
def get_pending(limit: int = 100):
    signals = list(db.signals.find({"status": "pending"}, {"_id": 0}).limit(limit))
    return {"count": len(signals), "signals": _serialise(signals)}


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    api_thread = threading.Thread(target=run_fastapi, daemon=True, name="FastAPI")
    api_thread.start()
    log.info("FastAPI running at http://0.0.0.0:8000")

    await asyncio.gather(
        start_reddit_listener(),
        start_twitter_listener(),
        start_rescore_listener(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL v9.13.0 — REDDIT (SERP + FETCH-ONCE-FOREVER KEYWORD CACHE")
    log.info("                   + FLINTEL_GOOGLE_POSTS CACHE + AUTO-SYNCED")
    log.info("                   FLINTEL_TARGETING_SUBREDDITS/KEYWORDS + URL-MATCH")
    log.info("                   SUBREDDIT RSS CONFIRMATION + RANDOM-FALLBACK VOLUME/")
    log.info("                   ENGAGEMENT) + TWITTER SIGNAL SCORER")
    log.info("=" * 70)
    log.info(f"  Client               : {CLIENT_ID}")
    log.info(f"  Platforms            : Reddit (SERP discovery, fetch-once-forever) + Twitter/X")
    log.info(f"  Reddit               : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED and bool(RAPIDAPI_KEY))}")
    log.info(f"  Reddit fetch method  : SERP (RapidAPI) -> flintel_google_posts cache -> "
             f"flintel_targeting_subreddits/keywords (auto-synced every pass) -> subreddit RSS "
             f"URL-match confirmation — credential-free, no OAuth/PRAW")
    log.info(f"  Reddit engagement    : RANDOM placeholder {REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MIN}-{REDDIT_ENGAGEMENT_RANDOM_FALLBACK_MAX} (upvotes/comments) — RSS has no real counts, always logged")
    log.info(f"  Twitter              : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Reddit keywords      : {len(REDDIT_SEARCH_KEYWORDS)} (used for SERP discovery + to seed brand-new flintel_keywords docs)")
    log.info(f"  Twitter keywords     : {len(TWITTER_SEARCH_KEYWORDS)} (used for Twitter search query)")
    log.info(f"  Keyword cache        : fetch-once-forever (no re-fetch, ever) | check every {KEYWORD_CHECK_INTERVAL_SECONDS}s | "
             f"last {SERP_MONTHS_BACK} months | depth {SERP_RESULTS_PER_KEYWORD} | UNCHANGED from v9.11.1")
    log.info(f"  Keyword due state    : read directly from flintel_keywords — NOT filtered by the current "
             f"REDDIT_SEARCH_KEYWORDS python list")
    log.info(f"  Search-volume seeding: batched loop, chunks of {SEARCH_VOLUME_BATCH_SIZE} keywords | UNCHANGED from v9.11.1")
    log.info(f"  Search-volume fallback: RANDOM placeholder {SEARCH_VOLUME_RANDOM_FALLBACK_MIN}-"
             f"{SEARCH_VOLUME_RANDOM_FALLBACK_MAX} on any failure/no-credits — always clearly logged")
    log.info(f"  flintel_google_posts : every SERP-discovered post_url + google_rank + matched_keyword "
             f"+ {FUZZY_KEYWORDS_PER_POST} auto fuzzy keywords + subreddit cached immediately, no wait on Reddit")
    log.info(f"  Targeting collections: NEW — flintel_targeting_subreddits + flintel_targeting_keywords, "
             f"fully auto-synced from flintel_google_posts pending state EVERY pass, no hardcoded python list")
    log.info(f"  Google-posts RSS     : independent thread | check every {GOOGLE_POSTS_RSS_CHECK_INTERVAL_SECONDS}s | "
             f"reads flintel_targeting_subreddits | confirms by exact post_url match ONLY | "
             f"{REDDIT_FETCH_MAX_RETRIES}x backoff + old.reddit.com fallback")
    log.info(f"  On match             : flintel_google_posts.fetched=True permanently + "
             f"flintel_targeting_keywords entry deleted immediately + raw text queued as-is "
             f"(no downstream keyword-phrase re-filter for Reddit)")
    log.info(f"  Reddit batch         : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch        : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch        : {RESCORE_BATCH_SIZE} items | poll {RESCORE_POLL_INTERVAL}s | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore source       : signals collection, status='pending' — never re-fetches, only re-scores")
    log.info(f"  Claude streaming     : True | prompt: generic 1-100 relevance/visibility/engagement")
    log.info(f"  RapidAPI config      : {bool(RAPIDAPI_KEY)} (SOLE provider — google_rank + search_volume)")
    log.info(f"  Telegram             : REMOVED")
    log.info(f"  Reddit per-post JSON/RSS-in-discovery : REMOVED (moved to flintel_google_posts + subreddit RSS)")
    log.info(f"  Reddit OAuth/PRAW    : REMOVED")
    log.info(f"  Fixed full-cycle sleep: REMOVED (each keyword + each google_post has its own independent fetch-once-forever state)")
    log.info(f"  MongoDB DB           : {MONGODB_DB}")
    log.info(f"  API auth             : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
