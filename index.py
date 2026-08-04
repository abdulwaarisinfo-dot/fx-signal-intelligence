"""
FX Signal Intelligence System — FLINTEL v7.9.1
=============================================
Platforms : Reddit (feedparser RSS) + Twitter/X + Telegram (Telethon)
            + Facebook (facebook-scraper3 RapidAPI) + LinkedIn (linkedin-data-scraper1 RapidAPI)

Changelog v7.9.1 (ONE CHANGE ONLY — everything else 100% unchanged from v7.9.0):

  CHANGE — TWITTER SEARCH CHUNKED (fixes HTTP 414 Request-URI Too Large).

           v7.9.0 combined EVERY keyword in KEYWORDS into ONE giant OR-query
           sent as a single GET request. With 1000+ keywords this produced
           URLs 100,000+ characters long, which RapidAPI's gateway rejected
           outright with HTTP 414 (Request-URI Too Large) — every single
           poll cycle, permanently broken.

           Fix: KEYWORDS is now split into chunks of TWITTER_CHUNK_SIZE
           (default 25) keywords each, combined into one OR-query per
           chunk — e.g. ("kw1" OR "kw2" OR ... OR "kw25"). This keeps each
           request's URL short enough that 414 never happens again.

           UNLIKE Facebook/LinkedIn (which cycle through their ENTIRE
           keyword list every single poll cycle, one keyword per request),
           Twitter sends exactly ONE chunk per TWITTER_POLL_INTERVAL, then
           advances to the next chunk on the following cycle, and so on —
           wrapping back to chunk #1 once the last chunk is reached. So
           with 1000 keywords in 40 chunks of 25, it takes 40 poll cycles
           to cover the full list once, then it restarts automatically
           from chunk #1. This keeps Twitter's request rate low and
           predictable (Twitter's RapidAPI plan/rate-limits don't tolerate
           the same per-keyword request volume Facebook/LinkedIn use).

           The current chunk position is persisted in MongoDB
           (flintel_state, key="twitter_chunk_index") so a restart/deploy
           resumes from wherever it left off instead of restarting the
           whole list from chunk #1 every time.

           RapidAPI key failover (429/403 → rotate to next configured key)
           from v7.9.0 is preserved exactly as-is, just applied per-chunk
           instead of per-full-query. A failed chunk attempt does NOT
           advance the chunk index — it retries the SAME chunk after
           rotating keys / waiting.

           NOTHING ELSE CHANGED — Reddit, Telegram, Facebook, LinkedIn,
           scoring logic, prompts, routing thresholds, Slack/HubSpot
           delivery, FastAPI routes, keyword list, batch timing, and every
           other platform's poll/dedup/queue behavior are byte-for-byte
           identical to v7.9.0.

Changelog v7.9.0 (ONE ADDITIVE CHANGE ONLY — everything else 100% unchanged from v7.8.0):

  CHANGE (superseded by v7.9.1 above for the query-building part) —
           Twitter search originally moved to a single combined OR-query
           with automatic RapidAPI key failover on 429/403. The key
           failover behavior is preserved in v7.9.1; the single-giant-query
           behavior is replaced by chunking as described above.

Changelog v7.8.0 (ONE ADDITIVE CHANGE ONLY — everything else 100% unchanged from v7.7.0):

  CHANGE — search_keyword NOW POPULATED FOR ALL 5 PLATFORMS.

           In v7.7.0, search_keyword was only ever set for Facebook and
           LinkedIn (the two platforms that search KEYWORD-BY-KEYWORD, so
           they already know which keyword they searched with at fetch
           time). Reddit, Twitter, and Telegram fetch first and filter
           after, via passes_keyword_filter(), which previously only
           returned True/False — so there was no way to know WHICH keyword
           in the KEYWORDS list actually matched.

           Fix: passes_keyword_filter() now returns the matched keyword
           STRING (the exact KEYWORDS entry) instead of True/False, or
           None if nothing matched. This is fully backward compatible —
           every existing call site used it in a boolean context
           (`if not passes_keyword_filter(text):`), and a non-empty
           string is truthy while None is falsy, so all existing
           match/no-match branching behaves identically to before.

           run_batch_processor() — the SAME generic function every
           platform already shares — now captures this matched keyword
           and sets item["search_keyword"] = matched_keyword, but ONLY
           when the item doesn't already carry a search_keyword. Facebook
           and LinkedIn already set it at poll time (v7.7.0) — that
           existing value is preserved exactly as-is and is NOT
           overwritten. Reddit, Twitter, and Telegram never had this
           field populated before, so they now get it filled in with
           whichever KEYWORDS entry matched their fetched text.

           Everything downstream (process_scored_item, save_signal,
           MongoDB storage, the search_keyword index added in v7.7.0) is
           completely unchanged — it already reads item.get("search_keyword")
           and stores it as-is, so no further changes were needed there.

           NOTHING ELSE CHANGED — scoring logic, prompts, routing
           thresholds, Slack/HubSpot delivery, FastAPI routes, keyword
           list, poll/batch timing, and Facebook/LinkedIn's own
           search_keyword behavior are byte-for-byte identical to v7.7.0.

Changelog v7.7.0 (ONE ADDITIVE CHANGE ONLY — everything else 100% unchanged from v7.6.0):

  CHANGE — NEW FIELD: search_keyword.

           Every signal document in MongoDB now additionally stores
           search_keyword — the exact KEYWORDS list entry that was being
           searched when this item was found. This is only meaningful (and
           only ever populated) for the two platforms that search
           KEYWORD-BY-KEYWORD: Facebook (poll_facebook) and LinkedIn
           (poll_linkedin). Both loops already iterate `for keyword in
           KEYWORDS:` — the keyword variable is simply carried onto the
           queued item as item["search_keyword"], flows unchanged through
           the SAME generic run_batch_processor()/process_scored_item()
           pipeline every platform already uses, and is stored as-is in
           save_signal(). Reddit (subreddit RSS, not keyword search),
           Twitter, and Telegram (group listener/poller, not keyword
           search) have no single keyword to attribute a match to for
           this version — search_keyword is simply None/absent, exactly
           as it always implicitly was.

           A matching index on "search_keyword" was added alongside the
           existing indexes (intent_score, platform, tier, etc.) so it can
           be queried/filtered efficiently later if needed.

Changelog v7.6.0 (ONE ADDITIVE CHANGE ONLY — everything else 100% unchanged from v7.5.0):

  CHANGE — LINKEDIN ADDED AS A 5TH PLATFORM.

           LinkedIn is wired in exactly like Facebook was in v7.5.0 — its own
           in-memory queue (linkedin_queue), its own persistent dedup set
           (flintel_seen_ids, platform="linkedin"), its own persistent batch
           state (flintel_pending_batch / flintel_batch_seconds, platform=
           "linkedin"), its own persistent raw-queue backup
           (flintel_queue_messages, platform="linkedin"), its own
           run_batch_processor() thread using the SAME generic batch
           processor function every other platform already uses, its own
           Claude scoring schema (CLAUDE_SYSTEM_PROMPT_LINKEDIN — same
           _SCORING_CORE, same thresholds, same routing), and its own
           FastAPI surface (/signals/linkedin) plus root/health reporting.
           Reddit, Twitter, Telegram, and Facebook's code paths, timing,
           and state are 100% unchanged — LinkedIn is purely additive.

           LinkedIn uses the linkedin-data-scraper1 RapidAPI product
           (x-rapidapi-host: linkedin-data-scraper1.p.rapidapi.com), and is
           built from THREE separate POST endpoints on that same product:

             1. Search LinkedIn   POST /search_linkedIn.php
                body: {"keywords": <kw>, "start": "0"}
                — cycled through the SAME KEYWORDS list already used for
                keyword pre-filtering on every other platform, one search
                per keyword per poll cycle (same pattern as Facebook's
                per-keyword cycle / Reddit's per-subreddit cycle).

             2. User Data         POST /get_user_data.php
                body: {"username_or_url": <profile url>}
                — called for every search result that survives the keyword
                filter, to enrich the signal with whatever profile detail
                the vendor returns (name, headline, email, phone, location,
                company, job title — each field is optional, since the
                vendor's response shape for this account tier is not
                guaranteed to include all of them).

             3. Company Data      POST /get_company_data.php
                body: {"company_name": <company>}
                — called when a company name is available (from the search
                result or from the User Data response), to enrich the
                signal with company detail (website, industry, size,
                headquarters, phone — again, each field optional).

           All three endpoints share the same RAPID_API_KEY, but each call
           is wrapped in its own try/except — exactly as requested: if the
           Search, User Data, or Company Data endpoint fails or runs out
           of quota, that ONE call degrades to "no enrichment" and logs a
           warning; it never raises, never crashes the poll cycle, and
           never blocks the other two endpoints or the rest of the
           pipeline. A LinkedIn item with zero enrichment (both User Data
           and Company Data failed) is still queued and scored using
           whatever the Search endpoint returned — nothing is dropped
           just because enrichment failed.

           Matched + enriched items are queued into linkedin_queue exactly
           like every other platform, then picked up by the SAME generic
           run_batch_processor() (batch of LINKEDIN_BATCH_SIZE items, or
           LINKEDIN_BATCH_TIMEOUT_SECONDS elapsed — whichever comes first —
           triggers ONE Claude call for that batch, e.g. "batch 1/25",
           "batch 2/25" in the logs, same as Reddit/Twitter/Telegram/
           Facebook). The enrichment fields (email/phone/location/company/
           industry/size) ride along on the queued item and are surfaced
           to Claude via a small ADDITIVE-ONLY block inside
           _build_batch_prompt() that only activates when
           item["platform"] == "linkedin" — every other platform's prompt
           text is byte-for-byte unchanged.

           New env vars (defaults match the other platforms' patterns):

             LINKEDIN_ENABLED               (default: false)
             LINKEDIN_BATCH_SIZE            (default: 10)
             LINKEDIN_BATCH_GAP_SECONDS     (default: 30)
             LINKEDIN_BATCH_TIMEOUT_SECONDS (default: 120)
             LINKEDIN_POLL_INTERVAL         (default: 300 — full keyword cycle)
             LINKEDIN_KEYWORD_GAP_SECONDS   (default: 2 — between each keyword search)

Prior changelogs (v7.0 → v7.5.0) are unchanged from the previous version of
this file and are omitted here only to keep this header readable — no
behavior from any prior version was altered. Summary of what's already in
place: feedparser RSS Reddit, Twitter/X search, Telegram (Telethon) with
auto-join + polling, Facebook (facebook-scraper3), persistent batch state
(FIX A), tolerant partial-JSON recovery (FIX B), Claude streaming (FIX C),
working indicators (FIX D), HubSpot error visibility + startup property
check (FIX E), manual rescore pipeline, per-platform batch gap/timeout,
persistent raw-queue backup + explicit batch-timeout persistence (v7.4.5),
and Facebook as the 4th platform (v7.5.0). Claude model: claude-sonnet-4-6.
"""

import asyncio
import logging
import os
import json
import time
import queue
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import html
import re
import feedparser
import anthropic
import httpx
from telethon import TelegramClient, events
from telethon.errors import (
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    ChannelPrivateError,
    FloodWaitError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import requests
from fastapi import FastAPI, HTTPException, Security, Depends, Body
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN
import uvicorn

# ─────────────────────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

REDDIT_POLL_INTERVAL = int(os.getenv("REDDIT_POLL_INTERVAL", "300"))

# v7.9.0 — RAPID_API_KEY is shared by Facebook + LinkedIn (same RapidAPI
# account/plan for both — intentional). Twitter uses its OWN separate
# TWITTER_RAPID_API_KEYS (defined down in the Twitter section) so that a
# Twitter rate-limit/quota issue never affects Facebook or LinkedIn, and
# vice versa. If Facebook/LinkedIn ever need to move to a different
# RapidAPI account/provider, only their build_*_client() headers need to
# change — nothing else in the pipeline changes.
RAPID_API_KEY        = os.getenv("RAPID_API_KEY")

TELEGRAM_API_ID      = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH    = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE       = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION     = os.getenv("TELEGRAM_SESSION", "flintel_telegram")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY")

MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "4"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))
CLIENT_ID        = os.getenv("CLIENT_ID", "settla")

# ── BATCH SIZE — already platform-specific since v7.2/v7.4, UNCHANGED ───────
REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
TELEGRAM_BATCH_SIZE = int(os.getenv("TELEGRAM_BATCH_SIZE", "10"))
FACEBOOK_BATCH_SIZE = int(os.getenv("FACEBOOK_BATCH_SIZE", "10"))
# v7.6.0 NEW — LinkedIn batch size lives alongside the other BATCH_SIZE
# vars, same section, same style, same defaults pattern.
LINKEDIN_BATCH_SIZE = int(os.getenv("LINKEDIN_BATCH_SIZE", "10"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))

# ── v7.4.4 — per-platform BATCH_GAP_SECONDS / BATCH_TIMEOUT_SECONDS ─────────
REDDIT_BATCH_GAP_SECONDS       = int(os.getenv("REDDIT_BATCH_GAP_SECONDS",       "30"))
REDDIT_BATCH_TIMEOUT_SECONDS   = int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",   "120"))

TWITTER_BATCH_GAP_SECONDS      = int(os.getenv("TWITTER_BATCH_GAP_SECONDS",      "30"))
TWITTER_BATCH_TIMEOUT_SECONDS  = int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS",  "120"))

TELEGRAM_BATCH_GAP_SECONDS     = int(os.getenv("TELEGRAM_BATCH_GAP_SECONDS",     "30"))
TELEGRAM_BATCH_TIMEOUT_SECONDS = int(os.getenv("TELEGRAM_BATCH_TIMEOUT_SECONDS", "120"))

# ── v7.5.0 — FACEBOOK: same per-platform batch gap/timeout pattern ─────────
FACEBOOK_BATCH_GAP_SECONDS     = int(os.getenv("FACEBOOK_BATCH_GAP_SECONDS",     "30"))
FACEBOOK_BATCH_TIMEOUT_SECONDS = int(os.getenv("FACEBOOK_BATCH_TIMEOUT_SECONDS", "120"))

# ── v7.6.0 NEW — LINKEDIN: same per-platform batch gap/timeout pattern as
# Reddit/Twitter/Telegram/Facebook. Independent env vars — never shared,
# never mixed with any other platform's timing.
LINKEDIN_BATCH_GAP_SECONDS     = int(os.getenv("LINKEDIN_BATCH_GAP_SECONDS",     "30"))
LINKEDIN_BATCH_TIMEOUT_SECONDS = int(os.getenv("LINKEDIN_BATCH_TIMEOUT_SECONDS", "120"))

# Rescore's own gap — independent of every platform (v7.4.4).
RESCORE_BATCH_GAP_SECONDS = int(os.getenv("RESCORE_BATCH_GAP_SECONDS", "30"))

DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

TELEGRAM_JOIN_GAP_SECONDS = int(os.getenv("TELEGRAM_JOIN_GAP_SECONDS", "30"))

# ── v7.5.0 — FACEBOOK: mirrors Reddit's per-cycle polling pattern ──────────
FACEBOOK_POLL_INTERVAL       = int(os.getenv("FACEBOOK_POLL_INTERVAL", "300"))
FACEBOOK_KEYWORD_GAP_SECONDS = int(os.getenv("FACEBOOK_KEYWORD_GAP_SECONDS", "2"))

# ── v7.6.0 NEW — LINKEDIN: same per-cycle-over-KEYWORDS pattern as Facebook,
# with its own poll interval and its own gap between individual keyword
# searches (kept separate so LinkedIn's rate limits never affect Facebook's
# or vice versa).
LINKEDIN_POLL_INTERVAL       = int(os.getenv("LINKEDIN_POLL_INTERVAL", "300"))
LINKEDIN_KEYWORD_GAP_SECONDS = int(os.getenv("LINKEDIN_KEYWORD_GAP_SECONDS", "2"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

CLAUDE_STREAM_TIMEOUT = int(os.getenv("CLAUDE_STREAM_TIMEOUT", "600"))

RESCORE_POLL_INTERVAL = int(os.getenv("RESCORE_POLL_INTERVAL", "10"))

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
# PLATFORM ENABLE / DISABLE FLAGS (unchanged logic; FIX D adds indicators)
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED   = _bool_env("REDDIT_ENABLED",   True)
TWITTER_ENABLED  = _bool_env("TWITTER_ENABLED",  True)
TELEGRAM_ENABLED = _bool_env("TELEGRAM_ENABLED", False)
FACEBOOK_ENABLED = _bool_env("FACEBOOK_ENABLED", False)
# v7.6.0 NEW
LINKEDIN_ENABLED = _bool_env("LINKEDIN_ENABLED", False)


def _working(flag: bool) -> str:
    """FIX D: human-readable indicator for enable/disable state."""
    return "✅ Working" if flag else "❌ Not Working"


# ─────────────────────────────────────────────────────────────────────────────
# TARGET SUBREDDITS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

TARGET_SUBREDDITS = [
    "Nigeria", "lagos", "Nigerians", "NigeriansAbroad",
    "AfricanDiaspora", "pakistan", "Pakistani", "PakistaniDiaspora",
    "PersonalFinanceCanada", "PersonalFinanceUK", "personalfinance",
    "entrepreneur", "smallbusiness", "digitalnomad", "africatech",
    "UKPersonalFinance", "Remittance", "moneytransfer",
    "CanadianInvestor", "ExpatFinance",
    "Scams", "personalfinance",
    "Stripe", "Banking", "freelance",
    "smallbusiness", "startups_marketing", "digital_marketing", "ProductManagement", "consulting",
    "startups", "Entrepreneur", "EntrepreneurRideAlong",
    "growmybusiness", "b2b_marketing", "marketing",
        "nocode", "automation", "productivity",
    "software", "SoftwareEngineering", "webdev", "smallbusinessowner", "solopreneur", "indiehackers",
    "microsaas", "SideProject", "Business_Ideas", "software", "SoftwareEngineering", "webdev",

    "logistics", "supplychain", "freight", "Truckers",
    "FreightBrokers", "Shipping", "portmanteau",

    "FulfillmentByAmazon", "ecommerce", "EtsySellers",
    "shopify", "AmazonSeller", "dropship", "Import_Export",

    "smallbusiness", "Entrepreneur", "EntrepreneurRideAlong",
    "startups", "manufacturing",

    "supplychainmanagement", "sourcing", "procurement",
    "warehouseautomation", "operationsmanagement",      "humanresources", "recruiting", "RecruitingHell",
    "HRTech", "AskHR", "Recruitment",

    "startups", "smallbusiness", "Entrepreneur",
    "EntrepreneurRideAlong", "growmybusiness",

    "jobs", "careerguidance", "cscareerquestions",
    "WorkOnline", "remotework",

    "Accounting", "Bookkeeping", "QuickBooks", "Xero",
    "smallbusiness", "tax", "IRS",

    "Entrepreneur", "EntrepreneurRideAlong", "startups",
    "smallbusinessowner", "solopreneur", "freelance",

    "personalfinance", "financialindependence", "CFA",
]

# ─────────────────────────────────────────────────────────────────────────────
# TARGET TELEGRAM GROUPS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

TARGET_TELEGRAM_GROUPS = [
    "nigeriansincanada", "nigeriansinuk", "nigeriansinusa",
    "nigeriansinaustralia", "nigeriandiaspora", "nigerianentrepreneurs",
    "lagosBusinessNetwork", "nigeriafinance", "pakistanisincanada",
    "pakistanisinuk", "pakistanisinusa", "pakistanidiaspora",
    "pakistanibusiness", "karachi_business", "remittancetalk",
    "moneytransfertips", "fxtraders_ng", "diaspora_finance",
    "crossborderpayments", "africabusiness", "africaentrepreneurs",
    "africatrade", "africafintech", "expatfinance", "diasporamoney",
    "internationaltransfer", "wisealternatives",
]

# ─────────────────────────────────────────────────────────────────────────────
# SHARED QUEUES — platform-isolated, never mixed
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:   queue.Queue = queue.Queue()
twitter_queue:  queue.Queue = queue.Queue()
telegram_queue: queue.Queue = queue.Queue()
facebook_queue: queue.Queue = queue.Queue()
# v7.6.0 NEW
linkedin_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER
# NOTE: intentionally left EMPTY here — paste your full KEYWORDS list back
# in below (same list already used across Reddit/Twitter/Telegram/Facebook/
# LinkedIn). Nothing else in the file depends on the list's *content*, only
# on it being a Python list of strings, so this section can be filled in
# independently without touching any other part of the system.
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS = [
    # PASTE YOUR FULL KEYWORDS LIST HERE.
     # ── SENDING MONEY ────────────────────────────────────────────────────────
    "send money to", "sending money to", "transfer money to",
    "transferring money to", "wire money to", "wiring money to",
    "move money to", "moving money to", "remit money to",
    "remitting money to", "pay my supplier", "paying my supplier",
    "pay a supplier", "paying a supplier", "pay my vendor",
    "paying my vendor", "pay my manufacturer", "pay my factory",
    "pay my partner", "pay my contractor", "pay an invoice",
    "paying an invoice", "settle an invoice", "settling an invoice",
    "pay a business", "business payment to", "supplier payment to",
    "vendor payment to", "invoice payment to", "international payment to",
    "overseas payment to", "cross border payment", "cross-border payment",
    "cross border transfer", "cross-border transfer",
    "international transfer", "international wire",
    "international wire transfer", "foreign wire transfer",
    "overseas wire transfer", "overseas transfer", "global payment",
    "global transfer", "b2b payment", "b2b transfer",
    "business to business payment",

    # ── BANK BLOCKING ────────────────────────────────────────────────────────
    "bank blocked my", "bank blocked my transfer", "bank blocked my payment",
    "bank blocked my wire", "bank blocked my transaction",
    "bank flagged my", "bank flagged my transfer", "bank flagged my payment",
    "bank rejected my", "bank rejected my transfer", "bank rejected my payment",
    "bank declined my", "bank declined my transfer",
    "bank won't let me transfer", "bank won't let me send",
    "bank refuses to", "bank holding my", "bank holding my funds",
    "bank holding my money", "bank froze my", "account frozen",
    "funds frozen", "money frozen", "transfer frozen", "payment frozen",
    "transfer blocked", "payment blocked", "wire blocked",
    "transaction blocked", "transfer rejected", "payment rejected",
    "wire rejected", "transfer declined", "payment declined",
    "transfer failed", "payment failed", "wire failed",
    "transfer stuck", "payment stuck", "money stuck", "funds stuck",
    "money held", "funds held", "money hostage", "holding my money",
    "holding my funds", "won't release my funds", "won't release my money",
    "compliance hold", "compliance review", "compliance check",
    "AML hold", "AML review", "AML flag", "flagged for review",
    "flagged as suspicious", "suspicious activity", "suspicious transaction",
    "frozen for review", "under review", "transfer delayed",
    "payment delayed", "wire delayed", "transfer pending",
    "stuck in pending", "days to process", "weeks to process",
    "10-14 days", "10 to 14 days", "two weeks to transfer",
    "transfer taking forever", "payment taking forever",
    "money hasn't arrived", "money still hasn't arrived",
    "payment hasn't arrived", "where is my transfer",
    "where is my payment", "where is my money", "where did my money go",
    "money disappeared", "payment disappeared", "transfer disappeared",
    "no tracking", "can't track my transfer", "can't track my payment",
    "no update on my transfer", "no update on my payment",

    # ── FEE FRUSTRATION ──────────────────────────────────────────────────────
    "SWIFT fees", "SWIFT charges", "wire transfer fees",
    "wire transfer charges", "international transfer fees",
    "international wire fees", "transfer fees too high",
    "transfer fees killing", "fees killing my margins",
    "fees eating my margins", "fees eating my profit",
    "exchange rate terrible", "exchange rate awful", "exchange rate bad",
    "terrible exchange rate", "awful exchange rate", "bad exchange rate",
    "worst exchange rate", "exchange rate ripoff", "exchange rate rip off",
    "hidden fees", "hidden charges", "unexpected fees",
    "unexpected charges", "FX fees", "FX charges", "FX markup",
    "FX spread", "currency conversion fee", "currency conversion charge",
    "conversion fee too high", "conversion markup",
    "losing money on transfer", "losing money on fees",
    "losing money to fees", "losing money exchanging",
    "percentage on transfer", "percentage on payment",
    "ripping me off", "highway robbery", "daylight robbery",
    "absolute ripoff", "total ripoff", "complete ripoff",
    "charging too much", "too expensive to send", "too expensive to transfer",
    "cheapest way to send", "cheapest way to transfer",
    "cheapest international transfer", "cheapest cross border",
    "better rate than", "better rates than", "cheaper than SWIFT",
    "cheaper than wire", "SWIFT alternative", "alternative to SWIFT",
    "avoid SWIFT fees", "avoid wire fees", "correspondent bank fees",
    "intermediary bank fees", "intermediary fees",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "Wise Business", "Wise business account", "Wise transfer",
    "Wise payment", "Wise blocked", "Wise restricted", "Wise suspended",
    "Wise account restricted", "Wise account suspended",
    "Wise account blocked", "Wise account closed", "Wise limit",
    "Wise holding", "leaving Wise", "left Wise", "moving off Wise",
    "moved off Wise", "switching from Wise", "switched from Wise",
    "never using Wise", "done with Wise", "Wise is terrible",
    "Wise is awful", "Wise is a joke", "hate Wise", "Wise disappointed",
    "TransferWise",
    "Remitly blocked", "Remitly restricted", "Remitly limit",
    "Remitly failed", "leaving Remitly", "switching from Remitly",
    "Remitly alternative",
    "Payoneer blocked", "Payoneer restricted", "Payoneer suspended",
    "Payoneer account blocked", "Payoneer account restricted",
    "Payoneer account suspended", "Payoneer limit", "Payoneer holding",
    "leaving Payoneer", "switching from Payoneer", "Payoneer alternative",
    "alternative to Payoneer",
    "WorldRemit failed", "WorldRemit blocked", "WorldRemit problem",
    "WorldRemit issue", "WorldRemit terrible",
    "Western Union failed", "Western Union blocked",
    "Western Union delayed", "Western Union problem",
    "leaving Western Union", "WU failed", "WU blocked",
    "OFX failed", "OFX blocked", "OFX problem", "OFX issue",
    "Revolut blocked", "Revolut restricted", "Revolut suspended",
    "Revolut Business blocked", "Revolut Business restricted",
    "Revolut account blocked", "Revolut account restricted",
    "Revolut holding", "leaving Revolut", "switching from Revolut",
    "Stripe blocked", "Stripe restricted", "Stripe account blocked",
    "Stripe account restricted",
    "Mercury blocked", "Mercury restricted", "Mercury bank blocked",
    "LemFi failed", "LemFi blocked", "LemFi problem",
    "Grey Finance failed", "Grey Finance blocked", "Grey Finance problem",
    "NALA failed", "NALA blocked", "NALA problem",
    "Chipper Cash failed", "Chipper Cash blocked", "Chipper Cash problem",
    "alternative to Wise", "alternative to Remitly",
    "alternative to Payoneer", "alternative to WorldRemit",
    "alternative to Western Union", "alternative to Revolut",
    "better than Wise", "better than Remitly", "better than Payoneer",
    "better than WorldRemit", "better than Western Union",
    "competitors to Wise", "Wise competitors", "Payoneer competitors",

    # ── RECOMMENDATION REQUESTS ──────────────────────────────────────────────
    "recommend a payment", "recommend a transfer", "recommend a service",
    "recommend a platform", "recommend an app", "recommend a provider",
    "recommend a solution", "anyone recommend", "can anyone recommend",
    "does anyone recommend", "what payment service", "what transfer service",
    "what payment platform", "what transfer platform", "what payment app",
    "what transfer app", "which payment service", "which transfer service",
    "which payment platform", "which transfer platform",
    "which payment app", "which transfer app", "which payment provider",
    "which service is best", "which platform is best", "which app is best",
    "best payment service", "best transfer service", "best payment platform",
    "best transfer platform", "best payment app", "best transfer app",
    "best way to send", "best way to transfer", "best way to pay",
    "fastest way to send", "fastest way to transfer",
    "cheapest way to send", "cheapest way to transfer",
    "how do I send", "how do I transfer", "how do I pay",
    "how can I send", "how can I transfer", "how can I pay",
    "looking for a payment", "looking for a transfer",
    "looking for a platform", "looking for a service",
    "looking for a solution", "searching for a payment",
    "need a payment solution", "need a transfer solution",
    "need a payment platform", "need a transfer platform",
    "anyone using", "does anyone use", "has anyone used",
    "who uses", "who do you use", "what do you use", "what are you using",
    "tried everything", "tried so many", "tried multiple", "tried several",
    "nothing works", "none of them work", "still haven't found",
    "still looking for", "still searching for",

    # ── BUSINESS CONTEXT ─────────────────────────────────────────────────────
    "my supplier", "my suppliers", "our supplier", "our suppliers",
    "my vendor", "my vendors", "our vendor", "our vendors",
    "my manufacturer", "my manufacturers", "our manufacturer",
    "my factory", "our factory", "my business partner",
    "our business partner", "my contractor", "our contractor",
    "my client overseas", "our client overseas",
    "import business", "importing business", "export business",
    "exporting business", "import export", "import/export",
    "importing goods", "exporting goods", "importing products",
    "exporting products", "buying from overseas", "buying from abroad",
    "sourcing from", "sourcing overseas", "sourcing abroad",
    "purchase order", "business invoice", "supplier invoice",
    "vendor invoice", "trade finance", "trade payment",
    "trade financing", "supply chain payment", "supply chain finance",
    "diaspora business", "diaspora entrepreneur",
    "running a business", "my business needs", "for my business",
    "business account", "business transfer", "business wire",
    "corporate payment", "corporate transfer", "corporate wire",
    "company payment", "company transfer", "B2B payment", "B2B transfer",
    "B2B transaction", "business to business",

    # ── CORRIDOR KEYWORDS ────────────────────────────────────────────────────
    "to Nigeria", "to Lagos", "to Abuja", "from Nigeria",
    "Nigeria payment", "Nigeria transfer", "Nigeria wire",
    "Nigerian supplier", "Nigerian vendor", "Nigerian manufacturer",
    "Nigeria business", "CAD to NGN", "GBP to NGN", "USD to NGN",
    "EUR to NGN", "AUD to NGN", "naira payment", "naira transfer",
    "send naira", "receive naira",
    "to Pakistan", "to Karachi", "to Lahore", "to Islamabad",
    "from Pakistan", "Pakistan payment", "Pakistan transfer",
    "Pakistan wire", "Pakistani supplier", "Pakistani vendor",
    "Pakistani manufacturer", "CAD to PKR", "GBP to PKR", "USD to PKR",
    "rupee payment", "rupee transfer",
    "to India", "to Mumbai", "to Delhi", "to Bangalore", "from India",
    "India payment", "India transfer", "India wire",
    "Indian supplier", "Indian vendor", "Indian manufacturer",
    "CAD to INR", "GBP to INR", "USD to INR",
    "to Ghana", "to Accra", "from Ghana", "Ghana payment",
    "Ghana transfer", "Ghanaian supplier", "GHS payment", "cedi payment",
    "to Kenya", "to Nairobi", "from Kenya", "Kenya payment",
    "Kenya transfer", "Kenyan supplier", "KES payment",
    "M-Pesa business", "Mpesa business",
    "to Ethiopia", "to Senegal", "to Ivory Coast", "to Cameroon",
    "to Tanzania", "to Uganda", "to Zimbabwe", "to South Africa",
    "to Johannesburg", "African supplier", "African vendor",
    "African manufacturer", "Africa payment", "Africa transfer",
    "from Canada", "from Toronto", "from Vancouver", "from Calgary",
    "from Ottawa", "from Montreal", "from UK", "from London",
    "from Manchester", "from Birmingham", "from Glasgow",
    "from USA", "from New York", "from Houston", "from Atlanta",
    "from Washington", "from Australia", "from Sydney", "from Melbourne",
    "from Perth", "from UAE", "from Dubai", "from Abu Dhabi",

    # ── AMOUNT SIGNALS ───────────────────────────────────────────────────────
    "$10,000", "$10k", "10 thousand", "$15,000", "$15k", "15 thousand",
    "$20,000", "$20k", "20 thousand", "$25,000", "$25k", "25 thousand",
    "$30,000", "$30k", "30 thousand", "$40,000", "$40k", "40 thousand",
    "$45,000", "$45k", "45 thousand", "$50,000", "$50k", "50 thousand",
    "$60,000", "$60k", "60 thousand", "$75,000", "$75k", "75 thousand",
    "$80,000", "$80k", "80 thousand", "$100,000", "$100k", "100 thousand",
    "$150,000", "$150k", "150 thousand", "$200,000", "$200k", "200 thousand",
    "$250,000", "$250k", "250 thousand", "$500,000", "$500k", "500 thousand",
    "$750,000", "$750k", "750 thousand",
    "$1 million", "$1m", "one million",
    "£10,000", "£10k", "£15,000", "£15k", "£20,000", "£20k",
    "£25,000", "£25k", "£30,000", "£30k", "£50,000", "£50k",
    "£100,000", "£100k", "£200,000", "£200k",
    "large transfer", "large amount", "large payment", "large wire",
    "large sum", "significant amount", "substantial amount",
    "big transfer", "big payment", "six figures", "seven figures",
    "six-figure", "seven-figure", "monthly volume", "weekly volume",

    # ── COMPLIANCE PAIN ──────────────────────────────────────────────────────
    "KYC rejected", "KYC failed", "KYC verification failed",
    "KYC problem", "KYC issue", "KYC nightmare",
    "AML rejected", "AML flagged", "AML hold", "AML review",
    "documentation rejected", "documents rejected",
    "proof of funds", "source of funds", "source of wealth",
    "proof of business", "business verification failed",
    "verification rejected", "verification failed",
    "compliance rejected", "compliance hold", "compliance review",
    "compliance nightmare", "compliance problem", "compliance issue",
    "Form M", "CBN compliance", "regulatory hold", "regulatory review",
    "regulatory problem", "regulatory issue",
    "submitted documents again", "sent documents again",
    "asking for documents again", "same documents again",
    "keep asking for documents", "keep rejecting documents",
    "third time submitting", "fourth time submitting",
    "rejected again", "blocked again", "failed again",
    "happening again", "third time", "fourth time",
    "keep blocking", "keeps blocking", "keeps rejecting", "keeps failing",
    "always blocks", "always rejects", "always fails",

    # ── URGENCY SIGNALS ──────────────────────────────────────────────────────
    "urgently", "urgent", "desperately", "desperate",
    "ASAP", "as soon as possible", "right now", "today",
    "this week", "by Friday", "by Monday", "by end of week",
    "by end of month", "deadline", "time sensitive",
    "need it done", "need it now", "need it today", "need it urgently",
    "waiting on payment", "supplier is waiting", "supplier waiting",
    "vendor is waiting", "vendor waiting", "manufacturer waiting",
    "partner waiting", "been waiting", "already delayed", "already late",
    "overdue", "past due", "losing the contract", "losing my supplier",
    "losing my vendor", "threatening to cancel", "might cancel",
    "going to cancel", "cancelling the order", "losing the deal",
    "deal at risk", "relationship at risk",
    "can't wait any longer", "running out of time", "no more time",

    # ── BUSINESS EXPANSION ───────────────────────────────────────────────────
    "just signed a supplier", "signed a new supplier", "found a supplier",
    "new supplier in", "signed a contract with", "new contract with",
    "starting to import", "starting an import", "starting to export",
    "starting an export", "launching in", "expanding to",
    "entering the market", "new market", "setting up payments",
    "need to set up payments", "need to transfer money",
    "will need to send", "will need to transfer", "going to need",
    "starting a business", "new business", "import business",
    "export business", "trading company", "sourcing products from",
    "sourcing goods from", "buying products from", "buying goods from",
    "manufacturing in", "producing in",

    # ── TREASURY & FX ────────────────────────────────────────────────────────
    "treasury management", "cash management", "liquidity management",
    "FX management", "FX exposure", "FX risk", "FX hedging",
    "currency hedging", "currency risk", "currency exposure",
    "FX solution", "FX platform", "FX tool",
    "treasury solution", "treasury platform", "cash flow management",
    "multi currency", "multi-currency", "multicurrency",
    "currency account", "foreign currency account",
    "international banking", "international bank account",
    "global banking", "global bank account", "correspondent banking",
    "banking relationship", "banking partner",
    "payment infrastructure", "payment rails", "payment solution",
    "payment platform", "payment provider", "payment partner",
    "fintech payment", "embedded payment", "embedded finance",
    "cross border banking", "international banking solution",
    "FX banking", "FX banking relationship", "FX liquidity",
    "cash pooling", "cash concentration",
    "intercompany payment", "intercompany transfer",

    # ── JOB SIGNALS ──────────────────────────────────────────────────────────
    "treasury manager", "treasury analyst", "FX manager", "FX analyst",
    "FX trader", "treasury director", "head of treasury", "VP treasury",
    "international payments manager", "global payments manager",
    "cross border payments", "payments operations manager",
    "payments specialist", "treasury specialist", "FX specialist",
    "international finance manager", "global finance manager",
    "head of payments", "director of payments", "VP payments",
    "chief financial officer", "head of finance", "finance director",
    "controller international", "global controller",
    
    # ----------- CYBER SECURITY ---------
    
    "we got hacked", "we got breached", "company got hacked",
    "company got breached", "data breach", "we had a breach",
    "security breach", "breach happened", "just got ransomwared",
    "ransomware attack", "ransomware hit us", "hit by ransomware",
    "encrypted our files", "files got encrypted", "systems encrypted",
    "locked out of our systems", "locked out of our servers",
    "attacker got in", "attackers got in", "unauthorized access",
    "someone accessed our", "someone breached our", "network compromised",
    "systems compromised", "account compromised", "accounts compromised",
    "email compromised", "credentials leaked", "credentials stolen",
    "password leaked", "passwords leaked", "data leaked", "data exposed",
    "customer data exposed", "customer data leaked", "PII exposed",
    "PII leaked", "source code leaked", "database leaked",
    "database exposed", "exfiltrated data", "data exfiltration",
    "phishing attack", "phishing email", "spear phishing",
    "business email compromise", "BEC attack", "CEO fraud",
    "invoice fraud", "wire fraud attack", "supply chain attack",
    "zero day exploit", "zero-day exploit", "actively exploited",
    "malware infection", "infected with malware", "trojan detected",
    "backdoor found", "backdoor discovered", "rootkit found",
    "DDoS attack", "under DDoS", "site went down attack",
    "insider threat", "insider attack", "third party breach",
    "vendor breach", "supplier breach", "MSP breach",

    # ── INCIDENT RESPONSE URGENCY ────────────────────────────────────────────
    "need incident response", "need an IR firm", "need a forensics team",
    "who do I call after a breach", "who to call after hack",
    "emergency incident response", "24/7 incident response",
    "need help now hacked", "actively being attacked",
    "attack in progress", "attacker still in our network",
    "containment help", "need containment", "need remediation",
    "recovering from ransomware", "ransomware recovery",
    "should we pay the ransom", "pay the ransom or not",
    "ransom demand", "ransom note", "threat actor demanding",
    "need to notify customers breach", "breach notification requirements",
    "legally required to disclose breach", "disclose the breach",

    # ── TOOLING / PLATFORM FRUSTRATION ───────────────────────────────────────
    "our SIEM missed it", "SIEM didn't catch it", "SIEM false positives",
    "too many false positives", "alert fatigue", "drowning in alerts",
    "no visibility into our network", "no visibility into endpoints",
    "can't see what's happening on our network",
    "our EDR didn't catch it", "EDR missed", "antivirus didn't catch it",
    "firewall got bypassed", "firewall wasn't enough",
    "our current tool isn't working", "outgrown our current tool",
    "outgrown our security stack", "current vendor isn't cutting it",
    "switching security vendors", "replacing our SIEM",
    "replacing our EDR", "need a new MDR", "need a new SOC",
    "understaffed security team", "no security team",
    "one person security team", "no dedicated security staff",
    "can't afford a full SOC", "need outsourced SOC",
    "need a virtual CISO", "need a fractional CISO", "need vCISO",

    # ── FEE / COST FRUSTRATION ───────────────────────────────────────────────
    "security tools too expensive", "cybersecurity budget too small",
    "can't justify the cost", "pricing is outrageous",
    "licensing costs killing us", "per-endpoint pricing too high",
    "hidden costs security vendor", "surprise renewal fees",
    "renewal price increase", "price hike renewal",
    "cheaper alternative to CrowdStrike", "cheaper alternative to SentinelOne",
    "cheaper EDR", "cheaper SIEM", "cheaper MDR",
    "affordable cybersecurity for small business",
    "budget-friendly security tools", "best value security platform",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "CrowdStrike outage", "CrowdStrike issue", "CrowdStrike problem",
    "CrowdStrike blocked", "CrowdStrike alternative",
    "SentinelOne problem", "SentinelOne issue", "SentinelOne alternative",
    "switching from CrowdStrike", "switching from SentinelOne",
    "leaving Microsoft Defender", "Defender missed", "Defender didn't catch",
    "Palo Alto issue", "Palo Alto problem", "Fortinet vulnerability",
    "Fortinet issue", "Fortinet exploit", "Cisco vulnerability",
    "Cisco exploit", "Sophos problem", "Sophos issue",
    "Trend Micro problem", "McAfee problem", "Norton problem",
    "Rapid7 issue", "Qualys issue", "Tenable issue", "Splunk too expensive",
    "Splunk alternative", "Datadog security alternative",
    "leaving our MSSP", "switching MSSPs", "MSSP isn't responsive",
    "our MSP dropped the ball", "MSP missed the breach",
    "alternative to Norton", "alternative to McAfee",
    "alternative to Splunk", "alternative to Rapid7",
    "better than CrowdStrike", "better than SentinelOne",

    # ── RECOMMENDATION REQUESTS ──────────────────────────────────────────────
    "recommend a SIEM", "recommend an EDR", "recommend an MDR",
    "recommend a firewall", "recommend a security vendor",
    "recommend a pentest firm", "recommend a security consultant",
    "anyone used", "has anyone used", "does anyone recommend",
    "what EDR do you use", "what SIEM do you use",
    "which security tool is best", "best EDR for small business",
    "best SIEM for startups", "best MDR provider",
    "best pentest company", "looking for a security vendor",
    "looking for a pentest firm", "looking for an MSSP",
    "need a security assessment", "need a vulnerability assessment",
    "need a penetration test", "need a pen test", "need a red team",
    "who should we hire for security", "who do you use for security",

    # ── COMPLIANCE PAIN ───────────────────────────────────────────────────────
    "SOC 2 audit failed", "failed SOC 2", "SOC 2 readiness",
    "need SOC 2 compliance", "preparing for SOC 2",
    "ISO 27001 certification", "need ISO 27001", "ISO 27001 audit",
    "PCI DSS compliance", "failed PCI audit", "PCI compliance issue",
    "HIPAA violation", "HIPAA compliance issue", "HIPAA audit",
    "GDPR fine", "GDPR violation", "GDPR compliance issue",
    "CMMC compliance", "CMMC certification", "NIST compliance",
    "NIST framework", "failed audit", "audit findings",
    "compliance deadline", "compliance gap", "compliance nightmare",
    "regulators are asking", "auditor flagged", "auditors flagged",

    # ── URGENCY SIGNALS ──────────────────────────────────────────────────────
    "urgently need", "critical vulnerability", "emergency patch",
    "patch immediately", "exploit in the wild", "actively exploited",
    "ASAP security", "need help immediately", "time sensitive breach",
    "board is asking questions", "customers are asking questions",
    "losing customers over breach", "losing the contract over security",
    "insurance requires", "cyber insurance requirement",
    "cyber insurance denied claim", "can't get cyber insurance",
    "insurance premium went up after breach",

    # ── BUSINESS EXPANSION / GROWTH ──────────────────────────────────────────
    "building our security program", "starting a security program",
    "hiring our first security hire", "hiring a CISO",
    "scaling our security team", "growing security team",
    "new compliance requirement", "new client requires SOC 2",
    "client requiring security review", "vendor security questionnaire",
    "security questionnaire from client", "need to pass security review",

    # ── JOB SIGNALS ───────────────────────────────────────────────────────────
    "CISO", "chief information security officer", "security engineer",
    "security analyst", "SOC analyst", "SOC manager",
    "head of security", "director of security", "VP security",
    "security operations manager", "threat intel analyst",
    "incident response manager", "GRC manager", "GRC analyst",
    "penetration tester", "red team lead", "blue team lead",
    "application security engineer", "cloud security engineer",
    "detection engineer", "security architect",
    
    # ----- CRM ----------
    
    # ── CRM PAIN POINTS ───────────────────────────────────────────────────────
    "our CRM is a mess", "CRM is too complicated", "CRM too complex",
    "CRM is clunky", "clunky CRM", "outdated CRM", "CRM feels outdated",
    "hate our CRM", "CRM is a nightmare", "CRM nightmare",
    "CRM isn't working for us", "CRM not working for our team",
    "outgrown our CRM", "outgrown our current CRM",
    "CRM doesn't scale", "CRM can't handle our volume",
    "CRM is too slow", "CRM keeps crashing", "CRM keeps freezing",
    "CRM data is a mess", "messy CRM data", "duplicate contacts CRM",
    "duplicate leads CRM", "CRM data quality issues",
    "no one updates the CRM", "reps don't update the CRM",
    "sales team hates the CRM", "sales team won't use the CRM",
    "low CRM adoption", "poor CRM adoption", "CRM adoption problem",
    "manual data entry CRM", "too much manual data entry",
    "spreadsheets instead of CRM", "still using spreadsheets for sales",
    "tracking leads in spreadsheets", "tracking deals in spreadsheets",
    "no visibility into pipeline", "no pipeline visibility",
    "can't see our pipeline", "pipeline is a black box",
    "forecasting is a guess", "sales forecasting is inaccurate",
    "inaccurate sales forecast", "forecast doesn't match reality",
    "reports take forever", "building reports manually",
    "CRM reporting is limited", "CRM reporting is weak",

    # ── SETUP / IMPLEMENTATION FRUSTRATION ───────────────────────────────────
    "CRM implementation nightmare", "CRM implementation failed",
    "CRM setup took months", "CRM migration nightmare",
    "migrating off our CRM", "migrating from our CRM",
    "CRM onboarding took forever", "took too long to set up CRM",
    "CRM customization is hard", "hard to customize our CRM",
    "need a developer to change anything", "too technical for our team",
    "CRM requires an admin", "need a dedicated CRM admin",
    "consultants to set up CRM", "paying consultants for CRM",

    # ── FEE / COST FRUSTRATION ───────────────────────────────────────────────
    "CRM is too expensive", "CRM pricing too high",
    "CRM cost too much", "per seat pricing CRM", "per user pricing CRM",
    "CRM licensing costs", "CRM renewal price increase",
    "CRM price hike", "surprise CRM fees", "hidden fees CRM",
    "add-ons cost extra CRM", "everything is an add-on",
    "paying for features we don't use", "paying for unused seats",
    "cheaper alternative to Salesforce", "cheaper than Salesforce",
    "cheaper CRM", "affordable CRM for small business",
    "budget-friendly CRM", "CRM on a budget", "best value CRM",
    "CRM ROI", "not seeing ROI from our CRM",

    # ── COMPETITOR / SALESFORCE MENTIONS ─────────────────────────────────────
    "Salesforce is too complex", "Salesforce too complicated",
    "Salesforce is overkill", "Salesforce overkill for small business",
    "Salesforce too expensive", "Salesforce pricing",
    "Salesforce alternative", "alternative to Salesforce",
    "leaving Salesforce", "switching from Salesforce",
    "migrating from Salesforce", "migrating off Salesforce",
    "moving away from Salesforce", "Salesforce is a pain",
    "Salesforce admin nightmare", "need a Salesforce admin",
    "HubSpot alternative", "alternative to HubSpot",
    "switching from HubSpot", "leaving HubSpot", "HubSpot too expensive",
    "HubSpot pricing", "HubSpot limitations",
    "Zoho CRM problem", "Zoho CRM issue", "switching from Zoho",
    "Pipedrive limitations", "Pipedrive alternative", "switching from Pipedrive",
    "Monday CRM problem", "Monday sales CRM issue",
    "Copper CRM problem", "Close CRM alternative",
    "Freshsales problem", "Freshsales alternative",
    "Insightly problem", "Insightly alternative",
    "Nimble CRM problem", "SugarCRM problem",
    "Microsoft Dynamics alternative", "Dynamics 365 too complex",
    "alternative to HubSpot", "alternative to Pipedrive",
    "alternative to Zoho", "alternative to Monday CRM",
    "better than Salesforce", "better than HubSpot",
    "better than Pipedrive", "competitors to Salesforce",
    "Salesforce competitors", "HubSpot competitors",

    # ── RECOMMENDATION REQUESTS ──────────────────────────────────────────────
    "recommend a CRM", "recommend a sales tool", "recommend a pipeline tool",
    "recommend a sales platform", "anyone recommend a CRM",
    "can anyone recommend a CRM", "does anyone recommend a CRM",
    "what CRM do you use", "what CRM should I use",
    "which CRM is best", "which CRM should we use",
    "best CRM for small business", "best CRM for startups",
    "best CRM for sales teams", "best CRM for agencies",
    "best CRM for real estate", "best CRM for solo founders",
    "best sales pipeline tool", "best pipeline management tool",
    "best sales tracking tool", "best lead tracking tool",
    "looking for a CRM", "looking for a sales tool",
    "looking for a pipeline tool", "searching for a CRM",
    "need a CRM", "need a sales tool", "need a pipeline tool",
    "need a better CRM", "need a simple CRM", "need an easy CRM",
    "anyone using a CRM", "does anyone use", "has anyone used",
    "who uses", "what do you use for sales", "what are you using for CRM",
    "tried several CRMs", "tried multiple CRMs", "tried everything CRM",
    "still looking for a CRM", "still haven't found a CRM",

    # ── SALES TOOLS / PIPELINE ────────────────────────────────────────────────
    "sales pipeline management", "pipeline management tool",
    "sales pipeline tracking", "deal tracking tool",
    "lead tracking software", "lead management tool",
    "lead scoring tool", "sales automation tool",
    "sales engagement platform", "sales enablement tool",
    "outbound sales tool", "cold outreach tool", "cold email tool",
    "sales prospecting tool", "prospecting software",
    "sales sequence tool", "email sequencing tool",
    "sales dialer", "auto dialer sales", "call tracking sales",
    "quote to cash", "proposal software sales", "contract management sales",
    "sales forecasting tool", "revenue operations tool",
    "RevOps tool", "sales analytics tool", "sales dashboard tool",
    "deal desk tool", "sales stack", "sales tech stack",
    "building our sales stack", "sales tools we use",
]


def passes_keyword_filter(text: str):
    """
    v7.8.0 CHANGE — now returns the MATCHED KEYWORD STRING (the exact
    KEYWORDS list entry that matched) instead of a plain True/False.
    Returns None if nothing matched.

    This is a backward-compatible change: every existing call site used
    this in a boolean context (`if not passes_keyword_filter(text):` /
    `if passes_keyword_filter(text):`), and a non-empty string is truthy
    while None is falsy — so all existing "matched / not matched" branch
    logic behaves exactly as before. The ONLY thing that changes is that
    callers can now also inspect WHICH keyword matched, if they choose to
    (used by run_batch_processor below to populate search_keyword for
    Reddit/Twitter/Telegram, the three platforms that don't already know
    their matching keyword the way Facebook/LinkedIn's per-keyword search
    loops do).
    """
    t = text.lower()
    for kw in KEYWORDS:
        if kw.lower() in t:
            return kw
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY — CHUNKED, ONE CHUNK PER POLL CYCLE
# v7.9.1 CHANGE (fixes HTTP 414 Request-URI Too Large) — v7.9.0 combined
# ALL keywords into ONE giant OR-query, producing URLs 100,000+ chars long
# that RapidAPI's gateway rejected with 414 every single cycle.
#
# Fix: KEYWORDS is split into chunks of TWITTER_CHUNK_SIZE (default 25)
# keywords each — same idea as Facebook/LinkedIn's per-keyword loop, just
# grouped 25-at-a-time instead of 1-at-a-time (Twitter's plan/rate-limits
# don't tolerate 2000+ separate requests per cycle the way Facebook/
# LinkedIn's setup does).
#
# UNLIKE Facebook/LinkedIn (which cycle through their ENTIRE list every
# poll cycle), Twitter sends ONE chunk per TWITTER_POLL_INTERVAL, then
# advances to the next chunk next cycle, and so on — wrapping back to
# chunk #1 once the last chunk is reached. So if there are 1000 keywords
# in 40 chunks of 25, it takes 40 poll cycles to cover the full list once,
# then it starts over from chunk #1 automatically. This keeps each
# individual request small (no 414) AND keeps Twitter's request rate low
# and steady. Reddit/Telegram/Facebook/LinkedIn are completely unaffected
# — their per-keyword/per-subreddit cycling behaviour is untouched.
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_CHUNK_SIZE = int(os.getenv("TWITTER_CHUNK_SIZE", "25"))


def _build_twitter_search_chunks() -> list:
    seen = set()
    unique_kws = []
    for kw in KEYWORDS:
        kl = kw.lower()
        if kl not in seen:
            seen.add(kl)
            unique_kws.append(kw)

    if not unique_kws:
        return [
            "(\"international transfer\" OR \"supplier payment\" OR \"bank blocked\""
            " OR \"Wise blocked\" OR \"cross border payment\") -is:retweet lang:en"
        ]

    chunks = []
    for i in range(0, len(unique_kws), TWITTER_CHUNK_SIZE):
        group = unique_kws[i:i + TWITTER_CHUNK_SIZE]
        parts = [f'"{kw}"' if " " in kw else kw for kw in group]
        query = "(" + " OR ".join(parts) + ") -is:retweet lang:en"
        chunks.append(query)

    log.info(
        f"Twitter search chunks built | total_keywords:{len(unique_kws)} | "
        f"chunk_size:{TWITTER_CHUNK_SIZE} | total_chunks:{len(chunks)} | "
        f"full_list_coverage_every:{len(chunks)} poll cycles "
        f"(~{len(chunks) * TWITTER_POLL_INTERVAL}s)"
    )
    return chunks


TWITTER_SEARCH_CHUNKS = _build_twitter_search_chunks()


# ─────────────────────────────────────────────────────────────────────────────
# DERIVE FIELDS LOCALLY (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _derive_fields(score: int) -> dict:
    if score >= 8:
        return {"signal_category": "high_intent", "tier": "immediate", "hubspot_priority": "high"}
    elif score >= 4:
        return {"signal_category": "mid_intent", "tier": "digest", "hubspot_priority": "medium"}
    elif score >= 3:
        return {"signal_category": "mid_intent", "tier": "watchlist", "hubspot_priority": "low"}
    else:
        return {"signal_category": "discard", "tier": "discard", "hubspot_priority": "skip"}


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SYSTEM PROMPTS — PLATFORM-SPECIFIC SCHEMAS
# Reddit/Twitter/Telegram/Facebook prompts byte-for-byte identical to
# v7.5.0. CLAUDE_SYSTEM_PROMPT_LINKEDIN is new (v7.6.0), same _SCORING_CORE.
# ─────────────────────────────────────────────────────────────────────────────

_SCORING_CORE = """
You are Flintel's AI signal intelligence analyst.

Your only job is to read a public social media post and determine
whether the person or company posting needs a cross-border B2B
payment solution right now.

You work exclusively for Settla — a premium B2B cross-border
payment company helping diaspora business owners move large amounts
internationally for supplier and trade payments.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHO SETTLA SERVES — KNOW THIS PERFECTLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Settla's ideal customer is a diaspora business owner who:

— Runs an import/export business, trading company, or has
  overseas suppliers and partners
— Needs to move $10,000 to $500,000 CAD/GBP/USD regularly
  for business payments — NOT personal remittances
— Is frustrated with banks blocking large international transfers
— Has been burned by consumer apps like Wise that restrict
  business volumes
— Is actively looking for a better cross-border payment solution
— Operates across these corridors:

PRIMARY:
Canada → Nigeria
UK → Nigeria
USA → Nigeria

SECONDARY:
Canada → Pakistan
UK → Pakistan
Canada → India
UK → India
Canada → Ghana
UK → Ghana
Australia → Asia
UK → Africa
UAE → Nigeria
UAE → Pakistan

Settla is NOT for:
— Individuals sending small personal remittances under $2,000
— People sending money to family for living expenses
— Consumers comparing holiday money rates
— Retail crypto traders
— US domestic banking problems with no international context
— E-commerce merchants looking for payment gateways
— Research chemical or high risk merchant categories

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL SCORING RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If a post contains NO international payment context —
score maximum 4 regardless of anything else.

International context means at least ONE of:
— Cross border payment or transfer mentioned
— International supplier or vendor mentioned
— Specific corridor mentioned — Nigeria, Pakistan, Ghana etc
— International clients or partners mentioned
— Multi currency or FX mentioned
— SWIFT or wire transfer mentioned in business context

Without international context = maximum score 4.
This rule cannot be overridden.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TWO ACCEPTABLE SIGNAL TYPES ONLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HIGH INTENT — Score 7 to 10:
Company or contact actively looking to COMPLETE an FX
transaction or international payment immediately.

MID INTENT — Score 4 to 6:
Company or contact actively SHOPPING for a solution.

DISCARD — Score 0 to 3:
NOT ACCEPTABLE. Never delivered to Settla team.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTOMATIC SCORE MODIFIERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADD +1 to score when:
+ Business owner confirmed in bio or post
+ Specific large amount mentioned — $10,000 or more
+ Multiple pain points in same post
+ Competitor mentioned negatively
+ Urgency words present — today, ASAP, urgent, this week
+ Active payment block or failure described
+ Supplier relationship at risk
+ Multiple international clients mentioned
+ Actively building payment partnerships

SUBTRACT 1 from score when:
- Small personal amount under $2,000
- Sending to family for personal expenses
- Anonymous account with no business bio
- Issue is now resolved
- Post is older than 7 days
- No specific payment amount mentioned
- General commentary not personal experience

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPETITOR INTELLIGENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If a competitor is mentioned negatively — score UP by 1.

Competitors to detect:
Wise / TransferWise / Wise Business
Remitly / Remitly Business
WorldRemit / WorldRemit Business
Western Union / MoneyGram
Payoneer
OFX / XE Money
Revolut / Revolut Business
LemFi / Grey Finance / NALA
Chipper Cash / Sendwave
TD Bank / RBC / HSBC / Barclays / Lloyds

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTREACH SCRIPT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Write outreach scripts for scores 4 and above ONLY.
Score 1 to 3 — DO NOT output any outreach fields at all.

OUTREACH RULES — NON NEGOTIABLE:
— Never start with I
— Never say I hope this message finds you well
— Never pitch features — pitch the outcome they want
— Always reference something specific they said
— Always end with one question or soft statement
— Maximum 3 sentences total per script
— Sound like a founder talking to another founder

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FINAL REMINDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You are identifying the exact moment a diaspora
business owner is ready to switch payment providers
or complete a large international transaction.

Be ruthless with noise.
Be generous with genuine international payment pain.
Be precise with every score.

Return JSON array only. Always. Every single time.
MINIMUM score is 1 — never return 0.
"""

CLAUDE_SYSTEM_PROMPT_REDDIT = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — REDDIT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
For scores 1-3: omit linkedin_message entirely — do NOT output the key.
For scores 4-10: include linkedin_message.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<source country to destination or null>",
    "estimated_amount": "<specific amount if mentioned or null>",
    "competitor_mentioned": "<competitor name or null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "linkedin_message": "<public reply to their Reddit post, max 3 sentences — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

CLAUDE_SYSTEM_PROMPT_TWITTER = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — TWITTER/X
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
For scores 1-3: omit twitter_reply and twitter_dm entirely — do NOT output those keys.
For scores 4-10: include both twitter_reply and twitter_dm.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<source country to destination or null>",
    "estimated_amount": "<specific amount if mentioned or null>",
    "competitor_mentioned": "<competitor name or null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "twitter_reply": "<2-sentence public reply to their tweet — OMIT KEY IF SCORE 1-3>",
    "twitter_dm": "<3-sentence private DM — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

CLAUDE_SYSTEM_PROMPT_TELEGRAM = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — TELEGRAM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
Telegram messages are from private groups — no public reply is possible.
Outreach is via DM only if the sender has a visible username.
For scores 1-3: omit telegram_dm entirely — do NOT output the key.
For scores 4-10: include telegram_dm.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<source country to destination or null>",
    "estimated_amount": "<specific amount if mentioned or null>",
    "competitor_mentioned": "<competitor name or null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "telegram_dm": "<3-sentence DM if username visible, else null — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

CLAUDE_SYSTEM_PROMPT_FACEBOOK = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — FACEBOOK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
Facebook posts are public — a public comment reply is possible.
For scores 1-3: omit facebook_comment entirely — do NOT output the key.
For scores 4-10: include facebook_comment.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<source country to destination or null>",
    "estimated_amount": "<specific amount if mentioned or null>",
    "competitor_mentioned": "<competitor name or null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "facebook_comment": "<public reply to their Facebook post, max 3 sentences — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

# v7.6.0 NEW — LinkedIn scoring schema. Same _SCORING_CORE, same
# thresholds/routing. Adds linkedin_reply (public comment) and
# linkedin_dm (connection/DM message) — deliberately NOT reusing the
# pre-existing "linkedin_message" key (that key already belongs to the
# REDDIT schema above, where it means "a LinkedIn-style outreach message
# posted as a reply to a Reddit thread" — an unrelated, older field).
CLAUDE_SYSTEM_PROMPT_LINKEDIN = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — LINKEDIN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
LinkedIn profiles/posts are public — a public comment reply and a
connection-request-style DM are both possible.
Each message may include an "Enrichment:" line (job title, company,
location, industry, company size) pulled from LinkedIn's User Data and
Company Data endpoints — use it to judge business context and size, but
it is optional and may be missing for some messages.
For scores 1-3: omit linkedin_reply and linkedin_dm entirely — do NOT output those keys.
For scores 4-10: include both linkedin_reply and linkedin_dm.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<source country to destination or null>",
    "estimated_amount": "<specific amount if mentioned or null>",
    "competitor_mentioned": "<competitor name or null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "linkedin_reply": "<public comment on their LinkedIn post/profile, max 3 sentences — OMIT KEY IF SCORE 1-3>",
    "linkedin_dm": "<3-sentence connection-request-style DM — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.signals.create_index(
            [("message_id", ASCENDING)], unique=True, name="message_id_unique"
        )
        for field in [
            "intent_score", "created_at", "client_id", "platform",
            "tier", "corridor", "competitor_mentioned", "pain_type",
            "is_business", "signal_category",
            "search_keyword",  # v7.7.0 NEW — additive only
        ]:
            db.signals.create_index([(field, ASCENDING)])

        db.flintel_state.create_index(
            [("key", ASCENDING)], unique=True, name="state_key_unique"
        )

        db.flintel_pending_batch.create_index(
            [("platform", ASCENDING)], unique=True, name="platform_unique"
        )
        db.flintel_seen_ids.create_index(
            [("platform", ASCENDING)], unique=True, name="seen_platform_unique"
        )

        db.flintel_rescore_messages.create_index(
            [("status", ASCENDING), ("requested_at", ASCENDING)],
            name="rescore_status_time",
        )
        db.flintel_rescore_messages.create_index(
            [("message_id", ASCENDING)],
            name="rescore_message_id",
        )

        db.flintel_queue_messages.create_index(
            [("_platform_key", ASCENDING), ("message_id", ASCENDING)],
            unique=True, name="queue_platform_message_unique",
        )

        db.flintel_batch_seconds.create_index(
            [("platform", ASCENDING)], unique=True, name="batch_seconds_platform_unique"
        )

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT — FIX C: uses streaming for all Claude calls
# ─────────────────────────────────────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(
    api_key=ANTHROPIC_API_KEY,
    http_client=httpx.Client(
        timeout=httpx.Timeout(
            connect=30.0,
            read=None,
            write=60.0,
            pool=30.0,
        )
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
# RETRY WITH EXPONENTIAL BACKOFF (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# OPERATOR SLACK ALERT (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def send_operator_alert(title: str, detail: str, level: str = "ERROR"):
    if not SLACK_WEBHOOK_URL:
        log.warning(f"[OPERATOR ALERT] {title} — {detail} (Slack not configured)")
        return
    try:
        emoji = "🔴" if level == "CRITICAL" else "🟡"
        payload = {
            "text": f"{emoji} [OPERATOR ALERT] {title}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} FLINTEL OPERATOR ALERT — {level}",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*System*\nFLINTEL v7.9.1"},
                        {"type": "mrkdwn", "text": f"*Client*\n{CLIENT_ID}"},
                        {"type": "mrkdwn", "text": f"*Alert*\n{title}"},
                        {"type": "mrkdwn", "text": f"*Time*\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Detail*\n```{detail[:1500]}```"},
                },
                {"type": "divider"},
            ],
        }
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        log.info(f"Operator alert sent to Slack: {title}")
    except Exception as exc:
        log.error(f"Failed to send operator alert: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# FIX A — PERSISTENT BATCH STATE HELPERS (unchanged)
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
            log.warning(
                f"[{platform.upper()}] Resuming persisted batch from MongoDB | "
                f"{len(items)} item(s) recovered from before restart."
            )
        return items, start_time
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_pending_batch error: {exc} — starting with empty batch.")
        return [], None


def save_pending_batch(platform: str, items: list, batch_start_time):
    try:
        start_dt = (
            datetime.fromtimestamp(batch_start_time, tz=timezone.utc)
            if batch_start_time is not None else None
        )
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "items": items,
                "batch_start_time": start_dt,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_pending_batch error: {exc}")


def clear_pending_batch(platform: str):
    try:
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "items": [],
                "batch_start_time": None,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_pending_batch error: {exc}")


def load_seen_ids(platform: str) -> set:
    try:
        doc = db.flintel_seen_ids.find_one({"platform": platform})
        if not doc:
            return set()
        return set(doc.get("ids", []))
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_seen_ids error: {exc} — starting with empty dedup set.")
        return set()


def save_seen_ids(platform: str, ids: set, cap: int = 200_000):
    try:
        id_list = list(ids)
        if len(id_list) > cap:
            id_list = id_list[-cap:]
        db.flintel_seen_ids.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "ids": id_list,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_seen_ids error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# flintel_queue_messages: persistent raw-queue storage (unchanged, v7.4.5)
# ─────────────────────────────────────────────────────────────────────────────

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
            {"_platform_key": platform, "message_id": mid},
            {"$set": doc},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_queue_message error: {exc}")


def remove_queue_message(platform: str, message_id: str):
    if not message_id:
        return
    try:
        db.flintel_queue_messages.delete_one(
            {"_platform_key": platform, "message_id": message_id}
        )
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
        log.error(f"[{platform.upper()}] load_queue_messages error: {exc} — starting with empty queue.")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# flintel_batch_seconds: explicit batch-timeout persistence (unchanged, v7.4.5)
# ─────────────────────────────────────────────────────────────────────────────

def save_batch_seconds(platform: str, batch_start_time):
    try:
        start_dt = (
            datetime.fromtimestamp(batch_start_time, tz=timezone.utc)
            if batch_start_time is not None else None
        )
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "batch_start_time": start_dt,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_batch_seconds error: {exc}")


def clear_batch_seconds(platform: str):
    try:
        db.flintel_batch_seconds.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "batch_start_time": None,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_batch_seconds error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        ctype     = item.get("content_type", "unknown").upper()
        platform  = item.get("platform", "unknown").upper()
        subreddit = item.get("subreddit", "")
        group     = item.get("telegram_group", "")
        username  = item.get("username", "unknown")
        text      = item.get("text", "")[:800]

        if subreddit:
            location = f"r/{subreddit}"
        elif group:
            location = f"tg/{group}"
        else:
            location = platform

        # v7.6.0 — LINKEDIN ADDITIVE ONLY: append whatever enrichment data
        # was fetched from the User Data / Company Data endpoints. This
        # block only ever activates when item["platform"] == "linkedin" —
        # every other platform's prompt text is byte-for-byte unchanged.
        extra = ""
        if item.get("platform") == "linkedin":
            extra_bits = []
            if item.get("linkedin_job_title"):
                extra_bits.append(f"Title: {item['linkedin_job_title']}")
            if item.get("linkedin_company"):
                extra_bits.append(f"Company: {item['linkedin_company']}")
            if item.get("linkedin_location"):
                extra_bits.append(f"Location: {item['linkedin_location']}")
            if item.get("linkedin_company_industry"):
                extra_bits.append(f"Industry: {item['linkedin_company_industry']}")
            if item.get("linkedin_company_size"):
                extra_bits.append(f"Company Size: {item['linkedin_company_size']}")
            if extra_bits:
                extra = "\nEnrichment: " + " | ".join(extra_bits)

        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Platform: {platform} | Source: {location} | Type: {ctype} | User: {username}\n"
            f"Content: {text}{extra}\n"
        )
    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Scoring unavailable.") -> dict:
    derived = _derive_fields(1)
    return {
        "index":                        index,
        "intent_score":                 1,
        "signal_category":              derived["signal_category"],
        "tier":                         derived["tier"],
        "hubspot_priority":             derived["hubspot_priority"],
        "is_business":                  False,
        "business_size":                "unknown",
        "has_international_context":    False,
        "corridor":                     None,
        "estimated_amount":             None,
        "competitor_mentioned":         None,
        "competitor_outreach_detected": False,
        "pain_type":                    None,
        "urgency":                      "none",
        "reason":                       reason,
        "suggested_action":             "Check system logs.",
        "twitter_reply":                None,
        "twitter_dm":                   None,
        "linkedin_message":             None,
        "telegram_dm":                  None,
        "facebook_comment":             None,
        # v7.6.0 NEW
        "linkedin_reply":               None,
        "linkedin_dm":                  None,
        "watchlist":                    False,
        "watchlist_reason":             None,
    }


def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    start = raw.find("[")
    if start == -1:
        return []

    objects = []
    depth = 0
    obj_start = None
    in_string = False
    escape = False

    i = start + 1
    n = len(raw)
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
                    log.warning("[Claude-Batch] Skipped one malformed salvaged object during recovery.")
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
        log.warning(
            f"[Claude-Batch] Full JSON parse failed ({exc}) — "
            f"attempting partial recovery from truncated response."
        )
        salvaged = _salvage_partial_json_array(cleaned)
        return salvaged, True


def _call_claude_batch(batch: list) -> list:
    platform = batch[0].get("platform", "reddit") if batch else "reddit"

    system_prompt = {
        "twitter":  CLAUDE_SYSTEM_PROMPT_TWITTER,
        "telegram": CLAUDE_SYSTEM_PROMPT_TELEGRAM,
        "facebook": CLAUDE_SYSTEM_PROMPT_FACEBOOK,
        # v7.6.0 NEW
        "linkedin": CLAUDE_SYSTEM_PROMPT_LINKEDIN,
    }.get(platform, CLAUDE_SYSTEM_PROMPT_REDDIT)

    prompt = _build_batch_prompt(batch)

    with anthropic_client.messages.stream(
        model      = "claude-sonnet-4-6",
        max_tokens = MAX_TOKENS,
        system     = system_prompt,
        messages   = [{"role": "user", "content": f"Score this batch:\n\n{prompt}"}],
    ) as stream:
        raw = stream.get_final_text().strip()

    results, was_truncated = _parse_claude_json(raw)

    if was_truncated:
        recovered_indices = {int(r["index"]) for r in results if isinstance(r, dict) and "index" in r}
        all_indices = set(range(1, len(batch) + 1))
        missing_indices = sorted(all_indices - recovered_indices)

        log.warning(
            f"[Claude-Batch] PARTIAL RECOVERY | platform:{platform} | "
            f"batch_size:{len(batch)} | recovered:{len(recovered_indices)} | "
            f"missing (fallback):{len(missing_indices)}"
        )
        send_operator_alert(
            title="Claude Response Truncated (max_tokens) — Partial Recovery",
            detail=(
                f"Platform: {platform}\n"
                f"Batch size: {len(batch)}\n"
                f"Successfully recovered: {len(recovered_indices)} item(s) — scored and delivered normally.\n"
                f"Lost to truncation (fallback score 1 applied): {len(missing_indices)} item(s) — "
                f"indices {missing_indices[:30]}{'...' if len(missing_indices) > 30 else ''}\n\n"
                f"Consider raising MAX_TOKENS (currently {MAX_TOKENS}) or lowering this platform's "
                f"batch size if this recurs."
            ),
            level="ERROR",
        )
        for idx in missing_indices:
            results.append(_fallback_score(idx, "Truncated by max_tokens — not recovered."))

    if not isinstance(results, list):
        raise ValueError("Claude returned non-list after parsing.")

    required = {"index", "intent_score", "is_business", "reason", "suggested_action"}
    optional_defaults = {
        "business_size":                "unknown",
        "has_international_context":    False,
        "corridor":                     None,
        "estimated_amount":             None,
        "competitor_mentioned":         None,
        "competitor_outreach_detected": False,
        "pain_type":                    None,
        "urgency":                      "none",
        "twitter_reply":                None,
        "twitter_dm":                   None,
        "linkedin_message":             None,
        "telegram_dm":                  None,
        "facebook_comment":             None,
        # v7.6.0 NEW
        "linkedin_reply":               None,
        "linkedin_dm":                  None,
        "watchlist":                    False,
    }

    for r in results:
        missing = required - r.keys()
        if missing:
            raise ValueError(f"Missing keys in Claude response: {missing}")
        for k, v in optional_defaults.items():
            r.setdefault(k, v)
        if r.get("intent_score", 1) < 1:
            r["intent_score"] = 1

        score   = r["intent_score"]
        derived = _derive_fields(score)
        r["signal_category"]  = derived["signal_category"]
        r["tier"]             = derived["tier"]
        r["hubspot_priority"] = derived["hubspot_priority"]
        r["watchlist_reason"] = r.get("reason") if r.get("watchlist") else None

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(
        _call_claude_batch, batch,
        retries=3, delay=5, label="Claude-Batch",
    )
    if result is None:
        send_operator_alert(
            title="Claude API Unavailable",
            detail=(
                f"All 3 retry attempts to score a batch of {len(batch)} items failed.\n"
                f"Batch platform: {batch[0].get('platform','unknown') if batch else 'unknown'}\n"
                f"Fallback scores (1) assigned. Check ANTHROPIC_API_KEY and API status."
            ),
            level="CRITICAL",
        )
        return [_fallback_score(i + 1) for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE — ALL scores 1-10 stored, nothing discarded.
# v7.6.0: additive LinkedIn fields only — every .get() defaults to None for
# every other platform's documents, so their stored shape is unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def save_signal(data: dict) -> bool:
    try:
        doc = {
            "message_id":                   data["message_id"],
            "platform":                     data.get("platform", "unknown"),
            "content_type":                 data.get("content_type", "unknown"),
            "subreddit":                    data.get("subreddit", ""),
            "telegram_group":               data.get("telegram_group", ""),
            "post_url":                     data.get("post_url", ""),
            "username":                     data.get("username", "unknown"),
            "message_text":                 data["message_text"],
            "intent_score":                 data["intent_score"],
            "signal_category":              data["signal_category"],
            "tier":                         data.get("tier", "discard"),
            "is_business":                  data.get("is_business", False),
            "business_size":                data.get("business_size", "unknown"),
            "corridor":                     data.get("corridor"),
            "estimated_amount":             data.get("estimated_amount"),
            "competitor_mentioned":         data.get("competitor_mentioned"),
            "competitor_outreach_detected": data.get("competitor_outreach_detected", False),
            "pain_type":                    data.get("pain_type"),
            "urgency":                      data.get("urgency", "none"),
            "reason":                       data["reason"],
            "suggested_action":             data["suggested_action"],
            "twitter_reply":                data.get("twitter_reply"),
            "twitter_dm":                   data.get("twitter_dm"),
            "linkedin_message":             data.get("linkedin_message"),
            "telegram_dm":                  data.get("telegram_dm"),
            "facebook_comment":             data.get("facebook_comment"),
            # v7.6.0 NEW — LinkedIn outreach + enrichment fields. Always
            # None/absent for Reddit/Twitter/Telegram/Facebook documents.
            "linkedin_reply":               data.get("linkedin_reply"),
            "linkedin_dm":                  data.get("linkedin_dm"),
            "linkedin_full_name":           data.get("linkedin_full_name"),
            "linkedin_headline":            data.get("linkedin_headline"),
            "linkedin_email":               data.get("linkedin_email"),
            "linkedin_phone":               data.get("linkedin_phone"),
            "linkedin_location":            data.get("linkedin_location"),
            "linkedin_company":             data.get("linkedin_company"),
            "linkedin_job_title":           data.get("linkedin_job_title"),
            "linkedin_profile_url":         data.get("linkedin_profile_url"),
            "linkedin_company_name":        data.get("linkedin_company_name"),
            "linkedin_company_website":     data.get("linkedin_company_website"),
            "linkedin_company_industry":    data.get("linkedin_company_industry"),
            "linkedin_company_size":        data.get("linkedin_company_size"),
            "linkedin_company_location":    data.get("linkedin_company_location"),
            "linkedin_company_phone":       data.get("linkedin_company_phone"),
            # v7.7.0 NEW — the KEYWORDS entry that produced this item
            # (Facebook/LinkedIn only; None for every other platform).
            "search_keyword":               data.get("search_keyword"),
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "client_id":                    CLIENT_ID,
            "alerted_slack":                False,
            "alerted_hubspot":              False,
            "digest_included":              False,
            "created_at":                   datetime.now(timezone.utc),
        }
        db.signals.insert_one(doc)

        platform = data.get("platform", "?").upper()
        score    = data["intent_score"]
        user     = data.get("username", "?")
        ctype    = data.get("content_type", "")
        sub      = data.get("subreddit", "")
        grp      = data.get("telegram_group", "")
        source   = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)

        log.info(
            f"SAVED [{platform}] | Score:{score} | Tier:{data.get('tier','?')} | "
            f"u/{user} | {ctype} | {source}"
        )
        return True
    except DuplicateKeyError:
        log.debug(f"Duplicate skipped: {data['message_id']}")
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        send_operator_alert(
            title="MongoDB Write Failed",
            detail=(
                f"Failed to save signal to MongoDB.\n"
                f"message_id: {data.get('message_id','unknown')}\n"
                f"platform: {data.get('platform','unknown')}\n"
                f"error: {exc}\n\n"
                f"Check MONGODB_URI and MongoDB Atlas status."
            ),
            level="CRITICAL",
        )
        return False


def update_signal(message_id: str, data: dict) -> bool:
    try:
        update_fields = {
            "intent_score":                 data["intent_score"],
            "signal_category":              data["signal_category"],
            "tier":                         data.get("tier", "discard"),
            "is_business":                  data.get("is_business", False),
            "business_size":                data.get("business_size", "unknown"),
            "corridor":                     data.get("corridor"),
            "estimated_amount":             data.get("estimated_amount"),
            "competitor_mentioned":         data.get("competitor_mentioned"),
            "competitor_outreach_detected": data.get("competitor_outreach_detected", False),
            "pain_type":                    data.get("pain_type"),
            "urgency":                      data.get("urgency", "none"),
            "reason":                       data["reason"],
            "suggested_action":             data["suggested_action"],
            "twitter_reply":                data.get("twitter_reply"),
            "twitter_dm":                   data.get("twitter_dm"),
            "linkedin_message":             data.get("linkedin_message"),
            "telegram_dm":                  data.get("telegram_dm"),
            "facebook_comment":             data.get("facebook_comment"),
            # v7.6.0 NEW
            "linkedin_reply":               data.get("linkedin_reply"),
            "linkedin_dm":                  data.get("linkedin_dm"),
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "rescored_at":                  datetime.now(timezone.utc),
            "alerted_slack":                False,
            "alerted_hubspot":              False,
        }
        result = db.signals.update_one(
            {"message_id": message_id},
            {"$set": update_fields},
        )
        if result.matched_count == 0:
            log.warning(f"[RESCORE] update_signal: no document found for message_id={message_id}")
            return False
        log.info(
            f"[RESCORE] UPDATED | message_id:{message_id} | "
            f"Score:{data['intent_score']} | Tier:{data.get('tier','?')}"
        )
        return True
    except Exception as exc:
        log.error(f"[RESCORE] update_signal error: {exc}")
        return False


def mark_slack_alerted(message_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {"alerted_slack": True, "alerted_slack_at": datetime.now(timezone.utc)}},
        )
    except Exception as exc:
        log.error(f"mark_slack_alerted error: {exc}")


def mark_hubspot_alerted(message_id: str, contact_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {
                "alerted_hubspot": True,
                "hubspot_contact_id": contact_id,
                "alerted_hubspot_at": datetime.now(timezone.utc),
            }},
        )
    except Exception as exc:
        log.error(f"mark_hubspot_alerted error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# WEEKLY REPORT STATE PERSISTENCE (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _get_state(key: str):
    try:
        doc = db.flintel_state.find_one({"key": key})
        return doc["value"] if doc else None
    except Exception as exc:
        log.error(f"get_state error for key={key}: {exc}")
        return None


def _set_state(key: str, value):
    try:
        db.flintel_state.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"set_state error for key={key}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SLACK DELIVERY
# v7.6.0: outreach lookup additionally checks linkedin_reply/linkedin_dm.
# Everything else in this function is unchanged from v7.5.0.
# ─────────────────────────────────────────────────────────────────────────────

def _safe(text: str, limit: int = 2900) -> str:
    if not text:
        return "—"
    return text[:limit] + ("…" if len(text) > limit else "")


def _post_to_slack(payload: dict):
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if r.status_code != 200:
        raise Exception(f"Slack {r.status_code}: {r.text}")
    return r


def send_slack_alert(data: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping.")
        return False

    score       = data["intent_score"]
    platform    = data.get("platform", "unknown").upper()
    ctype       = data.get("content_type", "post").upper()
    subreddit   = data.get("subreddit", "")
    tg_group    = data.get("telegram_group", "")
    post_url    = data.get("post_url", "")
    username    = data.get("username", "unknown")
    tier        = data.get("tier", "").upper()
    category    = data.get("signal_category", "").replace("_", " ").upper()
    is_biz      = data.get("is_business", False)
    corridor    = data.get("corridor") or "Unknown"
    amount      = data.get("estimated_amount") or "—"
    pain        = data.get("pain_type") or "—"
    competitor  = data.get("competitor_mentioned") or "—"
    urgency     = data.get("urgency", "none").upper()
    timestamp   = data.get("timestamp", "—")
    is_rescore  = data.get("is_rescore", False)

    if score >= 9:
        urgency_tag = "⚡ RESPOND WITHIN 30 MINUTES"
    elif score >= 7:
        urgency_tag = "⏰ RESPOND WITHIN 2 HOURS"
    elif score >= 5:
        urgency_tag = "📋 ADD TO TODAY'S OUTREACH LIST"
    else:
        urgency_tag = ""

    outreach = (
        data.get("twitter_reply") or
        data.get("twitter_dm") or
        data.get("telegram_dm") or
        data.get("linkedin_message") or
        data.get("facebook_comment") or
        data.get("linkedin_reply") or   # v7.6.0 NEW
        data.get("linkedin_dm") or      # v7.6.0 NEW
        ""
    )

    rescore_tag = " ♻️ RESCORED" if is_rescore else ""
    header_emoji = "🚨" if score >= 8 else "⚠️"
    header_text  = f"{header_emoji} {category} — {tier}{rescore_tag}"

    if subreddit:
        source_label = f"r/{subreddit}"
    elif tg_group:
        source_label = f"tg/{tg_group}"
    else:
        source_label = platform

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text[:150], "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Platform*\n{platform}"},
                {"type": "mrkdwn", "text": f"*Source*\n{source_label}"},
                {"type": "mrkdwn", "text": f"*Content Type*\n{ctype}"},
                {"type": "mrkdwn", "text": f"*User*\n{username}"},
                {"type": "mrkdwn", "text": f"*Tier*\n{tier}"},
                {"type": "mrkdwn", "text": f"*Profile*\n{'✅ Business' if is_biz else '👤 Individual'}"},
                {"type": "mrkdwn", "text": f"*Timestamp*\n{timestamp}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Corridor*\n{corridor}"},
                {"type": "mrkdwn", "text": f"*Estimated Amount*\n{amount}"},
                {"type": "mrkdwn", "text": f"*Pain Type*\n{pain}"},
                {"type": "mrkdwn", "text": f"*Competitor*\n{competitor}"},
                {"type": "mrkdwn", "text": f"*Urgency*\n{urgency}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Message*\n>{_safe(data['message_text'], 400)}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Reason*\n{_safe(data['reason'], 300)}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Recommended Action*\n🎯 {_safe(data['suggested_action'], 300)}"},
        },
    ]

    # v7.6.0 NEW — surface LinkedIn enrichment (company/location/title) in
    # Slack when present. Purely additive block: only appended when a
    # linkedin_company/linkedin_location/linkedin_job_title field exists
    # on the signal, which only ever happens for platform == "linkedin".
    li_bits = []
    if data.get("linkedin_job_title"):
        li_bits.append(f"*Title*\n{data['linkedin_job_title']}")
    if data.get("linkedin_company"):
        li_bits.append(f"*Company*\n{data['linkedin_company']}")
    if data.get("linkedin_location"):
        li_bits.append(f"*Location*\n{data['linkedin_location']}")
    if data.get("linkedin_email"):
        li_bits.append(f"*Email*\n{data['linkedin_email']}")
    if data.get("linkedin_phone"):
        li_bits.append(f"*Phone*\n{data['linkedin_phone']}")
    if li_bits:
        blocks.append({
            "type": "section",
            "fields": [{"type": "mrkdwn", "text": b} for b in li_bits[:10]],
        })

    if urgency_tag:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Response Window*\n{urgency_tag}"},
        })

    if outreach:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Outreach Script*\n💬 {_safe(outreach, 600)}"},
        })

    if post_url:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "View Original →"},
                "url": post_url,
                "style": "primary",
            }],
        })

    blocks.append({"type": "divider"})

    result = retry_with_backoff(
        _post_to_slack, {"text": header_text, "blocks": blocks},
        retries=3, delay=2, label="Slack",
    )
    if result:
        log.info(f"Slack sent | {platform} | u/{username} | Score:{score}")
        return True
    log.error("Slack delivery failed after all retries.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# HUBSPOT CRM
# v7.6.0: note body additionally includes LinkedIn outreach + enrichment
# lines when present. Everything else unchanged.
# ─────────────────────────────────────────────────────────────────────────────

HUBSPOT_BASE = "https://api.hubapi.com"

_HUBSPOT_REQUIRED_CONTACT_PROPERTIES = [
    "fx_intent_score",
    "fx_signal_category",
    "fx_tier",
    "fx_corridor",
    "fx_pain_type",
    "fx_competitor",
    "fx_platform",
    "fx_source_community",
    "fx_signal_reason",
    "fx_suggested_action",
]


def _hs_headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}


def _hs_log_http_error(label: str, exc: "requests.exceptions.HTTPError"):
    body_text = None
    try:
        if exc.response is not None:
            body_text = exc.response.text
    except Exception:
        body_text = None

    if body_text:
        log.error(f"{label}: {exc} | HubSpot response body: {body_text[:2000]}")
    else:
        log.error(f"{label}: {exc} | (no response body available)")


def _hs_verify_properties():
    if not HUBSPOT_API_KEY:
        log.info("[HubSpot] HUBSPOT_API_KEY not set — skipping property self-check.")
        return

    try:
        r = requests.get(
            f"{HUBSPOT_BASE}/crm/v3/properties/contacts",
            headers=_hs_headers(),
            timeout=10,
        )
        r.raise_for_status()
        existing = {p.get("name") for p in r.json().get("results", [])}

        missing = [p for p in _HUBSPOT_REQUIRED_CONTACT_PROPERTIES if p not in existing]

        if missing:
            log.warning(
                f"[HubSpot] STARTUP CHECK — {len(missing)} custom contact "
                f"property(ies) used by this script are MISSING from this "
                f"HubSpot portal: {missing}. Every contact create will 400 "
                f"until these are added under Settings → Properties → "
                f"Contact properties (type: single-line text is sufficient "
                f"for all of them)."
            )
        else:
            log.info(
                f"[HubSpot] STARTUP CHECK — all {len(_HUBSPOT_REQUIRED_CONTACT_PROPERTIES)} "
                f"required custom contact properties exist. ✅"
            )

    except requests.exceptions.HTTPError as exc:
        _hs_log_http_error("[HubSpot] STARTUP CHECK failed", exc)
        log.warning(
            "[HubSpot] Could not verify contact properties at startup "
            "(see error above). This does NOT block the system."
        )
    except Exception as exc:
        log.warning(f"[HubSpot] STARTUP CHECK failed (non-HTTP error): {exc}")


def _hs_find_contact(username: str) -> str | None:
    try:
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
            json={"filterGroups": [{"filters": [{"propertyName": "firstname", "operator": "EQ", "value": username}]}]},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0]["id"] if results else None
    except requests.exceptions.HTTPError as exc:
        _hs_log_http_error("HubSpot find contact error", exc)
        return None
    except Exception as exc:
        log.error(f"HubSpot find contact error: {exc}")
        return None


def _hs_create_contact(data: dict) -> str | None:
    try:
        sub = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        properties = {
            "firstname":           f"{data.get('username','unknown')}",
            "lastname":            f"{data.get('platform','?').upper()} Signal",
            "fx_intent_score":     str(data["intent_score"]),
            "fx_signal_category":  data["signal_category"],
            "fx_tier":             data.get("tier", ""),
            "fx_corridor":         data.get("corridor") or "",
            "fx_pain_type":        data.get("pain_type") or "",
            "fx_competitor":       data.get("competitor_mentioned") or "",
            "fx_platform":         data.get("platform", ""),
            "fx_source_community": sub,
            "fx_signal_reason":    data["reason"],
            "fx_suggested_action": data["suggested_action"],
        }
        # v7.6.0 — if LinkedIn enrichment gave us a real email/phone, use
        # HubSpot's native contact properties for them too (in addition to
        # the fx_* custom properties above, which stay platform-agnostic).
        if data.get("linkedin_email"):
            properties["email"] = data["linkedin_email"]
        if data.get("linkedin_phone"):
            properties["phone"] = data["linkedin_phone"]

        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
            json={"properties": properties},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        return r.json().get("id")
    except requests.exceptions.HTTPError as exc:
        _hs_log_http_error("HubSpot create contact error", exc)
        return None
    except Exception as exc:
        log.error(f"HubSpot create contact error: {exc}")
        return None


def _hs_create_note(data: dict, contact_id: str):
    try:
        sub = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        rescore_note = "\n[RESCORED SIGNAL]" if data.get("is_rescore") else ""

        # v7.6.0 — LinkedIn enrichment block, additive only, empty string
        # for every other platform.
        linkedin_block = ""
        if data.get("platform") == "linkedin":
            linkedin_block = (
                f"\n\nLinkedIn Profile: {data.get('linkedin_profile_url') or 'N/A'}\n"
                f"LinkedIn Name:     {data.get('linkedin_full_name') or 'N/A'}\n"
                f"LinkedIn Headline: {data.get('linkedin_headline') or 'N/A'}\n"
                f"LinkedIn Title:    {data.get('linkedin_job_title') or 'N/A'}\n"
                f"LinkedIn Email:    {data.get('linkedin_email') or 'N/A'}\n"
                f"LinkedIn Phone:    {data.get('linkedin_phone') or 'N/A'}\n"
                f"LinkedIn Location: {data.get('linkedin_location') or 'N/A'}\n"
                f"Company:           {data.get('linkedin_company_name') or data.get('linkedin_company') or 'N/A'}\n"
                f"Company Website:   {data.get('linkedin_company_website') or 'N/A'}\n"
                f"Company Industry:  {data.get('linkedin_company_industry') or 'N/A'}\n"
                f"Company Size:      {data.get('linkedin_company_size') or 'N/A'}\n"
                f"Company HQ:        {data.get('linkedin_company_location') or 'N/A'}\n"
                f"Company Phone:     {data.get('linkedin_company_phone') or 'N/A'}\n\n"
                f"LinkedIn Reply:\n{data.get('linkedin_reply') or 'N/A'}\n\n"
                f"LinkedIn DM:\n{data.get('linkedin_dm') or 'N/A'}"
            )

        note = (
            f"FLINTEL SIGNAL — v7.9.1{rescore_note}\n\n"
            f"Platform:     {data.get('platform','?').upper()}\n"
            f"Tier:         {data.get('tier','')}\n"
            f"Category:     {data['signal_category']}\n"
            f"Business:     {data.get('is_business', False)}\n"
            f"Business Size:{data.get('business_size','unknown')}\n"
            f"Corridor:     {data.get('corridor') or 'Unknown'}\n"
            f"Amount:       {data.get('estimated_amount') or 'Unknown'}\n"
            f"Competitor:   {data.get('competitor_mentioned') or 'None'}\n"
            f"Pain Type:    {data.get('pain_type') or 'Unknown'}\n"
            f"Urgency:      {data.get('urgency', 'none')}\n"
            f"Content Type: {data.get('content_type','unknown')}\n"
            f"Source:       {sub}\n"
            f"URL:          {data.get('post_url','N/A')}\n"
            f"Username:     {data.get('username','unknown')}\n"
            f"Timestamp:    {data.get('timestamp','N/A')}\n\n"
            f"Message:\n{data['message_text']}\n\n"
            f"Reason:       {data['reason']}\n"
            f"Action:       {data['suggested_action']}\n\n"
            f"Twitter Reply:\n{data.get('twitter_reply') or 'N/A'}\n\n"
            f"Twitter DM:\n{data.get('twitter_dm') or 'N/A'}\n\n"
            f"LinkedIn (legacy Reddit field):\n{data.get('linkedin_message') or 'N/A'}\n\n"
            f"Telegram DM:\n{data.get('telegram_dm') or 'N/A'}\n\n"
            f"Facebook Comment:\n{data.get('facebook_comment') or 'N/A'}"
            f"{linkedin_block}"
        )
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/notes",
            json={
                "properties": {
                    "hs_note_body": note,
                    "hs_timestamp": str(int(datetime.now(timezone.utc).timestamp() * 1000)),
                },
                "associations": [{
                    "to": {"id": contact_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 202}],
                }],
            },
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        _hs_log_http_error("HubSpot create note error", exc)
    except Exception as exc:
        log.error(f"HubSpot create note error: {exc}")


def _send_to_hubspot(data: dict) -> str | None:
    if not HUBSPOT_API_KEY:
        log.warning("HUBSPOT_API_KEY not set — skipping.")
        return None
    username   = data.get("username", "unknown")
    contact_id = _hs_find_contact(username)
    if not contact_id:
        contact_id = _hs_create_contact(data)
    if not contact_id:
        return None
    _hs_create_note(data, contact_id)
    log.info(f"HubSpot note attached | u/{username} | ID:{contact_id}")
    return contact_id


def send_to_hubspot(data: dict) -> str | None:
    return retry_with_backoff(_send_to_hubspot, data, retries=3, delay=3, label="HubSpot")


# ─────────────────────────────────────────────────────────────────────────────
# CORE SIGNAL PROCESSOR — v7.6.0: additive LinkedIn fields carried through
# from item + score_result into `data`. Routing logic 100% unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def process_scored_item(item: dict, score_result: dict, is_rescore: bool = False):
    score    = score_result.get("intent_score", 1)
    platform = item.get("platform", "unknown")

    data = {
        "message_id":                   item["message_id"],
        "platform":                     platform,
        "content_type":                 item.get("content_type", "unknown"),
        "subreddit":                    item.get("subreddit", ""),
        "telegram_group":               item.get("telegram_group", ""),
        "post_url":                     item.get("post_url", ""),
        "username":                     item.get("username", "unknown"),
        "message_text":                 item.get("text", "") or item.get("message_text", ""),
        "intent_score":                 score,
        "signal_category":              score_result.get("signal_category", "discard"),
        "tier":                         score_result.get("tier", "discard"),
        "is_business":                  score_result.get("is_business", False),
        "business_size":                score_result.get("business_size", "unknown"),
        "corridor":                     score_result.get("corridor"),
        "estimated_amount":             score_result.get("estimated_amount"),
        "competitor_mentioned":         score_result.get("competitor_mentioned"),
        "competitor_outreach_detected": score_result.get("competitor_outreach_detected", False),
        "pain_type":                    score_result.get("pain_type"),
        "urgency":                      score_result.get("urgency", "none"),
        "reason":                       score_result.get("reason", ""),
        "suggested_action":             score_result.get("suggested_action", ""),
        "twitter_reply":                score_result.get("twitter_reply"),
        "twitter_dm":                   score_result.get("twitter_dm"),
        "linkedin_message":             score_result.get("linkedin_message"),
        "telegram_dm":                  score_result.get("telegram_dm"),
        "facebook_comment":             score_result.get("facebook_comment"),
        # v7.6.0 NEW — outreach text comes from Claude (score_result);
        # enrichment data comes from the item itself (fetched at poll time
        # from the User Data / Company Data endpoints).
        "linkedin_reply":               score_result.get("linkedin_reply"),
        "linkedin_dm":                  score_result.get("linkedin_dm"),
        "linkedin_full_name":           item.get("linkedin_full_name"),
        "linkedin_headline":            item.get("linkedin_headline"),
        "linkedin_email":               item.get("linkedin_email"),
        "linkedin_phone":               item.get("linkedin_phone"),
        "linkedin_location":            item.get("linkedin_location"),
        "linkedin_company":             item.get("linkedin_company"),
        "linkedin_job_title":           item.get("linkedin_job_title"),
        "linkedin_profile_url":         item.get("linkedin_profile_url"),
        "linkedin_company_name":        item.get("linkedin_company_name"),
        "linkedin_company_website":     item.get("linkedin_company_website"),
        "linkedin_company_industry":    item.get("linkedin_company_industry"),
        "linkedin_company_size":        item.get("linkedin_company_size"),
        "linkedin_company_location":    item.get("linkedin_company_location"),
        "linkedin_company_phone":       item.get("linkedin_company_phone"),
        # v7.7.0 NEW — which KEYWORDS entry this item was fetched under.
        # Only ever set for platforms that loop per-keyword (Facebook,
        # LinkedIn) — None/absent for Reddit/Twitter/Telegram, unchanged.
        "search_keyword":               item.get("search_keyword"),
        "watchlist":                    score_result.get("watchlist", False),
        "watchlist_reason":             score_result.get("watchlist_reason"),
        "timestamp":                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "is_rescore":                   is_rescore,
    }

    if is_rescore:
        saved = update_signal(data["message_id"], data)
    else:
        saved = save_signal(data)

    if not saved:
        return

    if score < MIN_SCORE_MEDIUM:
        mode = "RESCORE-SILENT" if is_rescore else "SILENT SAVE"
        log.debug(
            f"{mode} | [{platform.upper()}] Score:{score} | "
            f"u/{data['username']} | {data['content_type']}"
        )
        return

    if MIN_SCORE_MEDIUM <= score < MIN_SCORE_HIGH:
        mode = "RESCORE-MEDIUM" if is_rescore else "MEDIUM"
        log.info(f"{mode} | [{platform.upper()}] Score:{score} | Slack + HubSpot | u/{data['username']}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])
        cid = send_to_hubspot(data)
        if cid:
            mark_hubspot_alerted(data["message_id"], cid)

    elif score >= MIN_SCORE_HIGH:
        mode = "RESCORE-HIGH" if is_rescore else "HIGH"
        log.info(f"{mode} | [{platform.upper()}] Score:{score} | Slack + HubSpot | u/{data['username']}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])
        cid = send_to_hubspot(data)
        if cid:
            mark_hubspot_alerted(data["message_id"], cid)


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC BATCH PROCESSOR (unchanged — reused as-is for LinkedIn)
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
    gap_seconds: int,
    timeout_seconds: int,
):
    platform_key = platform_label.lower()

    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{gap_seconds}s | "
        f"timeout:{timeout_seconds}s"
    )

    current_batch, batch_start_time = load_pending_batch(platform_key)
    if current_batch:
        log.info(
            f"[{platform_label}] Resumed [{len(current_batch)}/{batch_size}] "
            f"from persistent disk — continuing, NOT restarting at 1."
        )

    total_received   = 0
    total_matched    = 0
    total_dropped    = 0
    total_batches    = 0

    while True:
        try:
            if current_batch and batch_start_time is not None:
                elapsed   = time.time() - batch_start_time
                remaining = timeout_seconds - elapsed
                wait_time = max(0.1, remaining)
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

                text = item.get("text", "").strip()

                if not text or len(text) < 10:
                    q.task_done()
                    continue

                matched_keyword = passes_keyword_filter(text)
                if not matched_keyword:
                    total_dropped += 1
                    log.debug(
                        f"[{platform_label}] FILTERED | "
                        f"u/{item.get('username')} | {item.get('content_type','?')}"
                    )
                    q.task_done()
                    continue

                # v7.8.0 NEW — populate item["search_keyword"] with the
                # keyword that matched via passes_keyword_filter, but ONLY
                # if the item doesn't already carry one. Facebook and
                # LinkedIn already set search_keyword at poll time (the
                # exact keyword they searched with) — that value is
                # preserved as-is, unchanged, and is NOT overwritten here.
                # Reddit, Twitter, and Telegram never set this field
                # before, so they now get it filled in from whichever
                # KEYWORDS entry matched the fetched text.
                if not item.get("search_keyword"):
                    item["search_keyword"] = matched_keyword

                total_matched += 1

                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)
                save_pending_batch(platform_key, current_batch, batch_start_time)
                save_batch_seconds(platform_key, batch_start_time)

                log.info(
                    f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | "
                    f"{item.get('content_type','?').upper()} | u/{item.get('username')}"
                )

                q.task_done()

            should_fire = False
            fire_reason = ""

            if len(current_batch) >= batch_size:
                should_fire = True
                fire_reason = f"batch full ({batch_size} items)"
            elif current_batch and batch_start_time is not None:
                elapsed = time.time() - batch_start_time
                if elapsed >= timeout_seconds:
                    should_fire = True
                    fire_reason = f"timeout ({timeout_seconds}s) — partial batch {len(current_batch)}/{batch_size}"

            if should_fire and current_batch:
                total_batches += 1
                batch_to_send  = current_batch[:batch_size]
                current_batch  = current_batch[batch_size:]
                batch_start_time = None if not current_batch else time.time()

                if current_batch:
                    save_pending_batch(platform_key, current_batch, batch_start_time)
                    save_batch_seconds(platform_key, batch_start_time)
                else:
                    clear_pending_batch(platform_key)
                    clear_batch_seconds(platform_key)

                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | "
                    f"reason:{fire_reason} | items:{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (
                        scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch.")
                    )
                    process_scored_item(it, sr)

                log.info(
                    f"[{platform_label}] BATCH {total_batches} COMPLETE — "
                    f"{len(batch_to_send)} item(s) completed | waiting {gap_seconds}s..."
                )
                time.sleep(gap_seconds)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _rescore_queue_requests(message_ids: list, operator_note: str = "") -> list:
    inserted = []
    for mid in message_ids:
        try:
            doc = {
                "message_id":    mid,
                "status":        "pending",
                "operator_note": operator_note,
                "requested_at":  datetime.now(timezone.utc),
                "processed_at":  None,
                "rescore_result": None,
                "error":         None,
            }
            result = db.flintel_rescore_messages.insert_one(doc)
            inserted.append(str(result.inserted_id))
            log.info(f"[RESCORE] Queued | message_id:{mid} | req_id:{result.inserted_id}")
        except Exception as exc:
            log.error(f"[RESCORE] Failed to queue message_id:{mid} — {exc}")
    return inserted


def _rescore_fetch_pending(limit: int) -> list:
    try:
        return list(
            db.flintel_rescore_messages.find(
                {"status": "pending"}
            ).sort("requested_at", ASCENDING).limit(limit)
        )
    except Exception as exc:
        log.error(f"[RESCORE] fetch_pending error: {exc}")
        return []


def _rescore_mark_processing(req_ids: list):
    try:
        db.flintel_rescore_messages.update_many(
            {"_id": {"$in": req_ids}},
            {"$set": {"status": "processing"}},
        )
    except Exception as exc:
        log.error(f"[RESCORE] mark_processing error: {exc}")


def _rescore_mark_done(req_id, score_result: dict):
    try:
        db.flintel_rescore_messages.update_one(
            {"_id": req_id},
            {"$set": {
                "status":         "done",
                "rescore_result": score_result,
                "processed_at":   datetime.now(timezone.utc),
                "error":          None,
            }},
        )
    except Exception as exc:
        log.error(f"[RESCORE] mark_done error: {exc}")


def _rescore_mark_error(req_id, error: str):
    try:
        db.flintel_rescore_messages.update_one(
            {"_id": req_id},
            {"$set": {
                "status":       "error",
                "error":        error,
                "processed_at": datetime.now(timezone.utc),
            }},
        )
    except Exception as exc:
        log.error(f"[RESCORE] mark_error error: {exc}")


def run_rescore_processor():
    log.info(
        f"[RESCORE] Processor started | "
        f"batch_size:{RESCORE_BATCH_SIZE} | poll_interval:{RESCORE_POLL_INTERVAL}s | "
        f"gap:{RESCORE_BATCH_GAP_SECONDS}s"
    )
    total_rescored = 0
    total_batches  = 0

    while True:
        try:
            pending = _rescore_fetch_pending(RESCORE_BATCH_SIZE)
            if not pending:
                time.sleep(RESCORE_POLL_INTERVAL)
                continue

            req_ids = [p["_id"] for p in pending]
            _rescore_mark_processing(req_ids)

            items_for_claude = []
            req_map = {}

            for i, req in enumerate(pending, start=1):
                mid = req["message_id"]
                sig = db.signals.find_one({"message_id": mid}, {"_id": 0})
                if not sig:
                    log.warning(f"[RESCORE] Signal not found in DB: {mid} — marking error.")
                    _rescore_mark_error(req["_id"], f"Signal not found: {mid}")
                    continue

                item = {
                    "message_id":     mid,
                    "platform":       sig.get("platform", "reddit"),
                    "content_type":   sig.get("content_type", "unknown"),
                    "subreddit":      sig.get("subreddit", ""),
                    "telegram_group": sig.get("telegram_group", ""),
                    "post_url":       sig.get("post_url", ""),
                    "username":       sig.get("username", "unknown"),
                    "text":           sig.get("message_text", ""),
                    # v7.6.0 — carry LinkedIn enrichment through on rescore
                    # too, so a rescored LinkedIn signal keeps its
                    # company/contact detail instead of losing it.
                    "linkedin_full_name":        sig.get("linkedin_full_name"),
                    "linkedin_headline":         sig.get("linkedin_headline"),
                    "linkedin_email":            sig.get("linkedin_email"),
                    "linkedin_phone":            sig.get("linkedin_phone"),
                    "linkedin_location":         sig.get("linkedin_location"),
                    "linkedin_company":          sig.get("linkedin_company"),
                    "linkedin_job_title":        sig.get("linkedin_job_title"),
                    "linkedin_profile_url":      sig.get("linkedin_profile_url"),
                    "linkedin_company_name":     sig.get("linkedin_company_name"),
                    "linkedin_company_website":  sig.get("linkedin_company_website"),
                    "linkedin_company_industry": sig.get("linkedin_company_industry"),
                    "linkedin_company_size":     sig.get("linkedin_company_size"),
                    "linkedin_company_location": sig.get("linkedin_company_location"),
                    "linkedin_company_phone":    sig.get("linkedin_company_phone"),
                }
                items_for_claude.append(item)
                req_map[len(items_for_claude)] = req

            if not items_for_claude:
                time.sleep(RESCORE_POLL_INTERVAL)
                continue

            total_batches += 1
            log.info(
                f"[RESCORE] ━━━ RESCORE BATCH {total_batches} ━━━ | "
                f"items:{len(items_for_claude)} | "
                f"message_ids:{[it['message_id'] for it in items_for_claude]}"
            )

            scores = score_batch_with_claude(items_for_claude)
            score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

            for i, item in enumerate(items_for_claude):
                pos = i + 1
                req = req_map.get(pos)
                sr  = score_map.get(pos) or (
                    scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch.")
                )

                process_scored_item(item, sr, is_rescore=True)
                total_rescored += 1

                if req:
                    _rescore_mark_done(req["_id"], sr)

            log.info(
                f"[RESCORE] BATCH {total_batches} DONE | "
                f"rescored:{len(items_for_claude)} | total_ever:{total_rescored} | "
                f"waiting {RESCORE_BATCH_GAP_SECONDS}s..."
            )
            time.sleep(RESCORE_BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[RESCORE] processor error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — feedparser RSS poller (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

_reddit_seen_ids: set = load_seen_ids("reddit")
_reddit_seen_lock = threading.Lock()
_reddit_seen_dirty_count = 0


def _reddit_rss_is_seen(entry_id: str) -> bool:
    global _reddit_seen_ids, _reddit_seen_dirty_count
    with _reddit_seen_lock:
        if entry_id in _reddit_seen_ids:
            return True
        _reddit_seen_ids.add(entry_id)
        if len(_reddit_seen_ids) > 200_000:
            _reddit_seen_ids.clear()
        _reddit_seen_dirty_count += 1
        if _reddit_seen_dirty_count >= 10:
            save_seen_ids("reddit", _reddit_seen_ids)
            _reddit_seen_dirty_count = 0
        return False


def _get_reddit_rss(subreddit: str) -> list:
    url = f"https://www.reddit.com/r/{subreddit}/new.rss"
    items = []
    try:
        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            log.warning(f"[REDDIT-RSS] Feed parse issue for r/{subreddit}: {feed.bozo_exception}")
            return items

        for entry in feed.entries:
            entry_id = entry.get("id", "") or entry.get("link", "")
            if not entry_id:
                continue
            if _reddit_rss_is_seen(entry_id):
                continue

            title   = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(summary)).strip()

            text = title
            if summary_plain and summary_plain.lower() != title.lower():
                text = f"{title}\n\n{summary_plain}"

            author = entry.get("author", "unknown").lstrip("u/").strip() or "unknown"
            link   = entry.get("link", "")

            items.append({
                "message_id":     f"reddit_rss_{entry_id.split('/')[-1] or entry_id}",
                "platform":       "reddit",
                "content_type":   "post",
                "text":           text,
                "username":       author,
                "subreddit":      subreddit,
                "telegram_group": "",
                "post_url":       link,
            })

    except Exception as exc:
        log.error(f"[REDDIT-RSS] Error fetching r/{subreddit}: {exc}")

    return items


def poll_reddit_rss():
    log.info(
        f"[REDDIT-RSS] Poller started | {len(TARGET_SUBREDDITS)} subreddits | "
        f"poll interval: {REDDIT_POLL_INTERVAL}s per cycle | "
        f"dedup set resumed with {len(_reddit_seen_ids)} known ID(s)"
    )

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for subreddit in TARGET_SUBREDDITS:
            try:
                items = _get_reddit_rss(subreddit)
                for item in items:
                    reddit_queue.put(item)
                    save_queue_message("reddit", item)
                    total_new += 1
                if items:
                    log.info(
                        f"[REDDIT-RSS] r/{subreddit} → {len(items)} new items queued "
                        f"(queue size: {reddit_queue.qsize()})"
                    )
                time.sleep(2)
            except Exception as exc:
                log.error(f"[REDDIT-RSS] Unhandled error for r/{subreddit}: {exc}")
                total_errors += 1

        save_seen_ids("reddit", _reddit_seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[REDDIT-RSS] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {REDDIT_POLL_INTERVAL}s..."
        )
        time.sleep(REDDIT_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER
# v7.9.1 — sends ONE chunk (TWITTER_CHUNK_SIZE keywords combined into one
# OR-query) per TWITTER_POLL_INTERVAL, advancing through TWITTER_SEARCH_CHUNKS
# and wrapping back to chunk 0 once the last chunk is reached. Chunk
# position is persisted in MongoDB (flintel_state) so restarts resume where
# they left off instead of starting over from chunk 1 every time.
#
# RapidAPI key failover (429/403 → rotate to next configured key) from
# v7.9.0 is preserved — a failed attempt retries the SAME chunk, it does
# NOT advance to the next chunk.
#
# Facebook/LinkedIn/Reddit/Telegram are completely untouched by this change.
# ─────────────────────────────────────────────────────────────────────────────

# v7.9.0 NEW — one or more RapidAPI keys for Twitter, comma-separated.
# Falls back to the single RAPID_API_KEY (shared by Facebook/LinkedIn) if
# TWITTER_RAPID_API_KEYS isn't set, so nothing breaks for existing setups
# that only ever had one key.
TWITTER_RAPID_API_KEYS = [
    k.strip() for k in os.getenv("TWITTER_RAPID_API_KEYS", "").split(",") if k.strip()
]
if not TWITTER_RAPID_API_KEYS and RAPID_API_KEY:
    TWITTER_RAPID_API_KEYS = [RAPID_API_KEY]

_twitter_key_index = 0
_twitter_key_lock = threading.Lock()


def _twitter_current_headers() -> dict:
    """Returns the RapidAPI headers for whichever key is currently active."""
    with _twitter_key_lock:
        key = TWITTER_RAPID_API_KEYS[_twitter_key_index] if TWITTER_RAPID_API_KEYS else ""
    return {
        "x-rapidapi-key": key,
        "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
        "Content-Type": "application/json",
    }


def _twitter_rotate_key(reason: str = ""):
    """v7.9.0 — advances to the next configured RapidAPI key. Wraps around
    to key #1 after the last key, so failover keeps cycling."""
    global _twitter_key_index
    if len(TWITTER_RAPID_API_KEYS) <= 1:
        return
    with _twitter_key_lock:
        old_index = _twitter_key_index
        _twitter_key_index = (_twitter_key_index + 1) % len(TWITTER_RAPID_API_KEYS)
        new_index = _twitter_key_index
    log.warning(
        f"[TWITTER] RapidAPI key #{old_index + 1} appears exhausted/rate-limited"
        f"{' (' + reason + ')' if reason else ''} — switching to key #{new_index + 1}"
        f"/{len(TWITTER_RAPID_API_KEYS)}."
    )


def build_twitter_client() -> dict | None:
    if not TWITTER_RAPID_API_KEYS:
        log.warning("RAPID_API_KEY / TWITTER_RAPID_API_KEYS not set — Twitter platform disabled.")
        return None
    try:
        log.info(
            f"Twitter/X client initialised (twitter-api45) | "
            f"{len(TWITTER_RAPID_API_KEYS)} RapidAPI key(s) available for automatic failover."
        )
        return {"initialised": True}
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def _extract_tweets_from_twitter_api45(data: dict) -> list:
    tweets = []
    try:
        results = data.get("timeline") or data.get("results") or data.get("tweets") or []
        for t in results:
            if not isinstance(t, dict):
                continue
            tweet_id = str(t.get("tweet_id") or t.get("id") or "")
            if not tweet_id:
                continue
            text = t.get("text") or t.get("full_text") or ""
            author = t.get("author") or t.get("user") or {}
            username = (
                t.get("screen_name")
                or author.get("screen_name")
                or author.get("username")
                or f"user_{tweet_id}"
            )
            tweets.append({"id": tweet_id, "text": text, "username": username})
    except Exception as exc:
        log.error(f"Twitter response parse error: {exc}")
    return tweets


# v7.9.1 NEW — persisted chunk-rotation index so a restart doesn't reset
# progress back to chunk #1 every time (keeps steady coverage across the
# full keyword list even across deploys/crashes). Falls back to 0 if
# nothing persisted yet or if it's out of range for the current chunk count.
def _load_twitter_chunk_index() -> int:
    val = _get_state("twitter_chunk_index")
    if not isinstance(val, int) or not TWITTER_SEARCH_CHUNKS:
        return 0
    return val % len(TWITTER_SEARCH_CHUNKS)


def _save_twitter_chunk_index(idx: int):
    _set_state("twitter_chunk_index", idx)


def poll_twitter(client: dict):
    seen_ids: set = load_seen_ids("twitter")
    dirty = 0
    consecutive_key_failures = 0

    chunk_index = _load_twitter_chunk_index()
    total_chunks = len(TWITTER_SEARCH_CHUNKS)

    log.info(
        f"Twitter poll started | total_chunks:{total_chunks} | "
        f"chunk_size:{TWITTER_CHUNK_SIZE} | resuming at chunk:{chunk_index + 1}/{total_chunks} | "
        f"keys_available:{len(TWITTER_RAPID_API_KEYS)} | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    url = "https://twitter-api45.p.rapidapi.com/search.php"

    while True:
        try:
            chunk_query = TWITTER_SEARCH_CHUNKS[chunk_index]

            querystring = {
                "query":       chunk_query,
                "search_type": "Top",
            }
            headers = _twitter_current_headers()

            response = requests.get(url, headers=headers, params=querystring, timeout=30)

            # v7.9.0 — automatic RapidAPI key failover on 429/403. Stays on
            # the SAME chunk and retries next loop iteration after failover
            # (does NOT advance chunk_index on a failed attempt).
            if response.status_code in (429, 403):
                _twitter_rotate_key(reason=f"HTTP {response.status_code}")
                consecutive_key_failures += 1
                if consecutive_key_failures >= len(TWITTER_RAPID_API_KEYS):
                    log.error(
                        f"[TWITTER] All {len(TWITTER_RAPID_API_KEYS)} RapidAPI key(s) "
                        f"rate-limited/exhausted — waiting {TWITTER_POLL_INTERVAL}s "
                        f"before retrying chunk {chunk_index + 1}/{total_chunks}."
                    )
                    consecutive_key_failures = 0
                    time.sleep(TWITTER_POLL_INTERVAL)
                continue

            consecutive_key_failures = 0

            # v7.9.1 — with chunking this shouldn't happen, but handle
            # gracefully if TWITTER_CHUNK_SIZE is set too high via env var.
            if response.status_code == 414:
                log.error(
                    f"[TWITTER] Chunk {chunk_index + 1}/{total_chunks} still too long "
                    f"(HTTP 414) — query len:{len(chunk_query)}. Lower TWITTER_CHUNK_SIZE "
                    f"(currently {TWITTER_CHUNK_SIZE})."
                )
                chunk_index = (chunk_index + 1) % total_chunks
                _save_twitter_chunk_index(chunk_index)
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            response.raise_for_status()
            data = response.json()

            tweets = _extract_tweets_from_twitter_api45(data)

            new_count = 0
            for t in tweets:
                tweet_id = t["id"]
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                dirty += 1

                if len(seen_ids) > 50_000:
                    seen_ids.clear()

                text     = t["text"]
                username = t["username"]

                _tw_item = {
                    "message_id":     f"twitter_{tweet_id}",
                    "platform":       "twitter",
                    "content_type":   "tweet",
                    "text":           text,
                    "username":       username,
                    "subreddit":      "",
                    "telegram_group": "",
                    "post_url":       f"https://twitter.com/{username}/status/{tweet_id}",
                }
                twitter_queue.put(_tw_item)
                save_queue_message("twitter", _tw_item)
                new_count += 1

            if dirty >= 10:
                save_seen_ids("twitter", seen_ids)
                dirty = 0

            log.info(
                f"[TWITTER] chunk {chunk_index + 1}/{total_chunks} → "
                f"{new_count} new tweet(s) queued | queue_size:{twitter_queue.qsize()}"
            )

            # v7.9.1 — advance to next chunk, wrap to 0 after the last one.
            chunk_index = (chunk_index + 1) % total_chunks
            _save_twitter_chunk_index(chunk_index)

            if chunk_index == 0:
                log.info(
                    f"[TWITTER] Full keyword list cycle complete "
                    f"({total_chunks} chunks) — restarting from chunk 1."
                )

        except Exception as exc:
            log.error(
                f"[TWITTER] chunk {chunk_index + 1}/{total_chunks} error: {exc} — "
                f"retrying in {TWITTER_POLL_INTERVAL}s..."
            )

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# FACEBOOK POLLER (unchanged from v7.5.0)
# ─────────────────────────────────────────────────────────────────────────────

def build_facebook_client() -> dict | None:
    # v7.9.0 — shares RAPID_API_KEY with LinkedIn (same account/plan,
    # intentional). Twitter has its own separate key(s) — see Twitter
    # section. To move Facebook to a different RapidAPI provider later,
    # only this function's headers need to change.
    if not RAPID_API_KEY:
        log.warning("RAPID_API_KEY not set — Facebook platform disabled.")
        return None
    try:
        client = {
            "x-rapidapi-key":  RAPID_API_KEY,
            "x-rapidapi-host": "facebook-scraper3.p.rapidapi.com",
        }
        log.info("Facebook client initialised (facebook-scraper3).")
        return client
    except Exception as exc:
        log.error(f"Facebook client error: {exc}")
        return None


def _extract_posts_from_facebook_scraper3(data: dict) -> list:
    posts = []
    try:
        results = data.get("results") or data.get("posts") or data.get("data") or []
        if not isinstance(results, list):
            return posts
        for p in results:
            if not isinstance(p, dict):
                continue
            post_id = str(p.get("post_id") or p.get("id") or "")
            if not post_id:
                continue
            text = p.get("message") or p.get("text") or p.get("content") or ""
            author = p.get("author") or p.get("user") or {}
            if not isinstance(author, dict):
                author = {}
            username = (
                p.get("author_name")
                or author.get("name")
                or author.get("username")
                or f"user_{post_id}"
            )
            url = p.get("url") or p.get("post_url") or p.get("permalink") or ""
            posts.append({"id": post_id, "text": text, "username": username, "url": url})
    except Exception as exc:
        log.error(f"Facebook response parse error: {exc}")
    return posts


def poll_facebook(client: dict):
    seen_ids: set = load_seen_ids("facebook")
    dirty = 0
    log.info(
        f"Facebook poll started | keywords:{len(KEYWORDS)} | "
        f"poll interval: {FACEBOOK_POLL_INTERVAL}s per full cycle | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    url = "https://facebook-scraper3.p.rapidapi.com/search/posts"

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for keyword in KEYWORDS:
            try:
                params = {"query": keyword}
                response = requests.get(url=url, headers=client, params=params, timeout=30)

                # v7.9.0 NEW — explicit rate-limit/quota detection, purely
                # for clearer operator logs. This does NOT change isolation
                # behaviour — Facebook already runs in its own thread with
                # its own try/except, so a 429/403 here was ALREADY unable
                # to affect LinkedIn's thread in any way. This just makes
                # it obvious in the logs that it's a quota issue (not a
                # random error) and skips to the next keyword immediately.
                if response.status_code in (429, 403):
                    log.warning(
                        f"[FACEBOOK] Rate-limited/quota exhausted for keyword '{keyword}' "
                        f"(HTTP {response.status_code}) — skipping to next keyword. "
                        f"LinkedIn is unaffected (separate thread/poller)."
                    )
                    total_errors += 1
                    time.sleep(FACEBOOK_KEYWORD_GAP_SECONDS)
                    continue

                response.raise_for_status()
                data = response.json()

                posts = _extract_posts_from_facebook_scraper3(data)

                new_this_keyword = 0
                for p in posts:
                    post_id = p["id"]
                    if post_id in seen_ids:
                        continue
                    seen_ids.add(post_id)
                    dirty += 1

                    if len(seen_ids) > 50_000:
                        seen_ids.clear()

                    text     = p["text"]
                    username = p["username"]

                    _fb_item = {
                        "message_id":     f"facebook_{post_id}",
                        "platform":       "facebook",
                        "content_type":   "post",
                        "text":           text,
                        "username":       username,
                        "subreddit":      "",
                        "telegram_group": "",
                        "post_url":       p.get("url") or "",
                        # v7.7.0 NEW — which KEYWORDS entry this search
                        # cycle was on when this post came back, so it's
                        # traceable later which keyword found it.
                        "search_keyword": keyword,
                    }
                    facebook_queue.put(_fb_item)
                    save_queue_message("facebook", _fb_item)
                    total_new += 1
                    new_this_keyword += 1

                if new_this_keyword:
                    log.info(
                        f"[FACEBOOK] '{keyword}' → {new_this_keyword} new items queued "
                        f"(queue size: {facebook_queue.qsize()})"
                    )

                if dirty >= 10:
                    save_seen_ids("facebook", seen_ids)
                    dirty = 0

                time.sleep(FACEBOOK_KEYWORD_GAP_SECONDS)

            except Exception as exc:
                log.error(f"[FACEBOOK] Unhandled error for keyword '{keyword}': {exc}")
                total_errors += 1

        save_seen_ids("facebook", seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[FACEBOOK] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {FACEBOOK_POLL_INTERVAL}s..."
        )
        time.sleep(FACEBOOK_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# LINKEDIN POLLER — v7.6.0 NEW
# linkedin-data-scraper1.p.rapidapi.com
#   1) POST /search_linkedIn.php   {"keywords": <kw>, "start": "0"}
#   2) POST /get_user_data.php     {"username_or_url": <profile url>}
#   3) POST /get_company_data.php  {"company_name": <company>}
#
# Same per-keyword cycle shape as Facebook (poll_facebook). Each of the
# three endpoint calls is wrapped in its OWN try/except so that a failure
# or exhausted quota on any single endpoint never blocks the other two or
# crashes the poll cycle — it just degrades to "no enrichment for this
# item" and keeps going, exactly as requested.
# ─────────────────────────────────────────────────────────────────────────────

LINKEDIN_HOST         = "linkedin-data-scraper1.p.rapidapi.com"
LINKEDIN_SEARCH_URL   = f"https://{LINKEDIN_HOST}/search_linkedIn.php"
LINKEDIN_USER_URL     = f"https://{LINKEDIN_HOST}/get_user_data.php"
LINKEDIN_COMPANY_URL  = f"https://{LINKEDIN_HOST}/get_company_data.php"


def build_linkedin_client() -> dict | None:
    # v7.9.0 — shares RAPID_API_KEY with Facebook (same account/plan,
    # intentional). Twitter has its own separate key(s) — see Twitter
    # section. To move LinkedIn to a different RapidAPI provider later,
    # only this function's headers need to change.
    if not RAPID_API_KEY:
        log.warning("RAPID_API_KEY not set — LinkedIn platform disabled.")
        return None
    try:
        client = {
            "x-rapidapi-key":  RAPID_API_KEY,
            "x-rapidapi-host": LINKEDIN_HOST,
            "Content-Type":    "application/x-www-form-urlencoded",
        }
        log.info("LinkedIn client initialised (linkedin-data-scraper1).")
        return client
    except Exception as exc:
        log.error(f"LinkedIn client error: {exc}")
        return None


def _extract_results_from_linkedin_search(data: dict) -> list:
    """Best-effort walk of search_linkedIn.php's response into a list of
    {id, name, headline, url, company} dicts. Field names are checked
    defensively (results/data/people/items, id/profile_id/urn/url,
    name/full_name/title, headline/summary/snippet/description) since the
    vendor's exact schema for this endpoint hasn't been confirmed against a
    live response — same defensive approach already used for twitter-api45
    and facebook-scraper3 above, so a shape mismatch degrades to "no
    results this keyword" instead of a crash.
    """
    results = []
    try:
        raw = data.get("results") or data.get("data") or data.get("people") or data.get("items") or []
        if not isinstance(raw, list):
            return results
        for r in raw:
            if not isinstance(r, dict):
                continue
            rid = str(r.get("id") or r.get("profile_id") or r.get("urn") or r.get("url") or "")
            if not rid:
                continue
            name     = r.get("name") or r.get("full_name") or r.get("title") or "unknown"
            headline = r.get("headline") or r.get("summary") or r.get("snippet") or r.get("description") or ""
            url      = r.get("url") or r.get("profile_url") or r.get("link") or ""
            company  = r.get("company") or r.get("current_company") or ""
            results.append({
                "id": rid, "name": name, "headline": headline,
                "url": url, "company": company,
            })
    except Exception as exc:
        log.error(f"LinkedIn search response parse error: {exc}")
    return results


def _linkedin_fetch_user_data(client: dict, username_or_url: str) -> dict:
    """Calls the User Data endpoint for one matched profile. Never raises —
    a failure (network error, bad response shape, or the endpoint's quota
    being exhausted) degrades to an empty dict, which just means this one
    item is queued/scored WITHOUT enrichment rather than being dropped or
    crashing the poller. Independent of _linkedin_fetch_company_data below
    — one failing never blocks the other.
    """
    if not username_or_url:
        return {}
    try:
        payload = {"username_or_url": username_or_url}
        r = requests.post(LINKEDIN_USER_URL, data=payload, headers=client, timeout=30)
        r.raise_for_status()
        raw = r.json()
        d = raw.get("data") if isinstance(raw, dict) and isinstance(raw.get("data"), dict) else raw
        if not isinstance(d, dict):
            return {}
        return {
            "linkedin_full_name":    d.get("full_name") or d.get("name"),
            "linkedin_headline":     d.get("headline"),
            "linkedin_email":        d.get("email") or d.get("email_address"),
            "linkedin_phone":        d.get("phone") or d.get("phone_number"),
            "linkedin_location":     d.get("location") or d.get("geo_location") or d.get("city"),
            "linkedin_company":      d.get("company") or d.get("current_company"),
            "linkedin_job_title":    d.get("job_title") or d.get("position") or d.get("title"),
            "linkedin_profile_url":  d.get("profile_url") or d.get("url") or username_or_url,
        }
    except Exception as exc:
        log.warning(
            f"[LINKEDIN] User Data endpoint error for '{username_or_url}': {exc} "
            f"— continuing without this item's user enrichment."
        )
        return {}


def _linkedin_fetch_company_data(client: dict, company_name: str) -> dict:
    """Calls the Company Data endpoint. Same never-raise contract as
    _linkedin_fetch_user_data — independent failure, independent quota.
    """
    if not company_name:
        return {}
    try:
        payload = {"company_name": company_name}
        r = requests.post(LINKEDIN_COMPANY_URL, data=payload, headers=client, timeout=30)
        r.raise_for_status()
        raw = r.json()
        d = raw.get("data") if isinstance(raw, dict) and isinstance(raw.get("data"), dict) else raw
        if not isinstance(d, dict):
            return {}
        return {
            "linkedin_company_name":     d.get("name") or company_name,
            "linkedin_company_website":  d.get("website"),
            "linkedin_company_industry": d.get("industry"),
            "linkedin_company_size":     d.get("company_size") or d.get("employee_count") or d.get("size"),
            "linkedin_company_location": d.get("headquarter") or d.get("location") or d.get("headquarters"),
            "linkedin_company_phone":    d.get("phone"),
        }
    except Exception as exc:
        log.warning(
            f"[LINKEDIN] Company Data endpoint error for '{company_name}': {exc} "
            f"— continuing without this item's company enrichment."
        )
        return {}


def poll_linkedin(client: dict):
    seen_ids: set = load_seen_ids("linkedin")
    dirty = 0
    log.info(
        f"LinkedIn poll started | keywords:{len(KEYWORDS)} | "
        f"poll interval: {LINKEDIN_POLL_INTERVAL}s per full cycle | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for keyword in KEYWORDS:
            try:
                payload = {"keywords": keyword, "start": "0"}
                response = requests.post(
                    LINKEDIN_SEARCH_URL, data=payload, headers=client, timeout=30
                )

                # v7.9.0 NEW — same explicit rate-limit/quota detection as
                # Facebook above, same purpose: clearer logs only. LinkedIn
                # already runs in its own separate thread, so this was
                # ALREADY unable to affect Facebook's thread.
                if response.status_code in (429, 403):
                    log.warning(
                        f"[LINKEDIN] Rate-limited/quota exhausted for keyword '{keyword}' "
                        f"(HTTP {response.status_code}) — skipping to next keyword. "
                        f"Facebook is unaffected (separate thread/poller)."
                    )
                    total_errors += 1
                    time.sleep(LINKEDIN_KEYWORD_GAP_SECONDS)
                    continue

                response.raise_for_status()
                data = response.json()

                results = _extract_results_from_linkedin_search(data)

                new_this_keyword = 0
                for res in results:
                    rid = res["id"]
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    dirty += 1

                    if len(seen_ids) > 50_000:
                        seen_ids.clear()

                    text_parts = [p for p in [res.get("name"), res.get("headline"), res.get("company")] if p]
                    text = " | ".join(text_parts) or keyword

                    if not passes_keyword_filter(text):
                        continue

                    user_extra = _linkedin_fetch_user_data(client, res.get("url") or res.get("id"))

                    company_extra = {}
                    company_name = res.get("company") or user_extra.get("linkedin_company")
                    if company_name:
                        company_extra = _linkedin_fetch_company_data(client, company_name)

                    _li_item = {
                        "message_id":     f"linkedin_{rid}",
                        "platform":       "linkedin",
                        "content_type":   "profile",
                        "text":           text,
                        "username":       res.get("name") or "unknown",
                        "subreddit":      "",
                        "telegram_group": "",
                        "post_url":       res.get("url") or "",
                        "search_keyword": keyword,
                    }
                    _li_item.update(user_extra)
                    _li_item.update(company_extra)

                    linkedin_queue.put(_li_item)
                    save_queue_message("linkedin", _li_item)
                    total_new += 1
                    new_this_keyword += 1

                if new_this_keyword:
                    log.info(
                        f"[LINKEDIN] '{keyword}' → {new_this_keyword} new items queued "
                        f"(queue size: {linkedin_queue.qsize()})"
                    )

                if dirty >= 10:
                    save_seen_ids("linkedin", seen_ids)
                    dirty = 0

                time.sleep(LINKEDIN_KEYWORD_GAP_SECONDS)

            except Exception as exc:
                log.error(f"[LINKEDIN] Unhandled error for keyword '{keyword}': {exc}")
                total_errors += 1
                continue

        save_seen_ids("linkedin", seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[LINKEDIN] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {LINKEDIN_POLL_INTERVAL}s..."
        )
        time.sleep(LINKEDIN_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM LISTENER (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

_telegram_seen_ids: set = load_seen_ids("telegram")
_telegram_seen_lock = threading.Lock()
_telegram_seen_dirty_count = 0


def _telegram_is_seen(chat_id: int, msg_id: int) -> bool:
    global _telegram_seen_ids, _telegram_seen_dirty_count
    key = f"{chat_id}_{msg_id}"
    with _telegram_seen_lock:
        if key in _telegram_seen_ids:
            return True
        _telegram_seen_ids.add(key)
        if len(_telegram_seen_ids) > 100_000:
            _telegram_seen_ids.clear()
        _telegram_seen_dirty_count += 1
        if _telegram_seen_dirty_count >= 10:
            save_seen_ids("telegram", _telegram_seen_ids)
            _telegram_seen_dirty_count = 0
        return False


def _join_telegram_groups_sync(client: TelegramClient):
    log.info(
        f"Telegram: starting auto-join for {len(TARGET_TELEGRAM_GROUPS)} groups | "
        f"gap:{TELEGRAM_JOIN_GAP_SECONDS}s"
    )
    joined  = 0
    skipped = 0
    failed  = 0

    for group in TARGET_TELEGRAM_GROUPS:
        try:
            target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
            client.loop.run_until_complete(client(JoinChannelRequest(target)))
            joined += 1
            log.info(f"Telegram: joined {target} [{joined}/{len(TARGET_TELEGRAM_GROUPS)}]")
            time.sleep(TELEGRAM_JOIN_GAP_SECONDS)
        except UserAlreadyParticipantError:
            skipped += 1
            log.debug(f"Telegram: already in {group} — skip")
        except FloodWaitError as e:
            log.warning(f"Telegram: FloodWait {e.seconds}s for {group} — waiting...")
            time.sleep(e.seconds + 5)
            failed += 1
        except (ChannelPrivateError, InviteHashExpiredError) as exc:
            log.warning(f"Telegram: cannot join {group} — {exc}")
            failed += 1
        except Exception as exc:
            log.error(f"Telegram: join error for {group} — {exc}")
            failed += 1

    log.info(
        f"Telegram auto-join complete | "
        f"joined:{joined} already_in:{skipped} failed:{failed}"
    )


TELEGRAM_POLL_INTERVAL = int(os.getenv("TELEGRAM_POLL_INTERVAL", "300"))


async def _poll_telegram_groups(client: TelegramClient):
    if TELEGRAM_POLL_INTERVAL == 0:
        log.info("[TELEGRAM-POLL] Disabled (TELEGRAM_POLL_INTERVAL=0) — listener-only mode.")
        return

    log.info(
        f"[TELEGRAM-POLL] Poller started | {len(TARGET_TELEGRAM_GROUPS)} groups | "
        f"interval:{TELEGRAM_POLL_INTERVAL}s"
    )

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for group in TARGET_TELEGRAM_GROUPS:
            try:
                target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
                messages = await client.get_messages(target, limit=20)

                for msg in messages:
                    if not msg or not msg.text or len(msg.text) < 5:
                        continue

                    chat_id = msg.chat_id if msg.chat_id else 0
                    msg_id  = msg.id

                    if _telegram_is_seen(chat_id, msg_id):
                        continue

                    sender   = await msg.get_sender()
                    tg_user  = getattr(sender, "username", None) or f"user_{getattr(sender, 'id', 0)}"

                    _tg_item = {
                        "message_id":     f"telegram_{chat_id}_{msg_id}",
                        "platform":       "telegram",
                        "content_type":   "message",
                        "text":           msg.text,
                        "username":       tg_user,
                        "display_name":   tg_user,
                        "subreddit":      "",
                        "telegram_group": group,
                        "post_url":       "",
                    }
                    telegram_queue.put(_tg_item)
                    save_queue_message("telegram", _tg_item)
                    total_new += 1

                if total_new:
                    log.info(f"[TELEGRAM-POLL] {group} → queued new messages")

                await asyncio.sleep(2)

            except FloodWaitError as e:
                log.warning(f"[TELEGRAM-POLL] FloodWait {e.seconds}s for {group}")
                await asyncio.sleep(e.seconds + 5)
                total_errors += 1
            except Exception as exc:
                log.error(f"[TELEGRAM-POLL] Error for {group}: {exc}")
                total_errors += 1

        save_seen_ids("telegram", _telegram_seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[TELEGRAM-POLL] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {TELEGRAM_POLL_INTERVAL}s..."
        )
        await asyncio.sleep(TELEGRAM_POLL_INTERVAL)


async def _run_telegram_listener(client: TelegramClient):
    target_set = set()
    for g in TARGET_TELEGRAM_GROUPS:
        clean = g.lstrip("@").lower()
        target_set.add(clean)

    @client.on(events.NewMessage)
    async def _on_message(event):
        try:
            chat = await event.get_chat()

            username_attr = getattr(chat, "username", None)
            chat_title    = getattr(chat, "title", "") or ""

            if username_attr:
                group_key = username_attr.lower()
            else:
                group_key = chat_title.lower().replace(" ", "").replace("-", "").replace("_", "")

            if group_key not in target_set:
                return

            sender    = await event.get_sender()
            text      = event.raw_text or ""
            sender_id = getattr(sender, "id", 0)
            first     = getattr(sender, "first_name", "") or ""
            last      = getattr(sender, "last_name", "") or ""
            tg_user   = getattr(sender, "username", None) or f"user_{sender_id}"
            msg_id    = event.id
            chat_id   = event.chat_id

            if not text or len(text) < 5:
                return

            if _telegram_is_seen(chat_id, msg_id):
                return

            _tg_item = {
                "message_id":     f"telegram_{chat_id}_{msg_id}",
                "platform":       "telegram",
                "content_type":   "message",
                "text":           text,
                "username":       tg_user,
                "display_name":   f"{first} {last}".strip() or tg_user,
                "subreddit":      "",
                "telegram_group": username_attr or chat_title,
                "post_url":       "",
            }
            telegram_queue.put(_tg_item)
            save_queue_message("telegram", _tg_item)

        except Exception as exc:
            log.error(f"Telegram message handler error: {exc}")

    log.info("Telegram listener active — read-only, no interactions.")

    await asyncio.gather(
        client.run_until_disconnected(),
        _poll_telegram_groups(client),
    )


def run_telegram_listener_thread():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning(
            "Telegram disabled — set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE"
        )
        return

    try:
        loop   = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        client = TelegramClient(
            TELEGRAM_SESSION,
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
            loop=loop,
        )

        loop.run_until_complete(client.start(phone=TELEGRAM_PHONE))
        me = loop.run_until_complete(client.get_me())
        log.info(
            f"Telegram authenticated as {me.first_name} "
            f"(@{me.username or me.id})"
        )

        _join_telegram_groups_sync(client)
        loop.run_until_complete(_run_telegram_listener(client))

    except Exception as exc:
        log.error(f"Telegram listener thread error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULERS — Daily Digest + Weekly Report
# v7.6.0: platform breakdown additionally counts "linkedin". Everything
# else unchanged.
# ─────────────────────────────────────────────────────────────────────────────

def send_daily_digest():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        signals = list(
            db.signals.find({
                "client_id":       CLIENT_ID,
                "intent_score":    {"$gte": 6, "$lte": 7},
                "created_at":      {"$gte": since},
                "digest_included": False,
            }).sort("intent_score", -1)
        )

        if not signals:
            log.info("Daily digest: no medium signals in past 24h.")
            return

        lines = []
        for s in signals:
            preview  = s["message_text"][:120]
            if len(s["message_text"]) > 120:
                preview += "..."
            corridor = s.get("corridor") or "—"
            pain     = s.get("pain_type") or "—"
            platform = s.get("platform", "?").upper()
            sub      = s.get("subreddit", "")
            grp      = s.get("telegram_group", "")
            source   = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)
            lines.append(
                f"• *{s.get('username','?')}* | Score:{s['intent_score']}/10 "
                f"| {platform} | {source}\n"
                f"  Corridor: {corridor} | Pain: {pain}\n"
                f"  _{preview}_\n"
                f"  ↳ {s['suggested_action']}"
            )

        date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
        joined   = "\n\n".join(lines)
        chunks   = [joined[i:i+2900] for i in range(0, len(joined), 2900)]

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"📋 Daily Signal Digest — {date_str}", "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*{len(signals)} medium intent signals* (score 6–7) in the past 24 hours:"}},
        ]
        for chunk in chunks:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": chunk}})
        blocks += [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.9.1 | Client: {CLIENT_ID} | Reddit + Twitter + Telegram + Facebook + LinkedIn"}]},
        ]

        result = retry_with_backoff(
            _post_to_slack, {"text": f"📋 Daily Signal Digest — {date_str}", "blocks": blocks},
            retries=3, delay=2, label="Digest",
        )
        if result:
            ids = [s["message_id"] for s in signals]
            db.signals.update_many({"message_id": {"$in": ids}}, {"$set": {"digest_included": True}})
            log.info(f"Daily digest sent | {len(signals)} signals.")

    except Exception as exc:
        log.error(f"Daily digest error: {exc}")


def send_weekly_report():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since         = datetime.now(timezone.utc) - timedelta(days=7)
        all_signals   = list(db.signals.find({"client_id": CLIENT_ID, "created_at": {"$gte": since}}))
        high          = [s for s in all_signals if s["intent_score"] >= 8]
        medium        = [s for s in all_signals if 6 <= s["intent_score"] <= 7]
        business      = [s for s in all_signals if s.get("is_business")]
        reddit_sigs   = [s for s in all_signals if s.get("platform") == "reddit"]
        twitter_sigs  = [s for s in all_signals if s.get("platform") == "twitter"]
        telegram_sigs = [s for s in all_signals if s.get("platform") == "telegram"]
        facebook_sigs = [s for s in all_signals if s.get("platform") == "facebook"]
        linkedin_sigs = [s for s in all_signals if s.get("platform") == "linkedin"]  # v7.6.0
        total         = len(all_signals)

        if total == 0:
            log.info("Weekly report: no signals this week.")
            return

        def breakdown(key):
            counts: dict = {}
            for s in all_signals:
                v = s.get(key)
                if v:
                    counts[v] = counts.get(v, 0) + 1
            return "\n".join(
                f"  • {k}: {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])
            ) or "_None_"

        top3       = sorted(high, key=lambda x: x["intent_score"], reverse=True)[:3]
        top3_lines = [
            f"• *{s.get('username','?')}* | Score:{s['intent_score']}/10 "
            f"| {s.get('platform','?').upper()} | {s.get('corridor') or 'Unknown corridor'}\n"
            f"  _{s['message_text'][:100]}{'...' if len(s['message_text'])>100 else ''}_"
            for s in top3
        ]

        week_start = since.strftime("%b %d")
        week_end   = datetime.now(timezone.utc).strftime("%b %d, %Y")

        payload = {
            "text": f"📊 Weekly Signal Report — {week_start} to {week_end}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"📊 Weekly Signal Report — {week_start} to {week_end}", "emoji": True}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Total Signals*\n{total}"},
                    {"type": "mrkdwn", "text": f"*High Intent (8–10)*\n{len(high)}"},
                    {"type": "mrkdwn", "text": f"*Medium Intent (6–7)*\n{len(medium)}"},
                    {"type": "mrkdwn", "text": f"*Business Owners*\n{len(business)}"},
                    {"type": "mrkdwn", "text": f"*Reddit*\n{len(reddit_sigs)}"},
                    {"type": "mrkdwn", "text": f"*Twitter/X*\n{len(twitter_sigs)}"},
                    {"type": "mrkdwn", "text": f"*Telegram*\n{len(telegram_sigs)}"},
                    {"type": "mrkdwn", "text": f"*Facebook*\n{len(facebook_sigs)}"},
                    {"type": "mrkdwn", "text": f"*LinkedIn*\n{len(linkedin_sigs)}"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Corridor Breakdown*\n{breakdown('corridor')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Competitor Mentions*\n{breakdown('competitor_mentioned')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Pain Types*\n{breakdown('pain_type')}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top 3 Signals This Week*\n\n{_safe(chr(10).join(top3_lines), 2800)}"}},
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.9.1 | {CLIENT_ID} | Week ending {week_end}"}]},
            ],
        }

        result = retry_with_backoff(_post_to_slack, payload, retries=3, delay=2, label="WeeklyReport")
        if result:
            log.info(
                f"Weekly report sent | Total:{total} High:{len(high)} Med:{len(medium)} "
                f"Biz:{len(business)} Reddit:{len(reddit_sigs)} "
                f"Twitter:{len(twitter_sigs)} Telegram:{len(telegram_sigs)} "
                f"Facebook:{len(facebook_sigs)} LinkedIn:{len(linkedin_sigs)}"
            )

    except Exception as exc:
        log.error(f"Weekly report error: {exc}")


async def run_scheduler():
    log.info(
        f"Scheduler started | digest:{DAILY_DIGEST_HOUR}:00 UTC | "
        f"report Mon {WEEKLY_REPORT_HOUR}:00 UTC"
    )
    last_digest_date = None

    persisted_week = _get_state("last_report_week")
    last_report_week: int | None = persisted_week

    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)

        if now.hour == DAILY_DIGEST_HOUR and now.date() != last_digest_date:
            log.info("Scheduler: triggering daily digest...")
            await asyncio.to_thread(send_daily_digest)
            last_digest_date = now.date()

        current_week = now.isocalendar()[1]
        if (
            now.weekday() == WEEKLY_REPORT_DAY
            and now.hour == WEEKLY_REPORT_HOUR
            and current_week != last_report_week
        ):
            log.info("Scheduler: triggering weekly report...")
            await asyncio.to_thread(send_weekly_report)
            last_report_week = current_week
            _set_state("last_report_week", current_week)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED (REDDIT_ENABLED=false) — skipping.")
        return

    _resumed_reddit = load_queue_messages("reddit")
    for _item in _resumed_reddit:
        reddit_queue.put(_item)
    if _resumed_reddit:
        log.info(
            f"[REDDIT] Resumed {len(_resumed_reddit)} queue message(s) "
            f"from MongoDB after restart — NOT lost."
        )

    rss_thread = threading.Thread(
        target=poll_reddit_rss, daemon=True, name="Reddit-RSS"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT",
              REDDIT_BATCH_GAP_SECONDS, REDDIT_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Reddit-Batch",
    )

    rss_thread.start()
    btch_thread.start()
    log.info(
        f"Reddit threads running: RSS-Poller ✅ | Batch ✅ | "
        f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not rss_thread.is_alive():
            log.error("Reddit RSS thread died — restarting...")
            rss_thread = threading.Thread(
                target=poll_reddit_rss, daemon=True, name="Reddit-RSS"
            )
            rss_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT",
                      REDDIT_BATCH_GAP_SECONDS, REDDIT_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener():
    if not TWITTER_ENABLED:
        log.warning("Twitter platform DISABLED (TWITTER_ENABLED=false) — skipping.")
        return

    client = build_twitter_client()
    if client is None:
        log.warning("Twitter listener not started — credentials missing.")
        return

    _resumed_twitter = load_queue_messages("twitter")
    for _item in _resumed_twitter:
        twitter_queue.put(_item)
    if _resumed_twitter:
        log.info(
            f"[TWITTER] Resumed {len(_resumed_twitter)} queue message(s) "
            f"from MongoDB after restart — NOT lost."
        )

    poll_thread = threading.Thread(
        target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER",
              TWITTER_BATCH_GAP_SECONDS, TWITTER_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Twitter-Batch",
    )

    poll_thread.start()
    btch_thread.start()
    log.info(
        f"Twitter threads running: Poll ✅ | Batch ✅ | "
        f"gap:{TWITTER_BATCH_GAP_SECONDS}s | timeout:{TWITTER_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("Twitter poll thread died — restarting...")
            poll_thread = threading.Thread(
                target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll"
            )
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("Twitter batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER",
                      TWITTER_BATCH_GAP_SECONDS, TWITTER_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_telegram_listener():
    if not TELEGRAM_ENABLED:
        log.warning("Telegram platform DISABLED (TELEGRAM_ENABLED=false) — skipping.")
        return

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning(
            "Telegram listener not started — "
            "set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE in .env"
        )
        return

    _resumed_telegram = load_queue_messages("telegram")
    for _item in _resumed_telegram:
        telegram_queue.put(_item)
    if _resumed_telegram:
        log.info(
            f"[TELEGRAM] Resumed {len(_resumed_telegram)} queue message(s) "
            f"from MongoDB after restart — NOT lost."
        )

    tg_thread = threading.Thread(
        target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM",
              TELEGRAM_BATCH_GAP_SECONDS, TELEGRAM_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Telegram-Batch",
    )

    tg_thread.start()
    btch_thread.start()
    log.info(
        f"Telegram threads running: Listener ✅ | Batch ✅ | "
        f"Poller {'✅' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled'} | "
        f"gap:{TELEGRAM_BATCH_GAP_SECONDS}s | timeout:{TELEGRAM_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not tg_thread.is_alive():
            log.error("Telegram listener thread died — restarting...")
            tg_thread = threading.Thread(
                target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener"
            )
            tg_thread.start()
        if not btch_thread.is_alive():
            log.error("Telegram batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM",
                      TELEGRAM_BATCH_GAP_SECONDS, TELEGRAM_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Telegram-Batch",
            )
            btch_thread.start()


async def start_facebook_listener():
    if not FACEBOOK_ENABLED:
        log.warning("Facebook platform DISABLED (FACEBOOK_ENABLED=false) — skipping.")
        return

    client = build_facebook_client()
    if client is None:
        log.warning("Facebook listener not started — credentials missing.")
        return

    _resumed_facebook = load_queue_messages("facebook")
    for _item in _resumed_facebook:
        facebook_queue.put(_item)
    if _resumed_facebook:
        log.info(
            f"[FACEBOOK] Resumed {len(_resumed_facebook)} queue message(s) "
            f"from MongoDB after restart — NOT lost."
        )

    poll_thread = threading.Thread(
        target=poll_facebook, args=(client,), daemon=True, name="Facebook-Poll"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(facebook_queue, FACEBOOK_BATCH_SIZE, "FACEBOOK",
              FACEBOOK_BATCH_GAP_SECONDS, FACEBOOK_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Facebook-Batch",
    )

    poll_thread.start()
    btch_thread.start()
    log.info(
        f"Facebook threads running: Poll ✅ | Batch ✅ | "
        f"gap:{FACEBOOK_BATCH_GAP_SECONDS}s | timeout:{FACEBOOK_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("Facebook poll thread died — restarting...")
            poll_thread = threading.Thread(
                target=poll_facebook, args=(client,), daemon=True, name="Facebook-Poll"
            )
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("Facebook batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(facebook_queue, FACEBOOK_BATCH_SIZE, "FACEBOOK",
                      FACEBOOK_BATCH_GAP_SECONDS, FACEBOOK_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Facebook-Batch",
            )
            btch_thread.start()


async def start_linkedin_listener():
    """v7.6.0 NEW — mirrors start_facebook_listener() exactly."""
    if not LINKEDIN_ENABLED:
        log.warning("LinkedIn platform DISABLED (LINKEDIN_ENABLED=false) — skipping.")
        return

    client = build_linkedin_client()
    if client is None:
        log.warning("LinkedIn listener not started — credentials missing.")
        return

    _resumed_linkedin = load_queue_messages("linkedin")
    for _item in _resumed_linkedin:
        linkedin_queue.put(_item)
    if _resumed_linkedin:
        log.info(
            f"[LINKEDIN] Resumed {len(_resumed_linkedin)} queue message(s) "
            f"from MongoDB after restart — NOT lost."
        )

    poll_thread = threading.Thread(
        target=poll_linkedin, args=(client,), daemon=True, name="LinkedIn-Poll"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(linkedin_queue, LINKEDIN_BATCH_SIZE, "LINKEDIN",
              LINKEDIN_BATCH_GAP_SECONDS, LINKEDIN_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="LinkedIn-Batch",
    )

    poll_thread.start()
    btch_thread.start()
    log.info(
        f"LinkedIn threads running: Poll ✅ | Batch ✅ | "
        f"gap:{LINKEDIN_BATCH_GAP_SECONDS}s | timeout:{LINKEDIN_BATCH_TIMEOUT_SECONDS}s"
    )

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("LinkedIn poll thread died — restarting...")
            poll_thread = threading.Thread(
                target=poll_linkedin, args=(client,), daemon=True, name="LinkedIn-Poll"
            )
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("LinkedIn batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(linkedin_queue, LINKEDIN_BATCH_SIZE, "LINKEDIN",
                      LINKEDIN_BATCH_GAP_SECONDS, LINKEDIN_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="LinkedIn-Batch",
            )
            btch_thread.start()


async def start_rescore_listener():
    rescore_thread = threading.Thread(
        target=run_rescore_processor, daemon=True, name="Rescore-Processor"
    )
    rescore_thread.start()
    log.info("Rescore processor thread running ✅")

    while True:
        await asyncio.sleep(60)
        if not rescore_thread.is_alive():
            log.error("Rescore processor thread died — restarting...")
            rescore_thread = threading.Thread(
                target=run_rescore_processor, daemon=True, name="Rescore-Processor"
            )
            rescore_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI — REST API
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v7.9.1",
    description = (
        "Reddit (RSS) + Twitter + Telegram + Facebook + LinkedIn signals: "
        "monitor, score, store, alert. Persistent batch state. Streaming "
        "Claude. Manual rescore. HubSpot receives medium (4-7) AND high "
        "(8-10) signals. Per-platform BATCH_GAP_SECONDS and "
        "BATCH_TIMEOUT_SECONDS — every platform runs on its own "
        "independent gap/timeout, never mixed. Twitter search runs in "
        "TWITTER_CHUNK_SIZE-keyword chunks, one chunk per poll cycle."
    ),
    version     = "7.9.1",
)


def _serialise(signals: list) -> list:
    for s in signals:
        s.pop("_id", None)
        for f in ["created_at", "alerted_slack_at", "alerted_hubspot_at", "rescored_at"]:
            if f in s and s[f] is not None:
                s[f] = s[f].isoformat()
    return signals


def _serialise_rescore(docs: list) -> list:
    for d in docs:
        d["_id"] = str(d["_id"])
        for f in ["requested_at", "processed_at"]:
            if f in d and d[f] is not None:
                d[f] = d[f].isoformat()
    return docs


@app.get("/")
def root():
    return {
        "status":                  "running",
        "system":                  "FLINTEL v7.9.1",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter", "telegram", "facebook", "linkedin"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED),
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_RAPID_API_KEYS)),
        "telegram_enabled":        TELEGRAM_ENABLED,
        "telegram_status":         _working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)),
        "facebook_enabled":        FACEBOOK_ENABLED,
        "facebook_status":         _working(FACEBOOK_ENABLED and bool(RAPID_API_KEY)),
        "linkedin_enabled":        LINKEDIN_ENABLED,
        "linkedin_status":         _working(LINKEDIN_ENABLED and bool(RAPID_API_KEY)),
        "reddit_mode":             "feedparser RSS (no credentials required)",
        "reddit_poll_interval":    REDDIT_POLL_INTERVAL,
        "reddit_batch_size":       REDDIT_BATCH_SIZE,
        "twitter_batch_size":      TWITTER_BATCH_SIZE,
        "telegram_batch_size":     TELEGRAM_BATCH_SIZE,
        "facebook_batch_size":     FACEBOOK_BATCH_SIZE,
        "linkedin_batch_size":     LINKEDIN_BATCH_SIZE,
        "rescore_batch_size":      RESCORE_BATCH_SIZE,
        "telegram_poll_interval":  TELEGRAM_POLL_INTERVAL,
        "facebook_poll_interval":  FACEBOOK_POLL_INTERVAL,
        "linkedin_poll_interval":  LINKEDIN_POLL_INTERVAL,
        "twitter_rapid_api_keys_configured": len(TWITTER_RAPID_API_KEYS),
        "twitter_chunk_size":       TWITTER_CHUNK_SIZE,
        "twitter_total_chunks":     len(TWITTER_SEARCH_CHUNKS),
        "reddit_batch_gap_s":       REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":   REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter_batch_gap_s":      TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s":  TWITTER_BATCH_TIMEOUT_SECONDS,
        "telegram_batch_gap_s":     TELEGRAM_BATCH_GAP_SECONDS,
        "telegram_batch_timeout_s": TELEGRAM_BATCH_TIMEOUT_SECONDS,
        "facebook_batch_gap_s":     FACEBOOK_BATCH_GAP_SECONDS,
        "facebook_batch_timeout_s": FACEBOOK_BATCH_TIMEOUT_SECONDS,
        "linkedin_batch_gap_s":     LINKEDIN_BATCH_GAP_SECONDS,
        "linkedin_batch_timeout_s": LINKEDIN_BATCH_TIMEOUT_SECONDS,
        "rescore_batch_gap_s":      RESCORE_BATCH_GAP_SECONDS,
        "max_tokens":              MAX_TOKENS,
        "claude_stream_timeout_s": CLAUDE_STREAM_TIMEOUT,
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "facebook_queue_size":     facebook_queue.qsize(),
        "linkedin_queue_size":     linkedin_queue.qsize(),
        "telegram_groups":         len(TARGET_TELEGRAM_GROUPS),
        "auth_required":           bool(API_KEY),
        "output_schema":           "platform-specific (v7.2 cost optimisation, unchanged)",
        "persistent_batch_state":  True,
        "persistent_queue_messages": True,
        "persistent_batch_seconds": True,
        "partial_json_recovery":   True,
        "claude_streaming":        True,
        "rescore_enabled":         True,
        "hubspot_error_visibility": True,
        "hubspot_medium_signals":  True,
        "slack_hubspot_score_hidden": True,
        "per_platform_batch_timing": True,
        "linkedin_enrichment":     True,
        "twitter_chunked_search":  True,
        "twitter_rapid_api_failover": True,
        "min_score_medium":        MIN_SCORE_MEDIUM,
        "min_score_high":          MIN_SCORE_HIGH,
        "score_routing": {
            "1-3":  "MongoDB only",
            "4-7":  "MongoDB + Slack + HubSpot (score number hidden in message/note)",
            "8-10": "MongoDB + Slack + HubSpot (score number hidden in message/note)",
        },
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    reddit_working   = REDDIT_ENABLED
    twitter_working  = TWITTER_ENABLED and bool(TWITTER_RAPID_API_KEYS)
    telegram_working = TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)
    facebook_working = FACEBOOK_ENABLED and bool(RAPID_API_KEY)
    linkedin_working = LINKEDIN_ENABLED and bool(RAPID_API_KEY)

    pending_rescore = 0
    try:
        pending_rescore = db.flintel_rescore_messages.count_documents({"status": "pending"})
    except Exception:
        pass

    return {
        "status":                  "ok",
        "mongodb":                 mongo,
        "reddit":                  ("polling-rss" if REDDIT_ENABLED else "disabled"),
        "reddit_working":          reddit_working,
        "reddit_indicator":        _working(reddit_working),
        "reddit_batch_gap_s":      REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":  REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter":                 ("polling" if twitter_working else "disabled"),
        "twitter_working":         twitter_working,
        "twitter_indicator":       _working(twitter_working),
        "twitter_rapid_api_keys":  len(TWITTER_RAPID_API_KEYS),
        "twitter_chunk_size":      TWITTER_CHUNK_SIZE,
        "twitter_total_chunks":    len(TWITTER_SEARCH_CHUNKS),
        "twitter_batch_gap_s":     TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s": TWITTER_BATCH_TIMEOUT_SECONDS,
        "telegram":                ("listening" if telegram_working else "disabled"),
        "telegram_working":        telegram_working,
        "telegram_indicator":      _working(telegram_working),
        "telegram_batch_gap_s":    TELEGRAM_BATCH_GAP_SECONDS,
        "telegram_batch_timeout_s": TELEGRAM_BATCH_TIMEOUT_SECONDS,
        "facebook":                ("polling" if facebook_working else "disabled"),
        "facebook_working":        facebook_working,
        "facebook_indicator":      _working(facebook_working),
        "facebook_batch_gap_s":    FACEBOOK_BATCH_GAP_SECONDS,
        "facebook_batch_timeout_s": FACEBOOK_BATCH_TIMEOUT_SECONDS,
        "linkedin":                ("polling" if linkedin_working else "disabled"),
        "linkedin_working":        linkedin_working,
        "linkedin_indicator":      _working(linkedin_working),
        "linkedin_batch_gap_s":    LINKEDIN_BATCH_GAP_SECONDS,
        "linkedin_batch_timeout_s": LINKEDIN_BATCH_TIMEOUT_SECONDS,
        "hubspot_configured":      bool(HUBSPOT_API_KEY),
        "hubspot_indicator":       _working(bool(HUBSPOT_API_KEY)),
        "hubspot_medium_signals":  True,
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "facebook_queue_size":     facebook_queue.qsize(),
        "linkedin_queue_size":     linkedin_queue.qsize(),
        "rescore_pending":         pending_rescore,
        "rescore_working":         True,
        "rescore_indicator":       _working(True),
        "rescore_batch_gap_s":     RESCORE_BATCH_GAP_SECONDS,
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


@app.get("/hubspot/properties-check", dependencies=[Depends(verify_api_key)])
def get_hubspot_properties_check():
    if not HUBSPOT_API_KEY:
        return {"configured": False, "detail": "HUBSPOT_API_KEY not set."}
    try:
        r = requests.get(
            f"{HUBSPOT_BASE}/crm/v3/properties/contacts",
            headers=_hs_headers(),
            timeout=10,
        )
        r.raise_for_status()
        existing = {p.get("name") for p in r.json().get("results", [])}
        missing  = [p for p in _HUBSPOT_REQUIRED_CONTACT_PROPERTIES if p not in existing]
        return {
            "configured":          True,
            "required_properties": _HUBSPOT_REQUIRED_CONTACT_PROPERTIES,
            "missing_properties":  missing,
            "all_present":         len(missing) == 0,
        }
    except requests.exceptions.HTTPError as exc:
        body = None
        try:
            body = exc.response.text if exc.response is not None else None
        except Exception:
            body = None
        raise HTTPException(
            status_code=502,
            detail=f"HubSpot API error: {exc} | body: {body}",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/rescore", dependencies=[Depends(verify_api_key)])
def post_rescore(
    message_ids: list = Body(..., description="List of message_id strings to rescore"),
    operator_note: str = Body("", description="Optional operator note"),
):
    if not message_ids:
        raise HTTPException(status_code=400, detail="message_ids list is empty.")

    missing = []
    for mid in message_ids:
        if not db.signals.find_one({"message_id": mid}, {"_id": 1}):
            missing.append(mid)

    if missing:
        raise HTTPException(
            status_code=404,
            detail=f"Signal(s) not found in DB: {missing}. Cannot queue for rescore.",
        )

    req_ids = _rescore_queue_requests(message_ids, operator_note=operator_note)
    return {
        "queued":       len(req_ids),
        "request_ids":  req_ids,
        "message_ids":  message_ids,
        "operator_note": operator_note,
        "status":       "pending",
        "note":         "Rescore processor will pick these up within the next poll interval.",
    }


@app.get("/rescore/pending", dependencies=[Depends(verify_api_key)])
def get_rescore_pending(limit: int = 50):
    try:
        docs = list(
            db.flintel_rescore_messages.find(
                {"status": {"$in": ["pending", "processing"]}}
            ).sort("requested_at", ASCENDING).limit(limit)
        )
        return {"count": len(docs), "requests": _serialise_rescore(docs)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/rescore/history", dependencies=[Depends(verify_api_key)])
def get_rescore_history(limit: int = 100, status: str = None):
    try:
        query = {}
        if status:
            query["status"] = status
        else:
            query["status"] = {"$in": ["done", "error"]}
        docs = list(
            db.flintel_rescore_messages.find(query)
            .sort("processed_at", -1)
            .limit(limit)
        )
        return {"count": len(docs), "requests": _serialise_rescore(docs)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/rescore/status/{req_id}", dependencies=[Depends(verify_api_key)])
def get_rescore_status(req_id: str):
    try:
        from bson import ObjectId
        doc = db.flintel_rescore_messages.find_one({"_id": ObjectId(req_id)})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Rescore request not found: {req_id}")
        return _serialise_rescore([doc])[0]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/pending-batch", dependencies=[Depends(verify_api_key)])
def get_pending_batch():
    try:
        docs = list(db.flintel_pending_batch.find({}, {"_id": 0}))
        for d in docs:
            if d.get("batch_start_time"):
                d["batch_start_time"] = d["batch_start_time"].isoformat()
            if d.get("updated_at"):
                d["updated_at"] = d["updated_at"].isoformat()
            d["item_count"] = len(d.get("items", []))
        return {"pending_batches": docs}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals", dependencies=[Depends(verify_api_key)])
def get_signals(
    limit:       int  = 50,
    platform:    str  = None,
    category:    str  = None,
    min_score:   int  = None,
    subreddit:   str  = None,
    tg_group:    str  = None,
    tier:        str  = None,
    corridor:    str  = None,
    pain_type:   str  = None,
    is_business: bool = None,
):
    try:
        q: dict = {"client_id": CLIENT_ID}
        if platform:    q["platform"]        = platform
        if category:    q["signal_category"] = category
        if min_score is not None: q["intent_score"] = {"$gte": min_score}
        if subreddit:   q["subreddit"]       = subreddit
        if tg_group:    q["telegram_group"]  = {"$regex": tg_group, "$options": "i"}
        if tier:        q["tier"]            = tier
        if corridor:    q["corridor"]        = {"$regex": corridor, "$options": "i"}
        if pain_type:   q["pain_type"]       = pain_type
        if is_business is not None: q["is_business"] = is_business

        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/stats", dependencies=[Depends(verify_api_key)])
def get_stats():
    try:
        total    = db.signals.count_documents({"client_id": CLIENT_ID})
        biz      = db.signals.count_documents({"client_id": CLIENT_ID, "is_business": True})
        reddit   = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "reddit"})
        twitter  = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "twitter"})
        telegram = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "telegram"})
        facebook = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "facebook"})
        linkedin = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "linkedin"})
        rescored = db.signals.count_documents({"client_id": CLIENT_ID, "rescored_at": {"$exists": True}})

        def agg(group_field):
            return list(db.signals.aggregate([
                {"$match": {"client_id": CLIENT_ID, group_field: {"$ne": None}}},
                {"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]))

        return {
            "total_signals":    total,
            "business_owners":  biz,
            "reddit_signals":   reddit,
            "twitter_signals":  twitter,
            "telegram_signals": telegram,
            "facebook_signals": facebook,
            "linkedin_signals": linkedin,
            "rescored_signals": rescored,
            "corridors":        agg("corridor"),
            "pain_types":       agg("pain_type"),
            "competitors":      agg("competitor_mentioned"),
            "tiers":            agg("tier"),
            "reddit_queue":     reddit_queue.qsize(),
            "twitter_queue":    twitter_queue.qsize(),
            "telegram_queue":   telegram_queue.qsize(),
            "facebook_queue":   facebook_queue.qsize(),
            "linkedin_queue":   linkedin_queue.qsize(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/high-intent", dependencies=[Depends(verify_api_key)])
def get_high_intent(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$gte": 8}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/digest", dependencies=[Depends(verify_api_key)])
def get_digest(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$gte": 6, "$lte": 7}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/business", dependencies=[Depends(verify_api_key)])
def get_business(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "is_business": True}, {"_id": 0}
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/outreach", dependencies=[Depends(verify_api_key)])
def get_outreach(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {
                    "client_id":    CLIENT_ID,
                    "intent_score": {"$gte": 5},
                    "$or": [
                        {"twitter_reply":    {"$ne": None}},
                        {"twitter_dm":       {"$ne": None}},
                        {"linkedin_message": {"$ne": None}},
                        {"telegram_dm":      {"$ne": None}},
                        {"facebook_comment": {"$ne": None}},
                        {"linkedin_reply":   {"$ne": None}},  # v7.6.0
                        {"linkedin_dm":      {"$ne": None}},  # v7.6.0
                    ],
                },
                {"_id": 0},
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/twitter", dependencies=[Depends(verify_api_key)])
def get_twitter_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "twitter"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/reddit", dependencies=[Depends(verify_api_key)])
def get_reddit_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "reddit"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/telegram", dependencies=[Depends(verify_api_key)])
def get_telegram_signals(limit: int = 50, min_score: int = None, group: str = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "telegram"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        if group:
            q["telegram_group"] = {"$regex": group, "$options": "i"}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/facebook", dependencies=[Depends(verify_api_key)])
def get_facebook_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "facebook"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/linkedin", dependencies=[Depends(verify_api_key)])
def get_linkedin_signals(limit: int = 50, min_score: int = None):
    """v7.6.0 NEW — mirrors /signals/facebook exactly, filtered to
    platform == "linkedin". Returns the full enrichment fields (email,
    phone, location, company, etc) alongside the usual signal fields."""
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "linkedin"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/corridors", dependencies=[Depends(verify_api_key)])
def get_by_corridor(corridor: str, limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "corridor": {"$regex": corridor, "$options": "i"}},
                {"_id": 0},
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "corridor": corridor, "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/watchlist", dependencies=[Depends(verify_api_key)])
def get_watchlist(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "watchlist": True}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/silent", dependencies=[Depends(verify_api_key)])
def get_silent_signals(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$lte": 5}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/rescored", dependencies=[Depends(verify_api_key)])
def get_rescored_signals(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "rescored_at": {"$exists": True}},
                {"_id": 0},
            ).sort("rescored_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


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
        start_telegram_listener(),
        start_facebook_listener(),
        start_linkedin_listener(),   # v7.6.0 NEW
        start_rescore_listener(),
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v7.9.1")
    log.info("=" * 70)
    log.info(f"  Client             : {CLIENT_ID}")
    log.info(f"  Platforms          : Reddit (RSS) + Twitter/X + Telegram + Facebook + LinkedIn")
    log.info(f"  Reddit             : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED)}")
    log.info(f"  Twitter            : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_RAPID_API_KEYS))}")
    log.info(f"  Twitter keys       : {len(TWITTER_RAPID_API_KEYS)} RapidAPI key(s) configured for failover")
    log.info(f"  Twitter chunking   : {TWITTER_CHUNK_SIZE} keyword(s)/chunk | {len(TWITTER_SEARCH_CHUNKS)} total chunk(s) | 1 chunk sent per poll cycle")
    log.info(f"  Telegram           : {TELEGRAM_ENABLED} | {_working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID))}")
    log.info(f"  Facebook           : {FACEBOOK_ENABLED} | {_working(FACEBOOK_ENABLED and bool(RAPID_API_KEY))}")
    log.info(f"  LinkedIn           : {LINKEDIN_ENABLED} | {_working(LINKEDIN_ENABLED and bool(RAPID_API_KEY))}")
    log.info(f"  LinkedIn mode      : linkedin-data-scraper1 RapidAPI — search_linkedIn.php per keyword,")
    log.info(f"                     : + get_user_data.php + get_company_data.php enrichment per match")
    log.info(f"  LinkedIn poll gap  : {LINKEDIN_POLL_INTERVAL}s between full keyword cycles | {LINKEDIN_KEYWORD_GAP_SECONDS}s between keywords")
    log.info(f"  Reddit batch       : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch      : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Telegram batch     : {TELEGRAM_BATCH_SIZE} items OR {TELEGRAM_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {TELEGRAM_BATCH_GAP_SECONDS}s")
    log.info(f"  Facebook batch     : {FACEBOOK_BATCH_SIZE} items OR {FACEBOOK_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {FACEBOOK_BATCH_GAP_SECONDS}s")
    log.info(f"  LinkedIn batch     : {LINKEDIN_BATCH_SIZE} items OR {LINKEDIN_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {LINKEDIN_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch      : {RESCORE_BATCH_SIZE} items per Claude call | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Batch timing       : per-platform, independent — no platform shares gap/timeout with another")
    log.info(f"  max_tokens         : {MAX_TOKENS}")
    log.info(f"  Claude streaming   : True | {_working(True)}")
    log.info(f"  Score 1-3          : SILENT SAVE — MongoDB only, no alerts")
    log.info(f"  Score {MIN_SCORE_MEDIUM}-{MIN_SCORE_HIGH-1}          : MEDIUM — MongoDB + Slack + HubSpot (number hidden in message)")
    log.info(f"  Score {MIN_SCORE_HIGH}-10         : HIGH   — MongoDB + Slack + HubSpot (number hidden in message)")
    log.info(f"  MIN_SCORE_MEDIUM   : {MIN_SCORE_MEDIUM} (env-configurable)")
    log.info(f"  MIN_SCORE_HIGH     : {MIN_SCORE_HIGH} (env-configurable)")
    log.info(f"  MongoDB            : ALL scores 1-10 saved, nothing discarded")
    log.info(f"  Platform isolation : Reddit/Twitter/Telegram/Facebook/LinkedIn NEVER mixed")
    log.info(f"  Deduplication      : Persistent (MongoDB flintel_seen_ids) — survives restarts")
    log.info(f"  Batch state        : Persistent (MongoDB flintel_pending_batch) — survives restarts")
    log.info(f"  Queue messages     : Persistent (MongoDB flintel_queue_messages) — survives restarts")
    log.info(f"  Batch timeout      : Persistent (MongoDB flintel_batch_seconds) — survives restarts")
    log.info(f"  Twitter chunk pos  : Persistent (MongoDB flintel_state key=twitter_chunk_index) — survives restarts")
    log.info(f"  Rescore            : True | {_working(True)} — flintel_rescore_messages collection")
    log.info(f"  Rescore poll       : every {RESCORE_POLL_INTERVAL}s")
    log.info(f"  API auth           : {'True | ' + _working(True) + ' (API_KEY set)' if API_KEY else 'False | ' + _working(False) + ' (API_KEY not set — open access)'}")
    log.info(f"  Daily digest       : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report      : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits         : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Telegram groups    : {len(TARGET_TELEGRAM_GROUPS)} configured")
    log.info(f"  Keywords           : {len(KEYWORDS)} filters (shared by all 5 platforms) — FILL THIS LIST IN")
    log.info(f"  MongoDB DB         : {MONGODB_DB}")
    log.info(f"  HubSpot            : {'True | ' + _working(True) if HUBSPOT_API_KEY else 'False | ' + _working(False) + ' — set HUBSPOT_API_KEY'}")
    log.info(f"  Slack              : {'True | ' + _working(True) if SLACK_WEBHOOK_URL else 'False | ' + _working(False) + ' — set SLACK_WEBHOOK_URL'}")
    log.info(f"  v7.9.1 change      : Twitter search chunked into {TWITTER_CHUNK_SIZE}-keyword OR-queries — fixes")
    log.info(f"                     : HTTP 414 Request-URI Too Large from v7.9.0's single giant query. ONE chunk")
    log.info(f"                     : is sent per TWITTER_POLL_INTERVAL, advancing through all {len(TWITTER_SEARCH_CHUNKS)} chunk(s)")
    log.info(f"                     : and wrapping back to chunk 1 once the full list is covered. Chunk position")
    log.info(f"                     : persists across restarts. RapidAPI key failover (429/403) unchanged from")
    log.info(f"                     : v7.9.0. Reddit, Telegram, Facebook, LinkedIn, scoring, storage, delivery,")
    log.info(f"                     : and every FastAPI route are 100% untouched by this change.")
    log.info("=" * 70)

    _hs_verify_properties()

    asyncio.run(main())
