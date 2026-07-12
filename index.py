"""
FX Signal Intelligence System — FLINTEL v7.5.0
=============================================
Platforms : Reddit (feedparser RSS) + Twitter/X (tweepy v2) + Telegram (Telethon) + Facebook (facebook-scraper3 RapidAPI)

Changelog v7.5.0 (ONE ADDITIVE CHANGE ONLY — everything else 100% unchanged from v7.4.5):

  CHANGE — FACEBOOK ADDED AS A 4TH PLATFORM.

           Facebook is wired in exactly like Reddit/Twitter/Telegram — its own
           in-memory queue (facebook_queue), its own persistent dedup set
           (flintel_seen_ids, platform="facebook"), its own persistent batch
           state (flintel_pending_batch / flintel_batch_seconds, platform=
           "facebook"), its own persistent raw-queue backup
           (flintel_queue_messages, platform="facebook"), its own
           run_batch_processor() thread using the SAME generic batch
           processor function Reddit/Twitter/Telegram already use, its own
           Claude scoring schema (CLAUDE_SYSTEM_PROMPT_FACEBOOK — same
           _SCORING_CORE, same thresholds, same routing), and its own
           FastAPI surface (/signals/facebook) plus root/health reporting.
           Nothing about Reddit, Twitter, or Telegram's code paths, timing,
           or state changed — Facebook is purely additive, following the
           identical pattern used by every other platform.

           Facebook uses the facebook-scraper3 RapidAPI product
           (x-rapidapi-host: facebook-scraper3.p.rapidapi.com,
           GET /search/posts) — it works by cycling through the SAME
           KEYWORDS list already used for keyword pre-filtering on the
           other platforms, issuing one search/posts request per keyword
           per poll cycle, exactly like Reddit's per-subreddit cycle.
           New env vars (defaults match the other platforms' patterns):

             FACEBOOK_ENABLED               (default: false)
             FACEBOOK_BATCH_SIZE            (default: 10)
             FACEBOOK_BATCH_GAP_SECONDS     (default: 30)
             FACEBOOK_BATCH_TIMEOUT_SECONDS (default: 120)
             FACEBOOK_POLL_INTERVAL         (default: 300 — full keyword cycle)
             FACEBOOK_KEYWORD_GAP_SECONDS   (default: 2 — between each keyword search)

           NOTHING ELSE CHANGED. Reddit, Twitter, Telegram scoring logic,
           prompts, MongoDB signal storage, HubSpot fields, Slack formatting,
           FastAPI routes, thresholds (MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8),
           keyword list, per-platform BATCH_GAP_SECONDS/BATCH_TIMEOUT_SECONDS,
           FIX A/B/C/D/E, the rescore feature, and v7.4.5's queue/batch-seconds
           persistence are byte-for-byte identical to v7.4.5.

Changelog v7.4.5 (THREE ADDITIVE CHANGES ONLY — everything else 100% unchanged from v7.4.4):

  CHANGE 1 — BATCH COMPLETION LOG NOW SHOWS ITEM COUNT.
             The "[PLATFORM] BATCH N DONE" log line is now
             "[PLATFORM] BATCH N COMPLETE — X item(s) completed | waiting Ys...",
             where X = len(batch_to_send) — the number of items that batch
             actually processed. Purely a log-text change; no control flow,
             no scoring, no routing touched.

  CHANGE 2 — NEW COLLECTION: flintel_queue_messages (persistent raw queue).
             reddit_queue / twitter_queue / telegram_queue are in-memory
             queue.Queue objects — everything a poller fetches is put()
             into one of these BEFORE the keyword filter or batching ever
             runs. Previously, if the process restarted while items sat in
             that in-memory queue, they were silently lost.

             Fix: every item is now also persisted to MongoDB the instant
             it's put() into a queue (save_queue_message), and removed the
             instant it's dequeued by run_batch_processor
             (remove_queue_message) — regardless of whether it then gets
             filtered out or matched into a batch. On startup,
             start_reddit_listener() / start_twitter_listener() /
             start_telegram_listener() call load_queue_messages() and
             re-put() everything still persisted back into that platform's
             queue BEFORE the poller/batch threads start. Nothing fetched
             is lost across a restart.

  CHANGE 3 — NEW COLLECTION: flintel_batch_seconds (explicit batch-timeout
             persistence). batch_start_time was already being persisted
             inside flintel_pending_batch (FIX A, v7.3) — v7.4.5 additionally
             writes it to its own dedicated flintel_batch_seconds collection
             (save_batch_seconds / clear_batch_seconds) at every point
             batch_start_time changes in run_batch_processor. This is an
             explicit, separately-named store for the timeout clock, as
             requested — flintel_pending_batch remains the authoritative
             source read on startup (load_pending_batch), so resume
             behavior after a restart is unchanged: a partially-filled
             batch's timeout continues counting from where it left off,
             never resets to zero.

  NOTHING ELSE CHANGED. Scoring logic, prompts, MongoDB signal storage,
  HubSpot fields (including the v7.4.3 hidden-score-number note/Slack
  text), FastAPI routes, thresholds (MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8),
  keyword list, per-platform BATCH_GAP_SECONDS/BATCH_TIMEOUT_SECONDS
  (v7.4.4), FIX A/B/C/D/E, and the rescore feature are byte-for-byte
  identical to v7.4.4.

Changelog v7.4.4 (ONE CHANGE ONLY — everything else 100% unchanged from v7.4.3):

  CHANGE — PER-PLATFORM BATCH_GAP_SECONDS AND BATCH_TIMEOUT_SECONDS.

           In v7.4.3, BATCH_GAP_SECONDS and BATCH_TIMEOUT_SECONDS were single
           global values shared by all three platforms (Reddit, Twitter,
           Telegram). This meant changing the gap or timeout for one platform
           changed it for all three — there was no way to tune Reddit's batch
           timing independently of Twitter's or Telegram's.

           Fix: replaced the two global env vars with six platform-specific
           env vars:

             REDDIT_BATCH_GAP_SECONDS      (default: old BATCH_GAP_SECONDS default, 30)
             REDDIT_BATCH_TIMEOUT_SECONDS  (default: old BATCH_TIMEOUT_SECONDS default, 120)

             TWITTER_BATCH_GAP_SECONDS     (default: 30)
             TWITTER_BATCH_TIMEOUT_SECONDS (default: 120)

             TELEGRAM_BATCH_GAP_SECONDS     (default: 30)
             TELEGRAM_BATCH_TIMEOUT_SECONDS (default: 120)

           run_batch_processor() now accepts gap_seconds and timeout_seconds
           as parameters (instead of reading the old global BATCH_GAP_SECONDS
           / BATCH_TIMEOUT_SECONDS module-level constants directly), and each
           of start_reddit_listener() / start_twitter_listener() /
           start_telegram_listener() passes its OWN platform-specific gap and
           timeout values. No platform's timing can leak into another
           platform's — Reddit fires and sleeps strictly on Reddit's own
           numbers, Twitter on its own, Telegram on its own. Nothing is
           shared, nothing is mixed.

           RESCORE_BATCH_GAP_SECONDS / RESCORE's own BATCH_GAP_SECONDS
           usage in run_rescore_processor() is likewise now its own
           dedicated variable (RESCORE_BATCH_GAP_SECONDS, default: 30) so it
           is not tied to Reddit/Twitter/Telegram's gap either. Rescore's
           batch SIZE (RESCORE_BATCH_SIZE) was already independent since
           v7.4 and is untouched.

           BATCH_SIZE variables (REDDIT_BATCH_SIZE, TWITTER_BATCH_SIZE,
           TELEGRAM_BATCH_SIZE, RESCORE_BATCH_SIZE) were ALREADY
           platform-specific since v7.2/v7.4 and are completely untouched —
           same names, same defaults, same behavior.

           NOTHING ELSE CHANGED. Scoring logic, prompts, MongoDB storage,
           HubSpot fields (including the v7.4.3 hidden-score-number note/
           Slack text), FastAPI routes, thresholds (MIN_SCORE_MEDIUM=4,
           MIN_SCORE_HIGH=8), keyword list, FIX A (persistent batch state),
           FIX B (partial-JSON recovery), FIX C (Claude streaming), FIX D
           (working indicators), FIX E (HubSpot error visibility + startup
           property check), and the rescore feature's polling/queueing logic
           are byte-for-byte identical to v7.4.3.

Changelog v7.4.3 (ONE CHANGE ONLY — everything else 100% unchanged from v7.4.2):

  CHANGE — NUMERIC SCORE NO LONGER DISPLAYED IN SLACK MESSAGE OR HUBSPOT NOTE.

           In v7.4.2, the Slack alert header/body and the HubSpot note both
           showed the raw "intent_score" number (e.g. "Score 9/10"). This
           change removes that NUMBER from what is rendered to a human
           reading Slack or HubSpot. The underlying score:

             — is still computed by Claude exactly as before
             — is still stored in MongoDB on every signal, exactly as before
               (nothing changed in save_signal / update_signal)
             — still drives ALL routing decisions exactly as before
               (MIN_SCORE_MEDIUM / MIN_SCORE_HIGH, Slack-or-not,
               HubSpot-or-not — all unchanged, still score-driven)
             — is still sent to HubSpot as the "fx_intent_score" CONTACT
               PROPERTY (so it remains queryable/filterable inside HubSpot
               CRM lists/workflows) — only the human-readable NOTE text and
               the Slack message text no longer print the number

           Routing is 100% unchanged from v7.4.2:
             Score 1-3  → MongoDB only. No Slack. No HubSpot.
             Score 4-7  → MongoDB + Slack + HubSpot (MEDIUM, Slack/Digest tier).
             Score 8-10 → MongoDB + Slack + HubSpot (HIGH tier).

           What changed concretely (both purely cosmetic, no control flow):
             1. send_slack_alert(): header text and the "Score" field in the
                Slack block no longer include the intent_score number. Tier
                label (MEDIUM/HIGH) and category are still shown so the
                signal's priority is still obvious at a glance — just not
                the literal X/10 number.
             2. _hs_create_note(): the HubSpot note body text no longer
                prints "Score: X/10". The note still includes everything
                else (tier, category, corridor, amount, etc).

           NOTHING ELSE CHANGED. Scoring logic, prompts, MongoDB storage
           (including the stored intent_score field on every document),
           HubSpot fx_intent_score CONTACT PROPERTY, FastAPI routes
           (including /signals which still returns intent_score in the
           JSON, since that is an API for operators, not a "human-facing
           display"), thresholds (MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8),
           keyword list, FIX A (persistent batch state), FIX B (partial-JSON
           recovery), FIX C (Claude streaming), FIX D (working indicators),
           FIX E (HubSpot error visibility + startup property check), and
           the rescore feature are byte-for-byte identical to v7.4.2.

Changelog v7.4.2 (ONE CHANGE ONLY — everything else 100% unchanged from v7.4.1):

  CHANGE — HUBSPOT NOW RECEIVES MEDIUM INTENT SIGNALS (score 4-7) IN ADDITION
            TO HIGH INTENT (score 8-10).

            In v7.4.1 HubSpot only received score 8-10 signals. Slack received
            score 4-10. This created an asymmetry where medium signals were
            visible in Slack but not in HubSpot CRM.

            Fix: in process_scored_item(), the medium branch
            (MIN_SCORE_MEDIUM <= score < MIN_SCORE_HIGH) now calls
            send_to_hubspot() and mark_hubspot_alerted() in exactly the same
            way the high branch always did. The high branch is unchanged.

            New routing (confirmed):
              Score 1-3  → MongoDB only. No Slack. No HubSpot.
              Score 4-7  → MongoDB + Slack + HubSpot.
              Score 8-10 → MongoDB + Slack + HubSpot.

            NOTHING ELSE CHANGED. Scoring logic, prompts, Slack block
            formatting, HubSpot contact/note FIELD VALUES, FastAPI routes,
            thresholds (MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8), keyword list,
            FIX A (persistent batch state), FIX B (partial-JSON recovery),
            FIX C (Claude streaming), FIX D (working indicators), FIX E
            (HubSpot error visibility + startup property check), and the
            rescore feature are byte-for-byte identical to v7.4.1.

Changelog v7.4.1 (ONE FIX ONLY — everything else 100% unchanged from v7.4):

  FIX E — HUBSPOT 400 BAD REQUEST — REAL ERROR VISIBILITY + SAFE PAYLOAD.
           Problem in v7.4: _hs_create_contact() and _hs_find_contact() caught
           requests.exceptions.HTTPError with a bare `except Exception as exc`,
           which only logs str(exc) — e.g. "400 Client Error: Bad Request for
           url: ...". The actual reason HubSpot rejected the request (which
           property doesn't exist, which value is invalid, etc.) was contained
           in the response body (exc.response.text) but was NEVER logged, so
           operators could not diagnose the 400 without guessing.

           Fix (three parts, all additive — no existing behavior removed):

           1. _hs_create_contact() and _hs_find_contact() and _hs_create_note()
              now catch requests.exceptions.HTTPError specifically BEFORE the
              generic Exception handler, and log exc.response.text (HubSpot's
              actual JSON error body — e.g. {"message": "Property
              \\"fx_intent_score\\" does not exist", "category":
              "VALIDATION_ERROR"}). This is purely additive logging — no
              control flow changed, no return values changed, no properties
              changed. Same retries, same fallback behavior as v7.4.

           2. _hs_create_contact() now validates that HUBSPOT_API_KEY is a
              private-app token format before sending (defensive log warning
              only — does not block the call, since portal token formats can
              legitimately vary). This surfaces silent auth misconfiguration
              in logs without changing behavior.

           3. A one-time startup self-check function _hs_verify_properties()
              is added (called once at startup, non-blocking, best-effort).
              It checks whether the ten custom "fx_*" contact properties used
              by _hs_create_contact() actually exist in this HubSpot portal
              by calling GET /crm/v3/properties/contacts. If any are missing,
              it logs a clear, actionable WARNING listing exactly which
              properties are missing and a reminder to create them under
              Settings → Properties → Contact properties — BEFORE the first
              400 ever happens, instead of after. This call is read-only,
              wrapped in try/except, and never raises — if it fails for any
              reason (e.g. missing scope), it just logs a warning and the
              system continues exactly as v7.4 did. No properties are
              auto-created; HubSpot's API does not support that, and this
              fix does not pretend otherwise.

           NOTHING ELSE CHANGED. Scoring logic, prompts, Slack block
           formatting, HubSpot contact/note FIELD VALUES, FastAPI routes,
           thresholds (MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8), keyword list,
           FIX A (persistent batch state), FIX B (partial-JSON recovery),
           FIX C (Claude streaming), FIX D (working indicators), and the
           rescore feature are byte-for-byte identical to v7.4.

           Pipeline behavior (confirmed unchanged from v7.4):
             — Score 1-3  → MongoDB only. No Slack. No HubSpot.
             — Score 4-7  → MongoDB + Slack only. No HubSpot.
             — Score 8-10 → MongoDB + Slack + HubSpot (same message/signal,
                             sent to BOTH Slack and HubSpot — not split).
             — ALL scores 1-10 are stored in MongoDB exactly as returned by
               Claude — nothing is discarded at storage time.

Changelog v7.4 (two fixes + one new feature — all v7.3 logic 100% unchanged):

  FIX C — CLAUDE STREAMING (fixes "Streaming is required for operations
           that may take longer than 10 minutes" error).
           Problem in v7.3: _call_claude_batch used anthropic_client.messages.create()
           which is a blocking synchronous call. For large batches the Anthropic
           SDK raises an error requiring streaming for long-running requests.
           Fix: replaced messages.create() with the streaming context manager
           pattern using anthropic_client.messages.stream() as a context manager,
           calling stream.get_final_text() to collect the complete response once
           generation finishes. The raw text passed to _parse_claude_json() is
           byte-for-byte identical — only the transport layer changes. All scoring
           logic, prompts, FIX B partial-JSON recovery, and output schema are
           100% unchanged. Timeout is set to 600s (10 min) per call via httpx.Timeout.

  FIX D — ENABLE/DISABLE WORKING INDICATORS in logs.
           Platform enable/disable flags now log "✅ Working" or "❌ Not Working"
           next to each platform line at startup and in /health endpoint, so
           operators can confirm at a glance whether each platform is actually
           collecting. True = Working, False = Not Working.

  NEW   — RESCORE MESSAGES (flintel_rescore_messages collection).
           Operators can manually queue any signal by message_id (or a list of
           message_ids) for re-scoring by Claude. Rescore batches follow the
           same batch_size, gap, and timeout logic as live platform batches.
           After rescore, results overwrite the existing MongoDB signal document
           and re-trigger the same Slack + HubSpot pipeline (score 4-7 → Slack
           + HubSpot, score 8-10 → Slack + HubSpot) exactly as a live signal
           would.
           Logs show "[RESCORE] batch N | items M" same style as live batches.

           MongoDB collection: flintel_rescore_messages
             Fields per document:
               _id                : ObjectId (auto) or operator-supplied string ID
               message_id         : str  — must match an existing signals document
               status             : "pending" | "processing" | "done" | "error"
               requested_at       : datetime UTC
               processed_at       : datetime UTC (set on completion)
               rescore_result     : dict  — the new Claude score result
               error              : str   — set if status == "error"
               operator_note      : str   — optional free-text note from operator

           FastAPI endpoints (API-key protected):
             POST /rescore                  — queue one or many message_ids
             GET  /rescore/pending          — list pending rescore requests
             GET  /rescore/history          — list completed rescore requests (limit)
             GET  /rescore/status/{req_id}  — status of a specific rescore request

           Rescore processor runs as a dedicated background thread, polling
           flintel_rescore_messages for pending items every 10s.
           It batches up to RESCORE_BATCH_SIZE (default: same as REDDIT_BATCH_SIZE)
           pending items per Claude call, with its own RESCORE_BATCH_GAP_SECONDS
           between calls (see v7.4.4 changelog).

  NOTHING ELSE CHANGED. Scoring logic, prompts (_SCORING_CORE and all three
  platform schemas), Slack block formatting, HubSpot fields, FastAPI routes,
  thresholds, keyword list, FIX A (persistent batch state), FIX B (partial-JSON
  recovery), and the v7.2 OPT1-OPT6 token optimisations are byte-for-byte
  identical to v7.3.

Changelog v7.3 (two fixes only — all v7.2 scoring/output logic 100% unchanged):
  FIX A — PERSISTENT BATCH STATE (survives restarts).
  FIX B — TOLERANT PARTIAL-JSON RECOVERY (no longer all-or-nothing).

Changelog v7.2 (output cost optimisation — all scoring logic 100% unchanged):
  OPT 1 — Platform-specific JSON schemas.
  OPT 2 — Derived fields removed from Claude output (computed in Python).
  OPT 3 — Word caps enforced in prompt.
  OPT 4 — Outreach keys omitted entirely for score 1-3.
  OPT 5 — urgency_indicator and watchlist_reason removed from Claude output.
  OPT 6 — max_tokens raised to 8192.
  NET    — Per-item output: 320 tokens → ~140 tokens (-56%).
  ADD    — Telegram polling thread added.

Changelog v7.1 (bug fixes only — all logic 100% unchanged):
  FIX 1 — Twitter search query built dynamically from KEYWORDS list.
  FIX 2 — Reddit + Telegram in-memory dedup sets added.
  FIX 3 — Operator Slack alerts added for Claude API down + MongoDB drop.
  FIX 4 — FastAPI /signals and all data endpoints protected with API key auth.
  FIX 5 — Weekly report last_report_week persisted in MongoDB.
  NEW   — Platform enable/disable flags.
  RSS   — Reddit now uses feedparser RSS instead of PRAW.

Changelog v7.0:
  - Added Telegram platform (Telethon, human account, read-only listener)
  - MongoDB now stores ALL scores 1-10 (nothing silently discarded)
  - BATCH_TIMEOUT_SECONDS=120: partial batch sent to Claude after timeout
  - Claude model: claude-sonnet-4-6
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

# MIN_SCORE_MEDIUM=4, MIN_SCORE_HIGH=8 — confirmed defaults, unchanged.
# Score 1-3  → MongoDB only (silent save, no alerts)
# Score 4-7  → MongoDB + Slack + HubSpot
# Score 8-10 → MongoDB + Slack + HubSpot
MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "4"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))
CLIENT_ID        = os.getenv("CLIENT_ID", "settla")

# ── BATCH SIZE — already platform-specific since v7.2/v7.4, UNCHANGED ───────
REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
TELEGRAM_BATCH_SIZE = int(os.getenv("TELEGRAM_BATCH_SIZE", "10"))
FACEBOOK_BATCH_SIZE = int(os.getenv("FACEBOOK_BATCH_SIZE", "10"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))

# ── v7.4.4 CHANGE — BATCH_GAP_SECONDS and BATCH_TIMEOUT_SECONDS are now
# per-platform. Old single global BATCH_GAP_SECONDS / BATCH_TIMEOUT_SECONDS
# env vars are REPLACED by six dedicated vars below. Defaults match the old
# global defaults (30s gap, 120s timeout) so behavior is identical unless an
# operator explicitly sets a platform-specific override.
REDDIT_BATCH_GAP_SECONDS       = int(os.getenv("REDDIT_BATCH_GAP_SECONDS",       "30"))
REDDIT_BATCH_TIMEOUT_SECONDS   = int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",   "120"))

TWITTER_BATCH_GAP_SECONDS      = int(os.getenv("TWITTER_BATCH_GAP_SECONDS",      "30"))
TWITTER_BATCH_TIMEOUT_SECONDS  = int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS",  "120"))

TELEGRAM_BATCH_GAP_SECONDS     = int(os.getenv("TELEGRAM_BATCH_GAP_SECONDS",     "30"))
TELEGRAM_BATCH_TIMEOUT_SECONDS = int(os.getenv("TELEGRAM_BATCH_TIMEOUT_SECONDS", "120"))

# ── v7.5.0 — FACEBOOK: same per-platform batch gap/timeout pattern as
# Reddit/Twitter/Telegram. FACEBOOK_BATCH_SIZE lives alongside the other
# BATCH_SIZE vars below (same section, same style).
FACEBOOK_BATCH_GAP_SECONDS     = int(os.getenv("FACEBOOK_BATCH_GAP_SECONDS",     "30"))
FACEBOOK_BATCH_TIMEOUT_SECONDS = int(os.getenv("FACEBOOK_BATCH_TIMEOUT_SECONDS", "120"))

# Rescore's own gap — independent of Reddit/Twitter/Telegram (v7.4.4).
# Rescore batch SIZE (RESCORE_BATCH_SIZE) was already independent, untouched.
RESCORE_BATCH_GAP_SECONDS = int(os.getenv("RESCORE_BATCH_GAP_SECONDS", "30"))

DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

TELEGRAM_JOIN_GAP_SECONDS = int(os.getenv("TELEGRAM_JOIN_GAP_SECONDS", "30"))

# ── v7.5.0 — FACEBOOK: mirrors Reddit's per-cycle polling pattern (a full
# pass over TARGET_SUBREDDITS there / over KEYWORDS here), with a small
# gap between each individual request to stay rate-limit safe.
FACEBOOK_POLL_INTERVAL       = int(os.getenv("FACEBOOK_POLL_INTERVAL", "300"))
FACEBOOK_KEYWORD_GAP_SECONDS = int(os.getenv("FACEBOOK_KEYWORD_GAP_SECONDS", "2"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

# FIX C: timeout for Claude streaming calls (seconds). 10 minutes default.
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
TWITTER_ENABLED  = _bool_env("TWITTER_ENABLED",  False)
TELEGRAM_ENABLED = _bool_env("TELEGRAM_ENABLED", False)
FACEBOOK_ENABLED = _bool_env("FACEBOOK_ENABLED", True)


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
    "FreightBrokers", "Shipping", "portmanteau",  # note: check exact name — likely typo, remove if invalid

    # ── ECOMMERCE / FULFILLMENT ───────────────────────────────────────────────
    "FulfillmentByAmazon", "ecommerce", "EtsySellers",
    "shopify", "AmazonSeller", "dropship", "Import_Export",

    # ── BUSINESS / GENERAL ────────────────────────────────────────────────────
    "smallbusiness", "Entrepreneur", "EntrepreneurRideAlong",
    "startups", "manufacturing",

    # ── ADJACENT ───────────────────────────────────────────────────────────────
    "supplychainmanagement", "sourcing", "procurement",
    "warehouseautomation", "operationsmanagement",      "humanresources", "recruiting", "RecruitingHell",
    "HRTech", "AskHR", "Recruitment",

    # ── STARTUP / SMALL BUSINESS ──────────────────────────────────────────────
    "startups", "smallbusiness", "Entrepreneur",
    "EntrepreneurRideAlong", "growmybusiness",

    # ── ADJACENT / GENERAL WORK ────────────────────────────────────────────────
    "jobs", "careerguidance", "cscareerquestions",
    "WorkOnline", "remotework",
    
    "Accounting", "Bookkeeping", "QuickBooks", "Xero",
    "smallbusiness", "tax", "IRS",

    # ── STARTUP / SMALL BUSINESS ───────────────────────────────────────────────
    "Entrepreneur", "EntrepreneurRideAlong", "startups",
    "smallbusinessowner", "solopreneur", "freelance",

    # ── ADJACENT / GENERAL FINANCE ─────────────────────────────────────────────
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
# SHARED QUEUES — platform-isolated, never mixed (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:   queue.Queue = queue.Queue()
twitter_queue:  queue.Queue = queue.Queue()
telegram_queue: queue.Queue = queue.Queue()
facebook_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER (unchanged — identical list to v7.1 through v7.4.3)
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS = [
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
    
    # ------ CYBER -----------

    # ── INCIDENT / BREACH SIGNALS ────────────────────────────────────────────
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
    
    
    # ------- CRM -----------
    
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

    # ── BUSINESS CONTEXT ──────────────────────────────────────────────────────
    "my sales team", "our sales team", "small sales team",
    "growing sales team", "scaling our sales team", "sales reps need",
    "sales manager needs", "head of sales needs",
    "startup sales process", "our sales process", "no sales process",
    "informal sales process", "need a sales process",
    "founder-led sales", "solo founder sales", "one-person sales team",
    "agency CRM needs", "real estate CRM needs",
    "B2B sales pipeline", "B2B sales tool", "B2B sales software",
    "SaaS sales tool", "SaaS CRM", "startup CRM",

    # ── COMPLIANCE / DATA ─────────────────────────────────────────────────────
    "CRM data security", "CRM GDPR compliance", "CRM data privacy",
    "CRM permissions issue", "CRM access control",
    "data silos sales marketing", "sales and marketing not aligned",
    "CRM integration issue", "CRM doesn't integrate with",
    "CRM integration with email", "CRM integration with marketing",
    "CRM API limitations", "CRM lacks integrations",

    # ── URGENCY / EXPANSION SIGNALS ───────────────────────────────────────────
    "urgently need a CRM", "need a CRM ASAP", "need this set up quickly",
    "launching soon need CRM", "onboarding new sales hires",
    "just hired our first salesperson", "scaling our sales operations",
    "new sales hire needs a CRM", "board wants better reporting",
    "investors asking about pipeline", "need better reporting for investors",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "VP of sales", "head of sales", "sales operations manager",
    "RevOps manager", "revenue operations manager", "CRM administrator",
    "Salesforce administrator", "Salesforce admin", "Salesforce developer",
    "sales enablement manager", "director of sales operations",
    "chief revenue officer", "CRO", "sales operations analyst",
    

    # ── SHIPPING / DELIVERY PROBLEMS ──────────────────────────────────────────
    "shipment delayed", "shipment stuck", "shipment lost",
    "package delayed", "package stuck", "package lost",
    "order delayed", "order stuck in transit", "stuck in customs",
    "held at customs", "customs delay", "customs clearance issue",
    "customs holding my shipment", "customs rejected my shipment",
    "freight delayed", "freight stuck", "container delayed",
    "container stuck", "container held", "cargo delayed", "cargo stuck",
    "shipment damaged", "damaged in transit", "damaged freight",
    "damaged goods arrived", "arrived damaged", "goods lost in transit",
    "missing shipment", "missing package", "missing inventory",
    "tracking not updating", "no tracking updates",
    "can't track my shipment", "no visibility into shipment",
    "no visibility into freight", "no supply chain visibility",
    "shipment taking forever", "delivery taking forever",
    "late delivery", "missed delivery window", "missed delivery deadline",
    "delivery delayed again", "delayed again", "shipment delayed again",

    # ── CARRIER PROBLEMS / SWAPS ─────────────────────────────────────────────
    "carrier dropped my shipment", "carrier cancelled", "carrier failed",
    "carrier issue", "carrier problem", "carrier keeps delaying",
    "carrier unreliable", "unreliable carrier", "bad carrier experience",
    "switching carriers", "switched carriers", "need a new carrier",
    "looking for a new carrier", "carrier alternative",
    "alternative carrier", "carrier keeps raising rates",
    "carrier rate increase", "carrier surcharge", "unexpected surcharge",
    "fuel surcharge too high", "accessorial fees", "hidden carrier fees",
    "carrier capacity issues", "no capacity available",
    "can't get capacity", "capacity crunch", "space unavailable",
    "booking rejected carrier", "carrier booking cancelled",
    "carrier overbooked", "rolled shipment", "shipment rolled",
    "carrier keeps rolling my cargo", "blank sailing",
    "vessel delayed", "vessel skipped port", "port congestion",
    "port delays", "port backlog", "warehouse delays",
    "3PL problem", "3PL issue", "3PL dropped the ball",
    "switching 3PL", "leaving our 3PL", "need a new 3PL",
    "freight forwarder problem", "freight forwarder issue",
    "switching freight forwarders", "need a new freight forwarder",
    "trucking company problem", "trucking company unreliable",
    "LTL carrier issue", "FTL carrier issue", "drayage delay",
    "drayage problem", "last mile delivery problem",
    "last mile delivery issues", "final mile delivery problem",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "shipping costs too high", "freight costs too high",
    "shipping rates increased", "freight rates increased",
    "shipping fees killing margins", "freight fees eating margins",
    "logistics costs too high", "cheaper shipping option",
    "cheaper freight option", "cheaper carrier", "cheaper 3PL",
    "affordable freight forwarding", "reduce shipping costs",
    "lower our freight costs", "cut shipping costs",
    "shipping cost comparison", "compare freight rates",
    "best freight rates", "best shipping rates",

    # ── COMPETITOR / PLATFORM MENTIONS ───────────────────────────────────────
    "FedEx delayed", "FedEx lost my package", "FedEx problem",
    "FedEx issue", "UPS delayed", "UPS lost my package", "UPS problem",
    "USPS lost my package", "USPS delayed", "USPS problem",
    "DHL delayed", "DHL lost my package", "DHL problem",
    "Maersk delayed", "Maersk booking issue", "MSC delayed",
    "MSC booking issue", "CMA CGM delayed", "COSCO delayed",
    "Flexport problem", "Flexport issue", "Flexport alternative",
    "leaving Flexport", "switching from Flexport",
    "project44 alternative", "FourKites alternative",
    "ShipBob problem", "ShipBob issue", "ShipBob alternative",
    "leaving ShipBob", "ShipStation problem", "ShipStation alternative",
    "Shippo problem", "Shippo alternative", "EasyPost alternative",
    "Freightos alternative", "uShip problem", "Convoy shut down",
    "alternative to FedEx", "alternative to UPS", "alternative to DHL",
    "alternative to Flexport", "alternative to ShipBob",
    "better than Flexport", "better than ShipBob",
    "competitors to Flexport", "Flexport competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend a freight forwarder", "recommend a carrier",
    "recommend a 3PL", "recommend a logistics provider",
    "recommend a shipping company", "recommend a fulfillment company",
    "anyone recommend a carrier", "can anyone recommend a 3PL",
    "does anyone recommend a freight forwarder",
    "what carrier do you use", "what 3PL do you use",
    "which carrier is best", "which 3PL is best",
    "best freight forwarder for", "best 3PL for small business",
    "best fulfillment company", "best shipping carrier for ecommerce",
    "looking for a freight forwarder", "looking for a 3PL",
    "looking for a carrier", "looking for a fulfillment partner",
    "need a logistics partner", "need a shipping partner",
    "need a new supplier for shipping", "anyone using",
    "has anyone used", "who do you use for shipping",
    "what are you using for fulfillment", "tried several carriers",
    "tried multiple 3PLs", "still looking for a carrier",

    # ── SUPPLY CHAIN / SOURCING ───────────────────────────────────────────────
    "supply chain disruption", "supply chain issue", "supply chain problem",
    "supply chain delay", "supply chain risk", "supply chain visibility",
    "diversifying our supply chain", "diversify suppliers",
    "reduce supply chain risk", "supply chain resilience",
    "reshoring manufacturing", "nearshoring supply chain",
    "friend-shoring", "supplier diversification",
    "single source supplier risk", "backup supplier needed",
    "need a backup supplier", "supplier reliability issues",
    "supplier missed deadline", "supplier delay", "manufacturer delay",
    "factory delay", "production delay", "inventory shortage",
    "stock shortage", "out of stock supplier issue",
    "inventory management problem", "warehouse management issue",
    "demand planning problem", "forecasting supply chain",
    "procurement issue", "procurement delay", "sourcing new supplier",
    "sourcing new manufacturer", "vetting new supplier",
    "supplier audit", "supplier quality issue", "quality control issue factory",

    # ── BUSINESS CONTEXT ──────────────────────────────────────────────────────
    "our warehouse", "our fulfillment center", "our distribution center",
    "ecommerce fulfillment", "ecommerce shipping", "dropshipping supplier",
    "dropshipping issue", "import/export logistics", "cross-border shipping",
    "international shipping problem", "international freight",
    "B2B shipping", "wholesale shipping", "bulk shipping",
    "small business shipping", "startup logistics", "scaling logistics",
    "growing ecommerce brand shipping", "DTC brand fulfillment",
    "manufacturing overseas", "shipping from China", "shipping from Asia",
    "container shipping from China", "freight from China",

    # ── COMPLIANCE / DOCUMENTATION ────────────────────────────────────────────
    "bill of lading issue", "customs documentation error",
    "incoterms confusion", "wrong incoterms", "tariff increase",
    "tariff impact supply chain", "duties and tariffs issue",
    "import duties too high", "export documentation problem",
    "compliance issue shipping", "trade compliance", "HS code error",
    "denied party screening", "customs broker issue",
    "customs broker problem", "need a customs broker",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need shipping", "need this shipped ASAP",
    "customer waiting on shipment", "customers are angry about shipping",
    "losing customers over shipping delays", "losing the contract shipping",
    "peak season shipping", "holiday shipping delays",
    "need a solution before peak season", "running out of inventory",
    "can't fulfill orders", "backordered", "backlog of orders",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "supply chain manager", "logistics manager", "logistics coordinator",
    "procurement manager", "sourcing manager", "fulfillment manager",
    "warehouse manager", "operations manager logistics",
    "VP supply chain", "head of logistics", "head of supply chain",
    "director of logistics", "director of supply chain",
    "chief supply chain officer", "freight broker", "logistics analyst",
    
    
    # ── HIRING / RECRUITING PAIN POINTS ──────────────────────────────────────
    "hiring is a nightmare", "recruiting is a nightmare",
    "can't find good candidates", "can't find qualified candidates",
    "struggling to hire", "struggling to find talent",
    "talent shortage", "hard to find good talent",
    "too many unqualified applicants", "flooded with applications",
    "drowning in resumes", "too many resumes to screen",
    "resume screening taking forever", "screening candidates manually",
    "no time to screen candidates", "hiring process too slow",
    "recruiting process too slow", "hiring taking too long",
    "time to hire too long", "losing candidates to slow process",
    "losing candidates to other offers", "candidate ghosted us",
    "candidates ghosting", "no shows for interviews",
    "interview no shows", "offer rejected", "candidate declined offer",
    "high candidate drop off", "candidates dropping out of process",
    "bad candidate experience", "poor candidate experience",
    "our hiring process is broken", "broken hiring process",
    "no structured interview process", "inconsistent interview process",
    "hiring manager not responsive", "hiring managers slow to respond",
    "interview feedback delayed", "no feedback loop hiring",

    # ── ATS / TOOLING FRUSTRATION ─────────────────────────────────────────────
    "our ATS is a mess", "ATS is clunky", "clunky ATS",
    "outdated ATS", "ATS feels outdated", "hate our ATS",
    "ATS is too complicated", "ATS too complex", "ATS doesn't work for us",
    "outgrown our ATS", "outgrown our applicant tracking system",
    "ATS doesn't scale", "ATS can't handle our volume",
    "ATS is too slow", "ATS keeps crashing", "ATS keeps glitching",
    "ATS data is a mess", "duplicate candidates ATS",
    "no one updates the ATS", "recruiters don't use the ATS",
    "low ATS adoption", "manual tracking candidates",
    "tracking candidates in spreadsheets", "still using spreadsheets to hire",
    "no visibility into hiring pipeline", "can't see our hiring pipeline",
    "hiring pipeline is a black box", "reporting on hiring is hard",
    "ATS reporting is limited", "ATS doesn't integrate with",
    "ATS lacks integrations", "ATS integration issue",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "recruiting costs too high", "cost per hire too high",
    "ATS pricing too high", "HR software too expensive",
    "per seat pricing ATS", "per employee pricing HR software",
    "recruiter agency fees too high", "staffing agency fees too high",
    "hidden fees ATS", "surprise renewal fees HR software",
    "HR software renewal price increase", "job board costs too high",
    "job posting fees too expensive", "cheaper alternative to Greenhouse",
    "cheaper alternative to Workday", "cheaper ATS",
    "affordable HR software for small business", "budget-friendly ATS",
    "best value ATS", "not seeing ROI from our ATS",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "Greenhouse too complex", "Greenhouse too expensive",
    "Greenhouse alternative", "alternative to Greenhouse",
    "switching from Greenhouse", "leaving Greenhouse",
    "Workday is a nightmare", "Workday too complicated",
    "Workday alternative", "alternative to Workday",
    "switching from Workday", "leaving Workday",
    "Lever alternative", "switching from Lever", "leaving Lever",
    "BambooHR problem", "BambooHR alternative", "switching from BambooHR",
    "ADP problem", "ADP alternative", "switching from ADP",
    "Gusto problem", "Gusto alternative", "switching from Gusto",
    "Rippling problem", "Rippling alternative",
    "iCIMS problem", "iCIMS alternative", "switching from iCIMS",
    "JazzHR alternative", "Breezy HR alternative", "Recruitee alternative",
    "SmartRecruiters alternative", "Workable alternative",
    "Zoho Recruit alternative", "Indeed hiring platform problem",
    "LinkedIn Recruiter too expensive", "LinkedIn Recruiter alternative",
    "ZipRecruiter problem", "ZipRecruiter alternative",
    "alternative to Greenhouse", "alternative to Lever",
    "alternative to BambooHR", "alternative to Workable",
    "better than Greenhouse", "better than Workday",
    "competitors to Greenhouse", "Greenhouse competitors",
    "Workday competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend an ATS", "recommend a hiring tool", "recommend an HR platform",
    "recommend a recruiting tool", "recommend a payroll provider",
    "anyone recommend an ATS", "can anyone recommend a hiring tool",
    "does anyone recommend an HR platform", "what ATS do you use",
    "what HR software do you use", "which ATS is best",
    "which HR platform is best", "best ATS for small business",
    "best ATS for startups", "best HR software for small teams",
    "best recruiting software", "best payroll software",
    "best workforce management tool", "looking for an ATS",
    "looking for an HR platform", "looking for a recruiting tool",
    "looking for a payroll provider", "need an ATS",
    "need an HR platform", "need a recruiting tool",
    "need a simple ATS", "need an easy HR system",
    "anyone using an ATS", "does anyone use", "has anyone used",
    "who uses", "what are you using for hiring",
    "tried several ATS platforms", "tried multiple HR tools",
    "still looking for an ATS", "still haven't found the right HR software",

    # ── RECRUITING / HR TOOLS & CATEGORIES ────────────────────────────────────
    "applicant tracking system", "candidate relationship management",
    "recruiting CRM", "talent acquisition software",
    "employer branding tool", "job posting software",
    "interview scheduling tool", "automated interview scheduling",
    "background check software", "reference checking tool",
    "onboarding software", "employee onboarding tool",
    "payroll software", "benefits administration software",
    "performance management software", "employee engagement tool",
    "workforce management software", "HRIS platform",
    "time tracking software HR", "PTO tracking tool",
    "compensation management tool", "org chart software",
    "employee scheduling software", "shift scheduling tool",
    "recruitment marketing tool", "candidate sourcing tool",
    "sourcing candidates tool", "resume parsing tool",
    "skills assessment tool", "pre-employment testing",
    "video interview platform", "async video interview tool",

    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my HR team", "our HR team", "small HR team", "one person HR team",
    "no dedicated HR person", "wearing the HR hat", "founder doing HR",
    "growing our team", "scaling our hiring", "scaling headcount",
    "hiring our first employee", "hiring first HR hire",
    "startup hiring process", "no formal hiring process",
    "need a hiring process", "remote hiring challenges",
    "hiring remote employees", "distributed team hiring",
    "hiring across multiple countries", "international hiring",
    "hiring contractors vs employees", "EOR provider", "employer of record",
    "PEO provider", "professional employer organization",

    # ── COMPLIANCE / HR RISK ───────────────────────────────────────────────────
    "compliance issue HR", "employment law compliance",
    "wrongful termination risk", "HR compliance nightmare",
    "I-9 compliance", "E-Verify issue", "labor law compliance",
    "wage and hour compliance", "overtime compliance issue",
    "worker classification issue", "1099 vs W2 issue",
    "background check compliance", "EEOC complaint", "HR audit",
    "failed HR audit", "employee handbook outdated",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need to hire", "need to hire ASAP", "critical role open",
    "position open for months", "role has been open too long",
    "losing revenue because understaffed", "understaffed team",
    "burning out the team hiring slow", "board wants headcount plan",
    "investors asking about headcount", "need to scale hiring fast",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "head of talent", "head of HR", "head of people",
    "VP of people", "VP of talent", "chief people officer",
    "talent acquisition manager", "recruiting manager",
    "HR manager", "HR business partner", "people operations manager",
    "HRIS manager", "compensation and benefits manager",
    "director of talent acquisition", "director of people operations",
    "technical recruiter", "corporate recruiter", "recruiting coordinator",
    
    # ── ACCOUNTING / BOOKKEEPING PAIN POINTS ──────────────────────────────────
    "our books are a mess", "bookkeeping is a mess", "behind on bookkeeping",
    "months behind on bookkeeping", "need to catch up on books",
    "accounting is a nightmare", "accounting software is a nightmare",
    "hate our accounting software", "accounting software too complicated",
    "accounting software too complex", "outdated accounting software",
    "accounting software feels outdated", "outgrown our accounting software",
    "accounting software doesn't scale", "accounting software is too slow",
    "accounting software keeps crashing", "accounting software keeps glitching",
    "reconciliation is a nightmare", "bank reconciliation taking forever",
    "reconciling accounts manually", "manual data entry accounting",
    "too much manual data entry bookkeeping", "still using spreadsheets for accounting",
    "tracking expenses in spreadsheets", "invoicing manually",
    "sending invoices manually", "chasing invoices", "chasing late payments",
    "chasing unpaid invoices", "clients not paying invoices",
    "cash flow visibility problem", "no visibility into cash flow",
    "can't see our cash flow", "cash flow is a black box",
    "financial reporting takes forever", "building reports manually finance",
    "accounting reporting is limited", "accounting reporting is weak",
    "closing the books takes forever", "month end close takes too long",
    "month end close nightmare", "year end accounting nightmare",
    "tax season nightmare", "not ready for tax season",
    "no idea what we owe in taxes", "surprised by tax bill",
    "underestimated taxes owed", "quarterly taxes nightmare",

    # ── SETUP / MIGRATION FRUSTRATION ────────────────────────────────────────
    "accounting software migration nightmare", "migrating off our accounting software",
    "migrating from QuickBooks", "switching accounting software",
    "accounting software setup took months", "accounting software onboarding nightmare",
    "hard to customize our accounting software", "need an accountant to set this up",
    "need a bookkeeper to fix this", "paying a bookkeeper to clean up our books",
    "cleanup project bookkeeping", "books need a cleanup",

    # ── FEE / COST FRUSTRATION ────────────────────────────────────────────────
    "accounting software too expensive", "accounting software pricing too high",
    "bookkeeping fees too high", "accountant fees too high",
    "per user pricing accounting software", "accounting software renewal price increase",
    "accounting software price hike", "hidden fees accounting software",
    "add-ons cost extra accounting software", "paying for features we don't use accounting",
    "cheaper alternative to QuickBooks", "cheaper than QuickBooks",
    "cheaper accounting software", "affordable accounting software for small business",
    "budget-friendly accounting software", "best value accounting software",
    "not seeing ROI from accounting software",

    # ── COMPETITOR MENTIONS ───────────────────────────────────────────────────
    "QuickBooks is a nightmare", "QuickBooks too complicated",
    "QuickBooks too expensive", "QuickBooks pricing increase",
    "QuickBooks alternative", "alternative to QuickBooks",
    "leaving QuickBooks", "switching from QuickBooks",
    "migrating from QuickBooks", "QuickBooks customer support terrible",
    "Xero alternative", "alternative to Xero", "switching from Xero",
    "leaving Xero", "Xero problem", "Xero issue",
    "FreshBooks alternative", "switching from FreshBooks",
    "Wave accounting problem", "Wave accounting alternative",
    "Sage alternative", "Sage accounting problem", "switching from Sage",
    "NetSuite too complex", "NetSuite too expensive", "NetSuite alternative",
    "Zoho Books alternative", "switching from Zoho Books",
    "Bill.com problem", "Bill.com alternative",
    "Gusto payroll problem", "Gusto accounting integration issue",
    "Expensify problem", "Expensify alternative",
    "alternative to Xero", "alternative to FreshBooks",
    "alternative to NetSuite", "alternative to Sage",
    "better than QuickBooks", "better than Xero",
    "competitors to QuickBooks", "QuickBooks competitors",
    "Xero competitors",

    # ── RECOMMENDATION REQUESTS ───────────────────────────────────────────────
    "recommend an accounting software", "recommend a bookkeeping tool",
    "recommend an accountant", "recommend a bookkeeper",
    "anyone recommend an accounting software", "can anyone recommend a bookkeeper",
    "does anyone recommend an accountant", "what accounting software do you use",
    "which accounting software is best", "best accounting software for small business",
    "best accounting software for startups", "best accounting software for freelancers",
    "best invoicing software", "best expense tracking software",
    "best bookkeeping software", "best payroll and accounting software",
    "looking for an accounting software", "looking for a bookkeeper",
    "looking for an accountant", "need an accounting software",
    "need a bookkeeper", "need an accountant", "need a simple accounting tool",
    "anyone using", "does anyone use", "has anyone used",
    "who uses", "what are you using for accounting",
    "tried several accounting tools", "still looking for accounting software",
    "still haven't found the right accounting software",

    # ── ACCOUNTING TOOLS & CATEGORIES ────────────────────────────────────────
    "invoicing software", "expense tracking software", "expense management tool",
    "receipt scanning app", "mileage tracking app", "payroll software",
    "tax filing software", "tax preparation software", "sales tax software",
    "sales tax compliance tool", "1099 filing software", "W2 filing software",
    "accounts payable software", "accounts receivable software",
    "AP automation", "AR automation", "cash flow forecasting tool",
    "financial planning software", "FP&A tool", "budgeting software business",
    "multi-entity accounting software", "multi-currency accounting software",
    "inventory accounting software", "job costing software",
    "project accounting software", "nonprofit accounting software",
    "e-commerce accounting software", "Shopify accounting integration",
    "Amazon seller accounting software",

    # ── BUSINESS CONTEXT ───────────────────────────────────────────────────────
    "my bookkeeper", "our bookkeeper", "my accountant", "our accountant",
    "small business accounting", "startup accounting", "solo founder accounting",
    "freelancer accounting", "self employed accounting", "DIY bookkeeping",
    "doing my own books", "founder doing the books", "wearing the finance hat",
    "no dedicated finance person", "growing business need better accounting",
    "scaling finance operations", "outsourced bookkeeping", "outsourced accounting",
    "virtual CFO", "fractional CFO", "need a fractional CFO",
    "part time bookkeeper", "part time accountant",

    # ── COMPLIANCE / TAX RISK ─────────────────────────────────────────────────
    "IRS audit", "audit risk small business", "tax compliance issue",
    "missed tax deadline", "late filing penalty", "sales tax nexus issue",
    "multi-state tax compliance", "1099 compliance issue",
    "payroll tax compliance", "bookkeeping compliance issue",
    "financial statements for investors", "need clean books for investors",
    "due diligence financials", "GAAP compliance issue",

    # ── URGENCY SIGNALS ────────────────────────────────────────────────────────
    "urgently need a bookkeeper", "need books cleaned up ASAP",
    "tax deadline approaching", "need this done before tax season",
    "investors asking for financials", "due diligence deadline",
    "board wants updated financials", "need financials for loan application",
    "need financials for a loan", "applying for a business loan financials",

    # ── JOB SIGNALS ────────────────────────────────────────────────────────────
    "controller", "VP of finance", "head of finance", "director of finance",
    "chief financial officer", "CFO", "finance manager", "accounting manager",
    "bookkeeper", "staff accountant", "senior accountant",
    "accounts payable manager", "accounts receivable manager",
    "financial analyst", "FP&A manager",

]


def passes_keyword_filter(text: str) -> bool:
    t = text.lower()
    for kw in KEYWORDS:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    short_kws = [
        kw for kw in KEYWORDS
        if len(kw) <= 30 and " " not in kw or (
            " " in kw and len(kw) <= 25
        )
    ]

    seen = set()
    unique_kws = []
    for kw in short_kws:
        kl = kw.lower()
        if kl not in seen:
            seen.add(kl)
            unique_kws.append(kw)

    max_query_len = 480
    parts = []
    current_len = 0

    for kw in unique_kws:
        term = f'"{kw}"' if " " in kw else kw
        addition = len(term) + (4 if parts else 0)
        if current_len + addition > max_query_len:
            break
        parts.append(term)
        current_len += addition

    if not parts:
        return (
            "(\"international transfer\" OR \"supplier payment\" OR \"bank blocked\""
            " OR \"Wise blocked\" OR \"cross border payment\") -is:retweet lang:en"
        )

    query = "(" + " OR ".join(parts) + ") -is:retweet lang:en"
    log.info(f"Twitter search query built from KEYWORDS | terms:{len(parts)} | len:{len(query)}")
    return query


TWITTER_SEARCH_QUERY = _build_twitter_search_query()


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
# Byte-for-byte identical to v7.4.3. Scoring logic untouched.
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

Signs:
— Payment blocked or failing right now
— Supplier waiting for payment urgently
— Asking which platform to use TODAY
— Specific large amount mentioned with urgency
— Bank blocking transfer right now
— Competitor app restricted their account
— Explicitly leaving a competitor ASAP
— Actively looking for payment processor partners
— Building payment processing relationships internationally

MID INTENT — Score 4 to 6:
Company or contact actively SHOPPING for a solution.

Signs:
— Comparing multiple payment platforms
— Asking for recommendations on payment solutions
— Frustrated with current provider but not in crisis
— Researching FX rates and payment options for business
— Evaluating treasury or payment infrastructure
— Mentioned trying multiple competitors
— Pre-launch business setting up international payments
— Looking for partners with payment connections

DISCARD — Score 0 to 3:
NOT ACCEPTABLE. Never delivered to Settla team.

— Personal remittance under $2,000
— Sending money home to family
— Consumer banking complaints with no international context
— US domestic banking problems only
— High risk merchant categories — peptides, supplements, crypto
— E-commerce payment gateway requests
— General financial market commentary
— No business context whatsoever
— Academic or research requests

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCORING RULES — PRECISE AND ABSOLUTE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCORE 9 to 10 — IMMEDIATE SLACK ALERT:
ALL of these must be present:
✓ Clear business context confirmed
✓ International payment need confirmed
✓ Active crisis — blocked, failed, rejected, urgent
✓ Urgency words — today, ASAP, urgently, this week

Real examples that score 9 to 10:
"Bank blocked my $45k CAD payment to Lagos supplier AGAIN.
 Third time this month. Need a better solution urgently."
→ Business. International. Crisis. Urgency. Score 9.

"Wise Business restricted my account. Have $80k stuck.
 Pakistani supplier waiting. This is killing my business."
→ Business. Competitor restricted. Large amount. Crisis. Score 10.

"We are actively looking for partners with strong connections 
 to Asian payment processors. We have several clients from 
 Asia and are expecting more so we are keen to build reliable 
 processing relationships."
→ Business confirmed. International payment need confirmed.
  Actively looking now. No crisis but clear intent. Score 7.

SCORE 7 to 8 — IMMEDIATE SLACK ALERT:
Strong buying signal. One element missing.
✓ Business context confirmed
✓ International payment need confirmed
✗ Missing extreme urgency OR specific amount

"Anyone using a service better than Wise for business 
 payments to Nigeria? Bank rates are terrible."
→ Business. Comparing platforms. No crisis. Score 7.

SCORE 4 to 6 — DAILY DIGEST:
Researching but no immediate crisis.
✓ Business context implied
✓ International payment mentioned
✗ No urgency. No crisis.

"Starting an import business. How do people handle 
 supplier payments to Africa?"
→ Future intent. Business context. No urgency. Score 5.

"Mid-sized B2B accepting stablecoin from international 
 clients to cut wire fees. Bank flagging crypto activity."
→ Business confirmed. International clients confirmed.
  Wire fees pain. No immediate crisis. Score 6.

SCORE 3 — WATCHLIST ONLY:
Clear future potential within 30 to 60 days.
"Just signed my first supplier agreement in Lagos!"
→ New importer. Will need payments soon. Score 3.

SCORE 0 to 2 — DISCARD IMMEDIATELY:
"What is the best rate to send £500 to my mum in Lagos?"
→ Consumer. Personal. Small amount. Score 1.

"Research peptide website looking for payment processor."
→ Wrong industry. No international B2B context. Score 0.

"US Bank froze my Texas LLC account over documentation."
→ Domestic US banking. No international payment. Score 2.

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
- Issue is now resolved — kept account etc
- Post is older than 7 days
- No specific payment amount mentioned
- General commentary not personal experience

AUTOMATIC DISCARD regardless of other signals:
✗ Research peptides or supplements
✗ High risk merchant category
✗ Shopify e-commerce payment gateway
✗ Consumer subscription problems
✗ Personal PayPal or Cash App issues
✗ US domestic banking only
✗ Crypto trading discussions
✗ Academic or research requests
✗ News articles being shared
✗ Competitor companies doing outreach

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPETITOR INTELLIGENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If a competitor is mentioned negatively — score UP by 1.
Someone leaving a competitor is the hottest possible signal.

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

If post IS FROM a competitor doing outreach:
— Score 0 for that post
— Extract who they replied to
— Flag that person as HIGH INTENT signal separately

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

OUTREACH EXAMPLES BY SCORE:

Score 9 to 10 — acute pain:
"Wise restricting business accounts at that volume is 
unfortunately common. We handle large B2B transfers 
between Canada and Nigeria without the holds — worth a quick 
conversation before you commit to something else?"

Score 7 to 8 — strong signal:
"Building payment processing relationships across Asia is 
exactly what we do. Happy to connect you with the right 
processors for your client corridors — which specific 
countries are you focused on?"

Score 4 to 6 — researching:
"We work specifically with businesses moving money across 
international corridors. Happy to share how we handle the 
compliance side if useful for what you are building."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VALIDATION TESTS — CHECK BEFORE SCORING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Before returning any score above 4 ask yourself:

1. Is there a BUSINESS context? Not personal.
2. Is there an INTERNATIONAL payment context?
3. Is the post FROM someone with a problem — 
   not a company doing outreach?
4. Would Settla's SDR team find this actionable?
5. Would responding to this post embarrass Settla?

If any answer is no — reduce score accordingly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FINAL REMINDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You are not scoring sentiment.
You are not scoring general business pain.
You are not scoring domestic banking problems.

You are identifying the exact moment a diaspora 
business owner is ready to switch payment providers 
or complete a large international transaction.

That moment is worth thousands of dollars to Settla.

One converted client could process $50,000 to 
$500,000 per month through Settla.

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
        ]:
            db.signals.create_index([(field, ASCENDING)])

        db.flintel_state.create_index(
            [("key", ASCENDING)], unique=True, name="state_key_unique"
        )

        # FIX A: persistent batch state collections (unchanged)
        db.flintel_pending_batch.create_index(
            [("platform", ASCENDING)], unique=True, name="platform_unique"
        )
        db.flintel_seen_ids.create_index(
            [("platform", ASCENDING)], unique=True, name="seen_platform_unique"
        )

        # rescore messages collection
        db.flintel_rescore_messages.create_index(
            [("status", ASCENDING), ("requested_at", ASCENDING)],
            name="rescore_status_time",
        )
        db.flintel_rescore_messages.create_index(
            [("message_id", ASCENDING)],
            name="rescore_message_id",
        )

        # NEW — flintel_queue_messages: persists every item sitting in the
        # in-memory reddit_queue/twitter_queue/telegram_queue (queue.Queue)
        # BEFORE it is dequeued into a batch. queue.Queue is in-memory only,
        # so without this, a restart would silently drop anything fetched
        # but not yet consumed by run_batch_processor. On restart these are
        # reloaded back into the queue so nothing fetched is lost.
        db.flintel_queue_messages.create_index(
            [("_platform_key", ASCENDING), ("message_id", ASCENDING)],
            unique=True, name="queue_platform_message_unique",
        )

        # NEW — flintel_batch_seconds: explicit persistence of each
        # platform's batch_start_time, stored under its own dedicated
        # collection so the batch timeout countdown survives a restart
        # instead of restarting from zero.
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
    # FIX C: set a generous httpx timeout so the streaming connection is
    # never killed by the SDK before the model finishes generating. The
    # default timeout (10 min) is the threshold that triggers the error;
    # we use streaming instead, which removes the timeout constraint entirely
    # for the generation phase while still allowing a connect timeout.
    http_client=httpx.Client(
        timeout=httpx.Timeout(
            connect=30.0,
            read=None,    # no read timeout — stream can take as long as needed
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
# OPERATOR SLACK ALERT (unchanged — operator/system alerts, not signal alerts)
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
                        {"type": "mrkdwn", "text": f"*System*\nFLINTEL v7.5.0"},
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
# NEW — flintel_queue_messages: persistent raw-queue storage.
#
# reddit_queue / twitter_queue / telegram_queue are in-memory queue.Queue
# objects. Everything a poller fetches gets put() into one of these BEFORE
# run_batch_processor even looks at it (keyword filter hasn't run yet). If
# the process restarts while items are sitting in that in-memory queue,
# they are gone. These helpers persist every item the moment it's put()
# into a queue, and remove it the moment it's dequeued by
# run_batch_processor — so at any instant, flintel_queue_messages reflects
# exactly what's still waiting in that platform's in-memory queue. On
# startup, start_reddit_listener() / start_twitter_listener() /
# start_telegram_listener() reload these back into the queue before the
# poller threads start, so nothing fetched-but-unprocessed is lost.
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
# NEW — flintel_batch_seconds: explicit, dedicated persistence of each
# platform's batch_start_time (the batch timeout clock). This is stored
# under its own collection — separate from flintel_pending_batch — so the
# batch timeout countdown survives a restart instead of restarting from
# zero. Written alongside save_pending_batch/clear_pending_batch at every
# point batch_start_time changes.
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
# CLAUDE BATCH SCORER — FIX C: streaming transport, FIX B: partial-JSON recovery
# Scoring logic, prompts, output schema — 100% unchanged.
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

        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Platform: {platform} | Source: {location} | Type: {ctype} | User: {username}\n"
            f"Content: {text}\n"
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
        "watchlist":                    False,
        "watchlist_reason":             None,
    }


# FIX B — TOLERANT PARTIAL-JSON RECOVERY (unchanged)

def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    """
    Walks a possibly-truncated JSON array and extracts every complete,
    well-formed top-level object using brace-depth tracking. Unchanged.
    """
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
    """
    Returns (results_list, was_truncated_bool). Unchanged.
    Tries full json.loads first, falls back to salvage on failure.
    """
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
    """
    FIX C: uses streaming context manager instead of blocking .create().
    The stream.get_final_text() collects the complete response text once
    generation finishes. Unchanged.
    """
    platform = batch[0].get("platform", "reddit") if batch else "reddit"

    system_prompt = {
        "twitter":  CLAUDE_SYSTEM_PROMPT_TWITTER,
        "telegram": CLAUDE_SYSTEM_PROMPT_TELEGRAM,
        "facebook": CLAUDE_SYSTEM_PROMPT_FACEBOOK,
    }.get(platform, CLAUDE_SYSTEM_PROMPT_REDDIT)

    prompt = _build_batch_prompt(batch)

    # FIX C: streaming replaces blocking .create() — no more "Streaming required" error
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
# MONGODB STORAGE (unchanged) — ALL scores 1-10 stored, nothing discarded.
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
    """
    Used by rescore to overwrite an existing signal's score fields
    in-place. Unchanged.
    """
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
# SLACK DELIVERY (unchanged from v7.4.3 — numeric score hidden, tier/category shown)
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

    score       = data["intent_score"]  # still used internally to drive urgency_tag/response window — not printed
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
# HUBSPOT CRM (unchanged from v7.4.3)
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
            "(see error above — likely missing 'crm.schemas.contacts.read' "
            "scope on the private app token). This does NOT block the "
            "system; contact creation will proceed as normal and any real "
            "400 errors will still be logged in full at the time they occur."
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
        note = (
            f"FLINTEL SIGNAL — v7.5.0{rescore_note}\n\n"
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
            f"LinkedIn:\n{data.get('linkedin_message') or 'N/A'}\n\n"
            f"Telegram DM:\n{data.get('telegram_dm') or 'N/A'}\n\n"
            f"Facebook Comment:\n{data.get('facebook_comment') or 'N/A'}"
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
# CORE SIGNAL PROCESSOR (unchanged — routing 100% score-driven)
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
# GENERIC BATCH PROCESSOR — v7.4.4: now takes gap_seconds and timeout_seconds
# as PARAMETERS instead of reading a shared global. Each platform's caller
# (start_reddit_listener / start_twitter_listener / start_telegram_listener)
# passes its OWN dedicated gap/timeout values below — no platform borrows
# another platform's timing, no mixing.
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

                # NEW: item is now out of the in-memory queue — remove its
                # persisted copy from flintel_queue_messages so a restart
                # from this point on won't re-load something already
                # handled (matched into current_batch, or dropped).
                remove_queue_message(platform_key, item.get("message_id"))

                text = item.get("text", "").strip()

                if not text or len(text) < 10:
                    q.task_done()
                    continue

                if not passes_keyword_filter(text):
                    total_dropped += 1
                    log.debug(
                        f"[{platform_label}] FILTERED | "
                        f"u/{item.get('username')} | {item.get('content_type','?')}"
                    )
                    q.task_done()
                    continue

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

                # v7.4.5: completion log now shows how many items this
                # batch completed (len), e.g. "BATCH 1 COMPLETE — 4 items".
                log.info(
                    f"[{platform_label}] BATCH {total_batches} COMPLETE — "
                    f"{len(batch_to_send)} item(s) completed | waiting {gap_seconds}s..."
                )
                time.sleep(gap_seconds)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR — v7.4.4: uses its own RESCORE_BATCH_GAP_SECONDS,
# independent of Reddit/Twitter/Telegram gap values. Batch SIZE
# (RESCORE_BATCH_SIZE) was already independent, untouched.
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
    """
    Background thread: polls flintel_rescore_messages for pending items,
    batches them, sends to Claude, then calls process_scored_item() with
    is_rescore=True. Uses RESCORE_BATCH_GAP_SECONDS (its own, independent
    of Reddit/Twitter/Telegram gap values — v7.4.4).
    """
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
# TWITTER / X POLLER — testing twitter-api45.p.rapidapi.com/search.php
# ─────────────────────────────────────────────────────────────────────────────

def build_twitter_client() -> dict | None:
    if not RAPID_API_KEY:
        log.warning("RAPID_API_KEY not set — Twitter platform disabled.")
        return None
    try:
        client = {
            "x-rapidapi-key":  RAPID_API_KEY,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com",
        }
        log.info("Twitter/X client initialised (twitter-api45).")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def _extract_tweets_from_twitter_api45(data: dict) -> list:
    """Best-effort walk of twitter-api45's flat search.php response into a
    list of {id, text, username} dicts.

    twitter-api45 returns tweets as a flat list (not GraphQL-nested like
    twitter241) — confirmed field names for this vendor's endpoints include
    tweet_id, text, created_at, favorites, bookmarks, views, quotes. The
    per-tweet author field hasn't been confirmed against a live search.php
    response yet, so this checks the common variants defensively instead of
    assuming one and silently dropping every tweet.
    """
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


def poll_twitter(client: dict):
    seen_ids: set = load_seen_ids("twitter")
    dirty = 0
    log.info(
        f"Twitter poll started | query_len:{len(TWITTER_SEARCH_QUERY)} | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    while True:
        try:
            url = "https://twitter-api45.p.rapidapi.com/search.php"
            params = {
                "query":       TWITTER_SEARCH_QUERY,
                "search_type": "Top",
            }
            headers = client
            response = requests.get(url=url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            tweets = _extract_tweets_from_twitter_api45(data)

            if not tweets:
                log.debug("Twitter: no results this cycle.")
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

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

            if new_count:
                log.info(
                    f"Twitter: {new_count} new tweets queued | "
                    f"queue_size:{twitter_queue.qsize()}"
                )

        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# FACEBOOK POLLER — v7.5.0 NEW — facebook-scraper3.p.rapidapi.com/search/posts
#
# Mirrors the Twitter poller's shape (RapidAPI headers dict + requests.get),
# but cycles through the KEYWORDS list one search per keyword per cycle,
# exactly like Reddit's per-subreddit cycle (poll_reddit_rss). Nothing about
# Reddit/Twitter/Telegram's polling changes — this is a new, independent
# poller feeding its own facebook_queue.
# ─────────────────────────────────────────────────────────────────────────────

def build_facebook_client() -> dict | None:
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
    """Best-effort walk of facebook-scraper3's /search/posts response into a
    list of {id, text, username, url} dicts. Field names are checked
    defensively (results/posts/data, post_id/id, message/text/content,
    author_name/author.name/author.username) since the vendor's exact
    schema for this endpoint hasn't been confirmed against a live response —
    same defensive approach used for twitter-api45 above, so a shape
    mismatch degrades to "no posts this cycle" instead of a crash.
    """
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
# SCHEDULERS — Daily Digest + Weekly Report (unchanged)
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
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.5.0 | Client: {CLIENT_ID} | Reddit + Twitter + Telegram + Facebook"}]},
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
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Corridor Breakdown*\n{breakdown('corridor')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Competitor Mentions*\n{breakdown('competitor_mentioned')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Pain Types*\n{breakdown('pain_type')}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top 3 Signals This Week*\n\n{_safe(chr(10).join(top3_lines), 2800)}"}},
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.5.0 | {CLIENT_ID} | Week ending {week_end}"}]},
            ],
        }

        result = retry_with_backoff(_post_to_slack, payload, retries=3, delay=2, label="WeeklyReport")
        if result:
            log.info(
                f"Weekly report sent | Total:{total} High:{len(high)} Med:{len(medium)} "
                f"Biz:{len(business)} Reddit:{len(reddit_sigs)} "
                f"Twitter:{len(twitter_sigs)} Telegram:{len(telegram_sigs)} "
                f"Facebook:{len(facebook_sigs)}"
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
# v7.4.4: each start_*_listener() now passes its OWN gap_seconds and
# timeout_seconds into run_batch_processor(). Reddit uses
# REDDIT_BATCH_GAP_SECONDS/REDDIT_BATCH_TIMEOUT_SECONDS, Twitter uses
# TWITTER_BATCH_GAP_SECONDS/TWITTER_BATCH_TIMEOUT_SECONDS, Telegram uses
# TELEGRAM_BATCH_GAP_SECONDS/TELEGRAM_BATCH_TIMEOUT_SECONDS. No sharing,
# no mixing — restart logic below is otherwise unchanged.
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED (REDDIT_ENABLED=false) — skipping.")
        return

    # NEW: reload any raw queue items persisted in flintel_queue_messages
    # (fetched but not yet consumed before a restart) back into the
    # in-memory reddit_queue BEFORE the poller/batch threads start.
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

    # NEW: reload any raw queue items persisted in flintel_queue_messages
    # (fetched but not yet consumed before a restart) back into the
    # in-memory twitter_queue BEFORE the poller/batch threads start.
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

    # NEW: reload any raw queue items persisted in flintel_queue_messages
    # (fetched but not yet consumed before a restart) back into the
    # in-memory telegram_queue BEFORE the listener/batch threads start.
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

    # NEW: reload any raw queue items persisted in flintel_queue_messages
    # (fetched but not yet consumed before a restart) back into the
    # in-memory facebook_queue BEFORE the poller/batch threads start.
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


async def start_rescore_listener():
    """Starts the rescore processor thread and monitors for crashes."""
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
# FASTAPI — REST API (routes unchanged; version bumped to 7.4.4)
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v7.5.0",
    description = (
        "Reddit (RSS) + Twitter + Telegram + Facebook signals: monitor, score, "
        "store, alert. Persistent batch state. Streaming Claude. Manual rescore. "
        "HubSpot receives medium (4-7) AND high (8-10) signals. "
        "HubSpot error visibility fix (FIX E). "
        "Slack alert + HubSpot note no longer display the numeric score (v7.4.3). "
        "Per-platform BATCH_GAP_SECONDS and BATCH_TIMEOUT_SECONDS — Reddit, "
        "Twitter, Telegram, and Facebook each run on their own independent "
        "gap/timeout, never mixed (v7.4.4 / v7.5.0)."
    ),
    version     = "7.5.0",
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
        "system":                  "FLINTEL v7.5.0",
        "client":                  CLIENT_ID,
        "platforms":               ["reddit", "twitter", "telegram", "facebook"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED),
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(RAPID_API_KEY)),
        "telegram_enabled":        TELEGRAM_ENABLED,
        "telegram_status":         _working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)),
        "facebook_enabled":        FACEBOOK_ENABLED,
        "facebook_status":         _working(FACEBOOK_ENABLED and bool(RAPID_API_KEY)),
        "reddit_mode":             "feedparser RSS (no credentials required)",
        "reddit_poll_interval":    REDDIT_POLL_INTERVAL,
        "reddit_batch_size":       REDDIT_BATCH_SIZE,
        "twitter_batch_size":      TWITTER_BATCH_SIZE,
        "telegram_batch_size":     TELEGRAM_BATCH_SIZE,
        "facebook_batch_size":     FACEBOOK_BATCH_SIZE,
        "rescore_batch_size":      RESCORE_BATCH_SIZE,
        "telegram_poll_interval":  TELEGRAM_POLL_INTERVAL,
        "facebook_poll_interval":  FACEBOOK_POLL_INTERVAL,
        # v7.4.4 / v7.5.0: per-platform gap/timeout, no shared globals
        "reddit_batch_gap_s":       REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":   REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter_batch_gap_s":      TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s":  TWITTER_BATCH_TIMEOUT_SECONDS,
        "telegram_batch_gap_s":     TELEGRAM_BATCH_GAP_SECONDS,
        "telegram_batch_timeout_s": TELEGRAM_BATCH_TIMEOUT_SECONDS,
        "facebook_batch_gap_s":     FACEBOOK_BATCH_GAP_SECONDS,
        "facebook_batch_timeout_s": FACEBOOK_BATCH_TIMEOUT_SECONDS,
        "rescore_batch_gap_s":      RESCORE_BATCH_GAP_SECONDS,
        "max_tokens":              MAX_TOKENS,
        "claude_stream_timeout_s": CLAUDE_STREAM_TIMEOUT,
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "facebook_queue_size":     facebook_queue.qsize(),
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
    twitter_working  = TWITTER_ENABLED and bool(RAPID_API_KEY)
    telegram_working = TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)
    facebook_working = FACEBOOK_ENABLED and bool(RAPID_API_KEY)

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
        "hubspot_configured":      bool(HUBSPOT_API_KEY),
        "hubspot_indicator":       _working(bool(HUBSPOT_API_KEY)),
        "hubspot_medium_signals":  True,
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "facebook_queue_size":     facebook_queue.qsize(),
        "rescore_pending":         pending_rescore,
        "rescore_working":         True,
        "rescore_indicator":       _working(True),
        "rescore_batch_gap_s":     RESCORE_BATCH_GAP_SECONDS,
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
    }


# ── HUBSPOT DIAGNOSTIC ENDPOINT (unchanged) ──────────────────────────────────

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


# ── RESCORE ENDPOINTS (unchanged) ────────────────────────────────────────────

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


# ── EXISTING ENDPOINTS (unchanged) ───────────────────────────────────────────

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
            "rescored_signals": rescored,
            "corridors":        agg("corridor"),
            "pain_types":       agg("pain_type"),
            "competitors":      agg("competitor_mentioned"),
            "tiers":            agg("tier"),
            "reddit_queue":     reddit_queue.qsize(),
            "twitter_queue":    twitter_queue.qsize(),
            "telegram_queue":   telegram_queue.qsize(),
            "facebook_queue":   facebook_queue.qsize(),
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
        start_rescore_listener(),
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v7.5.0")
    log.info("=" * 70)
    log.info(f"  Client             : {CLIENT_ID}")
    log.info(f"  Platforms          : Reddit (RSS) + Twitter/X + Telegram + Facebook")
    log.info(f"  Reddit             : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED)}")
    log.info(f"  Reddit mode        : feedparser RSS — no credentials required")
    log.info(f"  Reddit poll gap    : {REDDIT_POLL_INTERVAL}s between full subreddit cycles")
    log.info(f"  Twitter            : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(RAPID_API_KEY))}")
    log.info(f"  Telegram           : {TELEGRAM_ENABLED} | {_working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID))}")
    log.info(f"  Facebook           : {FACEBOOK_ENABLED} | {_working(FACEBOOK_ENABLED and bool(RAPID_API_KEY))}")
    log.info(f"  Facebook mode      : facebook-scraper3 RapidAPI — search/posts per keyword")
    log.info(f"  Facebook poll gap  : {FACEBOOK_POLL_INTERVAL}s between full keyword cycles | {FACEBOOK_KEYWORD_GAP_SECONDS}s between keywords")
    log.info(f"  Telegram polling   : {'every ' + str(TELEGRAM_POLL_INTERVAL) + 's' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled (TELEGRAM_POLL_INTERVAL=0)'}")
    log.info(f"  Reddit batch       : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch      : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Telegram batch     : {TELEGRAM_BATCH_SIZE} items OR {TELEGRAM_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {TELEGRAM_BATCH_GAP_SECONDS}s")
    log.info(f"  Facebook batch     : {FACEBOOK_BATCH_SIZE} items OR {FACEBOOK_BATCH_TIMEOUT_SECONDS}s → 1 Claude call | gap {FACEBOOK_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch      : {RESCORE_BATCH_SIZE} items per Claude call | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Batch timing       : per-platform, independent — Reddit/Twitter/Telegram/Facebook never share gap or timeout (v7.4.4 / v7.5.0)")
    log.info(f"  max_tokens         : {MAX_TOKENS}")
    log.info(f"  Claude streaming   : True | {_working(True)} (FIX C — no more 10-min error)")
    log.info(f"  Claude read timeout: None (stream open until complete)")
    log.info(f"  Twitter poll       : every {TWITTER_POLL_INTERVAL}s (rate-limit safe)")
    log.info(f"  Twitter query      : built dynamically from KEYWORDS ({len(KEYWORDS)} keywords)")
    log.info(f"  Telegram join gap  : {TELEGRAM_JOIN_GAP_SECONDS}s between group joins")
    log.info(f"  Score 1-3          : SILENT SAVE — MongoDB only, no alerts")
    log.info(f"  Score {MIN_SCORE_MEDIUM}-{MIN_SCORE_HIGH-1}          : MEDIUM — MongoDB + Slack + HubSpot (number hidden in message)")
    log.info(f"  Score {MIN_SCORE_HIGH}-10         : HIGH   — MongoDB + Slack + HubSpot (number hidden in message)")
    log.info(f"  MIN_SCORE_MEDIUM   : {MIN_SCORE_MEDIUM} (env-configurable)")
    log.info(f"  MIN_SCORE_HIGH     : {MIN_SCORE_HIGH} (env-configurable)")
    log.info(f"  MongoDB            : ALL scores 1-10 saved, nothing discarded (score still stored as-is)")
    log.info(f"  Platform isolation : Reddit / Twitter / Telegram / Facebook NEVER mixed (queues, batch state, AND now batch timing)")
    log.info(f"  Deduplication      : Persistent (MongoDB flintel_seen_ids) — survives restarts")
    log.info(f"  Batch state        : Persistent (MongoDB flintel_pending_batch) — survives restarts")
    log.info(f"  Queue messages     : Persistent (MongoDB flintel_queue_messages) — survives restarts (v7.4.5)")
    log.info(f"  Batch timeout      : Persistent (MongoDB flintel_batch_seconds) — survives restarts (v7.4.5)")
    log.info(f"  Partial-JSON       : Truncated Claude responses now salvage completed items")
    log.info(f"                     : instead of discarding the whole batch (FIX B)")
    log.info(f"  Rescore            : True | {_working(True)} — flintel_rescore_messages collection")
    log.info(f"  Rescore pipeline   : POST /rescore → Claude → Slack/HubSpot (same as live)")
    log.info(f"  Rescore poll       : every {RESCORE_POLL_INTERVAL}s")
    log.info(f"  Operator alerts    : Claude API down + MongoDB failure + partial recovery → Slack")
    log.info(f"  API auth           : {'True | ' + _working(True) + ' (API_KEY set)' if API_KEY else 'False | ' + _working(False) + ' (API_KEY not set — open access)'}")
    log.info(f"  Weekly state       : Persisted in MongoDB (survives restarts)")
    log.info(f"  Daily digest       : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report      : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits         : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Telegram groups    : {len(TARGET_TELEGRAM_GROUPS)} configured")
    log.info(f"  Keywords           : {len(KEYWORDS)} filters (same for all 4 platforms; Facebook also uses them as its search query terms)")
    log.info(f"  MongoDB DB         : {MONGODB_DB}")
    log.info(f"  HubSpot            : {'True | ' + _working(True) if HUBSPOT_API_KEY else 'False | ' + _working(False) + ' — set HUBSPOT_API_KEY'}")
    log.info(f"  HubSpot coverage   : score 4-10 (medium AND high) — fx_intent_score property still set")
    log.info(f"  HubSpot note       : numeric score line removed from note body (v7.4.3)")
    log.info(f"  Slack              : {'True | ' + _working(True) if SLACK_WEBHOOK_URL else 'False | ' + _working(False) + ' — set SLACK_WEBHOOK_URL'}")
    log.info(f"  Slack message      : numeric score removed from header/fields (v7.4.3) — tier/category still shown")
    log.info(f"  Output schema      : Platform-specific JSON (unchanged from v7.2) — ~140 tokens/item")
    log.info(f"  v7.4.4 changes     : BATCH_GAP_SECONDS and BATCH_TIMEOUT_SECONDS split into six")
    log.info(f"                     : platform-specific env vars (REDDIT_/TWITTER_/TELEGRAM_ prefix).")
    log.info(f"                     : Rescore now uses its own RESCORE_BATCH_GAP_SECONDS. BATCH_SIZE")
    log.info(f"                     : variables were already per-platform and are untouched.")
    log.info(f"  v7.4.5 changes     : (1) Batch completion log now shows item count — 'BATCH N")
    log.info(f"                     : COMPLETE — X item(s) completed'. (2) New flintel_queue_messages")
    log.info(f"                     : collection — raw queue items now survive a restart. (3) New")
    log.info(f"                     : flintel_batch_seconds collection — explicit, dedicated batch")
    log.info(f"                     : timeout persistence alongside flintel_pending_batch. Everything")
    log.info(f"                     : else — scoring, routing, MongoDB signal storage, prompts,")
    log.info(f"                     : FastAPI routes, thresholds, FIX A/B/C/D/E, hidden-score")
    log.info(f"                     : Slack/HubSpot display — 100% unchanged from v7.4.4.")
    log.info(f"  v7.5.0 changes     : Facebook added as a 4th platform (facebook-scraper3 RapidAPI,")
    log.info(f"                     : GET /search/posts per KEYWORDS entry) — own queue, own persistent")
    log.info(f"                     : dedup/batch-state/queue-message collections, own Claude scoring")
    log.info(f"                     : schema, own FastAPI routes. Reddit/Twitter/Telegram code paths,")
    log.info(f"                     : timing, and state are 100% unchanged — purely additive.")
    log.info("=" * 70)

    # FIX E (part 3): one-time, best-effort, read-only HubSpot property
    # self-check at startup. Never blocks, never raises.
    _hs_verify_properties()

    asyncio.run(main())
