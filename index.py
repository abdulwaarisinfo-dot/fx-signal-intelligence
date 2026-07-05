"""
FLINTEL v8.0 — Generic Signal Scorer (Slack + HubSpot removed)
================================================================
Platforms : Reddit (feedparser RSS, public .json enrichment) +
            Twitter/X (tweepy v2) + Telegram (Telethon)

WHAT CHANGED FROM v7.4.5 — CONTENT + FORMAT ONLY, ARCHITECTURE AS-IS:

  REMOVED ENTIRELY:
    - Slack delivery (send_slack_alert, send_operator_alert-to-Slack,
      daily digest, weekly report, the whole scheduler loop)
    - HubSpot delivery (all _hs_* functions, HUBSPOT_API_KEY,
      fx_intent_score / fx_* contact properties)
    - Old FX-specific Claude prompts (_SCORING_CORE +
      CLAUDE_SYSTEM_PROMPT_REDDIT/TWITTER/TELEGRAM) and their output
      fields (corridor, competitor_mentioned, pain_type, twitter_dm,
      twitter_reply, linkedin_message, telegram_dm, tier,
      signal_category, hubspot_priority, MIN_SCORE_MEDIUM/HIGH tiers)

  REPLACED:
    - ONE new generic, niche-agnostic Claude prompt (CLAUDE_SYSTEM_PROMPT)
      used for all three platforms, scoring 1-100 via three weighted
      components (relevance / google visibility / engagement) instead of
      the old 1-10 FX-specific model. Output: intent_score, is_relevant,
      reply_draft — nothing else.
    - New MongoDB `signals` schema: message_id, platform, post_url, text,
      username, subreddit_or_channel, posted_at, fetched_at, google_rank,
      search_volume, upvotes, comments, search_keyword, intent_score,
      is_relevant, reply_draft, client_id, status, created_at.

  KEPT 100% AS-IS (architecture, not content):
    - queue.Queue per platform (reddit_queue/twitter_queue/telegram_queue)
    - Keyword pre-filter list + passes_keyword_filter()
    - TARGET_SUBREDDITS, TARGET_TELEGRAM_GROUPS — verbatim
    - Persistent batch state (flintel_pending_batch), persistent dedup
      (flintel_seen_ids), persistent raw-queue (flintel_queue_messages),
      persistent batch-timeout clock (flintel_batch_seconds) — all the
      v7.3/v7.4.5 restart-survival fixes
    - Per-platform BATCH_GAP_SECONDS / BATCH_TIMEOUT_SECONDS (v7.4.4)
    - Claude streaming transport (FIX C) + partial-JSON truncation
      recovery (FIX B)
    - Twitter search query built dynamically from KEYWORDS (FIX 1)
    - Platform enable/disable flags + working indicators (FIX D)
    - API-key-protected FastAPI read endpoints
    - Rescore batch/poll/gap timing variables and mechanism shape —
      only the SOURCE changed: instead of polling a separate
      flintel_rescore_messages collection, run_rescore_processor() now
      polls the `signals` collection directly for {"status": "pending"}
      documents (written by the separate rescore.py migration helper
      after it re-enriches an old document). Confirmed documents are
      never re-touched; new live documents never carry a pending status
      in the first place.

  NEW ENRICHMENT (real, not placeholder):
    - Reddit upvotes/comments: fetched from Reddit's public
      `<post_url>.json` endpoint — no OAuth/PRAW credentials needed,
      same "no credentials required" philosophy as the RSS poller.
    - Twitter likes/replies: already returned by tweepy's
      `public_metrics` tweet field at poll time — no extra call needed.
    - Telegram views/forwards: already present on the Telethon message
      object at poll/listen time — no extra call needed.
    - Google rank + search volume: fetched via SerpApi (SERPAPI_KEY) for
      rank, and via DataForSEO Labs (DATAFORSEO_LOGIN/PASSWORD) for
      monthly search volume. NOTE: Google Search Console is NOT the
      right tool here — GSC only reports performance for a site YOU
      OWN in Search Console; it cannot tell you a random Reddit post's
      organic rank for a keyword. A SERP-tracking API (SerpApi,
      DataForSEO, etc.) is what actually answers "where does this
      keyword rank on Google" for arbitrary URLs.
"""

import asyncio
import logging
import os
import json
import time
import queue
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv

import html
import re
import feedparser
import anthropic
import httpx
import tweepy
import requests
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

REDDIT_POLL_INTERVAL = int(os.getenv("REDDIT_POLL_INTERVAL", "300"))

TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

TELEGRAM_API_ID      = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH    = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE       = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION     = os.getenv("TELEGRAM_SESSION", "flintel_telegram")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")
CLIENT_ID   = os.getenv("CLIENT_ID", "settla")

# The single keyword/topic this deployment tracks. This is the ONLY
# "context" Claude ever receives about what niche/product this is for.
SEARCH_KEYWORD = os.getenv("SEARCH_KEYWORD", "")

# Google rank via SerpApi; search volume via DataForSEO Labs. Both real,
# functional integrations below — just need the account/keys set in .env.
SERPAPI_KEY          = os.getenv("SERPAPI_KEY", "")
DATAFORSEO_LOGIN     = os.getenv("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD  = os.getenv("DATAFORSEO_PASSWORD", "")

REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
TELEGRAM_BATCH_SIZE = int(os.getenv("TELEGRAM_BATCH_SIZE", "10"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))

REDDIT_BATCH_GAP_SECONDS       = int(os.getenv("REDDIT_BATCH_GAP_SECONDS",       "30"))
REDDIT_BATCH_TIMEOUT_SECONDS   = int(os.getenv("REDDIT_BATCH_TIMEOUT_SECONDS",   "120"))

TWITTER_BATCH_GAP_SECONDS      = int(os.getenv("TWITTER_BATCH_GAP_SECONDS",      "30"))
TWITTER_BATCH_TIMEOUT_SECONDS  = int(os.getenv("TWITTER_BATCH_TIMEOUT_SECONDS",  "120"))

TELEGRAM_BATCH_GAP_SECONDS     = int(os.getenv("TELEGRAM_BATCH_GAP_SECONDS",     "30"))
TELEGRAM_BATCH_TIMEOUT_SECONDS = int(os.getenv("TELEGRAM_BATCH_TIMEOUT_SECONDS", "120"))

RESCORE_BATCH_GAP_SECONDS = int(os.getenv("RESCORE_BATCH_GAP_SECONDS", "30"))
RESCORE_POLL_INTERVAL     = int(os.getenv("RESCORE_POLL_INTERVAL", "10"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))
TELEGRAM_POLL_INTERVAL = int(os.getenv("TELEGRAM_POLL_INTERVAL", "300"))
TELEGRAM_JOIN_GAP_SECONDS = int(os.getenv("TELEGRAM_JOIN_GAP_SECONDS", "30"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))
CLAUDE_STREAM_TIMEOUT = int(os.getenv("CLAUDE_STREAM_TIMEOUT", "600"))

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
# PLATFORM ENABLE / DISABLE FLAGS (unchanged; FIX D indicators)
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED   = _bool_env("REDDIT_ENABLED",   True)
TWITTER_ENABLED  = _bool_env("TWITTER_ENABLED",  False)
TELEGRAM_ENABLED = _bool_env("TELEGRAM_ENABLED", False)


def _working(flag: bool) -> str:
    return "✅ Working" if flag else "❌ Not Working"


# ─────────────────────────────────────────────────────────────────────────────
# TARGET SUBREDDITS — 100% AS-IS, verbatim from v7.4.5
# ─────────────────────────────────────────────────────────────────────────────

TARGET_SUBREDDITS = [
    "Nigeria", "lagos", "Nigerians", "NigeriansAbroad",
    "AfricanDiaspora", "pakistan", "Pakistani", "PakistaniDiaspora",
    "PersonalFinanceCanada", "PersonalFinanceUK", "personalfinance",
    "entrepreneur", "smallbusiness", "digitalnomad", "africatech",
    "UKPersonalFinance", "Remittance", "moneytransfer",
    "CanadianInvestor", "ExpatFinance", "cybersecurity",
    "netsec", "AskNetsec", "sysadmin", "msp",
    "blueteamsec", "Scams", "personalfinance", "computerforensics",
    "GRC", "ThreatIntel", "Malware", "ThreatIntel", "hacking", "privacy",
    "Stripe", "Banking", "SecurityCareerAdvice", "sales", "salestechniques", "SaaS", "CRM", "freelance",
    "salesforce", "Sales_Tech", "smallbusiness", "startups_marketing", "digital_marketing", "RevOps", "ProductManagement", "consulting",
    "startups", "Entrepreneur", "EntrepreneurRideAlong",
    "growmybusiness", "b2b_marketing", "marketing",
        "nocode", "automation", "productivity",
    "software", "SoftwareEngineering", "webdev", "smallbusinessowner", "solopreneur", "indiehackers",
    "microsaas", "SideProject", "Business_Ideas", "software", "SoftwareEngineering", "webdev",
]

# ─────────────────────────────────────────────────────────────────────────────
# TARGET TELEGRAM GROUPS — 100% AS-IS, verbatim from v7.4.5
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

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER — 100% AS-IS, verbatim from v7.4.5
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
]


def passes_keyword_filter(text: str) -> bool:
    t = text.lower()
    for kw in KEYWORDS:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY (unchanged mechanism — FIX 1)
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    short_kws = [
        kw for kw in KEYWORDS
        if len(kw) <= 30 and " " not in kw or (" " in kw and len(kw) <= 25)
    ]
    seen, unique_kws = set(), []
    for kw in short_kws:
        kl = kw.lower()
        if kl not in seen:
            seen.add(kl)
            unique_kws.append(kw)

    max_query_len = 480
    parts, current_len = [], 0
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
# NEW GENERIC CLAUDE PROMPT — niche-agnostic, one prompt for all platforms
# ─────────────────────────────────────────────────────────────────────────────

CLAUDE_SYSTEM_PROMPT = """
You are Flintel's signal intelligence analyst.

Your only job is to read one social media post (Reddit, X, or Telegram)
together with its metadata, and produce two things:

1. An intent_score from 1 to 100, built from three weighted components
2. A short, human-written-style reply draft the end user can personalize
   and post themselves, in their own voice, from their own account

You are niche-agnostic. You are never told what industry, product, or
company this is for. You score purely on what is IN the post and its
metadata — nothing else.

SCORING MODEL — 100 POINTS, THREE COMPONENTS

COMPONENT 1 — RELEVANCE MATCH (0-40 points)
Does this post genuinely discuss the same problem, need, or topic as
the search_keyword provided — in meaning, not just in shared words?

  36-40  Post is unambiguously about exactly this problem/need.
  25-35  Post is clearly related, but broader, tangential, or partial.
  10-24  Post mentions matching words but the actual subject differs.
  0-9    No genuine connection.

This component is a HARD GATE. If relevance scores below 10, is_relevant
must be false and intent_score must not exceed 15, no matter how strong
Google visibility or engagement look.

COMPONENT 2 — GOOGLE VISIBILITY (0-30 points)
  google_rank contribution (0-20):
    Rank 1 -> 20 | Rank 2-3 -> 16 | Rank 4-10 -> 11
    Rank 11-20 -> 6 | Not ranked/null -> 0
  search_volume contribution (0-10):
    10,000+/mo -> 10 | 3,000-9,999 -> 7
    500-2,999 -> 4 | Under 500/null -> 1

COMPONENT 3 — ENGAGEMENT SIGNAL (0-30 points)
Derived from upvotes and comments, scaled by platform norms (a tweet
with 200 likes is not the same as a Reddit post with 200 upvotes —
judge proportionally, not by raw thresholds alone).
  Strong engagement -> 22-30 | Moderate -> 10-21
  Low/negligible -> 0-9 | No data -> 0

FINAL intent_score = Component 1 + Component 2 + Component 3, capped at 100.

REPLY DRAFT — RULES
Only generate reply_draft when is_relevant is true.
- Generic and honest — never invent a fake personal story, dollar
  amount, or timeline not present in the input.
- Acknowledge the poster's situation in one clause, then offer one
  genuinely useful angle — not a pitch.
- 2-3 sentences maximum. No links, no "DM me," no product/company name
  (the end user adds that themselves if relevant).
- End on warmth or a question, never a call-to-action.

OUTPUT FORMAT
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
# MONGODB — same connection, same `signals` collection; new indexes for
# the new schema; persistent batch-state collections kept as-is.
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        for field in ["intent_score", "created_at", "client_id", "platform", "is_relevant", "status"]:
            db.signals.create_index([(field, ASCENDING)])

        # FIX A: persistent batch state (unchanged)
        db.flintel_pending_batch.create_index([("platform", ASCENDING)], unique=True, name="platform_unique")
        db.flintel_seen_ids.create_index([("platform", ASCENDING)], unique=True, name="seen_platform_unique")

        # v7.4.5 additive fixes — kept as-is
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
# ANTHROPIC CLIENT — FIX C: streaming (unchanged)
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
    """Slack removed — operator visibility is now log-only. Same call
    sites as before (Claude down, MongoDB write failure, truncation), just
    logging instead of posting to a webhook."""
    log.log(
        logging.CRITICAL if level == "CRITICAL" else logging.ERROR,
        f"[OPERATOR ALERT] {title} — {detail}",
    )


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


# ─────────────────────────────────────────────────────────────────────────────
# v7.4.5 fixes kept as-is — persistent raw-queue + persistent batch clock
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
# ENRICHMENT — REAL implementations (no PRAW/OAuth needed for Reddit,
# same "no credentials required" philosophy as the RSS poller).
# ─────────────────────────────────────────────────────────────────────────────

def fetch_reddit_stats(post_url: str) -> dict:
    """
    Reddit exposes upvotes + comment count publicly by appending .json to
    any post URL — no OAuth/PRAW client_id/secret needed. Example:
    https://www.reddit.com/r/X/comments/abc123/title/.json
    """
    if not post_url:
        return {"upvotes": None, "comments": None}
    try:
        url = post_url.rstrip("/") + ".json"
        r = requests.get(url, headers={"User-Agent": "flintel-enrichment/1.0"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        post_data = data[0]["data"]["children"][0]["data"]
        return {"upvotes": post_data.get("ups"), "comments": post_data.get("num_comments")}
    except Exception as exc:
        log.error(f"fetch_reddit_stats error for {post_url}: {exc}")
        return {"upvotes": None, "comments": None}


def fetch_google_rank(search_keyword: str) -> int | None:
    """
    Google Search Console is NOT the right tool for this — GSC only shows
    performance for a property YOU verify/own, it cannot report where an
    arbitrary Reddit/X post or a generic keyword ranks in results you
    don't own. A SERP-tracking API is what actually answers this.
    Real implementation via SerpApi (requires SERPAPI_KEY in .env).
    """
    if not SERPAPI_KEY or not search_keyword:
        return None
    try:
        r = requests.get(
            "https://serpapi.com/search",
            params={"engine": "google", "q": search_keyword, "api_key": SERPAPI_KEY},
            timeout=15,
        )
        r.raise_for_status()
        organic = r.json().get("organic_results", [])
        return organic[0].get("position") if organic else None
    except Exception as exc:
        log.error(f"fetch_google_rank error for {search_keyword!r}: {exc}")
        return None


def fetch_search_volume(search_keyword: str) -> int | None:
    """
    Monthly search volume via DataForSEO Labs (requires DATAFORSEO_LOGIN /
    DATAFORSEO_PASSWORD in .env — HTTP Basic Auth). SerpApi's core /search
    endpoint does not return volume directly; DataForSEO Labs' keyword
    endpoints do.
    """
    if not (DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD) or not search_keyword:
        return None
    try:
        r = requests.post(
            "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live",
            auth=(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD),
            json=[{"keywords": [search_keyword], "language_code": "en", "location_code": 2840}],
            timeout=20,
        )
        r.raise_for_status()
        result = r.json().get("tasks", [{}])[0].get("result", [])
        return result[0].get("search_volume") if result else None
    except Exception as exc:
        log.error(f"fetch_search_volume error for {search_keyword!r}: {exc}")
        return None


def fetch_google_stats(search_keyword: str) -> dict:
    return {
        "google_rank":   fetch_google_rank(search_keyword),
        "search_volume": fetch_search_volume(search_keyword),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER — same streaming transport + partial-JSON recovery
# mechanism (FIX B/C), new generic prompt + new input/output schema.
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
    return {"index": index, "intent_score": 1, "is_relevant": False, "reply_draft": None}


def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    """Brace-depth-tracking salvage of a truncated JSON array — unchanged (FIX B)."""
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
    """FIX C: streaming context manager — unchanged transport."""
    prompt = _build_batch_prompt(batch)
    with anthropic_client.messages.stream(
        model="claude-sonnet-4-6",
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
        return [_fallback_score(i + 1) for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE — new schema only.
# ─────────────────────────────────────────────────────────────────────────────

def save_new_signal(item: dict, score_result: dict) -> bool:
    """Brand-new LIVE items — stored already status='confirmed'."""
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
        "status":                 "confirmed",
        "created_at":             datetime.now(timezone.utc),
    }
    try:
        db.signals.insert_one(doc)
        log.info(
            f"SAVED [{doc['platform'].upper()}] score:{doc['intent_score']} "
            f"relevant:{doc['is_relevant']} | u/{doc['username']} | "
            f"upvotes:{doc['upvotes']} comments:{doc['comments']} "
            f"rank:{doc['google_rank']} volume:{doc['search_volume']}"
        )
        return True
    except DuplicateKeyError:
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        log_operator_alert("MongoDB Write Failed", str(exc), level="CRITICAL")
        return False


def replace_confirmed_signal(message_id: str, enrichment: dict, score_result: dict) -> bool:
    """
    Called by the rescore processor once Claude has scored a pending
    (previously-migrated) document. Fully REPLACES the document body —
    this is where old-schema leftovers get permanently wiped, since
    replace_one() overwrites the whole document.
    """
    existing = db.signals.find_one({"message_id": message_id})
    if not existing:
        log.warning(f"[RESCORE] No existing doc for {message_id} — skipping.")
        return False

    new_doc = {
        "message_id":            message_id,
        "platform":               existing.get("platform", "unknown"),
        "post_url":               existing.get("post_url", ""),
        "text":                   existing.get("text") or existing.get("message_text", ""),
        "username":               existing.get("username", "unknown"),
        "subreddit_or_channel":   existing.get("subreddit_or_channel") or existing.get("subreddit") or existing.get("telegram_group", ""),
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
# GENERIC BATCH PROCESSOR — same batching/timing mechanism (v7.4.4/v7.4.5),
# now enriches (Reddit upvotes/comments + Google rank/volume) right before
# scoring, using the new prompt/schema, and Slack/HubSpot delivery removed.
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

                if not passes_keyword_filter(text):
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

                # ── ENRICHMENT — real numbers, right before scoring ──────
                # Reddit: refetch fresh upvotes/comments via public .json.
                # Twitter/Telegram: upvotes/comments already populated at
                # poll time from public_metrics / msg.views/forwards.
                google_stats = fetch_google_stats(SEARCH_KEYWORD)
                for it in batch_to_send:
                    if it.get("platform") == "reddit":
                        it.update(fetch_reddit_stats(it.get("post_url", "")))
                    it.setdefault("upvotes", None)
                    it.setdefault("comments", None)
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
                    save_new_signal(it, sr)

                log.info(f"[{platform_label}] BATCH {total_batches} COMPLETE — "
                         f"{len(batch_to_send)} item(s) | waiting {gap_seconds}s...")
                time.sleep(gap_seconds)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR — same batch/poll/gap timing shape as v7.4.5, but now
# polls the `signals` collection DIRECTLY for {"status": "pending"}
# documents (written by rescore.py after re-enrichment). Confirmed
# documents are never re-touched; live documents never carry a pending
# status in the first place.
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
# REDDIT — feedparser RSS poller (unchanged mechanism)
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
            if not entry_id or _reddit_rss_is_seen(entry_id):
                continue

            title = entry.get("title", "").strip()
            summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(entry.get("summary", ""))).strip()
            text = f"{title}\n\n{summary_plain}" if summary_plain and summary_plain.lower() != title.lower() else title

            items.append({
                "message_id":           f"reddit_rss_{entry_id.split('/')[-1] or entry_id}",
                "platform":             "reddit",
                "text":                 text,
                "username":             entry.get("author", "unknown").lstrip("u/").strip() or "unknown",
                "subreddit_or_channel": subreddit,
                "post_url":             entry.get("link", ""),
                "posted_at":            entry.get("published", None),
                "search_keyword":       SEARCH_KEYWORD,
                "upvotes":              None,   # enriched at batch time
                "comments":             None,   # enriched at batch time
            })
    except Exception as exc:
        log.error(f"[REDDIT-RSS] Error fetching r/{subreddit}: {exc}")
    return items


def poll_reddit_rss():
    log.info(f"[REDDIT-RSS] Poller started | {len(TARGET_SUBREDDITS)} subreddits | "
             f"poll interval:{REDDIT_POLL_INTERVAL}s | dedup resumed with {len(_reddit_seen_ids)} ID(s)")

    while True:
        cycle_start, total_new, total_errors = time.time(), 0, 0

        for subreddit in TARGET_SUBREDDITS:
            try:
                items = _get_reddit_rss(subreddit)
                for item in items:
                    reddit_queue.put(item)
                    save_queue_message("reddit", item)
                    total_new += 1
                if items:
                    log.info(f"[REDDIT-RSS] r/{subreddit} → {len(items)} new items queued "
                              f"(queue size: {reddit_queue.qsize()})")
                time.sleep(2)
            except Exception as exc:
                log.error(f"[REDDIT-RSS] Unhandled error for r/{subreddit}: {exc}")
                total_errors += 1

        save_seen_ids("reddit", _reddit_seen_ids)
        log.info(f"[REDDIT-RSS] Cycle complete | new:{total_new} errors:{total_errors} | "
                 f"elapsed:{time.time()-cycle_start:.1f}s | sleeping {REDDIT_POLL_INTERVAL}s...")
        time.sleep(REDDIT_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER — unchanged mechanism, now also pulls public_metrics
# (like_count/reply_count) at poll time so no extra enrichment call needed.
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
# TELEGRAM LISTENER — unchanged mechanism, now also captures msg.views /
# msg.forwards at listen/poll time (already on the Telethon message object,
# no extra call needed).
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
    log.info(f"Telegram: starting auto-join for {len(TARGET_TELEGRAM_GROUPS)} groups | "
             f"gap:{TELEGRAM_JOIN_GAP_SECONDS}s")
    joined, skipped, failed = 0, 0, 0

    for group in TARGET_TELEGRAM_GROUPS:
        try:
            target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
            client.loop.run_until_complete(client(JoinChannelRequest(target)))
            joined += 1
            time.sleep(TELEGRAM_JOIN_GAP_SECONDS)
        except UserAlreadyParticipantError:
            skipped += 1
        except FloodWaitError as e:
            time.sleep(e.seconds + 5)
            failed += 1
        except (ChannelPrivateError, InviteHashExpiredError):
            failed += 1
        except Exception as exc:
            log.error(f"Telegram: join error for {group} — {exc}")
            failed += 1

    log.info(f"Telegram auto-join complete | joined:{joined} already_in:{skipped} failed:{failed}")


async def _poll_telegram_groups(client: TelegramClient):
    if TELEGRAM_POLL_INTERVAL == 0:
        log.info("[TELEGRAM-POLL] Disabled — listener-only mode.")
        return

    log.info(f"[TELEGRAM-POLL] Poller started | {len(TARGET_TELEGRAM_GROUPS)} groups | "
             f"interval:{TELEGRAM_POLL_INTERVAL}s")

    while True:
        cycle_start, total_new, total_errors = time.time(), 0, 0

        for group in TARGET_TELEGRAM_GROUPS:
            try:
                target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
                messages = await client.get_messages(target, limit=20)

                for msg in messages:
                    if not msg or not msg.text or len(msg.text) < 5:
                        continue
                    chat_id, msg_id = (msg.chat_id or 0), msg.id
                    if _telegram_is_seen(chat_id, msg_id):
                        continue

                    sender = await msg.get_sender()
                    tg_user = getattr(sender, "username", None) or f"user_{getattr(sender, 'id', 0)}"

                    _tg_item = {
                        "message_id":           f"telegram_{chat_id}_{msg_id}",
                        "platform":             "telegram",
                        "text":                 msg.text,
                        "username":             tg_user,
                        "subreddit_or_channel": group,
                        "post_url":             "",
                        "posted_at":            str(msg.date) if msg.date else None,
                        "search_keyword":       SEARCH_KEYWORD,
                        "upvotes":              getattr(msg, "views", None),
                        "comments":             getattr(msg, "forwards", None),
                    }
                    telegram_queue.put(_tg_item)
                    save_queue_message("telegram", _tg_item)
                    total_new += 1

                await asyncio.sleep(2)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 5)
                total_errors += 1
            except Exception as exc:
                log.error(f"[TELEGRAM-POLL] Error for {group}: {exc}")
                total_errors += 1

        save_seen_ids("telegram", _telegram_seen_ids)
        log.info(f"[TELEGRAM-POLL] Cycle complete | new:{total_new} errors:{total_errors} | "
                 f"elapsed:{time.time()-cycle_start:.1f}s | sleeping {TELEGRAM_POLL_INTERVAL}s...")
        await asyncio.sleep(TELEGRAM_POLL_INTERVAL)


async def _run_telegram_listener(client: TelegramClient):
    target_set = {g.lstrip("@").lower() for g in TARGET_TELEGRAM_GROUPS}

    @client.on(events.NewMessage)
    async def _on_message(event):
        try:
            chat = await event.get_chat()
            username_attr = getattr(chat, "username", None)
            chat_title = getattr(chat, "title", "") or ""
            group_key = (username_attr or chat_title).lower().replace(" ", "").replace("-", "").replace("_", "")

            if group_key not in target_set and (username_attr or "").lower() not in target_set:
                return

            sender = await event.get_sender()
            text = event.raw_text or ""
            sender_id = getattr(sender, "id", 0)
            tg_user = getattr(sender, "username", None) or f"user_{sender_id}"
            msg_id, chat_id = event.id, event.chat_id

            if not text or len(text) < 5 or _telegram_is_seen(chat_id, msg_id):
                return

            _tg_item = {
                "message_id":           f"telegram_{chat_id}_{msg_id}",
                "platform":             "telegram",
                "text":                 text,
                "username":             tg_user,
                "subreddit_or_channel": username_attr or chat_title,
                "post_url":             "",
                "posted_at":            str(event.date) if getattr(event, "date", None) else None,
                "search_keyword":       SEARCH_KEYWORD,
                "upvotes":              getattr(event.message, "views", None),
                "comments":             getattr(event.message, "forwards", None),
            }
            telegram_queue.put(_tg_item)
            save_queue_message("telegram", _tg_item)
        except Exception as exc:
            log.error(f"Telegram message handler error: {exc}")

    log.info("Telegram listener active — read-only, no interactions.")
    await asyncio.gather(client.run_until_disconnected(), _poll_telegram_groups(client))


def run_telegram_listener_thread():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning("Telegram disabled — set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
        return
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH, loop=loop)
        loop.run_until_complete(client.start(phone=TELEGRAM_PHONE))
        me = loop.run_until_complete(client.get_me())
        log.info(f"Telegram authenticated as {me.first_name} (@{me.username or me.id})")
        _join_telegram_groups_sync(client)
        loop.run_until_complete(_run_telegram_listener(client))
    except Exception as exc:
        log.error(f"Telegram listener thread error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart (unchanged shape)
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED — skipping.")
        return

    resumed = load_queue_messages("reddit")
    for it in resumed:
        reddit_queue.put(it)
    if resumed:
        log.info(f"[REDDIT] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    rss_thread = threading.Thread(target=poll_reddit_rss, daemon=True, name="Reddit-RSS")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS, REDDIT_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Reddit-Batch",
    )
    rss_thread.start()
    btch_thread.start()
    log.info(f"Reddit threads running: RSS-Poller ✅ | Batch ✅ | "
             f"gap:{REDDIT_BATCH_GAP_SECONDS}s | timeout:{REDDIT_BATCH_TIMEOUT_SECONDS}s")

    while True:
        await asyncio.sleep(60)
        if not rss_thread.is_alive():
            log.error("Reddit RSS thread died — restarting...")
            rss_thread = threading.Thread(target=poll_reddit_rss, daemon=True, name="Reddit-RSS")
            rss_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT", REDDIT_BATCH_GAP_SECONDS, REDDIT_BATCH_TIMEOUT_SECONDS),
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
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS, TWITTER_BATCH_TIMEOUT_SECONDS),
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
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER", TWITTER_BATCH_GAP_SECONDS, TWITTER_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_telegram_listener():
    if not TELEGRAM_ENABLED:
        log.warning("Telegram platform DISABLED — skipping.")
        return
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning("Telegram listener not started — set TELEGRAM_API_ID/HASH/PHONE.")
        return

    resumed = load_queue_messages("telegram")
    for it in resumed:
        telegram_queue.put(it)
    if resumed:
        log.info(f"[TELEGRAM] Resumed {len(resumed)} queue message(s) from MongoDB after restart.")

    tg_thread = threading.Thread(target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM", TELEGRAM_BATCH_GAP_SECONDS, TELEGRAM_BATCH_TIMEOUT_SECONDS),
        daemon=True, name="Telegram-Batch",
    )
    tg_thread.start()
    btch_thread.start()
    log.info(f"Telegram threads running: Listener ✅ | Batch ✅ | "
             f"gap:{TELEGRAM_BATCH_GAP_SECONDS}s | timeout:{TELEGRAM_BATCH_TIMEOUT_SECONDS}s")

    while True:
        await asyncio.sleep(60)
        if not tg_thread.is_alive():
            log.error("Telegram listener thread died — restarting...")
            tg_thread = threading.Thread(target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener")
            tg_thread.start()
        if not btch_thread.is_alive():
            log.error("Telegram batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM", TELEGRAM_BATCH_GAP_SECONDS, TELEGRAM_BATCH_TIMEOUT_SECONDS),
                daemon=True, name="Telegram-Batch",
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
# FASTAPI — read-only endpoints (Slack/HubSpot diagnostics removed)
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Flintel v8.0 — Generic Signal Scorer (Slack + HubSpot removed)",
    description=(
        "Reddit (RSS) + Twitter + Telegram signals: monitor, score (generic "
        "1-100 relevance/visibility/engagement model), store. Persistent "
        "batch state + queue + dedup (v7.3-v7.4.5 fixes kept as-is). "
        "Streaming Claude with partial-JSON recovery. Rescore reads "
        "pending status directly from `signals`. No Slack. No HubSpot."
    ),
    version="8.0.0",
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
    return {
        "status":                  "running",
        "system":                  "FLINTEL v8.0 (generic, Slack+HubSpot removed)",
        "client":                  CLIENT_ID,
        "search_keyword":          SEARCH_KEYWORD,
        "platforms":               ["reddit", "twitter", "telegram"],
        "reddit_enabled":          REDDIT_ENABLED,
        "reddit_status":           _working(REDDIT_ENABLED),
        "twitter_enabled":         TWITTER_ENABLED,
        "twitter_status":          _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "telegram_enabled":        TELEGRAM_ENABLED,
        "telegram_status":         _working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)),
        "reddit_batch_size":       REDDIT_BATCH_SIZE,
        "twitter_batch_size":      TWITTER_BATCH_SIZE,
        "telegram_batch_size":     TELEGRAM_BATCH_SIZE,
        "rescore_batch_size":      RESCORE_BATCH_SIZE,
        "reddit_batch_gap_s":      REDDIT_BATCH_GAP_SECONDS,
        "reddit_batch_timeout_s":  REDDIT_BATCH_TIMEOUT_SECONDS,
        "twitter_batch_gap_s":     TWITTER_BATCH_GAP_SECONDS,
        "twitter_batch_timeout_s": TWITTER_BATCH_TIMEOUT_SECONDS,
        "telegram_batch_gap_s":    TELEGRAM_BATCH_GAP_SECONDS,
        "telegram_batch_timeout_s": TELEGRAM_BATCH_TIMEOUT_SECONDS,
        "rescore_batch_gap_s":     RESCORE_BATCH_GAP_SECONDS,
        "serpapi_configured":     bool(SERPAPI_KEY),
        "dataforseo_configured":  bool(DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "auth_required":           bool(API_KEY),
        "slack_removed":           True,
        "hubspot_removed":         True,
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
        "reddit_working":          REDDIT_ENABLED,
        "reddit_indicator":        _working(REDDIT_ENABLED),
        "twitter_working":         TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN),
        "twitter_indicator":       _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "telegram_working":        TELEGRAM_ENABLED and bool(TELEGRAM_API_ID),
        "telegram_indicator":      _working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)),
        "reddit_queue_size":       reddit_queue.qsize(),
        "twitter_queue_size":      twitter_queue.qsize(),
        "telegram_queue_size":     telegram_queue.qsize(),
        "rescore_pending":         db.signals.count_documents({"status": "pending"}),
        "client_id":               CLIENT_ID,
        "timestamp":               datetime.now(timezone.utc).isoformat(),
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
        start_telegram_listener(),
        start_rescore_listener(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FLINTEL v8.0 — GENERIC SIGNAL SCORER (Slack + HubSpot removed)")
    log.info("=" * 70)
    log.info(f"  Client             : {CLIENT_ID}")
    log.info(f"  Search keyword     : {SEARCH_KEYWORD!r}")
    log.info(f"  Platforms          : Reddit (RSS) + Twitter/X + Telegram")
    log.info(f"  Reddit             : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED)}")
    log.info(f"  Twitter            : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Telegram           : {TELEGRAM_ENABLED} | {_working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID))}")
    log.info(f"  Reddit batch       : {REDDIT_BATCH_SIZE} items OR {REDDIT_BATCH_TIMEOUT_SECONDS}s | gap {REDDIT_BATCH_GAP_SECONDS}s")
    log.info(f"  Twitter batch      : {TWITTER_BATCH_SIZE} items OR {TWITTER_BATCH_TIMEOUT_SECONDS}s | gap {TWITTER_BATCH_GAP_SECONDS}s")
    log.info(f"  Telegram batch     : {TELEGRAM_BATCH_SIZE} items OR {TELEGRAM_BATCH_TIMEOUT_SECONDS}s | gap {TELEGRAM_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore batch      : {RESCORE_BATCH_SIZE} items | poll {RESCORE_POLL_INTERVAL}s | gap {RESCORE_BATCH_GAP_SECONDS}s")
    log.info(f"  Rescore source     : signals collection, status='pending' (no separate collection)")
    log.info(f"  Claude streaming   : True (FIX C) | prompt: generic 1-100 relevance/visibility/engagement")
    log.info(f"  SerpApi configured : {bool(SERPAPI_KEY)}  (google_rank)")
    log.info(f"  DataForSEO config  : {bool(DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD)}  (search_volume)")
    log.info(f"  Slack              : REMOVED")
    log.info(f"  HubSpot            : REMOVED")
    log.info(f"  MongoDB DB         : {MONGODB_DB}")
    log.info(f"  Subreddits         : {len(TARGET_SUBREDDITS)} monitored (unchanged)")
    log.info(f"  Telegram groups    : {len(TARGET_TELEGRAM_GROUPS)} configured (unchanged)")
    log.info(f"  Keywords           : {len(KEYWORDS)} filters (unchanged)")
    log.info(f"  API auth           : {'True | ' + _working(True) if API_KEY else 'False | ' + _working(False)}")
    log.info("=" * 70)

    asyncio.run(main())
