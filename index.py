"""
FX Signal Intelligence System — FLINTEL v7.5
=============================================
Platforms : Reddit (feedparser RSS) + Twitter/X (tweepy v2) + Telegram (Telethon)
Pipeline  :
  Reddit   → Poll /new.rss per subreddit via feedparser (no PRAW, no credentials)
  Twitter  → Fetch mentions / search / replies (rate-limit safe, 50/block)
  Telegram → Listen to group messages (human account, Telethon, read-only)
      ↓
  Keyword Pre-Filter        (free, fast — drops 80%+ noise)
      ↓
  Batch Collector:
    Reddit   — N items per Claude call  (or timeout)
    Twitter  — N items per Claude call  (or timeout)
    Telegram — N items per Claude call  (or timeout)
      ↓
  *** NEW v7.5 — APOLLO ENRICHMENT (runs BEFORE Claude) ***
    For each item in batch:
      → Send username + platform to Apollo
      → Apollo searches 275M+ contact database
      → Apollo returns: name, email, phone, company,
        job title, LinkedIn, location, industry, company size
      → Apollo data attached to item dict
      → Claude receives post text + Apollo context together
  ↓
  Gap                       (between each batch)
      ↓
  Claude AI Intent Scorer   (single merged prompt per batch, platform-specific schema)
  *** Claude now scores WITH Apollo context — more accurate scores ***
      ↓
  MongoDB Storage           (ALL scores 1-10 saved — apollo_enrichment field added)
      ↓
  Slack Alert               (score 6-10, professional blocks — Apollo section added)
      ↓
  HubSpot CRM               (score 8-10 only — Apollo data enriches contact)
      ↓
  FastAPI REST Endpoints
      ↓
  Daily Digest Scheduler    (score 6-7, 08:00 UTC)
      ↓
  Weekly Report Scheduler   (all signals, Monday 09:00 UTC)

Score rules (UNCHANGED from v7.4):
  1-5  → SAVED to MongoDB only — never alerted
  6-7  → MEDIUM  — MongoDB + Slack only
  8-10 → HIGH    — MongoDB + Slack + HubSpot

Apollo enrichment rules (NEW v7.5):
  - Runs BEFORE Claude for every item that passes keyword filter
  - Score threshold controlled by APOLLO_MIN_SCORE_ENRICH (default: 0 = all)
  - If Apollo finds no match → item proceeds without enrichment (graceful skip)
  - Apollo data saved to MongoDB as apollo_enrichment field
  - Apollo data shown as separate block in Slack alerts
  - Apollo data added to HubSpot contact properties
  - Apollo API key: APOLLO_API_KEY in .env

Changelog v7.5 (Apollo enrichment — all v7.4 logic 100% unchanged):

  NEW — APOLLO ENRICHMENT BEFORE CLAUDE SCORING
        Apollo enriches each signal BEFORE Claude sees it.
        Claude receives the original post text PLUS Apollo context
        (name, company, job title, industry, location) in the prompt.
        This means Claude scores with full knowledge of who the person
        is — not just what they wrote. A Fintech Founder complaining
        about fees scores higher than an anonymous user saying the same.

        Apollo search strategy per platform:
          Twitter  → search by Twitter handle (Apollo has 275M+ Twitter mappings)
          Reddit   → search by Reddit username as name hint + any company/name
                     Claude extracted in post text (best-effort)
          Telegram → search by username + any company/name in message

        Apollo graceful degradation:
          → If APOLLO_API_KEY not set: skip enrichment, log warning, proceed
          → If Apollo returns no match: skip enrichment, proceed normally
          → If Apollo API errors: skip enrichment, log error, proceed
          → Apollo never blocks the pipeline — it is always optional

        MongoDB: apollo_enrichment field added to signal document
          {
            "searched_at": "...",
            "matched": true/false,
            "search_query": {...},
            "full_name": "...",
            "email": "...",
            "phone": "...",
            "company": "...",
            "job_title": "...",
            "linkedin_url": "...",
            "location": "...",
            "industry": "...",
            "company_size": "...",
            "twitter_handle": "...",
            "apollo_id": "..."
          }

        Slack: Apollo block added after main signal fields
          ── APOLLO ENRICHMENT ──────────
          Full Name   : James Osei
          Email       : james@umoja.co.za
          Phone       : +27 82 345 6789
          Company     : Umoja Finance Ltd
          Job Title   : Founder & CEO
          LinkedIn    : linkedin.com/in/...
          Location    : Cape Town, South Africa
          Industry    : Fintech
          Size        : 11-50 employees

        HubSpot: Apollo fields added to contact properties
          apollo_full_name, apollo_email, apollo_phone,
          apollo_company, apollo_job_title, apollo_linkedin,
          apollo_location, apollo_industry, apollo_company_size

        FastAPI: new endpoints
          GET /signals/apollo-matched  — signals where Apollo found a match
          GET /signals/apollo-stats    — Apollo match rate stats

  NOTHING ELSE CHANGED from v7.4. Scoring logic, prompts, Slack block
  formatting (base), HubSpot note format (base), FastAPI routes, thresholds,
  keyword list, FIX A, FIX B, FIX C, FIX D, rescore feature — 100% unchanged.

Changelog v7.4:
  FIX C — Claude streaming.
  FIX D — Enable/disable working indicators.
  NEW   — Rescore messages feature.

Changelog v7.3: FIX A (persistent batch state) + FIX B (partial-JSON recovery).
Changelog v7.2: Output cost optimisation (OPT 1-6).
Changelog v7.1: Bug fixes + platform enable/disable flags.
Changelog v7.0: Telegram + MongoDB ALL scores + batch timeout.

Apollo endpoint fixes (2026):
  CHANGE 1 — APOLLO_BASE_URL: "https://api.apollo.io/v1"
           → "https://api.apollo.io/api/v1"
  CHANGE 2 — _apollo_search_by_twitter(): 2-step search→enrich
           using mixed_people/api_search + people/match
  CHANGE 3 — _apollo_search_by_name(): 2-step search→enrich
           using mixed_people/api_search + people/match
  CHANGE 4 — _apollo_search_by_domain(): 2-step search→enrich
           using mixed_people/api_search + people/match
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
import tweepy
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

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY")

# ── NEW v7.5: Apollo config ──────────────────────────────────────────────────
APOLLO_API_KEY         = os.getenv("APOLLO_API_KEY", "")
# ── CHANGE 1: Fixed base URL — added /api/ prefix (2026 requirement) ─────────
APOLLO_BASE_URL        = "https://api.apollo.io/api/v1"
# ─────────────────────────────────────────────────────────────────────────────
APOLLO_ENRICH_ALL      = os.getenv("APOLLO_ENRICH_ALL", "true").strip().lower() in ("1", "true", "yes", "on")
APOLLO_REQUEST_TIMEOUT = int(os.getenv("APOLLO_REQUEST_TIMEOUT", "10"))
# ─────────────────────────────────────────────────────────────────────────────

MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "4"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))
CLIENT_ID        = os.getenv("CLIENT_ID", "settla")

REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
TELEGRAM_BATCH_SIZE = int(os.getenv("TELEGRAM_BATCH_SIZE", "10"))
RESCORE_BATCH_SIZE  = int(os.getenv("RESCORE_BATCH_SIZE",  REDDIT_BATCH_SIZE))
BATCH_GAP_SECONDS   = int(os.getenv("BATCH_GAP_SECONDS",   "30"))

BATCH_TIMEOUT_SECONDS = int(os.getenv("BATCH_TIMEOUT_SECONDS", "120"))

DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

TELEGRAM_JOIN_GAP_SECONDS = int(os.getenv("TELEGRAM_JOIN_GAP_SECONDS", "30"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

CLAUDE_STREAM_TIMEOUT = int(os.getenv("CLAUDE_STREAM_TIMEOUT", "600"))

RESCORE_POLL_INTERVAL = int(os.getenv("RESCORE_POLL_INTERVAL", "10"))

# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH (unchanged from v7.4)
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
# PLATFORM ENABLE / DISABLE FLAGS (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED   = _bool_env("REDDIT_ENABLED",   False)
TWITTER_ENABLED  = _bool_env("TWITTER_ENABLED",  False)
TELEGRAM_ENABLED = _bool_env("TELEGRAM_ENABLED", False)


def _working(flag: bool) -> str:
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
# SHARED QUEUES (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:   queue.Queue = queue.Queue()
twitter_queue:  queue.Queue = queue.Queue()
telegram_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER (unchanged)
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
]


def passes_keyword_filter(text: str) -> bool:
    t = text.lower()
    for kw in KEYWORDS:
        if kw.lower() in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY (unchanged from v7.4)
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
# DERIVE FIELDS LOCALLY (unchanged from v7.4)
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
# NEW v7.5 — APOLLO ENRICHMENT
# ─────────────────────────────────────────────────────────────────────────────

_APOLLO_EMPTY = {
    "matched":        False,
    "full_name":      None,
    "email":          None,
    "phone":          None,
    "company":        None,
    "job_title":      None,
    "linkedin_url":   None,
    "location":       None,
    "industry":       None,
    "company_size":   None,
    "twitter_handle": None,
    "apollo_id":      None,
}


def _apollo_headers() -> dict:
    return {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }


# ── CHANGE 2: _apollo_search_by_twitter — 2-step search → enrich ─────────────
def _apollo_search_by_twitter(username: str) -> dict:
    """
    Search Apollo by Twitter handle.
    2026 fix: Step 1 mixed_people/api_search (free, no credits) → get Apollo ID
              Step 2 people/match (1 credit) → get full data including email/phone
    Best match rate for Twitter platform signals.
    """
    try:
        # ── Step 1: Search — free, no credits ──────────────────────────────
        r = requests.post(
            f"{APOLLO_BASE_URL}/mixed_people/api_search",
            json={
                "q_keywords": username,
                "per_page":   3,
                "page":       1,
            },
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        people = r.json().get("people", [])
        if not people:
            return {}

        # Confirm best match by Twitter URL if available
        best_match = None
        for p in people:
            twitter_url = p.get("twitter_url", "") or ""
            if username.lower() in twitter_url.lower():
                best_match = p
                log.debug(f"[APOLLO] Twitter URL confirmed: {twitter_url}")
                break
        if not best_match:
            best_match = people[0]

        apollo_id = best_match.get("id")
        if not apollo_id:
            return {}

        # ── Step 2: Enrich — 1 credit, returns email + phone ───────────────
        r2 = requests.post(
            f"{APOLLO_BASE_URL}/people/match",
            json={
                "id":                     apollo_id,
                "reveal_personal_emails": True,
                "reveal_phone_number":    True,
            },
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r2.raise_for_status()
        person = r2.json().get("person", {})
        return person if person else {}

    except Exception as exc:
        log.debug(f"[APOLLO] Twitter search error for @{username}: {exc}")
        return {}


# ── CHANGE 3: _apollo_search_by_name — 2-step search → enrich ────────────────
def _apollo_search_by_name(name: str, company: str = None) -> dict:
    """
    Search Apollo by name and optional company.
    2026 fix: Step 1 mixed_people/api_search (free) → get Apollo ID
              Step 2 people/match (1 credit) → get full data including email/phone
    Used for Reddit/Telegram where username may hint at real name.
    """
    try:
        # ── Step 1: Search — free, no credits ──────────────────────────────
        payload = {
            "q_keywords": name,
            "per_page":   3,
            "page":       1,
        }
        if company:
            payload["q_organization_name"] = company

        r = requests.post(
            f"{APOLLO_BASE_URL}/mixed_people/api_search",
            json=payload,
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        people = r.json().get("people", [])
        if not people:
            return {}

        apollo_id = people[0].get("id")
        if not apollo_id:
            return {}

        # ── Step 2: Enrich — 1 credit, returns email + phone ───────────────
        r2 = requests.post(
            f"{APOLLO_BASE_URL}/people/match",
            json={
                "id":                     apollo_id,
                "reveal_personal_emails": True,
                "reveal_phone_number":    True,
            },
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r2.raise_for_status()
        person = r2.json().get("person", {})
        return person if person else {}

    except Exception as exc:
        log.debug(f"[APOLLO] Name search error for {name}: {exc}")
        return {}


# ── CHANGE 4: _apollo_search_by_domain — 2-step search → enrich ──────────────
def _apollo_search_by_domain(domain: str) -> dict:
    """
    Search Apollo by company domain.
    2026 fix: Step 1 mixed_people/api_search (free) → get Apollo ID
              Step 2 people/match (1 credit) → get full data including email/phone
    Used when post text contains a website URL.
    """
    try:
        # ── Step 1: Search — free, no credits ──────────────────────────────
        r = requests.post(
            f"{APOLLO_BASE_URL}/mixed_people/api_search",
            json={
                "q_organization_domains": [domain],
                "per_page":               3,
                "page":                   1,
            },
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        people = r.json().get("people", [])
        if not people:
            return {}

        apollo_id = people[0].get("id")
        if not apollo_id:
            return {}

        # ── Step 2: Enrich — 1 credit, returns email + phone ───────────────
        r2 = requests.post(
            f"{APOLLO_BASE_URL}/people/match",
            json={
                "id":                     apollo_id,
                "reveal_personal_emails": True,
                "reveal_phone_number":    True,
            },
            headers=_apollo_headers(),
            timeout=APOLLO_REQUEST_TIMEOUT,
        )
        r2.raise_for_status()
        person = r2.json().get("person", {})
        return person if person else {}

    except Exception as exc:
        log.debug(f"[APOLLO] Domain search error for {domain}: {exc}")
        return {}


def _extract_domain_from_text(text: str) -> str | None:
    """Extract the first domain/URL from post text."""
    pattern = r'https?://(?:www\.)?([a-zA-Z0-9\-]+\.[a-zA-Z]{2,})'
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    bare = re.search(r'\b([a-zA-Z0-9\-]+\.(?:com|io|co|ng|co\.uk|org|net|biz))\b', text)
    if bare:
        return bare.group(1)
    return None


def _extract_company_from_text(text: str) -> str | None:
    """
    Try to extract a company name from post text.
    Looks for patterns like 'I run XYZ Ltd', 'CEO of XYZ', 'at XYZ'.
    """
    patterns = [
        r'(?:run|own|founded|ceo of|director of|founder of|head of|at)\s+([A-Z][A-Za-z0-9\s]{2,30}(?:Ltd|LLC|Inc|Co|Corp|Group|Finance|Tech|Solutions|Services)?)',
        r'([A-Z][A-Za-z0-9\s]{2,25}(?:Ltd|LLC|Inc|Co|Corp|Group|Finance|Tech|Solutions|Services))',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            company = m.group(1).strip()
            if len(company) > 3:
                return company
    return None


def _parse_apollo_person(person: dict) -> dict:
    """Parse Apollo person dict into our standard enrichment format."""
    if not person:
        return {}

    org = person.get("organization") or {}
    emp_ranges = {
        "1,10": "1-10",
        "11,20": "11-20",
        "21,50": "21-50",
        "51,100": "51-100",
        "101,200": "101-200",
        "201,500": "201-500",
        "501,1000": "501-1000",
        "1001,2000": "1001-2000",
        "2001,5000": "2001-5000",
        "5001,10000": "5001-10000",
    }
    raw_size     = org.get("estimated_num_employees") or ""
    company_size = emp_ranges.get(str(raw_size), str(raw_size) if raw_size else None)

    city    = person.get("city") or ""
    state   = person.get("state") or ""
    country = person.get("country") or ""
    location_parts = [p for p in [city, state, country] if p]
    location = ", ".join(location_parts) if location_parts else None

    # 2026: email may be in email field or emails array
    email = (
        person.get("email") or
        person.get("work_email") or
        (person.get("emails") or [{}])[0].get("email") if person.get("emails") else None
    )

    # 2026: phone may be in multiple fields
    phone = (
        person.get("phone") or
        person.get("sanitized_phone") or
        person.get("direct_phone")
    )

    return {
        "matched":        True,
        "full_name":      person.get("name"),
        "email":          email,
        "phone":          phone,
        "company":        org.get("name") or person.get("organization_name"),
        "job_title":      person.get("title"),
        "linkedin_url":   person.get("linkedin_url"),
        "location":       location,
        "industry":       org.get("industry") or person.get("industry"),
        "company_size":   company_size,
        "twitter_handle": person.get("twitter_url", "").split("/")[-1] if person.get("twitter_url") else None,
        "apollo_id":      person.get("id"),
    }


def enrich_with_apollo(item: dict) -> dict:
    """
    Main Apollo enrichment function.
    Called BEFORE Claude for every item passing keyword filter.
    All strategy logic unchanged — only underlying search functions updated.
    """
    if not APOLLO_API_KEY:
        log.debug("[APOLLO] APOLLO_API_KEY not set — skipping enrichment.")
        result = dict(_APOLLO_EMPTY)
        result["searched_at"]  = datetime.now(timezone.utc).isoformat()
        result["search_query"] = {"reason": "no_api_key"}
        return result

    if not APOLLO_ENRICH_ALL:
        result = dict(_APOLLO_EMPTY)
        result["searched_at"]  = datetime.now(timezone.utc).isoformat()
        result["search_query"] = {"reason": "enrichment_disabled"}
        return result

    platform     = item.get("platform", "").lower()
    username     = item.get("username", "")
    text         = item.get("text", "")

    searched_at  = datetime.now(timezone.utc).isoformat()
    search_query = {"platform": platform, "username": username}
    person       = {}

    try:
        # ── Strategy 1: Twitter handle search (most accurate) ──────────────
        if platform == "twitter" and username and not username.startswith("user_"):
            log.debug(f"[APOLLO] Searching by Twitter handle: @{username}")
            search_query["method"] = "twitter_handle"
            person = _apollo_search_by_twitter(username)

        # ── Strategy 2: Domain from post text (high accuracy when found) ───
        if not person and text:
            domain = _extract_domain_from_text(text)
            if domain:
                log.debug(f"[APOLLO] Searching by domain: {domain}")
                search_query["method"] = "domain"
                search_query["domain"] = domain
                person = _apollo_search_by_domain(domain)

        # ── Strategy 3: Company name extracted from post text ───────────────
        if not person and text:
            company = _extract_company_from_text(text)
            if company:
                log.debug(f"[APOLLO] Searching by company: {company}")
                search_query["method"] = "company_name"
                search_query["company"] = company
                person = _apollo_search_by_name(company)

        # ── Strategy 4: Username as name hint (Reddit/Telegram fallback) ────
        if not person and username and not username.startswith("user_") and platform != "twitter":
            name_hint = username.replace("_", " ").replace("-", " ").strip().title()
            if len(name_hint) > 3:
                log.debug(f"[APOLLO] Searching by username as name: {name_hint}")
                search_query["method"]    = "username_as_name"
                search_query["name_hint"] = name_hint
                person = _apollo_search_by_name(name_hint)

        if person:
            enrichment = _parse_apollo_person(person)
            enrichment["searched_at"]  = searched_at
            enrichment["search_query"] = search_query
            log.info(
                f"[APOLLO] MATCH | platform:{platform} | u/{username} | "
                f"name:{enrichment.get('full_name')} | "
                f"company:{enrichment.get('company')} | "
                f"email:{enrichment.get('email')} | "
                f"method:{search_query.get('method')}"
            )
            return enrichment
        else:
            result = dict(_APOLLO_EMPTY)
            result["searched_at"]  = searched_at
            result["search_query"] = search_query
            log.debug(f"[APOLLO] NO MATCH | platform:{platform} | u/{username}")
            return result

    except Exception as exc:
        log.error(f"[APOLLO] Unexpected error for u/{username}: {exc}")
        result = dict(_APOLLO_EMPTY)
        result["searched_at"]  = searched_at
        result["search_query"] = search_query
        result["error"]        = str(exc)
        return result


def _format_apollo_for_claude(enrichment: dict) -> str:
    """
    Format Apollo enrichment data as context string for Claude prompt.
    Only included when Apollo found a match.
    """
    if not enrichment or not enrichment.get("matched"):
        return ""

    lines = ["\nPERSON CONTEXT (verified by Apollo.io):"]
    if enrichment.get("full_name"):
        lines.append(f"  Full Name   : {enrichment['full_name']}")
    if enrichment.get("job_title"):
        lines.append(f"  Job Title   : {enrichment['job_title']}")
    if enrichment.get("company"):
        lines.append(f"  Company     : {enrichment['company']}")
    if enrichment.get("industry"):
        lines.append(f"  Industry    : {enrichment['industry']}")
    if enrichment.get("company_size"):
        lines.append(f"  Company Size: {enrichment['company_size']} employees")
    if enrichment.get("location"):
        lines.append(f"  Location    : {enrichment['location']}")
    if enrichment.get("email"):
        lines.append(f"  Email       : {enrichment['email']}")

    if len(lines) == 1:
        return ""
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SYSTEM PROMPTS (unchanged from v7.4)
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
PERSON CONTEXT (APOLLO ENRICHMENT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Some messages include a PERSON CONTEXT block verified by Apollo.io.
This contains real data about who the person actually is.
Use this context to score more accurately:
— A Fintech Founder scores higher than an anonymous user
— A CEO of a trading company scores higher than an individual
— Someone at an import/export company scores higher
— Industry matching Settla's corridors scores higher
If PERSON CONTEXT is present — use it. It is verified real data.

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

SCORE 7 to 8 — IMMEDIATE SLACK ALERT:
Strong buying signal. One element missing.
✓ Business context confirmed
✓ International payment need confirmed
✗ Missing extreme urgency OR specific amount

SCORE 4 to 6 — DAILY DIGEST:
Researching but no immediate crisis.
✓ Business context implied
✓ International payment mentioned
✗ No urgency. No crisis.

SCORE 3 — WATCHLIST ONLY:
Clear future potential within 30 to 60 days.

SCORE 0 to 2 — DISCARD IMMEDIATELY.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTOMATIC SCORE MODIFIERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADD +1 to score when:
+ Business owner confirmed in bio or post OR in Apollo context
+ Specific large amount mentioned — $10,000 or more
+ Multiple pain points in same post
+ Competitor mentioned negatively
+ Urgency words present — today, ASAP, urgent, this week
+ Active payment block or failure described
+ Supplier relationship at risk
+ Multiple international clients mentioned
+ Actively building payment partnerships
+ Apollo confirms Founder / CEO / Director / Owner title
+ Apollo confirms company in Fintech / Trading / Import / Export industry

SUBTRACT 1 from score when:
- Small personal amount under $2,000
- Sending to family for personal expenses
- Anonymous account with no business bio AND no Apollo match
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

If Apollo context is available — personalise the script:
Use their real name, company, or job title.
"James, seeing a Fintech Founder dealing with..." scores better 
than generic outreach.

OUTREACH RULES — NON NEGOTIABLE:
— Never start with I
— Never say I hope this message finds you well
— Never pitch features — pitch the outcome they want
— Always reference something specific they said
— Always end with one question or soft statement
— Maximum 3 sentences total per script
— Sound like a founder talking to another founder

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

You are identifying the exact moment a diaspora business owner 
is ready to switch payment providers or complete a large 
international transaction.

One converted client could process $50,000 to $500,000 per 
month through Settla.

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

        db.signals.create_index(
            [("apollo_enrichment.matched", ASCENDING)],
            name="apollo_matched",
            sparse=True,
        )

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

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT (unchanged from v7.4)
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
# OPERATOR SLACK ALERT (unchanged from v7.4)
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
                        {"type": "mrkdwn", "text": f"*System*\nFLINTEL v7.5"},
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
# FIX A — PERSISTENT BATCH STATE HELPERS (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def load_pending_batch(platform: str) -> tuple:
    try:
        doc = db.flintel_pending_batch.find_one({"platform": platform})
        if not doc:
            return [], None
        items      = doc.get("items", [])
        start_ts   = doc.get("batch_start_time")
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
                "platform":         platform,
                "items":            items,
                "batch_start_time": start_dt,
                "updated_at":       datetime.now(timezone.utc),
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
                "platform":         platform,
                "items":            [],
                "batch_start_time": None,
                "updated_at":       datetime.now(timezone.utc),
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
                "platform":   platform,
                "ids":        id_list,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_seen_ids error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER (unchanged from v7.4)
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

        apollo_context = ""
        enrichment = item.get("apollo_enrichment", {})
        if enrichment and enrichment.get("matched"):
            apollo_context = _format_apollo_for_claude(enrichment)

        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Platform: {platform} | Source: {location} | Type: {ctype} | User: {username}\n"
            f"Content: {text}"
            f"{apollo_context}\n"
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
    objects   = []
    depth     = 0
    obj_start = None
    in_string = False
    escape    = False
    i         = start + 1
    n         = len(raw)
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
        all_indices       = set(range(1, len(batch) + 1))
        missing_indices   = sorted(all_indices - recovered_indices)
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
                f"Consider raising MAX_TOKENS (currently {MAX_TOKENS}) or lowering batch size."
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
        score                = r["intent_score"]
        derived              = _derive_fields(score)
        r["signal_category"] = derived["signal_category"]
        r["tier"]            = derived["tier"]
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
# MONGODB STORAGE (unchanged from v7.4)
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
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "apollo_enrichment":            data.get("apollo_enrichment", {}),
            "client_id":                    CLIENT_ID,
            "alerted_slack":                False,
            "alerted_hubspot":              False,
            "digest_included":              False,
            "created_at":                   datetime.now(timezone.utc),
        }
        db.signals.insert_one(doc)

        platform       = data.get("platform", "?").upper()
        score          = data["intent_score"]
        user           = data.get("username", "?")
        ctype          = data.get("content_type", "")
        sub            = data.get("subreddit", "")
        grp            = data.get("telegram_group", "")
        source         = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)
        apollo_matched = data.get("apollo_enrichment", {}).get("matched", False)

        log.info(
            f"SAVED [{platform}] | Score:{score} | Tier:{data.get('tier','?')} | "
            f"u/{user} | {ctype} | {source} | Apollo:{'✅' if apollo_matched else '—'}"
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
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "apollo_enrichment":            data.get("apollo_enrichment", {}),
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
                "alerted_hubspot":    True,
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
# SLACK DELIVERY (unchanged from v7.4)
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


def _build_apollo_slack_block(enrichment: dict) -> dict | None:
    if not enrichment or not enrichment.get("matched"):
        return None

    lines = []
    if enrichment.get("full_name"):
        lines.append(f"*Full Name*\n{enrichment['full_name']}")
    if enrichment.get("job_title"):
        lines.append(f"*Job Title*\n{enrichment['job_title']}")
    if enrichment.get("company"):
        lines.append(f"*Company*\n{enrichment['company']}")
    if enrichment.get("industry"):
        lines.append(f"*Industry*\n{enrichment['industry']}")
    if enrichment.get("company_size"):
        lines.append(f"*Company Size*\n{enrichment['company_size']} employees")
    if enrichment.get("location"):
        lines.append(f"*Location*\n{enrichment['location']}")
    if enrichment.get("email"):
        lines.append(f"*Email*\n{enrichment['email']}")
    if enrichment.get("phone"):
        lines.append(f"*Phone*\n{enrichment['phone']}")
    if enrichment.get("linkedin_url"):
        lines.append(f"*LinkedIn*\n{enrichment['linkedin_url']}")

    if not lines:
        return None

    fields = [{"type": "mrkdwn", "text": line} for line in lines[:10]]

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "🔍 *APOLLO ENRICHMENT*"},
        "fields": fields,
    }


def send_slack_alert(data: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping.")
        return False

    score      = data["intent_score"]
    platform   = data.get("platform", "unknown").upper()
    ctype      = data.get("content_type", "post").upper()
    subreddit  = data.get("subreddit", "")
    tg_group   = data.get("telegram_group", "")
    post_url   = data.get("post_url", "")
    username   = data.get("username", "unknown")
    tier       = data.get("tier", "").upper()
    category   = data.get("signal_category", "").replace("_", " ").upper()
    is_biz     = data.get("is_business", False)
    corridor   = data.get("corridor") or "Unknown"
    amount     = data.get("estimated_amount") or "—"
    pain       = data.get("pain_type") or "—"
    competitor = data.get("competitor_mentioned") or "—"
    urgency    = data.get("urgency", "none").upper()
    timestamp  = data.get("timestamp", "—")
    is_rescore = data.get("is_rescore", False)

    enrichment     = data.get("apollo_enrichment", {})
    apollo_matched = enrichment.get("matched", False)
    apollo_name    = enrichment.get("full_name") or ""
    apollo_company = enrichment.get("company") or ""

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
        ""
    )

    rescore_tag  = " ♻️ RESCORED" if is_rescore else ""
    apollo_tag   = " 🔍 APOLLO" if apollo_matched else ""
    header_emoji = "🚨" if score >= 8 else "⚠️"
    header_text  = f"{header_emoji} {category} — Score {score}/10 | {tier}{rescore_tag}{apollo_tag}"

    if subreddit:
        source_label = f"r/{subreddit}"
    elif tg_group:
        source_label = f"tg/{tg_group}"
    else:
        source_label = platform

    display_name = f"{apollo_name} (@{username})" if apollo_name else f"@{username}"

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
                {"type": "mrkdwn", "text": f"*User*\n{display_name}"},
                {"type": "mrkdwn", "text": f"*Score*\n{score}/10"},
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

    if apollo_matched:
        blocks.append({"type": "divider"})
        apollo_block = _build_apollo_slack_block(enrichment)
        if apollo_block:
            blocks.append(apollo_block)
    else:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "🔍 Apollo: No match found for this user"}],
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
        log.info(
            f"Slack sent | {platform} | u/{username} | Score:{score} | "
            f"Apollo:{'✅ ' + (apollo_name or apollo_company) if apollo_matched else '—'}"
        )
        return True
    log.error("Slack delivery failed after all retries.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# HUBSPOT CRM (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

HUBSPOT_BASE = "https://api.hubapi.com"


def _hs_headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}


def _hs_find_contact(username: str, apollo_email: str = None) -> str | None:
    try:
        if apollo_email:
            r = requests.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
                json={"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": apollo_email}]}]},
                headers=_hs_headers(), timeout=10,
            )
            r.raise_for_status()
            results = r.json().get("results", [])
            if results:
                return results[0]["id"]

        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
            json={"filterGroups": [{"filters": [{"propertyName": "firstname", "operator": "EQ", "value": username}]}]},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0]["id"] if results else None
    except Exception as exc:
        log.error(f"HubSpot find contact error: {exc}")
        return None


def _hs_create_contact(data: dict) -> str | None:
    try:
        sub        = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        enrichment = data.get("apollo_enrichment", {})

        apollo_name = enrichment.get("full_name") or ""
        first_name  = apollo_name.split(" ")[0] if apollo_name else data.get("username", "unknown")
        last_name   = " ".join(apollo_name.split(" ")[1:]) if apollo_name and " " in apollo_name else f"{data.get('platform','?').upper()} Signal"

        props = {
            "firstname":           first_name,
            "lastname":            last_name,
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

        if enrichment.get("matched"):
            if enrichment.get("email"):
                props["email"]        = enrichment["email"]
            if enrichment.get("phone"):
                props["phone"]        = enrichment["phone"]
            if enrichment.get("company"):
                props["company"]      = enrichment["company"]
            if enrichment.get("job_title"):
                props["jobtitle"]     = enrichment["job_title"]
            if enrichment.get("linkedin_url"):
                props["linkedin_url"] = enrichment["linkedin_url"]
            if enrichment.get("industry"):
                props["industry"]     = enrichment["industry"]
            if enrichment.get("location"):
                props["city"]         = enrichment["location"]

        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
            json={"properties": props},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as exc:
        log.error(f"HubSpot create contact error: {exc}")
        return None


def _hs_create_note(data: dict, contact_id: str):
    try:
        sub          = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        rescore_note = "\n[RESCORED SIGNAL]" if data.get("is_rescore") else ""
        enrichment   = data.get("apollo_enrichment", {})

        apollo_section = ""
        if enrichment.get("matched"):
            apollo_section = (
                f"\n── APOLLO ENRICHMENT ──────────────────────\n"
                f"Full Name   : {enrichment.get('full_name') or 'N/A'}\n"
                f"Email       : {enrichment.get('email') or 'N/A'}\n"
                f"Phone       : {enrichment.get('phone') or 'N/A'}\n"
                f"Company     : {enrichment.get('company') or 'N/A'}\n"
                f"Job Title   : {enrichment.get('job_title') or 'N/A'}\n"
                f"LinkedIn    : {enrichment.get('linkedin_url') or 'N/A'}\n"
                f"Location    : {enrichment.get('location') or 'N/A'}\n"
                f"Industry    : {enrichment.get('industry') or 'N/A'}\n"
                f"Company Size: {enrichment.get('company_size') or 'N/A'}\n"
                f"Apollo ID   : {enrichment.get('apollo_id') or 'N/A'}\n"
                f"Searched At : {enrichment.get('searched_at') or 'N/A'}\n"
            )
        else:
            apollo_section = "\n── APOLLO ENRICHMENT ──────────────────────\nNo match found.\n"

        note = (
            f"FLINTEL SIGNAL — v7.5{rescore_note}\n\n"
            f"Platform:     {data.get('platform','?').upper()}\n"
            f"Score:        {data['intent_score']}/10\n"
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
            f"Telegram DM:\n{data.get('telegram_dm') or 'N/A'}"
            f"{apollo_section}"
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
    except Exception as exc:
        log.error(f"HubSpot create note error: {exc}")


def _send_to_hubspot(data: dict) -> str | None:
    if not HUBSPOT_API_KEY:
        log.warning("HUBSPOT_API_KEY not set — skipping.")
        return None
    enrichment   = data.get("apollo_enrichment", {})
    apollo_email = enrichment.get("email") if enrichment.get("matched") else None
    username     = data.get("username", "unknown")

    contact_id = _hs_find_contact(username, apollo_email=apollo_email)
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
# CORE SIGNAL PROCESSOR (unchanged from v7.4)
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
        "watchlist":                    score_result.get("watchlist", False),
        "watchlist_reason":             score_result.get("watchlist_reason"),
        "apollo_enrichment":            item.get("apollo_enrichment", {}),
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
        log.info(f"{mode} | [{platform.upper()}] Score:{score} | Slack only | u/{data['username']}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])

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
# GENERIC BATCH PROCESSOR (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
):
    platform_key = platform_label.lower()

    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{BATCH_GAP_SECONDS}s | "
        f"timeout:{BATCH_TIMEOUT_SECONDS}s"
    )

    current_batch, batch_start_time = load_pending_batch(platform_key)
    if current_batch:
        log.info(
            f"[{platform_label}] Resumed [{len(current_batch)}/{batch_size}] "
            f"from persistent disk — continuing, NOT restarting at 1."
        )

    total_received = 0
    total_matched  = 0
    total_dropped  = 0
    total_batches  = 0

    while True:
        try:
            if current_batch and batch_start_time is not None:
                elapsed   = time.time() - batch_start_time
                remaining = BATCH_TIMEOUT_SECONDS - elapsed
                wait_time = max(0.1, remaining)
            else:
                wait_time = 1.0

            try:
                item     = q.get(timeout=wait_time)
                got_item = True
            except queue.Empty:
                got_item = False

            if got_item:
                total_received += 1
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

                apollo_enrichment         = enrich_with_apollo(item)
                item["apollo_enrichment"] = apollo_enrichment

                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)
                save_pending_batch(platform_key, current_batch, batch_start_time)

                apollo_status = "✅" if apollo_enrichment.get("matched") else "—"
                log.info(
                    f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | "
                    f"{item.get('content_type','?').upper()} | u/{item.get('username')} | "
                    f"Apollo:{apollo_status}"
                )

                q.task_done()

            should_fire = False
            fire_reason = ""

            if len(current_batch) >= batch_size:
                should_fire = True
                fire_reason = f"batch full ({batch_size} items)"
            elif current_batch and batch_start_time is not None:
                elapsed = time.time() - batch_start_time
                if elapsed >= BATCH_TIMEOUT_SECONDS:
                    should_fire = True
                    fire_reason = f"timeout ({BATCH_TIMEOUT_SECONDS}s) — partial batch {len(current_batch)}/{batch_size}"

            if should_fire and current_batch:
                total_batches += 1
                batch_to_send    = current_batch[:batch_size]
                current_batch    = current_batch[batch_size:]
                batch_start_time = None if not current_batch else time.time()

                if current_batch:
                    save_pending_batch(platform_key, current_batch, batch_start_time)
                else:
                    clear_pending_batch(platform_key)

                apollo_matches = sum(1 for it in batch_to_send if it.get("apollo_enrichment", {}).get("matched"))
                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | "
                    f"reason:{fire_reason} | items:{len(batch_to_send)} | "
                    f"apollo_matches:{apollo_matches}/{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores    = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (
                        scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch.")
                    )
                    process_scored_item(it, sr)

                log.info(
                    f"[{platform_label}] BATCH {total_batches} DONE | "
                    f"waiting {BATCH_GAP_SECONDS}s..."
                )
                time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# RESCORE PROCESSOR (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def _rescore_queue_requests(message_ids: list, operator_note: str = "") -> list:
    inserted = []
    for mid in message_ids:
        try:
            doc = {
                "message_id":     mid,
                "status":         "pending",
                "operator_note":  operator_note,
                "requested_at":   datetime.now(timezone.utc),
                "processed_at":   None,
                "rescore_result": None,
                "error":          None,
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
        f"batch_size:{RESCORE_BATCH_SIZE} | poll_interval:{RESCORE_POLL_INTERVAL}s"
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
            req_map          = {}

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

                existing_enrichment = sig.get("apollo_enrichment", {})
                if existing_enrichment.get("matched"):
                    item["apollo_enrichment"] = existing_enrichment
                    log.debug(f"[RESCORE] Reusing existing Apollo enrichment for {mid}")
                else:
                    item["apollo_enrichment"] = enrich_with_apollo(item)

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

            scores    = score_batch_with_claude(items_for_claude)
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
                f"waiting {BATCH_GAP_SECONDS}s..."
            )
            time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[RESCORE] processor error: {exc}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — feedparser RSS poller (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

_reddit_seen_ids: set        = load_seen_ids("reddit")
_reddit_seen_lock            = threading.Lock()
_reddit_seen_dirty_count     = 0


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
    url   = f"https://www.reddit.com/r/{subreddit}/new.rss"
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

            title         = entry.get("title", "").strip()
            summary       = entry.get("summary", "").strip()
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
# TWITTER / X POLLER (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def build_twitter_client() -> tweepy.Client | None:
    if not TWITTER_BEARER_TOKEN:
        log.warning("TWITTER_BEARER_TOKEN not set — Twitter platform disabled.")
        return None
    try:
        client = tweepy.Client(
            bearer_token       = TWITTER_BEARER_TOKEN,
            consumer_key       = TWITTER_API_KEY,
            consumer_secret    = TWITTER_API_SECRET,
            wait_on_rate_limit = True,
        )
        log.info("Twitter/X client initialised.")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def poll_twitter(client: tweepy.Client):
    seen_ids: set = load_seen_ids("twitter")
    dirty         = 0
    log.info(
        f"Twitter poll started | query_len:{len(TWITTER_SEARCH_QUERY)} | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    while True:
        try:
            response = client.search_recent_tweets(
                query        = TWITTER_SEARCH_QUERY,
                max_results  = 50,
                tweet_fields = ["author_id", "created_at", "text", "conversation_id"],
                expansions   = ["author_id"],
                user_fields  = ["username", "name"],
            )

            if not response or not response.data:
                log.debug("Twitter: no results this cycle.")
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            user_map: dict = {}
            if response.includes and "users" in response.includes:
                for u in response.includes["users"]:
                    user_map[u.id] = u.username

            new_count = 0
            for tweet in response.data:
                tweet_id = str(tweet.id)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                dirty += 1

                if len(seen_ids) > 50_000:
                    seen_ids.clear()

                text     = tweet.text or ""
                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")

                twitter_queue.put({
                    "message_id":     f"twitter_{tweet_id}",
                    "platform":       "twitter",
                    "content_type":   "tweet",
                    "text":           text,
                    "username":       username,
                    "subreddit":      "",
                    "telegram_group": "",
                    "post_url":       f"https://twitter.com/{username}/status/{tweet_id}",
                })
                new_count += 1

            if dirty >= 10:
                save_seen_ids("twitter", seen_ids)
                dirty = 0

            if new_count:
                log.info(
                    f"Twitter: {new_count} new tweets queued | "
                    f"queue_size:{twitter_queue.qsize()}"
                )

        except tweepy.errors.TweepyException as exc:
            log.error(f"Twitter poll error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")
        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM LISTENER (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

_telegram_seen_ids: set      = load_seen_ids("telegram")
_telegram_seen_lock          = threading.Lock()
_telegram_seen_dirty_count   = 0


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
                target   = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
                messages = await client.get_messages(target, limit=20)

                for msg in messages:
                    if not msg or not msg.text or len(msg.text) < 5:
                        continue

                    chat_id = msg.chat_id if msg.chat_id else 0
                    msg_id  = msg.id

                    if _telegram_is_seen(chat_id, msg_id):
                        continue

                    sender  = await msg.get_sender()
                    tg_user = getattr(sender, "username", None) or f"user_{getattr(sender, 'id', 0)}"

                    telegram_queue.put({
                        "message_id":     f"telegram_{chat_id}_{msg_id}",
                        "platform":       "telegram",
                        "content_type":   "message",
                        "text":           msg.text,
                        "username":       tg_user,
                        "display_name":   tg_user,
                        "subreddit":      "",
                        "telegram_group": group,
                        "post_url":       "",
                    })
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

            telegram_queue.put({
                "message_id":     f"telegram_{chat_id}_{msg_id}",
                "platform":       "telegram",
                "content_type":   "message",
                "text":           text,
                "username":       tg_user,
                "display_name":   f"{first} {last}".strip() or tg_user,
                "subreddit":      "",
                "telegram_group": username_attr or chat_title,
                "post_url":       "",
            })

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
# SCHEDULERS (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

def send_daily_digest():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since   = datetime.now(timezone.utc) - timedelta(hours=24)
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
            preview = s["message_text"][:120]
            if len(s["message_text"]) > 120:
                preview += "..."
            corridor    = s.get("corridor") or "—"
            pain        = s.get("pain_type") or "—"
            platform    = s.get("platform", "?").upper()
            sub         = s.get("subreddit", "")
            grp         = s.get("telegram_group", "")
            source      = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)
            enrichment  = s.get("apollo_enrichment", {})
            apollo_name = enrichment.get("full_name") or ""
            apollo_co   = enrichment.get("company") or ""
            apollo_hint = f" | 🔍 {apollo_name}" + (f" @ {apollo_co}" if apollo_co else "") if apollo_name else ""

            lines.append(
                f"• *{s.get('username','?')}*{apollo_hint} | Score:{s['intent_score']}/10 "
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
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.5 | Client: {CLIENT_ID} | Reddit + Twitter + Telegram + Apollo"}]},
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
        apollo_sigs   = [s for s in all_signals if s.get("apollo_enrichment", {}).get("matched")]
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
        top3_lines = []
        for s in top3:
            enrichment  = s.get("apollo_enrichment", {})
            apollo_name = enrichment.get("full_name") or ""
            apollo_co   = enrichment.get("company") or ""
            display     = f"{apollo_name} @ {apollo_co}" if apollo_name and apollo_co else (apollo_name or s.get("username", "?"))
            top3_lines.append(
                f"• *{display}* | Score:{s['intent_score']}/10 "
                f"| {s.get('platform','?').upper()} | {s.get('corridor') or 'Unknown corridor'}\n"
                f"  _{s['message_text'][:100]}{'...' if len(s['message_text'])>100 else ''}_"
            )

        week_start  = since.strftime("%b %d")
        week_end    = datetime.now(timezone.utc).strftime("%b %d, %Y")
        apollo_rate = f"{len(apollo_sigs)}/{total} ({int(len(apollo_sigs)/total*100)}%)" if total else "0%"

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
                    {"type": "mrkdwn", "text": f"*🔍 Apollo Matched*\n{apollo_rate}"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Corridor Breakdown*\n{breakdown('corridor')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Competitor Mentions*\n{breakdown('competitor_mentioned')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Pain Types*\n{breakdown('pain_type')}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top 3 Signals This Week*\n\n{_safe(chr(10).join(top3_lines), 2800)}"}},
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.5 | {CLIENT_ID} | Week ending {week_end} | Apollo enrichment active"}]},
            ],
        }

        result = retry_with_backoff(_post_to_slack, payload, retries=3, delay=2, label="WeeklyReport")
        if result:
            log.info(
                f"Weekly report sent | Total:{total} High:{len(high)} Med:{len(medium)} "
                f"Biz:{len(business)} Apollo:{len(apollo_sigs)}/{total}"
            )

    except Exception as exc:
        log.error(f"Weekly report error: {exc}")


async def run_scheduler():
    log.info(
        f"Scheduler started | digest:{DAILY_DIGEST_HOUR}:00 UTC | "
        f"report Mon {WEEKLY_REPORT_HOUR}:00 UTC"
    )
    last_digest_date = None
    persisted_week   = _get_state("last_report_week")
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
# ASYNC LISTENERS (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED (REDDIT_ENABLED=false) — skipping.")
        return

    rss_thread  = threading.Thread(target=poll_reddit_rss, daemon=True, name="Reddit-RSS")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
        daemon=True, name="Reddit-Batch",
    )
    rss_thread.start()
    btch_thread.start()
    log.info("Reddit threads running: RSS-Poller ✅ | Batch ✅")

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
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
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

    poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER"),
        daemon=True, name="Twitter-Batch",
    )
    poll_thread.start()
    btch_thread.start()
    log.info("Twitter threads running: Poll ✅ | Batch ✅")

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
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER"),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_telegram_listener():
    if not TELEGRAM_ENABLED:
        log.warning("Telegram platform DISABLED (TELEGRAM_ENABLED=false) — skipping.")
        return

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning("Telegram listener not started — set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE in .env")
        return

    tg_thread   = threading.Thread(target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM"),
        daemon=True, name="Telegram-Batch",
    )
    tg_thread.start()
    btch_thread.start()
    log.info(
        f"Telegram threads running: Listener ✅ | Batch ✅ | "
        f"Poller {'✅' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled'}"
    )

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
                args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM"),
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
# FASTAPI (unchanged from v7.4)
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v7.5",
    description = (
        "Reddit (RSS) + Twitter + Telegram signals: monitor, score, store, alert. "
        "Apollo enrichment BEFORE Claude scoring. Persistent batch state. Streaming Claude. Manual rescore."
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
    apollo_enabled = bool(APOLLO_API_KEY) and APOLLO_ENRICH_ALL
    return {
        "status":                        "running",
        "system":                        "FLINTEL v7.5",
        "client":                        CLIENT_ID,
        "platforms":                     ["reddit", "twitter", "telegram"],
        "reddit_enabled":                REDDIT_ENABLED,
        "reddit_status":                 _working(REDDIT_ENABLED),
        "twitter_enabled":               TWITTER_ENABLED,
        "twitter_status":                _working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)),
        "telegram_enabled":              TELEGRAM_ENABLED,
        "telegram_status":               _working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)),
        "apollo_enabled":                apollo_enabled,
        "apollo_status":                 _working(apollo_enabled),
        "apollo_enriches_before_claude": True,
        "apollo_enrich_all":             APOLLO_ENRICH_ALL,
        "reddit_mode":                   "feedparser RSS (no credentials required)",
        "reddit_poll_interval":          REDDIT_POLL_INTERVAL,
        "reddit_batch_size":             REDDIT_BATCH_SIZE,
        "twitter_batch_size":            TWITTER_BATCH_SIZE,
        "telegram_batch_size":           TELEGRAM_BATCH_SIZE,
        "rescore_batch_size":            RESCORE_BATCH_SIZE,
        "telegram_poll_interval":        TELEGRAM_POLL_INTERVAL,
        "batch_gap_s":                   BATCH_GAP_SECONDS,
        "batch_timeout_s":               BATCH_TIMEOUT_SECONDS,
        "max_tokens":                    MAX_TOKENS,
        "claude_stream_timeout_s":       CLAUDE_STREAM_TIMEOUT,
        "reddit_queue_size":             reddit_queue.qsize(),
        "twitter_queue_size":            twitter_queue.qsize(),
        "telegram_queue_size":           telegram_queue.qsize(),
        "telegram_groups":               len(TARGET_TELEGRAM_GROUPS),
        "auth_required":                 bool(API_KEY),
        "persistent_batch_state":        True,
        "partial_json_recovery":         True,
        "claude_streaming":              True,
        "rescore_enabled":               True,
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    reddit_working   = REDDIT_ENABLED
    twitter_working  = TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN)
    telegram_working = TELEGRAM_ENABLED and bool(TELEGRAM_API_ID)
    apollo_working   = bool(APOLLO_API_KEY) and APOLLO_ENRICH_ALL

    pending_rescore = 0
    try:
        pending_rescore = db.flintel_rescore_messages.count_documents({"status": "pending"})
    except Exception:
        pass

    apollo_total_matches = 0
    try:
        apollo_total_matches = db.signals.count_documents(
            {"client_id": CLIENT_ID, "apollo_enrichment.matched": True}
        )
    except Exception:
        pass

    return {
        "status":               "ok",
        "mongodb":              mongo,
        "reddit":               ("polling-rss" if REDDIT_ENABLED else "disabled"),
        "reddit_working":       reddit_working,
        "reddit_indicator":     _working(reddit_working),
        "twitter":              ("polling" if twitter_working else "disabled"),
        "twitter_working":      twitter_working,
        "twitter_indicator":    _working(twitter_working),
        "telegram":             ("listening" if telegram_working else "disabled"),
        "telegram_working":     telegram_working,
        "telegram_indicator":   _working(telegram_working),
        "apollo":               ("enriching" if apollo_working else "disabled"),
        "apollo_working":       apollo_working,
        "apollo_indicator":     _working(apollo_working),
        "apollo_total_matches": apollo_total_matches,
        "reddit_queue_size":    reddit_queue.qsize(),
        "twitter_queue_size":   twitter_queue.qsize(),
        "telegram_queue_size":  telegram_queue.qsize(),
        "rescore_pending":      pending_rescore,
        "rescore_working":      True,
        "rescore_indicator":    _working(True),
        "client_id":            CLIENT_ID,
        "timestamp":            datetime.now(timezone.utc).isoformat(),
    }


@app.get("/signals/apollo-matched", dependencies=[Depends(verify_api_key)])
def get_apollo_matched(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "apollo_enrichment.matched": True}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(
            db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/apollo-stats", dependencies=[Depends(verify_api_key)])
def get_apollo_stats():
    try:
        total        = db.signals.count_documents({"client_id": CLIENT_ID})
        matched      = db.signals.count_documents({"client_id": CLIENT_ID, "apollo_enrichment.matched": True})
        has_email    = db.signals.count_documents({"client_id": CLIENT_ID, "apollo_enrichment.email": {"$ne": None}})
        has_phone    = db.signals.count_documents({"client_id": CLIENT_ID, "apollo_enrichment.phone": {"$ne": None}})
        has_linkedin = db.signals.count_documents({"client_id": CLIENT_ID, "apollo_enrichment.linkedin_url": {"$ne": None}})
        match_rate   = f"{int(matched/total*100)}%" if total else "0%"

        def agg_apollo(field):
            return list(db.signals.aggregate([
                {"$match": {"client_id": CLIENT_ID, f"apollo_enrichment.{field}": {"$ne": None}}},
                {"$group": {"_id": f"$apollo_enrichment.{field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ]))

        return {
            "total_signals":     total,
            "apollo_matched":    matched,
            "apollo_match_rate": match_rate,
            "has_email":         has_email,
            "has_phone":         has_phone,
            "has_linkedin":      has_linkedin,
            "top_industries":    agg_apollo("industry"),
            "top_job_titles":    agg_apollo("job_title"),
            "top_companies":     agg_apollo("company"),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/rescore", dependencies=[Depends(verify_api_key)])
def post_rescore(
    message_ids:   list = Body(..., description="List of message_id strings to rescore"),
    operator_note: str  = Body("",  description="Optional operator note"),
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
        "queued":        len(req_ids),
        "request_ids":   req_ids,
        "message_ids":   message_ids,
        "operator_note": operator_note,
        "status":        "pending",
        "note":          "Rescore processor will pick these up within the next poll interval.",
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
        if platform:              q["platform"]        = platform
        if category:              q["signal_category"] = category
        if min_score is not None: q["intent_score"]    = {"$gte": min_score}
        if subreddit:             q["subreddit"]       = subreddit
        if tg_group:              q["telegram_group"]  = {"$regex": tg_group, "$options": "i"}
        if tier:                  q["tier"]            = tier
        if corridor:              q["corridor"]        = {"$regex": corridor, "$options": "i"}
        if pain_type:             q["pain_type"]       = pain_type
        if is_business is not None: q["is_business"]   = is_business
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/stats", dependencies=[Depends(verify_api_key)])
def get_stats():
    try:
        total        = db.signals.count_documents({"client_id": CLIENT_ID})
        biz          = db.signals.count_documents({"client_id": CLIENT_ID, "is_business": True})
        reddit       = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "reddit"})
        twitter      = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "twitter"})
        telegram     = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "telegram"})
        rescored     = db.signals.count_documents({"client_id": CLIENT_ID, "rescored_at": {"$exists": True}})
        apollo_match = db.signals.count_documents({"client_id": CLIENT_ID, "apollo_enrichment.matched": True})

        def agg(group_field):
            return list(db.signals.aggregate([
                {"$match": {"client_id": CLIENT_ID, group_field: {"$ne": None}}},
                {"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]))

        return {
            "total_signals":     total,
            "business_owners":   biz,
            "reddit_signals":    reddit,
            "twitter_signals":   twitter,
            "telegram_signals":  telegram,
            "rescored_signals":  rescored,
            "apollo_matched":    apollo_match,
            "apollo_match_rate": f"{int(apollo_match/total*100)}%" if total else "0%",
            "corridors":         agg("corridor"),
            "pain_types":        agg("pain_type"),
            "competitors":       agg("competitor_mentioned"),
            "tiers":             agg("tier"),
            "reddit_queue":      reddit_queue.qsize(),
            "twitter_queue":     twitter_queue.qsize(),
            "telegram_queue":    telegram_queue.qsize(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/high-intent", dependencies=[Depends(verify_api_key)])
def get_high_intent(limit: int = 20):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "intent_score": {"$gte": 8}}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/digest", dependencies=[Depends(verify_api_key)])
def get_digest(limit: int = 50):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "intent_score": {"$gte": 6, "$lte": 7}}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/business", dependencies=[Depends(verify_api_key)])
def get_business(limit: int = 20):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "is_business": True}, {"_id": 0})
            .sort("intent_score", -1).limit(limit)
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


@app.get("/signals/corridors", dependencies=[Depends(verify_api_key)])
def get_by_corridor(corridor: str, limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "corridor": {"$regex": corridor, "$options": "i"}}, {"_id": 0}
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "corridor": corridor, "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/watchlist", dependencies=[Depends(verify_api_key)])
def get_watchlist(limit: int = 50):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "watchlist": True}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/silent", dependencies=[Depends(verify_api_key)])
def get_silent_signals(limit: int = 50):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "intent_score": {"$lte": 5}}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/rescored", dependencies=[Depends(verify_api_key)])
def get_rescored_signals(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "rescored_at": {"$exists": True}}, {"_id": 0}
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
        start_rescore_listener(),
        run_scheduler(),
    )


if __name__ == "__main__":
    apollo_enabled = bool(APOLLO_API_KEY) and APOLLO_ENRICH_ALL
    log.info("=" * 70)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v7.5")
    log.info("=" * 70)
    log.info(f"  Client             : {CLIENT_ID}")
    log.info(f"  Platforms          : Reddit (RSS) + Twitter/X + Telegram")
    log.info(f"  Reddit             : {REDDIT_ENABLED} | {_working(REDDIT_ENABLED)}")
    log.info(f"  Reddit mode        : feedparser RSS — no credentials required")
    log.info(f"  Reddit poll gap    : {REDDIT_POLL_INTERVAL}s between full subreddit cycles")
    log.info(f"  Twitter            : {TWITTER_ENABLED} | {_working(TWITTER_ENABLED and bool(TWITTER_BEARER_TOKEN))}")
    log.info(f"  Telegram           : {TELEGRAM_ENABLED} | {_working(TELEGRAM_ENABLED and bool(TELEGRAM_API_ID))}")
    log.info(f"  Telegram polling   : {'every ' + str(TELEGRAM_POLL_INTERVAL) + 's' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled (TELEGRAM_POLL_INTERVAL=0)'}")
    log.info(f"  ── APOLLO ENRICHMENT (NEW v7.5) ──────────────────────────")
    log.info(f"  Apollo             : {apollo_enabled} | {_working(apollo_enabled)}")
    log.info(f"  Apollo API key     : {'✅ SET' if APOLLO_API_KEY else '❌ NOT SET — add APOLLO_API_KEY to .env'}")
    log.info(f"  Apollo enrich all  : {APOLLO_ENRICH_ALL} (set APOLLO_ENRICH_ALL=false to disable)")
    log.info(f"  Apollo pipeline    : Fetch → Apollo enriches FIRST → Claude scores WITH context")
    log.info(f"  Apollo strategy    : Twitter=handle | Reddit/Telegram=domain→company→username")
    log.info(f"  Apollo base URL    : {APOLLO_BASE_URL} (fixed for 2026)")
    log.info(f"  Apollo search      : mixed_people/api_search (free) → people/match (1 credit)")
    log.info(f"  Apollo graceful    : No match = skip silently, pipeline continues normally")
    log.info(f"  Apollo in Claude   : Person context injected into Claude prompt per item")
    log.info(f"  Apollo in Slack    : 🔍 APOLLO ENRICHMENT block after outreach script")
    log.info(f"  Apollo in HubSpot  : Real name, email, phone, company added to contact")
    log.info(f"  Apollo in MongoDB  : apollo_enrichment field on every signal document")
    log.info(f"  Apollo endpoints   : GET /signals/apollo-matched | GET /signals/apollo-stats")
    log.info(f"  ────────────────────────────────────────────────────────────")
    log.info(f"  Reddit batch       : {REDDIT_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Twitter batch      : {TWITTER_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Telegram batch     : {TELEGRAM_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Rescore batch      : {RESCORE_BATCH_SIZE} items per Claude call")
    log.info(f"  Batch gap          : {BATCH_GAP_SECONDS}s between calls")
    log.info(f"  Batch timeout      : {BATCH_TIMEOUT_SECONDS}s (partial batch fires after timeout)")
    log.info(f"  max_tokens         : {MAX_TOKENS}")
    log.info(f"  Claude streaming   : True | {_working(True)} (FIX C)")
    log.info(f"  Score 1-5          : SILENT SAVE — MongoDB only, no alerts")
    log.info(f"  Score 6-7          : MEDIUM — MongoDB + Slack")
    log.info(f"  Score 8-10         : HIGH   — MongoDB + Slack + HubSpot")
    log.info(f"  MongoDB            : ALL scores 1-10 saved, nothing discarded")
    log.info(f"  Rescore            : True | {_working(True)} — flintel_rescore_messages")
    log.info(f"  Rescore Apollo     : Re-enriches on rescore if not previously matched")
    log.info(f"  Daily digest       : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report      : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits         : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Telegram groups    : {len(TARGET_TELEGRAM_GROUPS)} configured")
    log.info(f"  Keywords           : {len(KEYWORDS)} filters")
    log.info(f"  MongoDB DB         : {MONGODB_DB}")
    log.info(f"  HubSpot            : {'True | ' + _working(True) if HUBSPOT_API_KEY else 'False | ' + _working(False) + ' — set HUBSPOT_API_KEY'}")
    log.info(f"  Slack              : {'True | ' + _working(True) if SLACK_WEBHOOK_URL else 'False | ' + _working(False) + ' — set SLACK_WEBHOOK_URL'}")
    log.info(f"  v7.5 changes       : Apollo enrichment BEFORE Claude scoring")
    log.info(f"  Apollo 2026 fix    : 2-step search→enrich | correct base URL")
    log.info("=" * 70)

    asyncio.run(main())
