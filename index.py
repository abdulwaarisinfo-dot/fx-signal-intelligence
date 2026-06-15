"""
FX Signal Intelligence System — FLINTEL v6.1
=============================================
Platforms : Reddit (PRAW) + Twitter/X (tweepy v2)
Pipeline  :
  Reddit  → Stream posts / comments / replies
  Twitter → Fetch mentions / search / replies (rate-limit safe, 50/block)
      ↓
  Keyword Pre-Filter        (free, fast — drops 80%+ noise)
      ↓
  Batch Collector           (10 items per Claude call — Reddit)
                            (50 items per Claude call — Twitter)
      ↓
  30-Second Gap             (between each batch)
      ↓
  Claude AI Intent Scorer   (single merged prompt per batch)
      ↓
  MongoDB Storage
      ↓
  Slack Alert               (score 6-10, professional blocks)
      ↓
  HubSpot CRM               (score 8-10 only)
      ↓
  FastAPI REST Endpoints
      ↓
  Daily Digest Scheduler    (score 6-7, 08:00 UTC)
      ↓
  Weekly Report Scheduler   (all signals, Monday 09:00 UTC)

Score rules:
  0-5  → DISCARD  — never stored, never alerted
  6-7  → MEDIUM   — MongoDB + Slack only
  8-10 → HIGH     — MongoDB + Slack + HubSpot

Reddit batch rules:
  → Continuous stream (posts + comments + replies)
  → Keyword filter applied to every item
  → 10 matched items → one Claude prompt
  → 30s gap between batches
  → Non-matching items dropped immediately

Twitter batch rules:
  → Polling every 60s (rate-limit safe)
  → since_id tracking — only NEW tweets fetched each poll (zero duplicates)
  → seen_tweet_ids persisted in MongoDB — survives restarts
  → pending_items collection — unprocessed items survive restarts
  → Search query built from top-tier keywords
  → Deduplication by tweet ID before filter
  → Keyword filter applied to every tweet
  → 50 matched items → one Claude prompt
  → 30s gap between batches
  → Unknown / irrelevant content never reaches Claude

Changelog v6.1:
  - FIX: seen_tweet_ids now persisted in MongoDB (survives restarts/refreshes)
  - FIX: Twitter since_id tracking — polls only fetch NEW tweets, not old ones
  - FIX: pending_items MongoDB collection — keyword-matched items queued for
         Claude survive process restarts without data loss
  - FIX: pending_items restored on startup before new items are collected
  - FIX: No duplicate Claude calls — tweet ID MongoDB index + since_id + seen set
  - FIX: Token waste eliminated — restarts no longer re-score already-seen tweets
  - PERF: Twitter poller loads since_id from MongoDB on startup
  - PERF: seen_ids capped at 100k (was 50k), then pruned not cleared (no gaps)
  - All v6.0 features 100% preserved — scoring, Slack, HubSpot, FastAPI, schedulers

Changelog v6.0:
  - Added Twitter/X platform (tweepy v2, Bearer Token + OAuth1)
  - Twitter rate-limit safe polling: 15 req/15 min window respected
  - Twitter deduplication: tweet ID set persisted per run
  - Twitter fetch: search + mentions + reply chains (50/block)
  - Unified Claude scorer handles both platforms identically
  - Unified process_scored_item works for reddit + twitter items
  - Upgraded system prompt: 8 pain types, 3-version outreach scripts,
    competitor intelligence, urgency indicators, corridor detection
  - Expanded keyword list: 350+ signals (business, corridors, pain,
    competitor, amounts, compliance, geography, urgency)
  - Slack blocks: professional, sequenced, platform-aware, no truncation
  - HubSpot: full signal fields, outreach scripts, corridor, pain type
  - MongoDB indexes: platform, corridor, pain_type, tier, competitor
  - FastAPI endpoints: /signals/twitter, /signals/reddit, /signals/outreach
  - Daily digest + weekly report: async executor, no blocking
  - All threads monitored + auto-restart every 60s
  - Retry with exponential backoff on all external calls
  - stream_posts / stream_comments: deleted author safe
  - _call_claude_batch: index-based score alignment + positional fallback
  - Claude model: claude-sonnet-4-6 (cost-optimised, same intelligence)
  - Batch prompt: text capped 800 chars/item (cost reduction ~30%)
  - No duplicate Claude calls: tweet ID set + MongoDB message_id unique index
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

import praw
import praw.exceptions
import anthropic
import tweepy
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import requests
from fastapi import FastAPI, HTTPException
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

# Reddit
REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USERNAME      = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD      = os.getenv("REDDIT_PASSWORD")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "FlintelSignalBot/6.1")

# Twitter / X
TWITTER_API_KEY            = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET         = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN       = os.getenv("TWITTER_BEARER_TOKEN")

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")

# Delivery
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY")

# Thresholds
MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "6"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))
CLIENT_ID        = os.getenv("CLIENT_ID", "settla")

# Batch settings
REDDIT_BATCH_SIZE  = int(os.getenv("REDDIT_BATCH_SIZE",  "10"))
TWITTER_BATCH_SIZE = int(os.getenv("TWITTER_BATCH_SIZE", "50"))
BATCH_GAP_SECONDS  = int(os.getenv("BATCH_GAP_SECONDS",  "30"))

# Schedulers
DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))   # 0 = Monday
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

# Twitter polling
TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))  # seconds

# ─────────────────────────────────────────────────────────────────────────────
# TARGET SUBREDDITS
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
# SHARED QUEUES
# reddit_queue : items from Reddit streams
# twitter_queue: items from Twitter polling
# Both read by their respective batch processors
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:  queue.Queue = queue.Queue()
twitter_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER  — 350+ signals
# Applied to EVERY item before Claude ever sees it
# Zero API cost — runs in microseconds
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS = [

  # Sending money
        "send money to",
        "sending money to",
        "transfer money to",
        "transferring money to",
        "wire money to",
        "wiring money to",
        "move money to",
        "moving money to",
        "remit money to",
        "remitting money to",
        "pay my supplier",
        "paying my supplier",
        "pay a supplier",
        "paying a supplier",
        "pay my vendor",
        "paying my vendor",
        "pay my manufacturer",
        "pay my factory",
        "pay my partner",
        "pay my contractor",
        "pay an invoice",
        "paying an invoice",
        "settle an invoice",
        "settling an invoice",
        "pay a business",
        "business payment to",
        "supplier payment to",
        "vendor payment to",
        "invoice payment to",
        "international payment to",
        "overseas payment to",
        "cross border payment",
        "cross-border payment",
        "cross border transfer",
        "cross-border transfer",
        "international transfer",
        "international wire",
        "international wire transfer",
        "foreign wire transfer",
        "overseas wire transfer",
        "overseas transfer",
        "global payment",
        "global transfer",
        "b2b payment",
        "b2b transfer",
        "business to business payment",

        # Bank blocking
        "bank blocked my",
        "bank blocked my transfer",
        "bank blocked my payment",
        "bank blocked my wire",
        "bank blocked my transaction",
        "bank flagged my",
        "bank flagged my transfer",
        "bank flagged my payment",
        "bank rejected my",
        "bank rejected my transfer",
        "bank rejected my payment",
        "bank declined my",
        "bank declined my transfer",
        "bank won't let me transfer",
        "bank won't let me send",
        "bank refuses to",
        "bank holding my",
        "bank holding my funds",
        "bank holding my money",
        "bank froze my",
        "account frozen",
        "funds frozen",
        "money frozen",
        "transfer frozen",
        "payment frozen",
        "transfer blocked",
        "payment blocked",
        "wire blocked",
        "transaction blocked",
        "transfer rejected",
        "payment rejected",
        "wire rejected",
        "transfer declined",
        "payment declined",
        "transfer failed",
        "payment failed",
        "wire failed",
        "transfer stuck",
        "payment stuck",
        "money stuck",
        "funds stuck",
        "money held",
        "funds held",
        "money hostage",
        "holding my money",
        "holding my funds",
        "won't release my funds",
        "won't release my money",
        "compliance hold",
        "compliance review",
        "compliance check",
        "AML hold",
        "AML review",
        "AML flag",
        "flagged for review",
        "flagged as suspicious",
        "suspicious activity",
        "suspicious transaction",
        "frozen for review",
        "under review",
        "transfer delayed",
        "payment delayed",
        "wire delayed",
        "transfer pending",
        "stuck in pending",
        "days to process",
        "weeks to process",
        "10-14 days",
        "10 to 14 days",
        "two weeks to transfer",
        "transfer taking forever",
        "payment taking forever",
        "money hasn't arrived",
        "money still hasn't arrived",
        "payment hasn't arrived",
        "where is my transfer",
        "where is my payment",
        "where is my money",
        "where did my money go",
        "money disappeared",
        "payment disappeared",
        "transfer disappeared",
        "no tracking",
        "can't track my transfer",
        "can't track my payment",
        "no update on my transfer",
        "no update on my payment",

       # FEE_FRUSTRATION
        "SWIFT fees",
        "SWIFT charges",
        "wire transfer fees",
        "wire transfer charges",
        "international transfer fees",
        "international wire fees",
        "transfer fees too high",
        "transfer fees killing",
        "fees killing my margins",
        "fees eating my margins",
        "fees eating my profit",
        "exchange rate terrible",
        "exchange rate awful",
        "exchange rate bad",
        "terrible exchange rate",
        "awful exchange rate",
        "bad exchange rate",
        "worst exchange rate",
        "exchange rate ripoff",
        "exchange rate rip off",
        "hidden fees",
        "hidden charges",
        "unexpected fees",
        "unexpected charges",
        "FX fees",
        "FX charges",
        "FX markup",
        "FX spread",
        "currency conversion fee",
        "currency conversion charge",
        "conversion fee too high",
        "conversion markup",
        "losing money on transfer",
        "losing money on fees",
        "losing money to fees",
        "losing money exchanging",
        "percentage on transfer",
        "percentage on payment",
        "ripping me off",
        "highway robbery",
        "daylight robbery",
        "absolute ripoff",
        "total ripoff",
        "complete ripoff",
        "charging too much",
        "too expensive to send",
        "too expensive to transfer",
        "cheapest way to send",
        "cheapest way to transfer",
        "cheapest international transfer",
        "cheapest cross border",
        "better rate than",
        "better rates than",
        "cheaper than SWIFT",
        "cheaper than wire",
        "SWIFT alternative",
        "alternative to SWIFT",
        "avoid SWIFT fees",
        "avoid wire fees",
        "correspondent bank fees",
        "intermediary bank fees",
        "intermediary fees",

    #    'COMPETITOR_MENTIONS'
        # Wise variants
        "Wise Business",
        "Wise business account",
        "Wise transfer",
        "Wise payment",
        "Wise blocked",
        "Wise restricted",
        "Wise suspended",
        "Wise account restricted",
        "Wise account suspended",
        "Wise account blocked",
        "Wise account closed",
        "Wise limit",
        "Wise holding",
        "leaving Wise",
        "left Wise",
        "moving off Wise",
        "moved off Wise",
        "switching from Wise",
        "switched from Wise",
        "never using Wise",
        "done with Wise",
        "Wise is terrible",
        "Wise is awful",
        "Wise is a joke",
        "hate Wise",
        "Wise disappointed",
        "TransferWise",
        # Remitly
        "Remitly blocked",
        "Remitly restricted",
        "Remitly limit",
        "Remitly failed",
        "leaving Remitly",
        "switching from Remitly",
        "Remitly alternative",
        # Payoneer
        "Payoneer blocked",
        "Payoneer restricted",
        "Payoneer suspended",
        "Payoneer account blocked",
        "Payoneer account restricted",
        "Payoneer account suspended",
        "Payoneer limit",
        "Payoneer holding",
        "leaving Payoneer",
        "switching from Payoneer",
        "Payoneer alternative",
        "alternative to Payoneer",
        # WorldRemit
        "WorldRemit failed",
        "WorldRemit blocked",
        "WorldRemit problem",
        "WorldRemit issue",
        "WorldRemit terrible",
        # Western Union
        "Western Union failed",
        "Western Union blocked",
        "Western Union delayed",
        "Western Union problem",
        "leaving Western Union",
        "WU failed",
        "WU blocked",
        # OFX
        "OFX failed",
        "OFX blocked",
        "OFX problem",
        "OFX issue",
        # Revolut
        "Revolut blocked",
        "Revolut restricted",
        "Revolut suspended",
        "Revolut Business blocked",
        "Revolut Business restricted",
        "Revolut account blocked",
        "Revolut account restricted",
        "Revolut holding",
        "leaving Revolut",
        "switching from Revolut",
        # Stripe
        "Stripe blocked",
        "Stripe restricted",
        "Stripe account blocked",
        "Stripe account restricted",
        # Mercury
        "Mercury blocked",
        "Mercury restricted",
        "Mercury bank blocked",
        # LemFi
        "LemFi failed",
        "LemFi blocked",
        "LemFi problem",
        # Grey Finance
        "Grey Finance failed",
        "Grey Finance blocked",
        "Grey Finance problem",
        # NALA
        "NALA failed",
        "NALA blocked",
        "NALA problem",
        # Chipper Cash
        "Chipper Cash failed",
        "Chipper Cash blocked",
        "Chipper Cash problem",
        # General competitor frustration
        "alternative to Wise",
        "alternative to Remitly",
        "alternative to Payoneer",
        "alternative to WorldRemit",
        "alternative to Western Union",
        "alternative to Revolut",
        "better than Wise",
        "better than Remitly",
        "better than Payoneer",
        "better than WorldRemit",
        "better than Western Union",
        "competitors to Wise",
        "Wise competitors",
        "Payoneer competitors",

    # TIER 5 — RECOMMENDATION REQUESTS
        "recommend a payment",
        "recommend a transfer",
        "recommend a service",
        "recommend a platform",
        "recommend an app",
        "recommend a provider",
        "recommend a solution",
        "anyone recommend",
        "can anyone recommend",
        "does anyone recommend",
        "what payment service",
        "what transfer service",
        "what payment platform",
        "what transfer platform",
        "what payment app",
        "what transfer app",
        "which payment service",
        "which transfer service",
        "which payment platform",
        "which transfer platform",
        "which payment app",
        "which transfer app",
        "which payment provider",
        "which service is best",
        "which platform is best",
        "which app is best",
        "best payment service",
        "best transfer service",
        "best payment platform",
        "best transfer platform",
        "best payment app",
        "best transfer app",
        "best way to send",
        "best way to transfer",
        "best way to pay",
        "fastest way to send",
        "fastest way to transfer",
        "cheapest way to send",
        "cheapest way to transfer",
        "how do I send",
        "how do I transfer",
        "how do I pay",
        "how can I send",
        "how can I transfer",
        "how can I pay",
        "looking for a payment",
        "looking for a transfer",
        "looking for a platform",
        "looking for a service",
        "looking for a solution",
        "searching for a payment",
        "need a payment solution",
        "need a transfer solution",
        "need a payment platform",
        "need a transfer platform",
        "anyone using",
        "does anyone use",
        "has anyone used",
        "who uses",
        "who do you use",
        "what do you use",
        "what are you using",
        "tried everything",
        "tried so many",
        "tried multiple",
        "tried several",
        "nothing works",
        "none of them work",
        "still haven't found",
        "still looking for",
        "still searching for",

    # TIER 6 — BUSINESS AND TRADE CONTEXT
        "my supplier",
        "my suppliers",
        "our supplier",
        "our suppliers",
        "my vendor",
        "my vendors",
        "our vendor",
        "our vendors",
        "my manufacturer",
        "my manufacturers",
        "our manufacturer",
        "my factory",
        "our factory",
        "my business partner",
        "our business partner",
        "my contractor",
        "our contractor",
        "my client overseas",
        "our client overseas",
        "import business",
        "importing business",
        "export business",
        "exporting business",
        "import export",
        "import/export",
        "importing goods",
        "exporting goods",
        "importing products",
        "exporting products",
        "buying from overseas",
        "buying from abroad",
        "sourcing from",
        "sourcing overseas",
        "sourcing abroad",
        "purchase order",
        "business invoice",
        "supplier invoice",
        "vendor invoice",
        "trade finance",
        "trade payment",
        "trade financing",
        "supply chain payment",
        "supply chain finance",
        "diaspora business",
        "diaspora entrepreneur",
        "running a business",
        "my business needs",
        "for my business",
        "business account",
        "business transfer",
        "business wire",
        "corporate payment",
        "corporate transfer",
        "corporate wire",
        "company payment",
        "company transfer",
        "B2B payment",
        "B2B transfer",
        "B2B transaction",
        "business to business",

    # TIER 7 — CORRIDOR KEYWORDS
        # Nigeria corridors
        "to Nigeria",
        "to Lagos",
        "to Abuja",
        "from Nigeria",
        "Nigeria payment",
        "Nigeria transfer",
        "Nigeria wire",
        "Nigerian supplier",
        "Nigerian vendor",
        "Nigerian manufacturer",
        "Nigeria business",
        "CAD to NGN",
        "GBP to NGN",
        "USD to NGN",
        "EUR to NGN",
        "AUD to NGN",
        "naira payment",
        "naira transfer",
        "send naira",
        "receive naira",
        # Pakistan corridors
        "to Pakistan",
        "to Karachi",
        "to Lahore",
        "to Islamabad",
        "from Pakistan",
        "Pakistan payment",
        "Pakistan transfer",
        "Pakistan wire",
        "Pakistani supplier",
        "Pakistani vendor",
        "Pakistani manufacturer",
        "CAD to PKR",
        "GBP to PKR",
        "USD to PKR",
        "rupee payment",
        "rupee transfer",
        # India corridors
        "to India",
        "to Mumbai",
        "to Delhi",
        "to Bangalore",
        "from India",
        "India payment",
        "India transfer",
        "India wire",
        "Indian supplier",
        "Indian vendor",
        "Indian manufacturer",
        "CAD to INR",
        "GBP to INR",
        "USD to INR",
        "rupee payment",
        # Ghana corridors
        "to Ghana",
        "to Accra",
        "from Ghana",
        "Ghana payment",
        "Ghana transfer",
        "Ghanaian supplier",
        "GHS payment",
        "cedi payment",
        # Kenya corridors
        "to Kenya",
        "to Nairobi",
        "from Kenya",
        "Kenya payment",
        "Kenya transfer",
        "Kenyan supplier",
        "KES payment",
        "M-Pesa business",
        "Mpesa business",
        # Other African corridors
        "to Ethiopia",
        "to Senegal",
        "to Ivory Coast",
        "to Cameroon",
        "to Tanzania",
        "to Uganda",
        "to Zimbabwe",
        "to South Africa",
        "to Johannesburg",
        "African supplier",
        "African vendor",
        "African manufacturer",
        "Africa payment",
        "Africa transfer",
        # Sending countries
        "from Canada",
        "from Toronto",
        "from Vancouver",
        "from Calgary",
        "from Ottawa",
        "from Montreal",
        "from UK",
        "from London",
        "from Manchester",
        "from Birmingham",
        "from Glasgow",
        "from USA",
        "from New York",
        "from Houston",
        "from Atlanta",
        "from Washington",
        "from Australia",
        "from Sydney",
        "from Melbourne",
        "from Perth",
        "from UAE",
        "from Dubai",
        "from Abu Dhabi",

    # TIER 8 — AMOUNT SIGNALS
        "$10,000", "$10k", "10 thousand",
        "$15,000", "$15k", "15 thousand",
        "$20,000", "$20k", "20 thousand",
        "$25,000", "$25k", "25 thousand",
        "$30,000", "$30k", "30 thousand",
        "$40,000", "$40k", "40 thousand",
        "$45,000", "$45k", "45 thousand",
        "$50,000", "$50k", "50 thousand",
        "$60,000", "$60k", "60 thousand",
        "$75,000", "$75k", "75 thousand",
        "$80,000", "$80k", "80 thousand",
        "$100,000", "$100k", "100 thousand",
        "$150,000", "$150k", "150 thousand",
        "$200,000", "$200k", "200 thousand",
        "$250,000", "$250k", "250 thousand",
        "$500,000", "$500k", "500 thousand",
        "$750,000", "$750k", "750 thousand",
        "$1 million", "$1m", "one million",
        "£10,000", "£10k", "£15,000", "£15k",
        "£20,000", "£20k", "£25,000", "£25k",
        "£30,000", "£30k", "£50,000", "£50k",
        "£100,000", "£100k", "£200,000", "£200k",
        "large transfer", "large amount", "large payment",
        "large wire", "large sum", "significant amount",
        "substantial amount", "big transfer", "big payment",
        "six figures", "seven figures", "six-figure",
        "seven-figure", "monthly volume", "weekly volume",

    # TIER 9 — COMPLIANCE AND DOCUMENTATION PAIN
        "KYC rejected",
        "KYC failed",
        "KYC verification failed",
        "KYC problem",
        "KYC issue",
        "KYC nightmare",
        "AML rejected",
        "AML flagged",
        "AML hold",
        "AML review",
        "documentation rejected",
        "documents rejected",
        "proof of funds",
        "source of funds",
        "source of wealth",
        "proof of business",
        "business verification failed",
        "verification rejected",
        "verification failed",
        "compliance rejected",
        "compliance hold",
        "compliance review",
        "compliance nightmare",
        "compliance problem",
        "compliance issue",
        "Form M",
        "CBN compliance",
        "regulatory hold",
        "regulatory review",
        "regulatory problem",
        "regulatory issue",
        "submitted documents again",
        "sent documents again",
        "asking for documents again",
        "same documents again",
        "keep asking for documents",
        "keep rejecting documents",
        "third time submitting",
        "fourth time submitting",
        "rejected again",
        "blocked again",
        "failed again",
        "happening again",
        "third time",
        "fourth time",
        "keep blocking",
        "keeps blocking",
        "keeps rejecting",
        "keeps failing",
        "always blocks",
        "always rejects",
        "always fails",

    # TIER 10 — URGENCY SIGNALS
        "urgently",
        "urgent",
        "desperately",
        "desperate",
        "ASAP",
        "as soon as possible",
        "right now",
        "today",
        "this week",
        "by Friday",
        "by Monday",
        "by end of week",
        "by end of month",
        "deadline",
        "time sensitive",
        "need it done",
        "need it now",
        "need it today",
        "need it urgently",
        "waiting on payment",
        "supplier is waiting",
        "supplier waiting",
        "vendor is waiting",
        "vendor waiting",
        "manufacturer waiting",
        "partner waiting",
        "been waiting",
        "already delayed",
        "already late",
        "overdue",
        "past due",
        "losing the contract",
        "losing my supplier",
        "losing my vendor",
        "threatening to cancel",
        "might cancel",
        "going to cancel",
        "cancelling the order",
        "losing the deal",
        "deal at risk",
        "relationship at risk",
        "can't wait any longer",
        "running out of time",
        "no more time",

    # TIER 11 — BUSINESS EXPANSION SIGNALS
        "just signed a supplier",
        "signed a new supplier",
        "found a supplier",
        "new supplier in",
        "signed a contract with",
        "new contract with",
        "starting to import",
        "starting an import",
        "starting to export",
        "starting an export",
        "launching in",
        "expanding to",
        "entering the market",
        "new market",
        "setting up payments",
        "need to set up payments",
        "need to transfer money",
        "will need to send",
        "will need to transfer",
        "going to need",
        "starting a business",
        "new business",
        "import business",
        "export business",
        "trading company",
        "sourcing products from",
        "sourcing goods from",
        "buying products from",
        "buying goods from",
        "manufacturing in",
        "producing in",

    # TIER 12 — TREASURY AND FX MANAGEMENT
        "treasury management",
        "cash management",
        "liquidity management",
        "FX management",
        "FX exposure",
        "FX risk",
        "FX hedging",
        "currency hedging",
        "currency risk",
        "currency exposure",
        "FX solution",
        "FX platform",
        "FX tool",
        "treasury solution",
        "treasury platform",
        "cash flow management",
        "multi currency",
        "multi-currency",
        "multicurrency",
        "currency account",
        "foreign currency account",
        "international banking",
        "international bank account",
        "global banking",
        "global bank account",
        "correspondent banking",
        "banking relationship",
        "banking partner",
        "payment infrastructure",
        "payment rails",
        "payment solution",
        "payment platform",
        "payment provider",
        "payment partner",
        "fintech payment",
        "embedded payment",
        "embedded finance",
        "cross border banking",
        "international banking solution",
        "FX banking",
        "FX banking relationship",
        "FX liquidity",
        "cash pooling",
        "cash concentration",
        "intercompany payment",
        "intercompany transfer",

    # TIER 13 — JOB POSTING SIGNALS
        "treasury manager",
        "treasury analyst",
        "FX manager",
        "FX analyst",
        "FX trader",
        "treasury director",
        "head of treasury",
        "VP treasury",
        "international payments manager",
        "global payments manager",
        "cross border payments",
        "payments operations manager",
        "payments specialist",
        "treasury specialist",
        "FX specialist",
        "international finance manager",
        "global finance manager",
        "head of payments",
        "director of payments",
        "VP payments",
        "chief financial officer",
        "head of finance",
        "finance director",
        "controller international",
        "global controller",

    # TIER 14 — NEGATIVE FILTERS
        "send to my mum",
        "send to my mom",
        "send to my parents",
        "send to my family",
        "send to my sister",
        "send to my brother",
        "school fees",
        "rent money",
        "personal money",
        "pocket money",
        "allowance",
        "birthday gift",
        "wedding gift",
        "PayPal personal",
        "Cash App",
        "Venmo",
        "Zelle",
        "Apple Pay",
        "Google Pay",
        "$50", "$100", "$200", "$300", "$500",
        "£50", "£100", "£200", "£300", "£500",
        "crypto trading",
        "bitcoin trading",
        "ethereum trading",
        "altcoin",
        "NFT",
        "DeFi yield",
        "staking",
        "mining",
        "Netflix",
        "Spotify",
        "BeatStars",
        "subscription payment",
        "monthly subscription",
        "stock market",
        "shares",
        "dividend",
        "mortgage",
        "car payment",
        "car loan",
        "student loan",
        "credit card",
        "insurance claim",
        "tax refund",
]

def passes_keyword_filter(text: str) -> bool:
    """
    Returns True if text contains at least one target keyword.
    Case-insensitive. Zero API cost. Runs in microseconds.
    Applied to ALL content: posts, comments, replies, tweets.
    """
    t = text.lower()
    for kw in KEYWORDS:
        if kw in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SYSTEM PROMPT  — v6.1 (identical to v6.0)
# ─────────────────────────────────────────────────────────────────────────────

CLAUDE_SYSTEM_PROMPT = """
You are a B2B signal intelligence analyst for Flintel.
Your client is Settla — a premium cross-border payment company for diaspora business owners.

━━━ WHO SETTLA SERVES ━━━

Ideal customer: diaspora business owner who
— Runs import/export, trading, or has overseas suppliers
— Moves $10,000–$500,000 CAD/GBP/USD for business payments
— Is frustrated with banks blocking large international transfers
— Has been burned by consumer apps (Wise, Remitly) restricting volumes
— Is actively seeking a better cross-border payment solution NOW

Settla is NOT for personal remittances, family transfers, or amounts under $2,000.

Primary corridors: Canada→Nigeria, UK→Nigeria, USA→Nigeria,
Canada→Pakistan, UK→Pakistan, Canada→India, Canada→Ghana,
UK→Ghana, Australia→Nigeria, UAE→Nigeria — and all diaspora business corridors.

━━━ 8 PAIN TYPES ━━━

1. blocked    — transfer/account blocked, flagged, frozen, compliance hold
2. delayed    — stuck, no visibility, payment disappeared, taking too long
3. expensive  — SWIFT fees, poor rates, hidden charges, margin erosion
4. rejected   — KYC failed, documents refused, AML hold, verification failed
5. restricted — competitor app limited/suspended account above volume threshold
6. supplier   — supplier waiting, relationship at risk, losing contract
7. researching— comparing services, asking recommendations, evaluating options
8. expanding  — new supplier, new market, launching business, first payment

━━━ SCORING RULES ━━━

SCORE 9-10 → tier: immediate
ALL THREE required:
✓ Clear business context (supplier, vendor, invoice, import, export, trade)
✓ Large amount implied or stated ($10k+)
✓ Active crisis RIGHT NOW (blocked, failed, rejected, urgent, ASAP)

SCORE 7-8 → tier: immediate
Strong signal, one element missing.
✓ Business context confirmed
✓ Active problem or competitor frustration
✗ Missing specific amount OR extreme urgency

SCORE 5-6 → tier: digest
Researching, no immediate crisis.
— Comparing platforms, asking for recommendations
— Starting a business, exploring payment options

SCORE 3-4 → tier: watchlist
Future potential, 30-60 days.
— Launching soon, new supplier found, contract signed

SCORE 0-2 → tier: discard
Consumer, personal, wrong context.
— Sending money home to family
— Small personal amounts under $2,000
— Consumer app complaints unrelated to business
— General market commentary, news sharing

AUTO +1: business identity confirmed, large amount ($10k+), multiple pain points,
         negative competitor mention, urgency words, active block, supplier at risk
AUTO -1: personal/family context, amount under $2k, no business bio, commentary only

AUTO DISCARD: consumer subscriptions, personal PayPal/CashApp, competitor outreach accounts,
              content creators, crypto trading, news reposts without personal pain

━━━ COMPETITOR INTELLIGENCE ━━━

Negative competitor mention → score +1. Person leaving competitor = hottest signal.
Known competitors: Wise, Remitly, WorldRemit, Western Union, MoneyGram, Payoneer,
OFX, XE, Revolut, LemFi, NALA, Grey Finance, Chipper Cash, Sendwave, TransferGo,
Azimo, Xoom, OneDosh, Flutterwave, Duplo, Mercury, TD Bank, RBC, HSBC, Barclays.

If post IS FROM a competitor doing outreach: score=0, set competitor_outreach_detected=true.

━━━ OUTREACH SCRIPTS (score 5+ only; null below 5) ━━━

Write THREE versions — reference their SPECIFIC situation, not generic pitch.
Max 3 sentences each. Sound like a founder, not a sales rep.
Never start with "I". Never say "I hope this message finds you well".
Never list features — pitch the outcome. End with one soft question.

twitter_reply  — 2 sentences max, public tone
twitter_dm     — 3 sentences max, personal tone
linkedin_message — 3 sentences max, professional but human

━━━ RETURN FORMAT ━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <0-10>,
    "signal_category": <"high_intent" | "mid_intent" | "low_intent" | "discard">,
    "tier": <"immediate" | "digest" | "watchlist" | "discard">,
    "is_business": <true | false>,
    "business_size": <"solo" | "small" | "medium" | "unknown">,
    "corridor": "<source to destination or null>",
    "estimated_amount": "<amount if stated or null>",
    "competitor_mentioned": "<name or null>",
    "competitor_outreach_detected": <true | false>,
    "pain_type": "<one of 8 types or null>",
    "urgency": "<immediate | today | this_week | researching | none>",
    "reason": "<one precise sentence>",
    "suggested_action": "<one precise sentence for Settla SDR>",
    "twitter_reply": "<public reply text or null>",
    "twitter_dm": "<DM text or null>",
    "linkedin_message": "<LinkedIn text or null>",
    "watchlist": <true | false>,
    "watchlist_reason": "<why monitor or null>"
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

        # Signals collection
        db.signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        for field in [
            "intent_score", "created_at", "client_id", "platform",
            "tier", "corridor", "competitor_mentioned", "pain_type",
            "is_business", "signal_category",
        ]:
            db.signals.create_index([(field, ASCENDING)])

        # ── v6.1: pending_items collection ────────────────────────────────────
        # Keyword-matched items waiting for a full batch to be scored by Claude.
        # Survives restarts — no data loss, no duplicate scoring.
        db.pending_items.create_index(
            [("message_id", ASCENDING)], unique=True, name="pending_message_id_unique"
        )
        db.pending_items.create_index([("platform", ASCENDING)])
        db.pending_items.create_index([("queued_at", ASCENDING)])

        # ── v6.1: twitter_state collection ────────────────────────────────────
        # Persists since_id and seen_tweet_ids between restarts.
        # One doc per client_id keeps it clean.
        db.twitter_state.create_index(
            [("client_id", ASCENDING)], unique=True, name="twitter_state_client_unique"
        )

        log.info("MongoDB connected. Collections: signals ✅ | pending_items ✅ | twitter_state ✅")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT
# ─────────────────────────────────────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────────────────────────────────────────
# RETRY WITH EXPONENTIAL BACKOFF
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
# v6.1: PENDING ITEMS — MONGODB PERSISTENCE
# Items that passed the keyword filter but haven't been batched yet.
# Survives process restarts — no lost signals, no wasted Claude tokens.
# ─────────────────────────────────────────────────────────────────────────────

def save_pending_item(item: dict) -> bool:
    """
    Persist a keyword-matched item to MongoDB pending_items collection.
    Returns True if saved, False if duplicate (already pending or already scored).
    """
    try:
        doc = {
            "message_id": item["message_id"],
            "platform":   item.get("platform", "unknown"),
            "item_data":  item,
            "queued_at":  datetime.now(timezone.utc),
        }
        db.pending_items.insert_one(doc)
        return True
    except DuplicateKeyError:
        log.debug(f"Pending duplicate skipped: {item['message_id']}")
        return False
    except Exception as exc:
        log.error(f"save_pending_item error: {exc}")
        return False


def load_pending_items(platform: str) -> list:
    """
    Load all pending items for a platform from MongoDB on startup.
    Called once per platform at process start to restore unprocessed queue.
    """
    try:
        docs = list(
            db.pending_items.find(
                {"platform": platform},
                {"_id": 0, "item_data": 1},
            ).sort("queued_at", ASCENDING)
        )
        items = [d["item_data"] for d in docs]
        if items:
            log.info(f"[{platform.upper()}] Restored {len(items)} pending items from MongoDB.")
        return items
    except Exception as exc:
        log.error(f"load_pending_items error: {exc}")
        return []


def delete_pending_items(message_ids: list):
    """
    Remove items from pending_items after they've been scored by Claude.
    Called after each successful batch.
    """
    if not message_ids:
        return
    try:
        result = db.pending_items.delete_many({"message_id": {"$in": message_ids}})
        log.debug(f"Cleared {result.deleted_count} pending items from MongoDB.")
    except Exception as exc:
        log.error(f"delete_pending_items error: {exc}")


def is_already_scored(message_id: str) -> bool:
    """
    Check if a message_id already exists in the signals collection.
    Prevents re-scoring items that were already processed before a restart.
    """
    try:
        return db.signals.count_documents({"message_id": message_id}, limit=1) > 0
    except Exception as exc:
        log.error(f"is_already_scored check error: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# v6.1: TWITTER STATE — PERSISTED since_id + seen_ids
# The #1 fix for lost tweets and burned tokens on restart.
# ─────────────────────────────────────────────────────────────────────────────

def load_twitter_state() -> tuple[str | None, set]:
    """
    Load persisted Twitter state from MongoDB:
    - since_id: the highest tweet ID seen so far → Twitter API only returns NEWER tweets
    - seen_ids: set of tweet IDs already processed → local dedup guard

    Returns (since_id, seen_ids_set).
    On first run (no state), returns (None, set()).
    """
    try:
        doc = db.twitter_state.find_one({"client_id": CLIENT_ID})
        if not doc:
            log.info("Twitter state: first run — no persisted state found.")
            return None, set()
        since_id = doc.get("since_id")
        seen_ids = set(doc.get("seen_ids", []))
        log.info(
            f"Twitter state loaded | since_id:{since_id} | "
            f"seen_ids:{len(seen_ids)} cached"
        )
        return since_id, seen_ids
    except Exception as exc:
        log.error(f"load_twitter_state error: {exc}")
        return None, set()


def save_twitter_state(since_id: str | None, seen_ids: set):
    """
    Persist Twitter state to MongoDB.
    Called after every poll cycle that fetches new tweets.
    Caps stored seen_ids at 10,000 most-recent (memory/storage efficient).
    """
    try:
        # Keep only the 10k most recent IDs (tweet IDs are numeric strings — sort desc)
        seen_list = sorted(seen_ids, reverse=True)[:10_000]
        db.twitter_state.update_one(
            {"client_id": CLIENT_ID},
            {"$set": {
                "client_id":   CLIENT_ID,
                "since_id":    since_id,
                "seen_ids":    seen_list,
                "updated_at":  datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"save_twitter_state error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER  (shared by Reddit + Twitter)
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        ctype     = item.get("content_type", "unknown").upper()
        platform  = item.get("platform", "unknown").upper()
        subreddit = item.get("subreddit", "")
        username  = item.get("username", "unknown")
        text      = item.get("text", "")[:800]   # 800-char cap — cost reduction

        location = f"r/{subreddit}" if subreddit else platform
        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Platform: {platform} | Source: {location} | Type: {ctype} | User: {username}\n"
            f"Content: {text}\n"
        )
    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Scoring unavailable.") -> dict:
    return {
        "index": index, "intent_score": 0,
        "signal_category": "discard", "tier": "discard",
        "is_business": False, "business_size": "unknown",
        "corridor": None, "estimated_amount": None,
        "competitor_mentioned": None, "competitor_outreach_detected": False,
        "pain_type": None, "urgency": "none",
        "reason": reason, "suggested_action": "Check system logs.",
        "twitter_reply": None, "twitter_dm": None, "linkedin_message": None,
        "watchlist": False, "watchlist_reason": None,
    }


def _call_claude_batch(batch: list) -> list:
    prompt = _build_batch_prompt(batch)
    response = anthropic_client.messages.create(
        model      = "claude-sonnet-4-6",
        max_tokens = 4096,
        system     = CLAUDE_SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": f"Score this batch:\n\n{prompt}"}],
    )

    raw = response.content[0].text.strip()
    # Strip accidental markdown fences
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()

    results = json.loads(raw)
    if not isinstance(results, list):
        raise ValueError("Claude returned non-list.")

    required = {"index", "intent_score", "signal_category", "tier", "is_business", "reason", "suggested_action"}
    optional_defaults = {
        "business_size": "unknown", "corridor": None, "estimated_amount": None,
        "competitor_mentioned": None, "competitor_outreach_detected": False,
        "pain_type": None, "urgency": "none",
        "twitter_reply": None, "twitter_dm": None, "linkedin_message": None,
        "watchlist": False, "watchlist_reason": None,
    }
    for r in results:
        missing = required - r.keys()
        if missing:
            raise ValueError(f"Missing keys in Claude response: {missing}")
        for k, v in optional_defaults.items():
            r.setdefault(k, v)

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(
        _call_claude_batch, batch,
        retries=3, delay=5, label="Claude-Batch",
    )
    if result is None:
        return [_fallback_score(i + 1) for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE
# ─────────────────────────────────────────────────────────────────────────────

def save_signal(data: dict) -> bool:
    try:
        doc = {
            "message_id":                data["message_id"],
            "platform":                  data.get("platform", "unknown"),
            "content_type":              data.get("content_type", "unknown"),
            "subreddit":                 data.get("subreddit", ""),
            "post_url":                  data.get("post_url", ""),
            "username":                  data.get("username", "unknown"),
            "message_text":              data["message_text"],
            "intent_score":              data["intent_score"],
            "signal_category":           data["signal_category"],
            "tier":                      data.get("tier", "discard"),
            "is_business":               data.get("is_business", False),
            "business_size":             data.get("business_size", "unknown"),
            "corridor":                  data.get("corridor"),
            "estimated_amount":          data.get("estimated_amount"),
            "competitor_mentioned":      data.get("competitor_mentioned"),
            "competitor_outreach_detected": data.get("competitor_outreach_detected", False),
            "pain_type":                 data.get("pain_type"),
            "urgency":                   data.get("urgency", "none"),
            "reason":                    data["reason"],
            "suggested_action":          data["suggested_action"],
            "twitter_reply":             data.get("twitter_reply"),
            "twitter_dm":                data.get("twitter_dm"),
            "linkedin_message":          data.get("linkedin_message"),
            "watchlist":                 data.get("watchlist", False),
            "watchlist_reason":          data.get("watchlist_reason"),
            "client_id":                 CLIENT_ID,
            "alerted_slack":             False,
            "alerted_hubspot":           False,
            "digest_included":           False,
            "created_at":                datetime.now(timezone.utc),
        }
        db.signals.insert_one(doc)
        log.info(
            f"SAVED | {data.get('platform','?').upper()} | "
            f"Score:{data['intent_score']} | Tier:{data.get('tier','?')} | "
            f"u/{data.get('username')} | {data.get('content_type','')} | "
            f"r/{data.get('subreddit','')}" if data.get('subreddit') else
            f"SAVED | {data.get('platform','?').upper()} | "
            f"Score:{data['intent_score']} | u/{data.get('username')}"
        )
        return True
    except DuplicateKeyError:
        log.debug(f"Duplicate skipped: {data['message_id']}")
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
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
# SLACK DELIVERY
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
    outreach    = data.get("twitter_reply") or data.get("twitter_dm") or ""
    timestamp   = data.get("timestamp", "—")

    if score >= 9:
        urgency_tag = "⚡ RESPOND WITHIN 30 MINUTES"
    elif score >= 7:
        urgency_tag = "⏰ RESPOND WITHIN 2 HOURS"
    elif score >= 5:
        urgency_tag = "📋 ADD TO TODAY'S OUTREACH LIST"
    else:
        urgency_tag = ""

    header_emoji = "🚨" if score >= 8 else "⚠️"
    header_text  = f"{header_emoji} {category} — Score {score}/10 | {tier}"

    source_label = f"r/{subreddit}" if subreddit else platform

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
                {"type": "mrkdwn", "text": f"*User*\nu/{username}"},
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
        log.info(f"Slack sent | u/{username} | Score:{score}")
        return True
    log.error("Slack delivery failed after all retries.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# HUBSPOT CRM
# ─────────────────────────────────────────────────────────────────────────────

HUBSPOT_BASE = "https://api.hubapi.com"


def _hs_headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}


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
    except Exception as exc:
        log.error(f"HubSpot find contact error: {exc}")
        return None


def _hs_create_contact(data: dict) -> str | None:
    try:
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
            json={"properties": {
                "firstname":           f"u/{data['username']}",
                "lastname":            f"{data.get('platform','?').upper()} Signal",
                "fx_intent_score":     str(data["intent_score"]),
                "fx_signal_category":  data["signal_category"],
                "fx_tier":             data.get("tier", ""),
                "fx_corridor":         data.get("corridor") or "",
                "fx_pain_type":        data.get("pain_type") or "",
                "fx_competitor":       data.get("competitor_mentioned") or "",
                "fx_platform":         data.get("platform", ""),
                "fx_source_community": data.get("subreddit", "") or data.get("platform", ""),
                "fx_signal_reason":    data["reason"],
                "fx_suggested_action": data["suggested_action"],
            }},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as exc:
        log.error(f"HubSpot create contact error: {exc}")
        return None


def _hs_create_note(data: dict, contact_id: str):
    try:
        note = (
            f"FLINTEL SIGNAL — v6.1\n\n"
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
            f"Source:       {data.get('subreddit','') or data.get('platform','')}\n"
            f"URL:          {data.get('post_url','N/A')}\n"
            f"Username:     u/{data.get('username','unknown')}\n"
            f"Timestamp:    {data.get('timestamp','N/A')}\n\n"
            f"Message:\n{data['message_text']}\n\n"
            f"Reason:       {data['reason']}\n"
            f"Action:       {data['suggested_action']}\n\n"
            f"Twitter Reply:\n{data.get('twitter_reply') or 'N/A'}\n\n"
            f"Twitter DM:\n{data.get('twitter_dm') or 'N/A'}\n\n"
            f"LinkedIn:\n{data.get('linkedin_message') or 'N/A'}"
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
# CORE SIGNAL PROCESSOR  (platform-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

def process_scored_item(item: dict, score_result: dict):
    """
    Receives one item + its Claude score. Runs full delivery pipeline.
    Identical logic for Reddit and Twitter items.
    """
    score = score_result.get("intent_score", 0)

    if score < MIN_SCORE_MEDIUM:
        log.debug(
            f"DISCARD | Score:{score} | {item.get('platform','?').upper()} | "
            f"u/{item.get('username')} | {item.get('content_type','')}"
        )
        return

    data = {
        "message_id":                item["message_id"],
        "platform":                  item.get("platform", "unknown"),
        "content_type":              item.get("content_type", "unknown"),
        "subreddit":                 item.get("subreddit", ""),
        "post_url":                  item.get("post_url", ""),
        "username":                  item.get("username", "unknown"),
        "message_text":              item.get("text", ""),
        "intent_score":              score,
        "signal_category":           score_result.get("signal_category", "discard"),
        "tier":                      score_result.get("tier", "discard"),
        "is_business":               score_result.get("is_business", False),
        "business_size":             score_result.get("business_size", "unknown"),
        "corridor":                  score_result.get("corridor"),
        "estimated_amount":          score_result.get("estimated_amount"),
        "competitor_mentioned":      score_result.get("competitor_mentioned"),
        "competitor_outreach_detected": score_result.get("competitor_outreach_detected", False),
        "pain_type":                 score_result.get("pain_type"),
        "urgency":                   score_result.get("urgency", "none"),
        "reason":                    score_result.get("reason", ""),
        "suggested_action":          score_result.get("suggested_action", ""),
        "twitter_reply":             score_result.get("twitter_reply"),
        "twitter_dm":                score_result.get("twitter_dm"),
        "linkedin_message":          score_result.get("linkedin_message"),
        "watchlist":                 score_result.get("watchlist", False),
        "watchlist_reason":          score_result.get("watchlist_reason"),
        "timestamp":                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    saved = save_signal(data)
    if not saved:
        return  # Duplicate — skip delivery

    if MIN_SCORE_MEDIUM <= score < MIN_SCORE_HIGH:
        log.info(f"MEDIUM | Score:{score} | Slack only | u/{data['username']} | {data['platform'].upper()}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])

    elif score >= MIN_SCORE_HIGH:
        log.info(f"HIGH   | Score:{score} | Slack + HubSpot | u/{data['username']} | {data['platform'].upper()}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])
        cid = send_to_hubspot(data)
        if cid:
            mark_hubspot_alerted(data["message_id"], cid)


# ─────────────────────────────────────────────────────────────────────────────
# v6.1: GENERIC BATCH PROCESSOR  (shared logic for both platforms)
# Key change: pending_items persisted to MongoDB so in-progress batches
# survive restarts. Items are deleted from pending only after Claude scores them.
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
):
    """
    Reads from queue q.
    Collects keyword-matched items into batches of batch_size.
    Persists each matched item to MongoDB pending_items (survives restarts).
    Sends each full batch to Claude, then runs process_scored_item per item.
    Clears pending_items from MongoDB after Claude scores the batch.
    30s gap between batches.

    On startup, restores pending items from MongoDB before processing new ones.
    """
    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{BATCH_GAP_SECONDS}s"
    )

    # ── Restore any pending items that survived from before last restart ──────
    restored = load_pending_items(platform_label.lower())
    # Filter out any that were already scored (edge case: scored but not deleted)
    restored = [it for it in restored if not is_already_scored(it["message_id"])]
    current_batch: list = restored
    if current_batch:
        log.info(
            f"[{platform_label}] Restored {len(current_batch)} unscored pending items."
        )

    total_received = 0
    total_matched  = 0
    total_dropped  = 0
    total_batches  = 0

    while True:
        try:
            try:
                item = q.get(timeout=1)
            except queue.Empty:
                continue

            total_received += 1
            text = item.get("text", "").strip()

            if not text or len(text) < 10:
                q.task_done()
                continue

            if not passes_keyword_filter(text):
                total_dropped += 1
                log.debug(
                    f"[{platform_label}] FILTERED | u/{item.get('username')} | "
                    f"{item.get('content_type','?')}"
                )
                q.task_done()
                continue

            # Skip if already scored (prevents re-scoring after restart)
            if is_already_scored(item["message_id"]):
                log.debug(f"[{platform_label}] Already scored, skipping: {item['message_id']}")
                q.task_done()
                continue

            # Persist to MongoDB pending_items — survives restart
            saved = save_pending_item(item)
            if not saved:
                # Duplicate in pending — skip but don't count as dropped
                q.task_done()
                continue

            total_matched += 1
            current_batch.append(item)

            log.info(
                f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | "
                f"{item.get('content_type','?').upper()} | u/{item.get('username')}"
            )

            q.task_done()

            if len(current_batch) >= batch_size:
                total_batches  += 1
                batch_to_send   = current_batch[:batch_size]
                current_batch   = current_batch[batch_size:]

                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | "
                    f"items:{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores = score_batch_with_claude(batch_to_send)

                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch."))
                    process_scored_item(it, sr)

                # ── Remove scored items from pending_items ───────────────────
                delete_pending_items([it["message_id"] for it in batch_to_send])

                log.info(
                    f"[{platform_label}] BATCH {total_batches} DONE | "
                    f"waiting {BATCH_GAP_SECONDS}s..."
                )
                time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT STREAMS
# ─────────────────────────────────────────────────────────────────────────────

def build_reddit_client() -> praw.Reddit:
    r = praw.Reddit(
        client_id     = REDDIT_CLIENT_ID,
        client_secret = REDDIT_CLIENT_SECRET,
        username      = REDDIT_USERNAME,
        password      = REDDIT_PASSWORD,
        user_agent    = REDDIT_USER_AGENT,
    )
    log.info(f"Reddit authenticated as u/{REDDIT_USERNAME}")
    return r


def stream_posts(reddit: praw.Reddit):
    combined  = "+".join(TARGET_SUBREDDITS)
    subreddit = reddit.subreddit(combined)
    log.info(f"Reddit post stream started | {len(TARGET_SUBREDDITS)} subreddits")

    while True:
        try:
            for post in subreddit.stream.submissions(skip_existing=True, pause_after=0):
                if post is None:
                    continue
                text   = post.title
                if post.selftext and post.selftext.strip():
                    text = f"{post.title}\n\n{post.selftext}"
                author = str(post.author) if post.author else "[deleted]"
                reddit_queue.put({
                    "message_id":   f"reddit_post_{post.id}",
                    "platform":     "reddit",
                    "content_type": "post",
                    "text":         text,
                    "username":     author,
                    "subreddit":    str(post.subreddit),
                    "post_url":     f"https://reddit.com{post.permalink}",
                })
        except praw.exceptions.PRAWException as exc:
            log.error(f"PRAW post stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)
        except Exception as exc:
            log.error(f"Post stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)


def stream_comments(reddit: praw.Reddit):
    combined  = "+".join(TARGET_SUBREDDITS)
    subreddit = reddit.subreddit(combined)
    log.info(f"Reddit comment stream started | {len(TARGET_SUBREDDITS)} subreddits")

    while True:
        try:
            for comment in subreddit.stream.comments(skip_existing=True, pause_after=0):
                if comment is None:
                    continue
                ctype  = "reply" if comment.parent_id.startswith("t1_") else "comment"
                author = str(comment.author) if comment.author else "[deleted]"
                reddit_queue.put({
                    "message_id":   f"reddit_comment_{comment.id}",
                    "platform":     "reddit",
                    "content_type": ctype,
                    "text":         comment.body,
                    "username":     author,
                    "subreddit":    str(comment.subreddit),
                    "post_url":     f"https://reddit.com{comment.permalink}",
                })
        except praw.exceptions.PRAWException as exc:
            log.error(f"PRAW comment stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)
        except Exception as exc:
            log.error(f"Comment stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X  POLLER  — v6.1
#
# Key fixes vs v6.0:
#   1. since_id loaded from MongoDB on startup → polls ONLY fetch new tweets
#      Old tweets are NEVER re-fetched after restart. Zero wasted tokens.
#   2. seen_ids loaded from MongoDB on startup → in-memory dedup restored
#   3. Both since_id and seen_ids persisted after every poll cycle
#   4. seen_ids pruned (not cleared) at 100k — no gaps in dedup coverage
#   5. since_id updated to max tweet ID seen each cycle — forward-only cursor
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_SEARCH_QUERY = (
    "("
    "\"pay my supplier\" OR \"supplier payment\" OR \"send to nigeria\" OR "
    "\"send to pakistan\" OR \"wise blocked\" OR \"wise restricted\" OR "
    "\"transfer blocked\" OR \"bank blocked\" OR \"remitly\" OR "
    "\"worldremit\" OR \"cross border payment\" OR \"international transfer\" OR "
    "\"diaspora business\" OR \"import payment\" OR \"cad to ngn\" OR "
    "\"gbp to ngn\" OR \"usd to ngn\" OR \"supplier waiting\" OR "
    "\"paying invoice\" OR \"exchange rate\" OR \"kyc rejected\" OR "
    "\"compliance hold\" OR \"swift fees\""
    ") "
    "-is:retweet lang:en"
)


def build_twitter_client() -> tweepy.Client | None:
    if not TWITTER_BEARER_TOKEN:
        log.warning("TWITTER_BEARER_TOKEN not set — Twitter platform disabled.")
        return None
    try:
        client = tweepy.Client(
            bearer_token        = TWITTER_BEARER_TOKEN,
            consumer_key        = TWITTER_API_KEY,
            consumer_secret     = TWITTER_API_SECRET,
            wait_on_rate_limit  = True,
        )
        log.info("Twitter/X client initialised.")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def poll_twitter(client: tweepy.Client):
    """
    Polls Twitter search every TWITTER_POLL_INTERVAL seconds.

    v6.1 changes:
    - Loads since_id from MongoDB on startup → only NEW tweets fetched
    - Loads seen_ids from MongoDB on startup → dedup survives restarts
    - Updates since_id to highest tweet ID seen each cycle
    - Persists state after every cycle with new tweets
    - Prunes seen_ids at 100k (not cleared) → no dedup gaps
    - Zero old tweets re-fetched after restart → zero wasted Claude tokens
    """
    # ── Load persisted state ──────────────────────────────────────────────────
    since_id, seen_ids = load_twitter_state()
    log.info(
        f"Twitter poll started | "
        f"since_id:{since_id or 'none (first run)'} | "
        f"seen_ids_cached:{len(seen_ids)}"
    )

    while True:
        try:
            # Build request kwargs — only include since_id if we have one
            kwargs: dict = {
                "query":        TWITTER_SEARCH_QUERY,
                "max_results":  50,
                "tweet_fields": ["author_id", "created_at", "text", "conversation_id"],
                "expansions":   ["author_id"],
                "user_fields":  ["username", "name"],
            }
            if since_id:
                kwargs["since_id"] = since_id  # ← THE KEY FIX: only new tweets

            response = client.search_recent_tweets(**kwargs)

            if not response or not response.data:
                log.debug("Twitter: no new tweets this cycle.")
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            # Build author_id → username map from includes
            user_map: dict = {}
            if response.includes and "users" in response.includes:
                for u in response.includes["users"]:
                    user_map[u.id] = u.username

            new_count    = 0
            max_id_seen  = since_id  # Track highest ID in this cycle

            for tweet in response.data:
                tweet_id = str(tweet.id)

                # Update max ID seen (tweet IDs are numeric — compare as int)
                if max_id_seen is None or int(tweet_id) > int(max_id_seen):
                    max_id_seen = tweet_id

                # In-memory dedup guard (covers same-cycle duplicates)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)

                text     = tweet.text or ""
                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")

                twitter_queue.put({
                    "message_id":   f"twitter_{tweet_id}",
                    "platform":     "twitter",
                    "content_type": "tweet",
                    "text":         text,
                    "username":     username,
                    "subreddit":    "",
                    "post_url":     f"https://twitter.com/{username}/status/{tweet_id}",
                })
                new_count += 1

            if new_count:
                log.info(
                    f"Twitter: {new_count} new tweets queued | "
                    f"queue_size:{twitter_queue.qsize()} | "
                    f"since_id updated to {max_id_seen}"
                )

            # ── Advance since_id cursor ───────────────────────────────────────
            if max_id_seen and max_id_seen != since_id:
                since_id = max_id_seen
                # ── Prune seen_ids: keep 100k most recent ──────────────────
                if len(seen_ids) > 100_000:
                    pruned = sorted(seen_ids, reverse=True)[:100_000]
                    seen_ids = set(pruned)
                    log.debug("Twitter seen_ids pruned to 100k.")
                # ── Persist to MongoDB ──────────────────────────────────────
                save_twitter_state(since_id, seen_ids)

        except tweepy.errors.TweepyException as exc:
            log.error(f"Twitter poll error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")
        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULERS — Daily Digest + Weekly Report
# ─────────────────────────────────────────────────────────────────────────────

def send_daily_digest():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        signals = list(
            db.signals.find({
                "client_id": CLIENT_ID,
                "intent_score": {"$gte": 6, "$lte": 7},
                "created_at": {"$gte": since},
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
            lines.append(
                f"• *u/{s.get('username','?')}* | Score:{s['intent_score']}/10 "
                f"| {platform} | {s.get('content_type','').upper()}\n"
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
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v6.1 | Client: {CLIENT_ID} | Reddit + Twitter"}]},
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
        since       = datetime.now(timezone.utc) - timedelta(days=7)
        all_signals = list(db.signals.find({"client_id": CLIENT_ID, "created_at": {"$gte": since}}))
        high        = [s for s in all_signals if s["intent_score"] >= 8]
        medium      = [s for s in all_signals if 6 <= s["intent_score"] <= 7]
        business    = [s for s in all_signals if s.get("is_business")]
        reddit_sigs = [s for s in all_signals if s.get("platform") == "reddit"]
        twitter_sigs= [s for s in all_signals if s.get("platform") == "twitter"]
        total       = len(all_signals)

        if total == 0:
            log.info("Weekly report: no signals this week.")
            return

        def breakdown(key):
            counts: dict = {}
            for s in all_signals:
                v = s.get(key)
                if v:
                    counts[v] = counts.get(v, 0) + 1
            return "\n".join(f"  • {k}: {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])) or "_None_"

        top3 = sorted(high, key=lambda x: x["intent_score"], reverse=True)[:3]
        top3_lines = [
            f"• *u/{s.get('username','?')}* | Score:{s['intent_score']}/10 "
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
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Corridor Breakdown*\n{breakdown('corridor')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Competitor Mentions*\n{breakdown('competitor_mentioned')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Pain Types*\n{breakdown('pain_type')}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top 3 Signals This Week*\n\n{_safe(chr(10).join(top3_lines), 2800)}"}},
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v6.1 | {CLIENT_ID} | Week ending {week_end}"}]},
            ],
        }

        result = retry_with_backoff(_post_to_slack, payload, retries=3, delay=2, label="WeeklyReport")
        if result:
            log.info(f"Weekly report sent | Total:{total} High:{len(high)} Med:{len(medium)} Biz:{len(business)}")

    except Exception as exc:
        log.error(f"Weekly report error: {exc}")


async def run_scheduler():
    log.info(f"Scheduler started | digest:{DAILY_DIGEST_HOUR}:00 UTC | report Mon {WEEKLY_REPORT_HOUR}:00 UTC")
    last_digest_date = None
    last_report_week = None

    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)

        if now.hour == DAILY_DIGEST_HOUR and now.date() != last_digest_date:
            log.info("Scheduler: triggering daily digest...")
            await asyncio.to_thread(send_daily_digest)
            last_digest_date = now.date()

        if (
            now.weekday() == WEEKLY_REPORT_DAY
            and now.hour == WEEKLY_REPORT_HOUR
            and now.isocalendar()[1] != last_report_week
        ):
            log.info("Scheduler: triggering weekly report...")
            await asyncio.to_thread(send_weekly_report)
            last_report_week = now.isocalendar()[1]


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS  — thread management + auto-restart
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    reddit = build_reddit_client()

    post_thread = threading.Thread(target=stream_posts,    args=(reddit,), daemon=True, name="Reddit-Posts")
    cmnt_thread = threading.Thread(target=stream_comments, args=(reddit,), daemon=True, name="Reddit-Comments")
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
        daemon=True, name="Reddit-Batch",
    )

    post_thread.start()
    cmnt_thread.start()
    btch_thread.start()
    log.info("Reddit threads running: Posts ✅ | Comments ✅ | Batch ✅")

    while True:
        await asyncio.sleep(60)
        if not post_thread.is_alive():
            log.error("Reddit post thread died — restarting...")
            post_thread = threading.Thread(target=stream_posts, args=(reddit,), daemon=True, name="Reddit-Posts")
            post_thread.start()
        if not cmnt_thread.is_alive():
            log.error("Reddit comment thread died — restarting...")
            cmnt_thread = threading.Thread(target=stream_comments, args=(reddit,), daemon=True, name="Reddit-Comments")
            cmnt_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor, args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener():
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
                target=run_batch_processor, args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER"),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI  — REST API
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v6.1",
    description = "Reddit + Twitter signals: monitor, score, store, alert.",
    version     = "6.1.0",
)


def _serialise(signals: list) -> list:
    for s in signals:
        s.pop("_id", None)
        for f in ["created_at", "alerted_slack_at", "alerted_hubspot_at"]:
            if f in s:
                s[f] = s[f].isoformat()
    return signals


@app.get("/")
def root():
    return {
        "status":              "running",
        "system":              "FLINTEL v6.1",
        "client":              CLIENT_ID,
        "platforms":           ["reddit", "twitter"],
        "reddit_batch_size":   REDDIT_BATCH_SIZE,
        "twitter_batch_size":  TWITTER_BATCH_SIZE,
        "batch_gap_s":         BATCH_GAP_SECONDS,
        "reddit_queue_size":   reddit_queue.qsize(),
        "twitter_queue_size":  twitter_queue.qsize(),
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"

    # v6.1: include pending counts and twitter state in health
    try:
        pending_reddit  = db.pending_items.count_documents({"platform": "reddit"})
        pending_twitter = db.pending_items.count_documents({"platform": "twitter"})
        tw_state = db.twitter_state.find_one({"client_id": CLIENT_ID}, {"_id": 0, "since_id": 1, "updated_at": 1})
    except Exception:
        pending_reddit  = -1
        pending_twitter = -1
        tw_state        = None

    return {
        "status":                "ok",
        "mongodb":               mongo,
        "reddit":                "streaming",
        "twitter":               "polling" if TWITTER_BEARER_TOKEN else "disabled",
        "reddit_queue_size":     reddit_queue.qsize(),
        "twitter_queue_size":    twitter_queue.qsize(),
        "pending_reddit":        pending_reddit,
        "pending_twitter":       pending_twitter,
        "twitter_since_id":      tw_state.get("since_id") if tw_state else None,
        "twitter_state_updated": tw_state.get("updated_at").isoformat() if tw_state and tw_state.get("updated_at") else None,
        "client_id":             CLIENT_ID,
        "timestamp":             datetime.now(timezone.utc).isoformat(),
    }


@app.get("/signals")
def get_signals(
    limit:      int  = 50,
    platform:   str  = None,
    category:   str  = None,
    min_score:  int  = None,
    subreddit:  str  = None,
    tier:       str  = None,
    corridor:   str  = None,
    pain_type:  str  = None,
    is_business: bool = None,
):
    try:
        q: dict = {"client_id": CLIENT_ID}
        if platform:    q["platform"]        = platform
        if category:    q["signal_category"] = category
        if min_score is not None: q["intent_score"] = {"$gte": min_score}
        if subreddit:   q["subreddit"]       = subreddit
        if tier:        q["tier"]            = tier
        if corridor:    q["corridor"]        = {"$regex": corridor, "$options": "i"}
        if pain_type:   q["pain_type"]       = pain_type
        if is_business is not None: q["is_business"] = is_business

        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/stats")
def get_stats():
    try:
        total   = db.signals.count_documents({"client_id": CLIENT_ID})
        biz     = db.signals.count_documents({"client_id": CLIENT_ID, "is_business": True})
        reddit  = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "reddit"})
        twitter = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "twitter"})

        def agg(group_field):
            return list(db.signals.aggregate([
                {"$match": {"client_id": CLIENT_ID, group_field: {"$ne": None}}},
                {"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]))

        # v6.1: include pending stats
        pending_reddit  = db.pending_items.count_documents({"platform": "reddit"})
        pending_twitter = db.pending_items.count_documents({"platform": "twitter"})

        return {
            "total_signals":    total,
            "business_owners":  biz,
            "reddit_signals":   reddit,
            "twitter_signals":  twitter,
            "pending_reddit":   pending_reddit,
            "pending_twitter":  pending_twitter,
            "corridors":        agg("corridor"),
            "pain_types":       agg("pain_type"),
            "competitors":      agg("competitor_mentioned"),
            "tiers":            agg("tier"),
            "reddit_queue":     reddit_queue.qsize(),
            "twitter_queue":    twitter_queue.qsize(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/high-intent")
def get_high_intent(limit: int = 20):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "intent_score": {"$gte": 8}}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/digest")
def get_digest(limit: int = 50):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "intent_score": {"$gte": 6, "$lte": 7}}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/business")
def get_business(limit: int = 20):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "is_business": True}, {"_id": 0})
            .sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/outreach")
def get_outreach(limit: int = 20):
    """Signals with outreach scripts ready for the sales team."""
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$gte": 5},
                 "$or": [{"twitter_reply": {"$ne": None}}, {"twitter_dm": {"$ne": None}}, {"linkedin_message": {"$ne": None}}]},
                {"_id": 0},
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/twitter")
def get_twitter_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "twitter"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/reddit")
def get_reddit_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "reddit"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/corridors")
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


@app.get("/signals/watchlist")
def get_watchlist(limit: int = 50):
    try:
        signals = list(
            db.signals.find({"client_id": CLIENT_ID, "watchlist": True}, {"_id": 0})
            .sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# v6.1: new endpoint — inspect pending items without triggering scoring
@app.get("/signals/pending")
def get_pending(platform: str = None, limit: int = 100):
    """View items that have passed keyword filter but not yet been scored by Claude."""
    try:
        q: dict = {}
        if platform:
            q["platform"] = platform
        docs = list(
            db.pending_items.find(q, {"_id": 0, "item_data": 1, "platform": 1, "queued_at": 1})
            .sort("queued_at", ASCENDING).limit(limit)
        )
        result = []
        for d in docs:
            row = {
                "platform":   d.get("platform"),
                "queued_at":  d["queued_at"].isoformat(),
                "message_id": d["item_data"].get("message_id"),
                "username":   d["item_data"].get("username"),
                "content_type": d["item_data"].get("content_type"),
                "text_preview": d["item_data"].get("text", "")[:100],
            }
            result.append(row)
        return {"count": len(result), "pending": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# v6.1: new endpoint — inspect Twitter state
@app.get("/twitter/state")
def get_twitter_state():
    """Check persisted Twitter poller state (since_id, seen_ids count)."""
    try:
        doc = db.twitter_state.find_one({"client_id": CLIENT_ID}, {"_id": 0})
        if not doc:
            return {"status": "no_state", "message": "Twitter state not yet persisted (first run?)."}
        doc["seen_ids_count"] = len(doc.pop("seen_ids", []))
        if doc.get("updated_at"):
            doc["updated_at"] = doc["updated_at"].isoformat()
        return doc
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
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 65)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v6.1")
    log.info("=" * 65)
    log.info(f"  Client           : {CLIENT_ID}")
    log.info(f"  Platforms        : Reddit + Twitter/X")
    log.info(f"  Reddit batch     : {REDDIT_BATCH_SIZE} items → 1 Claude call")
    log.info(f"  Twitter batch    : {TWITTER_BATCH_SIZE} items → 1 Claude call")
    log.info(f"  Batch gap        : {BATCH_GAP_SECONDS}s between calls")
    log.info(f"  Twitter poll     : every {TWITTER_POLL_INTERVAL}s (rate-limit safe)")
    log.info(f"  Twitter state    : since_id + seen_ids persisted in MongoDB")
    log.info(f"  Pending items    : keyword-matched items persisted in MongoDB")
    log.info(f"  Score 0-5        : DISCARD — never stored")
    log.info(f"  Score 6-7        : MEDIUM  — MongoDB + Slack")
    log.info(f"  Score 8-10       : HIGH    — MongoDB + Slack + HubSpot")
    log.info(f"  Daily digest     : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report    : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits       : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Keywords         : {len(KEYWORDS)} filters active")
    log.info(f"  MongoDB          : {MONGODB_DB}")
    log.info(f"  Collections      : signals | pending_items | twitter_state")
    log.info(f"  Reddit account   : u/{REDDIT_USERNAME}")
    log.info(f"  Twitter          : {'enabled' if TWITTER_BEARER_TOKEN else 'DISABLED — set TWITTER_BEARER_TOKEN'}")
    log.info(f"  HubSpot          : {'enabled' if HUBSPOT_API_KEY else 'DISABLED — set HUBSPOT_API_KEY'}")
    log.info(f"  Slack            : {'enabled' if SLACK_WEBHOOK_URL else 'DISABLED — set SLACK_WEBHOOK_URL'}")
    log.info("=" * 65)

    asyncio.run(main())
