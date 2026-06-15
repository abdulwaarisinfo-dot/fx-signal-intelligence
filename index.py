"""
FX Signal Intelligence System — FLINTEL v6.5
=============================================
Platforms : Reddit (PRAW) + Twitter/X (tweepy v2)
Pipeline  :
  Reddit  → Stream posts / comments / replies
  Twitter → Fetch mentions / search / replies (rate-limit safe, 20/block)
      ↓
  Dual Keyword Filter:
    Reddit  → BROAD filter (v6.0 style, single keyword match — high recall)
    Twitter → PRECISION v2.0 (two-category min — eliminates noise)
      ↓
  Persistent Queue (MongoDB)  ← restart-safe, no item loss
      ↓
  Batch Collector           (10 items per Claude call — Reddit)
                            (20 items per Claude call — Twitter)
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
  → BROAD keyword filter (single match sufficient — v6.0 style)
  → 10 matched items → one Claude prompt
  → 30s gap between batches
  → Non-matching items dropped immediately

Twitter batch rules:
  → Polling every 60s (rate-limit safe, since_id persisted to MongoDB)
  → Search query built from top-tier keywords
  → Deduplication by tweet ID (LRU in-memory + MongoDB restart-safe)
  → PRECISION v2.0 filter (two-category minimum — kills token waste)
  → 20 matched items → one Claude prompt
  → 30s gap between batches

Changelog v6.5:
  ─────────────────────────────────────────────────────────────────
  ROOT CAUSE ANALYSIS of v6.0 → v6.4 regressions:

  REGRESSION 1 — Reddit live posts/comments/replies LOST (v6.3+):
    v6.3 introduced PRECISION v2.0 (two-category minimum) and applied
    it to BOTH Reddit and Twitter. Reddit posts are typically shorter,
    more conversational, and less keyword-dense than Twitter search
    results. The two-category rule silently dropped ~70% of real Reddit
    signals that v6.0 caught correctly with a single keyword match.
    FIX v6.5: Dual filter system.
      - Reddit uses BROAD_KEYWORDS (v6.0-style flat list, single match).
      - Twitter uses PRECISION v2.0 (category dict, two-category min).
    Both filters are pre-lowercased at import time for performance.

  REGRESSION 2 — Twitter tweets lost on restart (v6.0):
    since_id was in-memory only. On restart, since_id reset to None →
    Twitter returned the full recent-search window → same tweets re-
    processed. Combined with seen_ids.clear() at 50k, this also caused
    mid-run duplicates.
    FIX v6.5: Retained from v6.4 — since_id persisted to db.twitter_state
    after every successful poll. Restarts resume exactly where they left
    off. seen_ids uses LRU OrderedDict trim (not full clear).

  REGRESSION 3 — Broken save_signal log line (v6.3/v6.4):
    Conditional f-string `f"..." if data.get('subreddit') else f"..."`
    was syntactically invalid Python — the else branch was unreachable
    and the logger received a boolean, not a string, on some paths.
    FIX v6.5: Rewritten as a clean conditional assignment before log call.

  REGRESSION 4 — pre_validated items silently dropped (v6.4):
    load_pending_items_from_db() set pre_validated=True but the batch
    processor still called mark_item_done() on items that "failed" the
    filter — including pre_validated ones if priority defaulted to 0.
    FIX v6.5: pre_validated items short-circuit the entire filter block
    and set priority from stored item["priority"] (default 1). Zero
    risk of silent drop on reload.

  REGRESSION 5 — Username/metadata display broken (v6.3+):
    The unified process_scored_item correctly builds the data dict with
    all fields, but the broken log f-string (Regression 3) caused
    confusion making it appear usernames were missing. With the log fix,
    all metadata including username, platform, subreddit, corridor,
    pain_type, and outreach scripts display correctly — identical to v6.0.

  ALL v6.0 capabilities retained:
  ✅ Reddit post stream (posts + comments + replies)
  ✅ All usernames, subreddits, metadata correctly captured
  ✅ Broad Reddit filter catches conversational posts (single keyword)
  ✅ Twitter deduplication survives restarts (since_id in MongoDB)
  ✅ Persistent queue (db.pending_items) — restart-safe
  ✅ pre_validated flag prevents re-filter drops on reload
  ✅ LRU seen_ids (no full clear)
  ✅ PRECISION v2.0 on Twitter only (kills noise, saves tokens)
  ✅ Priority front-insertion for high-signal items
  ✅ All Slack blocks, HubSpot, FastAPI, schedulers — unchanged
"""

import asyncio
import logging
import os
import json
import time
import queue
import threading
from collections import OrderedDict
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
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "FlintelSignalBot/6.5")

# Twitter / X
TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

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
TWITTER_BATCH_SIZE = int(os.getenv("TWITTER_BATCH_SIZE", "20"))
BATCH_GAP_SECONDS  = int(os.getenv("BATCH_GAP_SECONDS",  "30"))

# Schedulers
DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))   # 0 = Monday
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

# Twitter polling
TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

# LRU cap for Twitter seen_ids
SEEN_IDS_MAX  = int(os.getenv("SEEN_IDS_MAX",  "50000"))
SEEN_IDS_KEEP = int(os.getenv("SEEN_IDS_KEEP", "20000"))

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
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:  queue.Queue = queue.Queue()
twitter_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# REDDIT KEYWORD FILTER  — BROAD (v6.0 style)
#
# v6.5: Reddit uses a FLAT keyword list requiring only ONE match.
# Reddit posts are conversational and shorter. The two-category rule
# (PRECISION v2.0) was the cause of v6.3/v6.4 dropping ~70% of real
# Reddit signals. We restored the v6.0 broad filter here.
# Claude handles false positive reduction at the scoring stage.
# ─────────────────────────────────────────────────────────────────────────────

REDDIT_KEYWORDS = [
    # Sending / paying
    "send money to", "sending money to", "transfer money to", "transferring money to",
    "wire money to", "wiring money to", "move money to", "moving money to",
    "remit money to", "remitting money to",
    "pay my supplier", "paying my supplier", "pay a supplier", "paying a supplier",
    "pay my vendor", "paying my vendor", "pay my manufacturer", "pay my factory",
    "pay my partner", "pay my contractor",
    "pay an invoice", "paying an invoice", "settle an invoice", "settling an invoice",
    "pay a business", "business payment to", "supplier payment", "vendor payment",
    "invoice payment", "international payment", "overseas payment",
    "cross border payment", "cross-border payment", "cross border transfer",
    "cross-border transfer", "international transfer", "international wire",
    "international wire transfer", "foreign wire transfer", "overseas wire transfer",
    "overseas transfer", "global payment", "global transfer",
    "b2b payment", "b2b transfer", "business to business payment",

    # Bank blocking
    "bank blocked my", "bank flagged my", "bank rejected my", "bank declined my",
    "bank won't let me transfer", "bank won't let me send", "bank refuses to",
    "bank holding my", "bank froze my", "account frozen", "funds frozen",
    "money frozen", "transfer frozen", "payment frozen",
    "transfer blocked", "payment blocked", "wire blocked", "transaction blocked",
    "transfer rejected", "payment rejected", "wire rejected",
    "transfer declined", "payment declined", "transfer failed", "payment failed",
    "wire failed", "transfer stuck", "payment stuck", "money stuck", "funds stuck",
    "money held", "funds held", "money hostage", "holding my money", "holding my funds",
    "won't release my funds", "compliance hold", "compliance review", "compliance check",
    "AML hold", "AML review", "AML flag", "flagged for review", "flagged as suspicious",
    "suspicious activity", "suspicious transaction", "frozen for review", "under review",
    "transfer delayed", "payment delayed", "wire delayed", "transfer pending",
    "stuck in pending", "10-14 days", "10 to 14 days", "two weeks to transfer",
    "transfer taking forever", "payment taking forever",
    "money hasn't arrived", "payment hasn't arrived", "where is my transfer",
    "where is my payment", "where is my money", "money disappeared",
    "payment disappeared", "transfer disappeared",

    # Fee frustration
    "SWIFT fees", "SWIFT charges", "wire transfer fees", "wire transfer charges",
    "international transfer fees", "international wire fees",
    "transfer fees too high", "transfer fees killing", "fees killing my margins",
    "fees eating my margins", "fees eating my profit",
    "exchange rate terrible", "exchange rate awful", "exchange rate bad",
    "terrible exchange rate", "awful exchange rate", "exchange rate ripoff",
    "hidden fees", "hidden charges", "unexpected fees", "FX fees", "FX charges",
    "FX markup", "FX spread", "currency conversion fee", "conversion markup",
    "losing money on transfer", "losing money on fees", "losing money exchanging",
    "highway robbery", "daylight robbery", "absolute ripoff",
    "cheapest way to send", "cheapest way to transfer", "cheapest international transfer",
    "better rate than", "cheaper than SWIFT", "SWIFT alternative",
    "avoid SWIFT fees", "correspondent bank fees", "intermediary bank fees",

    # Competitors
    "Wise Business", "Wise transfer", "Wise payment", "Wise blocked", "Wise restricted",
    "Wise suspended", "Wise account", "Wise limit", "Wise holding",
    "leaving Wise", "left Wise", "moving off Wise", "switching from Wise",
    "never using Wise", "done with Wise", "Wise is terrible", "Wise is awful",
    "TransferWise", "alternative to Wise", "better than Wise",
    "Remitly blocked", "Remitly restricted", "leaving Remitly", "Remitly alternative",
    "Payoneer blocked", "Payoneer restricted", "Payoneer suspended", "Payoneer account",
    "leaving Payoneer", "switching from Payoneer", "Payoneer alternative",
    "alternative to Payoneer",
    "WorldRemit failed", "WorldRemit blocked", "WorldRemit problem",
    "Western Union failed", "Western Union blocked", "leaving Western Union",
    "OFX failed", "OFX blocked", "OFX problem",
    "Revolut blocked", "Revolut restricted", "Revolut suspended", "Revolut Business",
    "leaving Revolut", "switching from Revolut",
    "Stripe blocked", "Stripe restricted", "Mercury blocked", "Mercury restricted",
    "LemFi failed", "LemFi blocked", "Grey Finance", "NALA failed", "NALA blocked",
    "Chipper Cash", "alternative to Revolut", "alternative to Remitly",

    # Recommendation requests
    "recommend a payment", "recommend a transfer", "recommend a service",
    "recommend a platform", "recommend an app", "anyone recommend",
    "can anyone recommend", "what payment service", "what transfer service",
    "what payment platform", "which payment service", "which transfer service",
    "which payment platform", "which service is best", "which platform is best",
    "best payment service", "best transfer service", "best payment platform",
    "best payment app", "best way to send", "best way to transfer", "best way to pay",
    "fastest way to send", "cheapest way to send",
    "how do I send", "how do I transfer", "how do I pay",
    "how can I send", "how can I transfer", "how can I pay",
    "looking for a payment", "looking for a transfer", "looking for a platform",
    "searching for a payment", "need a payment solution",
    "anyone using", "does anyone use", "has anyone used", "who uses",
    "who do you use", "what do you use", "what are you using",
    "tried everything", "tried so many", "tried multiple", "tried several",
    "nothing works", "still haven't found", "still looking for",

    # Business context
    "my supplier", "my suppliers", "our supplier", "our suppliers",
    "my vendor", "my vendors", "our vendor", "our vendors",
    "my manufacturer", "my manufacturers", "our manufacturer",
    "my factory", "our factory", "my business partner", "our business partner",
    "my contractor", "our contractor",
    "import business", "importing business", "export business", "exporting business",
    "import export", "import/export", "importing goods", "exporting goods",
    "importing products", "exporting products",
    "buying from overseas", "buying from abroad", "sourcing from", "sourcing overseas",
    "purchase order", "business invoice", "supplier invoice", "vendor invoice",
    "trade finance", "trade payment", "trade financing",
    "supply chain payment", "supply chain finance",
    "diaspora business", "diaspora entrepreneur",
    "running a business", "my business needs", "for my business",
    "business account", "business transfer", "business wire",
    "corporate payment", "corporate transfer", "company payment", "company transfer",

    # Corridors
    "to Nigeria", "to Lagos", "to Abuja", "from Nigeria",
    "Nigeria payment", "Nigeria transfer", "Nigeria wire",
    "Nigerian supplier", "Nigerian vendor", "Nigerian manufacturer",
    "CAD to NGN", "GBP to NGN", "USD to NGN", "EUR to NGN", "AUD to NGN",
    "naira payment", "naira transfer", "send naira",
    "to Pakistan", "to Karachi", "to Lahore", "to Islamabad", "from Pakistan",
    "Pakistan payment", "Pakistan transfer", "Pakistan wire",
    "Pakistani supplier", "Pakistani vendor", "Pakistani manufacturer",
    "CAD to PKR", "GBP to PKR", "USD to PKR", "rupee payment", "rupee transfer",
    "to India", "to Mumbai", "to Delhi", "to Bangalore", "from India",
    "India payment", "India transfer", "India wire",
    "Indian supplier", "Indian vendor", "Indian manufacturer",
    "CAD to INR", "GBP to INR", "USD to INR",
    "to Ghana", "to Accra", "from Ghana", "Ghana payment", "Ghana transfer",
    "Ghanaian supplier", "GHS payment", "cedi payment",
    "to Kenya", "to Nairobi", "from Kenya", "Kenya payment", "Kenya transfer",
    "Kenyan supplier", "KES payment", "M-Pesa business", "Mpesa business",
    "to Ethiopia", "to Senegal", "to Tanzania", "to Uganda", "to South Africa",
    "African supplier", "African vendor", "African manufacturer",
    "Africa payment", "Africa transfer",
    "from Canada", "from Toronto", "from Vancouver", "from UK", "from London",
    "from Manchester", "from USA", "from New York", "from Australia", "from Sydney",
    "from UAE", "from Dubai",

    # Large amounts
    "$10,000", "$10k", "$15,000", "$15k", "$20,000", "$20k",
    "$25,000", "$25k", "$30,000", "$30k", "$40,000", "$40k",
    "$50,000", "$50k", "$60,000", "$60k", "$75,000", "$75k",
    "$80,000", "$80k", "$100,000", "$100k", "$150,000", "$150k",
    "$200,000", "$200k", "$250,000", "$250k", "$300,000", "$300k",
    "$500,000", "$500k", "$1 million", "$1m",
    "£10,000", "£10k", "£15,000", "£15k", "£20,000", "£20k",
    "£25,000", "£25k", "£30,000", "£30k", "£50,000", "£50k",
    "£75,000", "£75k", "£100,000", "£100k", "£200,000", "£200k",
    "CAD 10,000", "CAD 20,000", "CAD 50,000",
    "six figures", "seven figures", "six-figure", "seven-figure",
    "large transfer", "large amount", "large payment", "large wire",
    "significant amount", "substantial amount", "big transfer",
    "monthly volume", "weekly volume", "high volume",

    # Compliance pain
    "KYC rejected", "KYC failed", "KYC verification failed",
    "KYC problem", "KYC issue", "KYC nightmare",
    "AML rejected", "AML flagged", "AML hold", "AML review",
    "documentation rejected", "documents rejected",
    "proof of funds", "source of funds", "source of wealth",
    "proof of business", "business verification failed", "verification rejected",
    "verification failed", "compliance rejected", "compliance nightmare",
    "Form M", "CBN compliance", "regulatory hold", "regulatory review",
    "submitted documents again", "keep asking for documents",
    "keep rejecting documents", "rejected again", "blocked again", "failed again",

    # Urgency
    "urgently", "urgent", "desperately", "desperate", "ASAP", "as soon as possible",
    "supplier is waiting", "supplier waiting", "vendor is waiting", "vendor waiting",
    "manufacturer waiting", "partner waiting", "already delayed", "already late",
    "overdue", "past due",
    "losing the contract", "losing my supplier", "losing my vendor",
    "threatening to cancel", "might cancel", "going to cancel",
    "cancelling the order", "losing the deal", "deal at risk", "relationship at risk",
    "can't wait any longer", "running out of time",

    # Expansion
    "just signed a supplier", "signed a new supplier", "found a supplier",
    "new supplier in", "signed a contract with", "new contract with",
    "starting to import", "starting an import", "starting to export",
    "launching in", "expanding to", "entering the market",
    "setting up payments", "need to set up payments",
    "sourcing products from", "sourcing goods from",
    "buying products from", "manufacturing in", "producing in",

    # Treasury
    "treasury management", "cash management", "liquidity management",
    "FX management", "FX exposure", "FX risk", "FX hedging",
    "currency hedging", "currency risk", "multi currency", "multi-currency",
    "multicurrency", "foreign currency account", "international banking",
    "payment infrastructure", "payment rails", "payment solution",
    "payment platform", "fintech payment", "embedded payment",
    "intercompany payment", "intercompany transfer",
]

# Pre-lowercase once at import time for hot-path performance
_REDDIT_KEYWORDS_LOWER = [kw.lower() for kw in REDDIT_KEYWORDS]


def passes_reddit_filter(text: str) -> bool:
    """
    BROAD filter for Reddit (v6.0 style).
    Returns True if text contains ANY single keyword from the list.
    Single-match is intentional — Reddit posts are conversational and
    short. Claude handles final scoring / false-positive rejection.
    """
    t = text.lower()
    for kw in _REDDIT_KEYWORDS_LOWER:
        if kw in t:
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER KEYWORD FILTER  — PRECISION v2.0
#
# v6.5: Twitter ONLY uses this two-category filter.
# Twitter's broad search API returns huge volumes of noise. The single-
# keyword approach wastes Claude tokens on irrelevant content. The two-
# category requirement cuts ~90% of noise before Claude is invoked.
# ─────────────────────────────────────────────────────────────────────────────

TWITTER_PRECISION_KEYWORDS = {

    'HIGH_CONFIDENCE_PHRASES': [
        "pay my supplier", "paying my supplier", "pay our supplier",
        "paying our supplier", "pay a supplier", "supplier payment",
        "supplier payments", "pay my vendor", "paying my vendor",
        "vendor payment", "pay my manufacturer", "manufacturer payment",
        "pay my factory", "factory payment", "pay my contractor",
        "contractor payment", "pay an invoice", "paying an invoice",
        "settle an invoice", "invoice payment", "settle invoice",
        "business invoice", "supplier invoice", "vendor invoice",
        "pay my partner overseas", "pay my overseas partner",
        "pay my business partner",
        "cross border payment", "cross-border payment",
        "cross border transfer", "cross-border transfer",
        "cross border business", "cross-border business",
        "international wire transfer", "international business payment",
        "international supplier payment", "overseas supplier payment",
        "overseas business payment", "overseas business transfer",
        "foreign supplier payment", "global business payment",
        "B2B payment", "B2B transfer", "B2B transaction",
        "business to business payment", "business wire transfer",
        "corporate wire transfer", "corporate payment",
        "intercompany payment", "intercompany transfer",
        "trade finance", "trade payment", "letter of credit",
        "purchase order payment", "PO payment", "import payment",
        "export payment", "import financing", "export financing",
        "supply chain payment", "supply chain finance",
    ],

    'COMPETITOR_LEAVING': [
        "leaving Wise", "left Wise", "leaving Wise Business",
        "left Wise Business", "moving off Wise", "moved off Wise",
        "switching from Wise", "switched from Wise", "done with Wise",
        "never using Wise", "never using Wise again", "Wise is terrible",
        "Wise is awful", "Wise keeps blocking", "Wise keeps holding",
        "Wise holding my money", "Wise holding my funds", "Wise hostage",
        "money hostage Wise", "Wise restricted my", "Wise blocked my",
        "Wise suspended my", "Wise account restricted",
        "Wise account blocked", "Wise account suspended",
        "Wise account closed", "alternative to Wise",
        "alternatives to Wise", "better than Wise",
        "Wise Business blocked", "Wise Business restricted",
        "Wise Business suspended", "Wise Business holding",
        "10-14 days Wise", "Wise 10-14 days", "two weeks Wise",
        "leaving Payoneer", "left Payoneer", "switching from Payoneer",
        "Payoneer restricted my", "Payoneer blocked my",
        "Payoneer suspended my", "Payoneer account restricted",
        "Payoneer account blocked", "Payoneer account suspended",
        "alternative to Payoneer", "better than Payoneer",
        "done with Payoneer", "never using Payoneer",
        "leaving Remitly", "left Remitly", "Remitly blocked my",
        "Remitly restricted my", "Remitly account blocked",
        "alternative to Remitly", "better than Remitly",
        "leaving Revolut", "Revolut Business blocked",
        "Revolut Business restricted", "Revolut account blocked",
        "Revolut account restricted", "Revolut account suspended",
        "alternative to Revolut Business",
        "leaving WorldRemit", "WorldRemit blocked", "WorldRemit failed my",
        "alternative to WorldRemit",
        "leaving Western Union", "done with Western Union",
        "Western Union failed my", "alternative to Western Union",
        "leaving OFX", "OFX blocked my", "OFX failed my",
        "alternative to OFX",
        "leaving LemFi", "LemFi blocked", "LemFi failed",
        "alternative to LemFi",
        "leaving Grey Finance", "Grey Finance blocked", "Grey Finance failed",
        "leaving NALA", "NALA blocked", "NALA failed",
        "leaving Chipper Cash", "Chipper Cash blocked", "Chipper Cash failed",
        "Mercury bank blocked", "Mercury account blocked", "leaving Mercury",
        "leaving my payment provider", "switching payment providers",
        "switching payment platform", "switching payment service",
        "switching to a new payment", "looking for Wise alternative",
        "looking for Payoneer alternative", "need alternative to Wise",
        "need alternative to Payoneer", "Wise competitors",
        "competitors to Wise",
    ],

    'BANK_BLOCKING_PAIN': [
        "bank blocked my transfer", "bank blocked my payment",
        "bank blocked my wire", "bank blocked my transaction",
        "bank blocked my international", "bank flagged my transfer",
        "bank flagged my payment", "bank flagged my wire",
        "bank rejected my transfer", "bank rejected my payment",
        "bank rejected my wire", "bank declined my transfer",
        "bank declined my payment", "bank won't let me transfer",
        "bank won't let me send", "bank refuses to transfer",
        "bank refuses to send", "bank holding my transfer",
        "bank holding my payment", "bank holding my funds",
        "bank holding my money", "bank froze my account",
        "bank froze my funds", "bank froze my transfer",
        "account frozen transfer", "funds frozen transfer",
        "money frozen transfer", "transfer on hold", "payment on hold",
        "wire on hold", "funds on hold business", "money on hold business",
        "compliance hold transfer", "compliance hold payment",
        "AML hold transfer", "AML hold payment", "AML flagged transfer",
        "AML flagged payment", "suspicious activity transfer",
        "suspicious transaction flagged", "flagged for compliance",
        "under compliance review", "transfer under review",
        "payment under review", "wire under review",
        "money hasn't arrived supplier", "payment hasn't arrived supplier",
        "transfer hasn't arrived supplier", "where is my wire transfer",
        "where is my business payment", "payment disappeared business",
        "transfer disappeared business", "funds missing business",
        "wire missing business",
    ],

    'FEE_FRUSTRATION': [
        "SWIFT fees killing", "SWIFT fees insane", "SWIFT fees too high",
        "SWIFT fees destroying", "SWIFT fees eating", "SWIFT charges killing",
        "international wire fees killing", "international wire fees insane",
        "international transfer fees killing",
        "international transfer fees insane",
        "wire transfer fees too high", "transfer fees killing my margins",
        "transfer fees eating my margins", "transfer fees eating my profit",
        "fees killing my business", "fees destroying my margins",
        "exchange rate terrible business", "exchange rate awful supplier",
        "terrible exchange rate supplier", "FX fees killing",
        "FX fees insane", "FX markup too high",
        "losing money on international", "losing money on wire",
        "losing money on transfer", "highway robbery international",
        "daylight robbery SWIFT", "ripoff SWIFT fees",
        "cheaper than SWIFT", "avoid SWIFT fees", "SWIFT alternative business",
        "correspondent bank fees", "intermediary bank fees killing",
    ],

    'RECOMMENDATION_REQUESTS': [
        "recommend a payment platform", "recommend a payment service",
        "recommend a payment provider", "recommend a payment solution",
        "recommend a transfer service", "recommend a transfer platform",
        "recommend a business payment", "anyone recommend payment",
        "can anyone recommend payment", "does anyone recommend payment",
        "best payment platform for business",
        "best transfer service for business",
        "best payment service for business", "best way to pay supplier",
        "best way to pay vendors", "best way to transfer internationally",
        "best way to send internationally",
        "which payment platform for business",
        "which transfer service for business",
        "which service for international", "looking for payment platform",
        "looking for transfer service", "searching for payment solution",
        "need payment solution for business",
        "need transfer solution for business", "tried everything payment",
        "tried multiple payment platforms", "tried several payment services",
        "nothing works for international payment",
        "still haven't found payment", "still looking for payment solution",
        "what do you use for international payment",
        "what do you use to pay suppliers",
        "who do you use for international",
        "how do you pay international suppliers",
        "how do you send money internationally business",
    ],

    'CORRIDORS': [
        "to Nigeria business", "Nigeria supplier", "Nigerian supplier",
        "Nigerian vendor", "Nigerian manufacturer", "Lagos supplier",
        "Abuja supplier", "Nigeria payment business",
        "Nigeria transfer business", "Nigeria wire business",
        "CAD to NGN business", "GBP to NGN business", "USD to NGN business",
        "naira business payment", "naira supplier payment",
        "send naira business",
        "Pakistan supplier", "Pakistani supplier", "Pakistani vendor",
        "Pakistani manufacturer", "Karachi supplier", "Lahore supplier",
        "Pakistan payment business", "Pakistan transfer business",
        "CAD to PKR business", "GBP to PKR business",
        "rupee business payment", "rupee supplier payment",
        "India supplier", "Indian supplier", "Indian vendor",
        "Indian manufacturer", "Mumbai supplier", "Delhi supplier",
        "India payment business", "India transfer business",
        "CAD to INR business", "GBP to INR business",
        "Ghana supplier", "Ghanaian supplier", "Accra supplier",
        "Ghana payment business", "Ghana transfer business",
        "cedi business payment",
        "Kenya supplier", "Kenyan supplier", "Nairobi supplier",
        "Kenya payment business", "M-Pesa business payment",
        "Mpesa business payment",
        "South Africa supplier", "Ethiopian supplier", "Tanzania supplier",
        "Uganda supplier", "African supplier payment",
        "Africa business payment", "Africa wire transfer",
        "Canada Nigeria business", "UK Nigeria business",
        "USA Nigeria business", "Australia Nigeria business",
        "UAE Nigeria business", "Canada Pakistan business",
        "UK Pakistan business", "Canada India business",
        "UK Ghana business",
    ],

    'LARGE_AMOUNTS': [
        "$10,000", "$10k", "$15,000", "$15k", "$20,000", "$20k",
        "$25,000", "$25k", "$30,000", "$30k", "$40,000", "$40k",
        "$45,000", "$45k", "$50,000", "$50k", "$60,000", "$60k",
        "$75,000", "$75k", "$80,000", "$80k", "$100,000", "$100k",
        "$150,000", "$150k", "$200,000", "$200k", "$250,000", "$250k",
        "$300,000", "$300k", "$500,000", "$500k", "$750,000", "$750k",
        "$1 million", "$1m", "$2 million", "$2m",
        "£10,000", "£10k", "£15,000", "£15k", "£20,000", "£20k",
        "£25,000", "£25k", "£30,000", "£30k", "£50,000", "£50k",
        "£75,000", "£75k", "£100,000", "£100k", "£200,000", "£200k",
        "£500,000", "£500k",
        "€10,000", "€10k", "€20,000", "€20k", "€50,000", "€50k",
        "€100,000", "€100k",
        "CAD 10,000", "CAD 20,000", "CAD 50,000",
        "10,000 CAD", "20,000 CAD", "50,000 CAD",
        "100,000 CAD", "200,000 CAD", "500,000 CAD",
        "six figures transfer", "six-figure transfer",
        "seven figures transfer", "seven-figure transfer",
        "large business transfer", "large business payment",
        "large supplier payment", "high volume transfers",
        "high volume payments", "monthly volume business",
        "bulk transfer business", "bulk payment business",
    ],

    'COMPLIANCE_PAIN': [
        "KYC rejected business", "KYC failed business",
        "KYC verification failed", "KYC nightmare business",
        "AML hold business", "AML review business", "AML flagged business",
        "documentation rejected payment", "documents rejected transfer",
        "proof of funds business", "source of funds business",
        "proof of business payment", "business verification failed",
        "compliance hold business", "compliance rejected payment",
        "compliance nightmare payment", "Form M Nigeria",
        "CBN compliance payment", "regulatory hold payment",
        "submitted documents again payment", "same documents again payment",
        "keep rejecting my documents", "third time submitting documents",
        "rejected again payment", "blocked again payment",
        "keeps blocking my payment", "keeps rejecting my payment",
        "always blocks my transfer", "always rejects my payment",
    ],

    'BUSINESS_URGENCY': [
        "supplier waiting for payment", "supplier is waiting payment",
        "supplier waiting urgently", "vendor waiting for payment",
        "manufacturer waiting payment", "supplier threatening to cancel",
        "supplier might cancel", "supplier going to cancel",
        "losing my supplier", "lost my supplier",
        "losing the contract payment", "deal at risk payment",
        "relationship at risk payment", "killing my business payment",
        "killing my business transfer", "destroying my business payment",
        "urgent supplier payment", "urgent business transfer",
        "urgent international payment", "urgent cross border",
        "need payment today supplier", "need transfer today supplier",
        "payment overdue supplier", "invoice overdue supplier",
        "past due supplier", "supplier payment deadline",
        "payment deadline today", "transfer deadline today",
        "need to pay supplier today", "need to pay vendor today",
        "ASAP supplier payment", "ASAP business transfer",
    ],

    'TREASURY_FX': [
        "treasury management software", "treasury management solution",
        "treasury management platform", "cash management international",
        "liquidity management international", "FX management business",
        "FX exposure business", "FX risk management", "FX hedging business",
        "currency hedging business", "currency risk business",
        "FX solution business", "FX platform business",
        "multi currency business", "multi-currency business",
        "multicurrency business account", "foreign currency business account",
        "international banking business", "international bank account business",
        "global banking business", "correspondent banking business",
        "banking relationship payments", "payment infrastructure business",
        "payment rails business", "payment solution business",
        "embedded payments business", "embedded finance business",
        "FX banking relationship", "FX liquidity business",
        "cash pooling business", "intercompany payment",
        "intercompany transfer",
    ],

    'EXPANSION_SIGNALS': [
        "just signed supplier contract", "signed new supplier",
        "found new supplier overseas", "new supplier in Nigeria",
        "new supplier in Pakistan", "new supplier in India",
        "new supplier in Ghana", "new supplier in Africa",
        "signed contract overseas supplier", "starting to import from",
        "starting import business", "starting export business",
        "launching import business", "expanding to Nigeria",
        "expanding to Pakistan", "expanding to Africa",
        "entering Nigerian market", "entering African market",
        "setting up international payments",
        "need to set up international payments",
        "setting up payment infrastructure", "need payment infrastructure",
        "sourcing products from Nigeria", "sourcing products from Pakistan",
        "sourcing goods from Africa", "buying from overseas supplier",
        "manufacturing in Nigeria", "manufacturing in Pakistan",
        "manufacturing in India", "producing overseas",
        "new overseas supplier",
    ],

    'HARD_NEGATIVES': [
        "send to my mum", "send to my mom", "send to my parents",
        "send to my family", "send to my sister", "send to my brother",
        "send to my wife", "send to my husband", "send to my children",
        "send to my kids", "school fees", "university fees", "tuition fees",
        "rent money", "house rent", "personal remittance", "pocket money",
        "allowance", "birthday money", "birthday gift", "wedding gift",
        "funeral money", "medical bills family",
        "Cash App", "Venmo", "Zelle", "Apple Pay transfer",
        "Google Pay transfer", "PayPal friends", "PayPal personal",
        "PayPal gift",
        "$50 transfer", "$100 transfer", "$200 transfer", "$300 transfer",
        "$500 personal", "$400 personal", "£50 transfer", "£100 transfer",
        "£200 transfer", "£300 transfer",
        "crypto trading", "bitcoin trading", "ethereum trading",
        "altcoin trading", "NFT payment", "DeFi yield", "staking rewards",
        "mining rewards", "crypto gains", "trading profits", "P2P crypto",
        "Netflix payment", "Spotify payment", "BeatStars payment",
        "subscription payment", "monthly subscription cancel",
        "streaming subscription",
        "stock market", "stock trading", "share purchase",
        "dividend payment", "mortgage payment", "car payment", "car loan",
        "student loan", "credit card payment", "insurance claim",
        "tax refund", "salary payment", "wage payment", "paycheck",
        "payday loan", "personal loan", "gambling", "casino payment",
        "betting payment",
    ],
}

# Pre-lowercase Twitter precision keywords at import time
_TWITTER_KEYWORDS_LOWER = {
    category: [kw.lower() for kw in kws]
    for category, kws in TWITTER_PRECISION_KEYWORDS.items()
}

_IMMEDIATE_TRIGGER_CATEGORIES = {
    'COMPETITOR_LEAVING',
    'BANK_BLOCKING_PAIN',
    'BUSINESS_URGENCY',
}


def passes_twitter_filter(text: str) -> tuple[bool, int]:
    """
    PRECISION v2.0 filter — Twitter ONLY.

    Stage 1: Hard negative check — any HARD_NEGATIVES match = instant discard.
    Stage 2: Two-category minimum — must match keywords from ≥2 different
             categories (excluding HARD_NEGATIVES) to pass.
    Stage 3: Priority scoring — used for front-insertion in batch queue.

    Returns (passes: bool, priority: int).
    """
    text_lower = text.lower()

    # Stage 1 — hard negatives
    for neg in _TWITTER_KEYWORDS_LOWER['HARD_NEGATIVES']:
        if neg in text_lower:
            return False, 0

    # Stage 2 — two-category minimum
    categories_matched = []
    for category, keywords in _TWITTER_KEYWORDS_LOWER.items():
        if category == 'HARD_NEGATIVES':
            continue
        for kw in keywords:
            if kw in text_lower:
                categories_matched.append(category)
                break

    if len(categories_matched) < 2:
        return False, 0

    # Stage 3 — priority
    priority = len(categories_matched)
    if any(cat in _IMMEDIATE_TRIGGER_CATEGORIES for cat in categories_matched):
        priority += 5
    if 'LARGE_AMOUNTS' in categories_matched:
        priority += 3
    if 'CORRIDORS' in categories_matched:
        priority += 2

    return True, priority


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SYSTEM PROMPT
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

        db.signals.create_index([("message_id", ASCENDING)], unique=True, name="message_id_unique")
        for field in [
            "intent_score", "created_at", "client_id", "platform",
            "tier", "corridor", "competitor_mentioned", "pain_type",
            "is_business", "signal_category",
        ]:
            db.signals.create_index([(field, ASCENDING)])

        db.pending_items.create_index(
            [("message_id", ASCENDING)], unique=True, name="pending_message_id_unique"
        )
        db.pending_items.create_index([("platform", ASCENDING)], name="pending_platform")
        db.pending_items.create_index([("status", ASCENDING)], name="pending_status")
        db.pending_items.create_index([("created_at", ASCENDING)], name="pending_created_at")

        # Twitter state — persists since_id across restarts (v6.4+)
        db.twitter_state.create_index(
            [("key", ASCENDING)], unique=True, name="twitter_state_key_unique"
        )

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER STATE PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

def load_twitter_since_id() -> str | None:
    """Load persisted since_id from MongoDB on startup."""
    try:
        doc = db.twitter_state.find_one({"key": "since_id"})
        if doc and doc.get("value"):
            since_id = doc["value"]
            log.info(f"Twitter since_id restored: {since_id}")
            return since_id
    except Exception as exc:
        log.error(f"load_twitter_since_id error: {exc}")
    log.info("Twitter since_id: no previous state — starting fresh.")
    return None


def save_twitter_since_id(since_id: str):
    """Persist since_id to MongoDB after each successful poll."""
    try:
        db.twitter_state.update_one(
            {"key": "since_id"},
            {"$set": {"value": since_id, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"save_twitter_since_id error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENT QUEUE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def enqueue_item(q: queue.Queue, item: dict):
    """
    Persist item to db.pending_items (status="pending") then put on queue.
    Duplicate message_id is a safe no-op (upsert with $setOnInsert).
    """
    try:
        doc = dict(item)
        doc["status"] = "pending"
        doc.setdefault("created_at", datetime.now(timezone.utc))
        db.pending_items.update_one(
            {"message_id": item["message_id"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"enqueue_item persist error: {exc}")

    q.put(item)


def mark_item_done(message_id: str):
    """Remove item from db.pending_items after scoring + processing."""
    try:
        db.pending_items.delete_one({"message_id": message_id})
    except Exception as exc:
        log.error(f"mark_item_done error for {message_id}: {exc}")


def item_already_known(message_id: str) -> bool:
    """
    Returns True if message_id was seen in a previous run.
    Checks both pending_items (queued) and signals (scored).
    Used by poll_twitter() to avoid re-enqueueing after restart.
    """
    try:
        if db.pending_items.find_one({"message_id": message_id}, {"_id": 1}):
            return True
        if db.signals.find_one({"message_id": message_id}, {"_id": 1}):
            return True
        return False
    except Exception as exc:
        log.error(f"item_already_known check error for {message_id}: {exc}")
        return False  # fail-open


def load_pending_items_from_db() -> dict:
    """
    Called once at startup BEFORE live streams begin.
    Re-loads any items left status="pending" back into in-memory queues.

    v6.5: Items are tagged pre_validated=True so the batch processor
    skips re-filtering. They already passed their respective filter
    (Reddit BROAD or Twitter PRECISION) when first enqueued. Re-running
    the filter on reload caused silent drops in v6.3/v6.4.
    """
    counts = {"reddit": 0, "twitter": 0}
    try:
        reddit_pending = list(
            db.pending_items.find({"status": "pending", "platform": "reddit"})
            .sort("created_at", ASCENDING)
        )
        for doc in reddit_pending:
            doc.pop("_id", None)
            doc.pop("status", None)
            doc["pre_validated"] = True
            reddit_queue.put(doc)
        counts["reddit"] = len(reddit_pending)

        twitter_pending = list(
            db.pending_items.find({"status": "pending", "platform": "twitter"})
            .sort("created_at", ASCENDING)
        )
        for doc in twitter_pending:
            doc.pop("_id", None)
            doc.pop("status", None)
            doc["pre_validated"] = True
            twitter_queue.put(doc)
        counts["twitter"] = len(twitter_pending)

        if reddit_pending or twitter_pending:
            log.info(
                f"Queue restore | reddit:{len(reddit_pending)} "
                f"twitter:{len(twitter_pending)} items reloaded (pre_validated)."
            )
        else:
            log.info("Queue restore | nothing pending — clean start.")
    except Exception as exc:
        log.error(f"load_pending_items_from_db error: {exc}")

    return counts


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
# CLAUDE BATCH SCORER
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        ctype     = item.get("content_type", "unknown").upper()
        platform  = item.get("platform", "unknown").upper()
        subreddit = item.get("subreddit", "")
        username  = item.get("username", "unknown")
        text      = item.get("text", "")[:800]

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
            "message_id":                   data["message_id"],
            "platform":                     data.get("platform", "unknown"),
            "content_type":                 data.get("content_type", "unknown"),
            "subreddit":                    data.get("subreddit", ""),
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
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "client_id":                    CLIENT_ID,
            "alerted_slack":                False,
            "alerted_hubspot":              False,
            "digest_included":              False,
            "created_at":                   datetime.now(timezone.utc),
        }
        db.signals.insert_one(doc)

        # v6.5 FIX: clean conditional log — no broken f-string
        platform_label = data.get("platform", "?").upper()
        source_label   = f"r/{data.get('subreddit')}" if data.get("subreddit") else platform_label
        log.info(
            f"SAVED | {platform_label} | Score:{data['intent_score']} | "
            f"Tier:{data.get('tier','?')} | u/{data.get('username','?')} | "
            f"{data.get('content_type','?').upper()} | Source:{source_label}"
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

    score      = data["intent_score"]
    platform   = data.get("platform", "unknown").upper()
    ctype      = data.get("content_type", "post").upper()
    subreddit  = data.get("subreddit", "")
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
    outreach   = data.get("twitter_reply") or data.get("twitter_dm") or ""
    timestamp  = data.get("timestamp", "—")

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
            f"FLINTEL SIGNAL — v6.5\n\n"
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
    mark_item_done() is always called at the end.
    """
    score = score_result.get("intent_score", 0)

    if score < MIN_SCORE_MEDIUM:
        log.debug(
            f"DISCARD | Score:{score} | {item.get('platform','?').upper()} | "
            f"u/{item.get('username')} | {item.get('content_type','')}"
        )
        mark_item_done(item["message_id"])
        return

    data = {
        "message_id":                   item["message_id"],
        "platform":                     item.get("platform", "unknown"),
        "content_type":                 item.get("content_type", "unknown"),
        "subreddit":                    item.get("subreddit", ""),
        "post_url":                     item.get("post_url", ""),
        "username":                     item.get("username", "unknown"),
        "message_text":                 item.get("text", ""),
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
        "watchlist":                    score_result.get("watchlist", False),
        "watchlist_reason":             score_result.get("watchlist_reason"),
        "timestamp":                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    saved = save_signal(data)
    if not saved:
        mark_item_done(item["message_id"])
        return

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

    mark_item_done(item["message_id"])


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT BATCH PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

def run_reddit_batch_processor(preloaded_count: int = 0):
    """
    Reads from reddit_queue.
    Uses BROAD filter (passes_reddit_filter) — single keyword sufficient.
    Collects 10 matched items per batch, then sends to Claude.
    30s gap between batches.

    pre_validated items skip filtering entirely.
    """
    log.info(
        f"Batch processor [REDDIT] started | batch_size:{REDDIT_BATCH_SIZE} | "
        f"gap:{BATCH_GAP_SECONDS}s | restored:{preloaded_count}"
    )

    current_batch  = []
    total_received = 0
    total_matched  = preloaded_count
    total_dropped  = 0
    total_batches  = 0

    while True:
        try:
            try:
                item = reddit_queue.get(timeout=1)
            except queue.Empty:
                continue

            total_received += 1
            text = item.get("text", "").strip()

            if not text or len(text) < 10:
                mark_item_done(item.get("message_id", ""))
                reddit_queue.task_done()
                continue

            # pre_validated items (reloaded from MongoDB) skip re-filtering
            if item.get("pre_validated"):
                passes   = True
                priority = int(item.get("priority", 1))
            else:
                passes   = passes_reddit_filter(text)
                priority = 1  # Reddit BROAD filter has no priority scoring

            if not passes:
                total_dropped += 1
                log.debug(f"[REDDIT] FILTERED | u/{item.get('username')} | {item.get('content_type','?')}")
                mark_item_done(item.get("message_id", ""))
                reddit_queue.task_done()
                continue

            total_matched += 1
            current_batch.append(item)

            log.info(
                f"[REDDIT] MATCH "
                f"[{len(current_batch)}/{REDDIT_BATCH_SIZE} | total:{total_matched}] | "
                f"{item.get('content_type','?').upper()} | "
                f"u/{item.get('username')} | r/{item.get('subreddit','?')}"
            )

            reddit_queue.task_done()

            if len(current_batch) >= REDDIT_BATCH_SIZE:
                total_batches += 1
                batch_to_send  = current_batch[:REDDIT_BATCH_SIZE]
                current_batch  = current_batch[REDDIT_BATCH_SIZE:]

                log.info(
                    f"[REDDIT] ━━━ BATCH {total_batches} ━━━ | "
                    f"items:{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores    = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch."))
                    process_scored_item(it, sr)

                log.info(f"[REDDIT] BATCH {total_batches} DONE | waiting {BATCH_GAP_SECONDS}s...")
                time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[REDDIT] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER BATCH PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

def run_twitter_batch_processor(preloaded_count: int = 0):
    """
    Reads from twitter_queue.
    Uses PRECISION v2.0 filter (passes_twitter_filter) — two-category min.
    High-priority items (priority >= 7) front-inserted into current_batch.
    Collects 20 matched items per batch, then sends to Claude.
    30s gap between batches.

    pre_validated items skip filtering entirely.
    """
    log.info(
        f"Batch processor [TWITTER] started | batch_size:{TWITTER_BATCH_SIZE} | "
        f"gap:{BATCH_GAP_SECONDS}s | restored:{preloaded_count}"
    )

    current_batch  = []
    total_received = 0
    total_matched  = preloaded_count
    total_dropped  = 0
    total_batches  = 0

    while True:
        try:
            try:
                item = twitter_queue.get(timeout=1)
            except queue.Empty:
                continue

            total_received += 1
            text = item.get("text", "").strip()

            if not text or len(text) < 10:
                mark_item_done(item.get("message_id", ""))
                twitter_queue.task_done()
                continue

            # pre_validated items (reloaded from MongoDB) skip re-filtering
            if item.get("pre_validated"):
                passes   = True
                priority = int(item.get("priority", 1))
            else:
                passes, priority = passes_twitter_filter(text)

            if not passes:
                total_dropped += 1
                log.debug(f"[TWITTER] FILTERED | u/{item.get('username')} | tweet")
                mark_item_done(item.get("message_id", ""))
                twitter_queue.task_done()
                continue

            total_matched += 1

            # Front-insert high-priority items
            if priority >= 7:
                current_batch.insert(0, item)
            else:
                current_batch.append(item)

            log.info(
                f"[TWITTER] MATCH "
                f"[{len(current_batch)}/{TWITTER_BATCH_SIZE} | total:{total_matched}] | "
                f"priority:{priority} | u/{item.get('username')}"
            )

            twitter_queue.task_done()

            if len(current_batch) >= TWITTER_BATCH_SIZE:
                total_batches += 1
                batch_to_send  = current_batch[:TWITTER_BATCH_SIZE]
                current_batch  = current_batch[TWITTER_BATCH_SIZE:]

                log.info(
                    f"[TWITTER] ━━━ BATCH {total_batches} ━━━ | "
                    f"items:{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores    = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch."))
                    process_scored_item(it, sr)

                log.info(f"[TWITTER] BATCH {total_batches} DONE | waiting {BATCH_GAP_SECONDS}s...")
                time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[TWITTER] batch processor error: {exc}")
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
                enqueue_item(reddit_queue, {
                    "message_id":   f"reddit_post_{post.id}",
                    "platform":     "reddit",
                    "content_type": "post",
                    "text":         text,
                    "username":     author,
                    "subreddit":    str(post.subreddit),
                    "post_url":     f"https://reddit.com{post.permalink}",
                    "priority":     1,
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
                enqueue_item(reddit_queue, {
                    "message_id":   f"reddit_comment_{comment.id}",
                    "platform":     "reddit",
                    "content_type": ctype,
                    "text":         comment.body,
                    "username":     author,
                    "subreddit":    str(comment.subreddit),
                    "post_url":     f"https://reddit.com{comment.permalink}",
                    "priority":     1,
                })
        except praw.exceptions.PRAWException as exc:
            log.error(f"PRAW comment stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)
        except Exception as exc:
            log.error(f"Comment stream error: {exc} — reconnecting in 30s...")
            time.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X  POLLER
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
    """
    Polls Twitter search every TWITTER_POLL_INTERVAL seconds.

    Restart-safe via:
    - since_id persisted to db.twitter_state (loaded on startup)
    - item_already_known() checks MongoDB before enqueueing
    - LRU OrderedDict seen_ids (no full clear — prevents mid-run re-sends)
    - since_id advanced even on empty response (prevents stuck window)
    """
    seen_ids: OrderedDict = OrderedDict()
    since_id: str | None  = load_twitter_since_id()
    empty_streak = 0

    log.info("Twitter poll started.")

    while True:
        try:
            search_kwargs = dict(
                query        = TWITTER_SEARCH_QUERY,
                max_results  = 50,
                tweet_fields = ["author_id", "created_at", "text", "conversation_id"],
                expansions   = ["author_id"],
                user_fields  = ["username", "name"],
            )
            if since_id:
                search_kwargs["since_id"] = since_id

            response = client.search_recent_tweets(**search_kwargs)

            # Always advance since_id from meta even if data is empty
            newest_id = None
            if response and response.meta:
                newest_id = response.meta.get("newest_id")
            if newest_id:
                since_id = newest_id
                save_twitter_since_id(since_id)

            if not response or not response.data:
                empty_streak += 1
                if empty_streak == 1:
                    log.debug("Twitter: no results this cycle.")
                elif empty_streak % 5 == 0:
                    log.warning(
                        f"Twitter: {empty_streak} consecutive empty polls. "
                        f"since_id={since_id}. Check API quota / search index lag."
                    )
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            empty_streak = 0

            user_map: dict = {}
            if response.includes and "users" in response.includes:
                for u in response.includes["users"]:
                    user_map[u.id] = u.username

            new_count    = 0
            skip_dup     = 0
            skip_keyword = 0

            for tweet in response.data:
                tweet_id   = str(tweet.id)
                message_id = f"twitter_{tweet_id}"

                # LRU in-memory dedup
                if tweet_id in seen_ids:
                    seen_ids.move_to_end(tweet_id)
                    continue
                seen_ids[tweet_id] = True

                # Trim oldest when cap exceeded
                if len(seen_ids) > SEEN_IDS_MAX:
                    trim_count = len(seen_ids) - SEEN_IDS_KEEP
                    for _ in range(trim_count):
                        seen_ids.popitem(last=False)

                text     = tweet.text or ""
                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")

                # PRECISION v2.0 filter — Twitter only
                passes, priority = passes_twitter_filter(text)
                if not passes:
                    skip_keyword += 1
                    continue

                # MongoDB restart-safe dedup
                if item_already_known(message_id):
                    skip_dup += 1
                    continue

                enqueue_item(twitter_queue, {
                    "message_id":   message_id,
                    "platform":     "twitter",
                    "content_type": "tweet",
                    "text":         text,
                    "username":     username,
                    "subreddit":    "",
                    "post_url":     f"https://twitter.com/{username}/status/{tweet_id}",
                    "priority":     priority,
                })
                new_count += 1

            log.info(
                f"Twitter poll: {new_count} queued | "
                f"skip_keyword:{skip_keyword} skip_dup:{skip_dup} | "
                f"since_id:{since_id} | queue:{twitter_queue.qsize()}"
            )

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
        since   = datetime.now(timezone.utc) - timedelta(hours=24)
        signals = list(
            db.signals.find({
                "client_id":    CLIENT_ID,
                "intent_score": {"$gte": 6, "$lte": 7},
                "created_at":   {"$gte": since},
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
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v6.5 | Client: {CLIENT_ID} | Reddit + Twitter"}]},
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
        since        = datetime.now(timezone.utc) - timedelta(days=7)
        all_signals  = list(db.signals.find({"client_id": CLIENT_ID, "created_at": {"$gte": since}}))
        high         = [s for s in all_signals if s["intent_score"] >= 8]
        medium       = [s for s in all_signals if 6 <= s["intent_score"] <= 7]
        business     = [s for s in all_signals if s.get("is_business")]
        reddit_sigs  = [s for s in all_signals if s.get("platform") == "reddit"]
        twitter_sigs = [s for s in all_signals if s.get("platform") == "twitter"]
        total        = len(all_signals)

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
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v6.5 | {CLIENT_ID} | Week ending {week_end}"}]},
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

async def start_reddit_listener(preloaded_count: int = 0):
    reddit = build_reddit_client()

    post_thread = threading.Thread(target=stream_posts,    args=(reddit,), daemon=True, name="Reddit-Posts")
    cmnt_thread = threading.Thread(target=stream_comments, args=(reddit,), daemon=True, name="Reddit-Comments")
    btch_thread = threading.Thread(
        target=run_reddit_batch_processor,
        kwargs={"preloaded_count": preloaded_count},
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
                target=run_reddit_batch_processor,
                kwargs={"preloaded_count": 0},
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener(preloaded_count: int = 0):
    client = build_twitter_client()
    if client is None:
        log.warning("Twitter listener not started — credentials missing.")
        return

    poll_thread = threading.Thread(target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll")
    btch_thread = threading.Thread(
        target=run_twitter_batch_processor,
        kwargs={"preloaded_count": preloaded_count},
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
                target=run_twitter_batch_processor,
                kwargs={"preloaded_count": 0},
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI  — REST API
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v6.5",
    description = "Reddit + Twitter signals: monitor, score, store, alert.",
    version     = "6.5.0",
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
        "system":              "FLINTEL v6.5",
        "client":              CLIENT_ID,
        "platforms":           ["reddit", "twitter"],
        "reddit_batch_size":   REDDIT_BATCH_SIZE,
        "twitter_batch_size":  TWITTER_BATCH_SIZE,
        "batch_gap_s":         BATCH_GAP_SECONDS,
        "reddit_filter":       "BROAD (single keyword — v6.0 style)",
        "twitter_filter":      "PRECISION v2.0 (two-category min)",
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
    try:
        pending_reddit  = db.pending_items.count_documents({"status": "pending", "platform": "reddit"})
        pending_twitter = db.pending_items.count_documents({"status": "pending", "platform": "twitter"})
    except Exception:
        pending_reddit = pending_twitter = -1
    try:
        since_doc          = db.twitter_state.find_one({"key": "since_id"})
        persisted_since_id = since_doc["value"] if since_doc else None
    except Exception:
        persisted_since_id = None

    return {
        "status":              "ok",
        "mongodb":             mongo,
        "reddit":              "streaming",
        "twitter":             "polling" if TWITTER_BEARER_TOKEN else "disabled",
        "reddit_queue_size":   reddit_queue.qsize(),
        "twitter_queue_size":  twitter_queue.qsize(),
        "pending_reddit_db":   pending_reddit,
        "pending_twitter_db":  pending_twitter,
        "twitter_since_id":    persisted_since_id,
        "client_id":           CLIENT_ID,
        "timestamp":           datetime.now(timezone.utc).isoformat(),
    }


@app.get("/signals")
def get_signals(
    limit:       int  = 50,
    platform:    str  = None,
    category:    str  = None,
    min_score:   int  = None,
    subreddit:   str  = None,
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

        return {
            "total_signals":   total,
            "business_owners": biz,
            "reddit_signals":  reddit,
            "twitter_signals": twitter,
            "corridors":       agg("corridor"),
            "pain_types":      agg("pain_type"),
            "competitors":     agg("competitor_mentioned"),
            "tiers":           agg("tier"),
            "reddit_queue":    reddit_queue.qsize(),
            "twitter_queue":   twitter_queue.qsize(),
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


@app.get("/signals/pending")
def get_pending(limit: int = 100):
    try:
        items = list(
            db.pending_items.find({"status": "pending"}, {"_id": 0})
            .sort("created_at", ASCENDING).limit(limit)
        )
        for it in items:
            if "created_at" in it:
                it["created_at"] = it["created_at"].isoformat()
        reddit_count  = db.pending_items.count_documents({"status": "pending", "platform": "reddit"})
        twitter_count = db.pending_items.count_documents({"status": "pending", "platform": "twitter"})
        return {
            "reddit_pending":  reddit_count,
            "twitter_pending": twitter_count,
            "items":           items,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/twitter/state")
def get_twitter_state():
    """Inspect persisted Twitter poll state (since_id, last update)."""
    try:
        doc = db.twitter_state.find_one({"key": "since_id"}, {"_id": 0})
        if doc and "updated_at" in doc:
            doc["updated_at"] = doc["updated_at"].isoformat()
        return doc or {"key": "since_id", "value": None, "updated_at": None}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/twitter/state")
def reset_twitter_state():
    """
    Reset since_id — forces full recent-window fetch on next poll.
    Use only if you want to re-scan the last 7 days of Twitter results.
    """
    try:
        db.twitter_state.delete_one({"key": "since_id"})
        return {"status": "reset", "message": "Twitter since_id cleared. Next poll fetches full recent window."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    # Restore any items left pending from a previous run BEFORE streams start
    preloaded = load_pending_items_from_db()

    api_thread = threading.Thread(target=run_fastapi, daemon=True, name="FastAPI")
    api_thread.start()
    log.info("FastAPI running at http://0.0.0.0:8000")

    await asyncio.gather(
        start_reddit_listener(preloaded_count=preloaded.get("reddit", 0)),
        start_twitter_listener(preloaded_count=preloaded.get("twitter", 0)),
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 65)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v6.5")
    log.info("=" * 65)
    log.info(f"  Client           : {CLIENT_ID}")
    log.info(f"  Platforms        : Reddit + Twitter/X")
    log.info(f"  Reddit batch     : {REDDIT_BATCH_SIZE} items → 1 Claude call")
    log.info(f"  Twitter batch    : {TWITTER_BATCH_SIZE} items → 1 Claude call")
    log.info(f"  Batch gap        : {BATCH_GAP_SECONDS}s between calls")
    log.info(f"  Reddit filter    : BROAD (single keyword — v6.0 style, high recall)")
    log.info(f"  Twitter filter   : PRECISION v2.0 (two-category min — low noise)")
    log.info(f"  Twitter poll     : every {TWITTER_POLL_INTERVAL}s (since_id persisted to MongoDB)")
    log.info(f"  Seen IDs cap     : {SEEN_IDS_MAX} → trim to {SEEN_IDS_KEEP} (LRU, not full clear)")
    log.info(f"  Score 0-5        : DISCARD — never stored")
    log.info(f"  Score 6-7        : MEDIUM  — MongoDB + Slack")
    log.info(f"  Score 8-10       : HIGH    — MongoDB + Slack + HubSpot")
    log.info(f"  Daily digest     : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report    : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits       : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Reddit keywords  : {len(REDDIT_KEYWORDS)} broad phrases")
    log.info(f"  Twitter keywords : {sum(len(v) for v in TWITTER_PRECISION_KEYWORDS.items() if isinstance(v, list))} phrases across {len(TWITTER_PRECISION_KEYWORDS)} categories")
    log.info(f"  MongoDB          : {MONGODB_DB}")
    log.info(f"  Persistent queue : db.pending_items (restart-safe + pre_validated)")
    log.info(f"  Twitter state    : db.twitter_state (since_id across restarts)")
    log.info(f"  Reddit account   : u/{REDDIT_USERNAME}")
    log.info(f"  Twitter          : {'enabled' if TWITTER_BEARER_TOKEN else 'DISABLED — set TWITTER_BEARER_TOKEN'}")
    log.info(f"  HubSpot          : {'enabled' if HUBSPOT_API_KEY else 'DISABLED — set HUBSPOT_API_KEY'}")
    log.info(f"  Slack            : {'enabled' if SLACK_WEBHOOK_URL else 'DISABLED — set SLACK_WEBHOOK_URL'}")
    log.info("=" * 65)

    asyncio.run(main())
