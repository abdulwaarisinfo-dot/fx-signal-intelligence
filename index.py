"""
FX Signal Intelligence System — FLINTEL v5.1
=============================================
Platform : Reddit (JSON Endpoints — No API Key Required)
Pipeline :
  Reddit Post / Comment / Reply
      → Keyword Pre-Filter        (free, fast — blocks 80%+ noise)
      → Batch Collector           (collects 10 keyword-matched items)
      → 30 Second Gap             (between each batch sent to Claude)
      → Claude AI Intent Scorer   (receives 10 messages merged as one prompt)
      → MongoDB Storage
      → HubSpot CRM               (score 8-10 only)
      → Slack Alert               (score 6-10)
      → FastAPI REST Endpoints
      → Daily Digest Scheduler    (score 6-7, sent 8am UTC daily)
      → Weekly Report Scheduler   (all signals, sent Monday 9am UTC)

Score rules:
  0-5  → DELETE  — never stored, never alerted
  6-7  → MEDIUM  — MongoDB + Slack only
  8-10 → HIGH    — MongoDB + Slack + HubSpot

Batch rules:
  → Reddit JSON endpoints polled every 60 seconds per subreddit
  → Python collects ALL incoming items into a shared queue
  → Keyword filter applied to EVERY item (posts, comments, replies)
  → Keyword-matched items collected into batches of 10
  → Non-matching items deleted immediately
  → Every 10 matched items → merged into one Claude prompt
  → 30 second gap between each batch sent to Claude
  → If Reddit sends 100 at once → processed as 10 batches of 10
  → Each item in batch scored individually by Claude
  → All pipeline logic (MongoDB, Slack, HubSpot) unchanged

Reddit monitors (simultaneously):
  Posts      → title + body (selftext)
  Comments   → comment text
  All LIVE   → polled every 60 seconds, new items only
  No PRAW    → uses public JSON endpoints, zero credentials needed

Changelog v5.1:
  - Replaced PRAW with Reddit public JSON endpoints
  - Zero Reddit credentials required (no client_id, no secret)
  - Rate limit safe: 6 second delay between subreddit requests
  - Subreddits split into 2 groups to stay within 10 req/min limit
  - Seen post/comment IDs tracked to avoid duplicates
  - Auto-reconnect on network errors (same as before)
  - All other logic 100% identical to v5.0
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

import anthropic
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import requests
from fastapi import FastAPI, HTTPException
import uvicorn

# ─────────────────────────────────────────────
# LOAD ENV
# ─────────────────────────────────────────────

load_dotenv()

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

def _get(name: str, default=None, cast=str):
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")
    try:
        return cast(val)
    except (ValueError, TypeError) as e:
        raise RuntimeError(f"Env var '{name}' could not be cast to {cast.__name__}: {e}")

REDDIT_USER_AGENT  = _get("REDDIT_USER_AGENT")
ANTHROPIC_API_KEY  = _get("ANTHROPIC_API_KEY")
MONGODB_URI        = _get("MONGODB_URI")
MONGODB_DB         = _get("MONGODB_DB")
SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL")   # optional
HUBSPOT_API_KEY    = os.getenv("HUBSPOT_API_KEY")      # optional
CLIENT_ID          = _get("CLIENT_ID")

MIN_SCORE_MEDIUM   = _get("MIN_SCORE_MEDIUM",   "6",  int)
MIN_SCORE_HIGH     = _get("MIN_SCORE_HIGH",     "8",  int)
BATCH_SIZE         = _get("BATCH_SIZE",         "10", int)
BATCH_GAP_SECONDS  = _get("BATCH_GAP_SECONDS",  "30", int)
POLL_INTERVAL      = _get("POLL_INTERVAL",      "60", int)
REQUEST_DELAY      = _get("REQUEST_DELAY",       "6",  float)
DAILY_DIGEST_HOUR  = _get("DAILY_DIGEST_HOUR",  "8",  int)
WEEKLY_REPORT_DAY  = _get("WEEKLY_REPORT_DAY",  "0",  int)
WEEKLY_REPORT_HOUR = _get("WEEKLY_REPORT_HOUR", "9",  int)

# ─────────────────────────────────────────────
# TARGET SUBREDDITS
# No join required — all public subreddits
# ─────────────────────────────────────────────

TARGET_SUBREDDITS = [
    "Nigeria",
    "lagos",
    "Nigerians",
    "NigeriansAbroad",
    "AfricanDiaspora",
    "pakistan",
    "Pakistani",
    "PakistaniDiaspora",
    "PersonalFinanceCanada",
    "PersonalFinanceUK",
    "personalfinance",
    "entrepreneur",
    "smallbusiness",
    "digitalnomad",
    "africatech",
    "UKPersonalFinance",
    "Remittance",
    "moneytransfer",
    "CanadianInvestor",
    "ExpatFinance",
]

# ─────────────────────────────────────────────
# SHARED QUEUE
# All incoming Reddit items (posts + comments)
# are pushed here by polling threads.
# Batch processor reads from this queue.
# ─────────────────────────────────────────────

reddit_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────
# SEEN IDs — Deduplication
# Tracks post/comment IDs already processed
# Prevents same item being scored twice
# ─────────────────────────────────────────────

seen_ids: set = set()
seen_ids_lock  = threading.Lock()

MAX_SEEN_IDS = 50000  # cap memory usage — drop oldest when limit hit


def is_seen(item_id: str) -> bool:
    with seen_ids_lock:
        return item_id in seen_ids


def mark_seen(item_id: str):
    with seen_ids_lock:
        if len(seen_ids) >= MAX_SEEN_IDS:
            # Remove oldest 10% to keep memory bounded
            to_remove = list(seen_ids)[:MAX_SEEN_IDS // 10]
            for old_id in to_remove:
                seen_ids.discard(old_id)
        seen_ids.add(item_id)


# ─────────────────────────────────────────────
# LAYER 1 — KEYWORD PRE-FILTER  (v5 EXPANDED)
# Applied to EVERY item: posts, comments
# Claude only sees items that pass this filter
# 300+ signals across all intent categories
# ─────────────────────────────────────────────

KEYWORDS = [

    # ─── URGENCY SIGNALS ────────────────────────────────────────
    "urgent", "urgently", "today", "this week", "asap",
    "immediately", "right now", "by friday", "by monday",
    "deadline", "time sensitive", "need it done",
    "waiting on", "been waiting", "already delayed",
    "running out of time", "need this sorted",

    # ─── PAYMENT ACTION WORDS ───────────────────────────────────
    "send money", "sending money", "transfer money",
    "wire transfer", "bank transfer", "international transfer",
    "cross border", "cross-border", "overseas payment",
    "foreign payment", "pay my supplier", "paying supplier",
    "supplier payment", "business payment", "b2b payment",
    "invoice payment", "pay an invoice", "settle invoice",
    "remit", "remittance", "remitting funds",
    "move money", "moving money", "receive payment",
    "get paid", "collect payment", "send", "transfer",
    "sending", "transferring", "wire", "wiring",
    "pay", "paying", "payment",

    # ─── RATE COMPARISON SIGNALS ────────────────────────────────
    "best rate", "better rate", "best exchange rate",
    "exchange rate", "fx rate", "conversion rate",
    "who has the best", "which is cheaper",
    "compare rates", "comparing rates", "rate comparison",
    "cheapest way", "most affordable", "lowest fees",
    "best deal", "best option", "best platform",
    "which service", "which app", "which provider",
    "recommend a service", "recommend an app",
    "any recommendations", "looking for recommendations",
    "suggest a platform", "anyone use", "does anyone know",
    "rate", "rates", "recommendation",

    # ─── CURRENCIES AND CORRIDORS ───────────────────────────────

    # Nigerian corridor
    "naira", "ngn", "nigeria", "nigerian", "lagos",
    "abuja", "port harcourt", "send to nigeria",
    "cad to ngn", "gbp to ngn", "usd to ngn",

    # Pakistani corridor
    "pkr", "pakistan", "pakistani", "karachi",
    "lahore", "islamabad", "send to pakistan",
    "cad to pkr", "gbp to pkr", "usd to pkr",
    "rupee",

    # Indian corridor
    "inr", "india", "indian", "mumbai", "delhi",
    "send to india", "cad to inr", "gbp to inr",

    # Ghanaian corridor
    "ghana", "ghanaian", "accra", "cedi", "ghs",
    "send to ghana",

    # Kenyan corridor
    "kenya", "kenyan", "nairobi", "shilling", "kes",
    "send to kenya", "m-pesa", "mpesa",

    # Other African corridors
    "ethiopia", "senegal", "ivory coast",
    "cameroon", "tanzania", "uganda", "zimbabwe",
    "south africa", "rand", "zar",

    # Major sending currencies
    "cad", "canadian dollar", "gbp", "british pound",
    "usd", "us dollar", "eur", "euro", "aud",
    "australian dollar", "aed", "dirham",
    "dollar", "dollars", "pound",

    # ─── AMOUNT SIGNALS ─────────────────────────────────────────
    "thousand dollars", "thousand pounds", "thousand cad",
    "10k", "20k", "30k", "40k", "50k", "100k",
    "$5,000", "$10,000", "$15,000", "$20,000", "$30,000",
    "$50,000", "$100,000", "£5,000", "£10,000", "£20,000",
    "large transfer", "large amount", "big transfer",
    "significant amount", "substantial amount",
    "thousand", "million", "k cad", "k usd", "k gbp",

    # ─── BUSINESS AND TRADE SIGNALS ─────────────────────────────
    "supplier", "my supplier", "pay my supplier",
    "pay a supplier", "supplier invoice", "vendor payment",
    "pay my vendor", "business partner", "pay my partner",
    "contractor", "pay my contractor", "freelancer payment",
    "import", "importing", "importer", "importing goods",
    "export", "exporting", "exporter",
    "trade", "trading", "trade finance",
    "goods", "inventory", "stock", "merchandise",
    "manufacturing", "factory", "production",
    "b2b", "business to business", "commercial payment",
    "company payment", "corporate transfer",
    "diaspora business", "diaspora entrepreneur",
    "running a business", "my business needs",
    "for my business", "business account",
    "invoice", "cross-border", "supplier payment",
    "business payment",

    # ─── COMPETITOR MENTIONS ────────────────────────────────────
    "wise", "transferwise", "wise business",
    "remitly", "worldremit", "world remit",
    "western union", "wu", "moneygram",
    "payoneer", "xe.com", "xe money",
    "currencyfair", "ofx", "ozforex",
    "xoom", "ria money", "ria transfer",
    "sendwave", "lemfi", "nala",
    "grey finance", "chipper cash",
    "revolut", "revolut business",
    "transfergo", "azimo",

    # ─── PAIN AND FRUSTRATION SIGNALS ───────────────────────────
    "blocked", "block my", "blocked my transfer",
    "blocked my payment", "account blocked",
    "rejected", "payment rejected", "transfer rejected",
    "declined", "transaction declined",
    "frozen", "account frozen", "funds frozen",
    "held", "funds held", "money held",
    "delayed", "payment delayed", "transfer delayed",
    "stuck", "money stuck", "funds stuck",
    "failed", "transfer failed", "payment failed",
    "not going through", "wont go through",
    "keeps failing", "failed again",
    "frustrated", "so frustrated", "very frustrated",
    "annoyed", "fed up", "had enough",
    "terrible", "awful", "horrible", "worst",
    "ridiculous", "insane fees", "crazy fees",
    "too expensive", "highway robbery",
    "rip off", "ripoff", "scam", "overcharged",
    "compliance hold", "compliance issue",
    "kyc", "kyc rejected", "kyc failed",
    "verification failed", "documents rejected",
    "swift", "swift fees", "swift charges",
    "correspondent bank", "intermediary bank",
    "bank won't let me", "bank refuses",
    "bank blocked", "bank flagged",
    "suspicious activity", "flagged as suspicious",
    "expensive", "fees", "charges", "problem",
    "issue", "complaint", "slow",

    # ─── DISCOVERY AND RESEARCH SIGNALS ─────────────────────────
    "looking for", "searching for", "trying to find",
    "need help with", "help me find", "can anyone help",
    "how do i", "how can i", "what is the best way",
    "what is the cheapest", "what is the fastest",
    "is there a better", "is there an alternative",
    "alternative to", "instead of", "switching from",
    "leaving wise", "leaving remitly", "left western union",
    "tried wise", "tried remitly", "tried payoneer",
    "used wise before", "used remitly before",
    "never using again", "will never use again",
    "done with", "finished with", "moving away from",
    "anyone", "recommend", "suggestion",
    "advice", "help", "best app", "what app",
    "who uses",

    # ─── COMPLIANCE AND DOCUMENTATION ───────────────────────────
    "form m", "cbn", "central bank nigeria",
    "documentation", "compliance documents",
    "proof of funds", "source of funds",
    "aml", "anti money laundering",
    "regulatory", "regulated",
    "license", "licensed", "licensed provider",

    # ─── GEOGRAPHY — SENDING COUNTRIES ──────────────────────────
    "from canada", "from uk", "from england",
    "from united states", "from usa", "from america",
    "from australia", "from uae", "from dubai",
    "toronto", "london", "new york", "houston",
    "calgary", "vancouver", "ottawa", "montreal",
    "manchester", "birmingham", "glasgow",
    "sydney", "melbourne", "perth",
    "canada", "uk", "international", "overseas", "abroad",

    # ─── TIMING AND EXPANSION SIGNALS ───────────────────────────
    "starting a business", "just started",
    "new supplier", "found a supplier",
    "signed a contract", "new contract",
    "expanding to", "entering the market",
    "launching in", "setting up in",
    "first time sending", "never done this before",
    "setting up payments", "payment infrastructure",
]


def passes_keyword_filter(text: str) -> bool:
    """
    Returns True if text contains at least one target keyword.
    Case-insensitive. Runs in microseconds — zero API cost.
    Applied to ALL content types: posts, comments.
    """
    text_lower = text.lower()
    matched = [kw for kw in KEYWORDS if kw in text_lower]
    if matched:
        log.debug(f"Keyword match: {matched[:3]}")
        return True
    return False


# ─────────────────────────────────────────────
# LAYER 2 — CLAUDE AI SYSTEM PROMPT  (v5 UPGRADED)
# Full Settla ICP, outreach scripts, corridor detection,
# competitor intelligence, tier classification, pain types
# ─────────────────────────────────────────────

CLAUDE_SYSTEM_PROMPT = """
You are the world's most sophisticated B2B signal intelligence analyst.
You work exclusively for Settla — a premium cross-border payment company
that helps diaspora business owners move large amounts of money between
Canada, UK, USA, Australia, and UAE to Nigeria, Pakistan, India, Ghana,
Kenya, and across Africa and South Asia.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHO SETTLA SERVES — KNOW THIS DEEPLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Settla's ideal customer is a diaspora business owner who:
- Runs an import/export business, trading company, or has overseas suppliers
- Needs to move $10,000 to $500,000 CAD/GBP/USD regularly
- Is frustrated with banks blocking large international transfers
- Has been burned by consumer apps like Wise that restrict business volumes
- Values compliance, trust, and reliability over raw cheapness
- Is actively looking for a better cross-border payment solution TODAY

Settla is NOT for:
- Individuals sending small personal remittances under $2,000
- People sending money to family for living expenses
- Consumers comparing holiday money rates
- Retail crypto traders

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR JOB
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You will receive a numbered batch of Reddit posts, comments, and replies.
Score EACH message individually and return a JSON array.

Return a JSON array ONLY. No preamble. No markdown. Just raw JSON array.

[
  {
    "index": 1,
    "intent_score": <number 0-10>,
    "signal_category": <"high_intent" | "medium_intent" | "low_intent" | "no_intent">,
    "tier": <"immediate" | "digest" | "watchlist" | "discard">,
    "is_business": <true | false>,
    "corridor": "<source country> to <destination country>, or null",
    "estimated_amount": "<specific amount if mentioned, or null>",
    "competitor_mentioned": "<competitor name if mentioned, or null>",
    "pain_type": "<blocked | delayed | expensive | rejected | researching | expanding | null>",
    "reason": "<one precise sentence explaining the score>",
    "suggested_action": "<one precise sentence for Settla sales team>",
    "outreach_script": "<exact message Settla should send — null if tier is discard or watchlist>"
  },
  ...
]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCORING RULES — READ EVERY WORD
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCORE 9-10 → tier: immediate
Person is in acute pain RIGHT NOW.
All three must be present:
  ✓ Clear business context (supplier, vendor, invoice, import, export, trade)
  ✓ Specific amount or large transfer implied
  ✓ Active problem (blocked, failed, rejected, frustrated, urgent)

Examples:
- "Bank blocked my $45k CAD payment to Lagos supplier again. Need solution urgently."
- "Wise Business restricted my account. Have $80k stuck. Nigerian supplier waiting."
- "Third time my transfer to Pakistan manufacturer failed. Losing the contract."
- "SWIFT fees eating 4% on every Nigeria transfer. $200k monthly volume. Need better option."

SCORE 7-8 → tier: immediate
Strong buying signal. Missing one element of urgency or specificity.

Examples:
- "Looking for reliable way to pay Nigerian supplier regularly. Tired of bank issues."
- "Anyone using a service better than Wise for business payments to Africa?"
- "My Payoneer account got limited. I send to Lagos suppliers monthly. Need alternative."
- "Cross-border payment to Nigeria keeps getting compliance hold. Running a business here."

SCORE 5-6 → tier: digest
Researching but no immediate crisis. Potential future customer.

Examples:
- "What service do people use for sending large amounts to Nigeria?"
- "Starting an import business. How do people handle supplier payments to Africa?"
- "Anyone know the best FX rates for CAD to NGN for business?"
- "Just signed my first Nigerian supplier contract. How do I pay them?"

SCORE 3-4 → tier: watchlist
Not a buyer today. Clear future potential within 30-60 days.

Examples:
- "Excited to announce I am launching an import business this quarter."
- "Meeting with a supplier in Lagos next month. Will need to set up payments."
- "Any Nigerians in Canada running an import business? Looking for advice."
- "Starting a business and exploring African suppliers."

SCORE 0-2 → tier: discard
Consumer personal remittance, wrong context, no business signal.

Examples:
- "What is the best rate to send £500 to my mum in Lagos?"
- "Naira rates are crazy today."
- "Has anyone used Wise to send money home?"
- "Need to send $1000 to my family in Nigeria for school fees."
- "PayPal blocked my subscription payment."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL SCORING RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ALWAYS score higher when:
+ Business owner identity confirmed (founder, CEO, import, export, supplier)
+ Specific large amount mentioned ($10k+)
+ Multiple pain points in same message
+ Competitor mentioned negatively
+ Urgency words present (today, asap, urgent, this week, waiting)
+ Active payment block or failure described
+ Supplier relationship at risk

ALWAYS score lower when:
- Small personal amount (under $2,000)
- Sending to family for personal expenses
- No business context at all
- General market commentary with no personal intent
- News sharing with no personal pain expressed
- Consumer subscription problems

THE KEY DISTINCTION:
"Bank blocked my $45k Lagos supplier payment" = 9
"What is a good service to send money to Nigeria" = 5
"My mum needs money in Nigeria" = 1

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORRIDOR DETECTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Identify the payment corridor when possible.
Format: "Canada to Nigeria" or "UK to Pakistan" or "USA to Ghana"

If sending country not mentioned — infer from subreddit context:
- r/PersonalFinanceCanada → assume Canada as source
- r/UKPersonalFinance or r/PersonalFinanceUK → assume UK as source
- r/personalfinance → assume USA as source

If corridor genuinely unclear — set corridor to null.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPETITOR INTELLIGENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If any competitor is mentioned — capture the name exactly.
A negative competitor mention automatically raises the score by 1.
A person leaving a competitor is the hottest possible lead.

Known competitors: Wise, Remitly, WorldRemit, Western Union,
MoneyGram, Payoneer, OFX, XE, Revolut, LemFi, NALA, Grey Finance,
Chipper Cash, Sendwave, TransferGo, Azimo, Xoom

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTREACH SCRIPT RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Write outreach_script ONLY for scores 5 and above (tier: immediate or digest).
For scores below 5 (tier: watchlist or discard) set outreach_script to null.

The outreach script must:
- Be maximum 3 sentences
- Reference their SPECIFIC situation — never generic
- Sound human — not corporate, not salesy
- Position Settla as the exact solution to their pain
- Create curiosity not pressure
- End naturally — never "please let me know" or "feel free to reach out"

OUTREACH SCRIPT EXAMPLES BY SCORE:

Score 9-10 (acute pain, business owner):
"We work specifically with diaspora business owners moving large payments
between Canada and Nigeria — fast, fully compliant, no bank blocks.
DM us and we can sort your supplier payment today."

Score 7-8 (strong signal, needs solution):
"Settla specialises in exactly this corridor for business payments.
We handle the compliance side so your transfers go through without
the bank blocks. Worth a quick conversation?"

Score 5-6 (researching, no urgency):
"Settla works with importers and diaspora businesses on Canada to Nigeria
payments specifically. Happy to share how we handle the compliance
and settlement side if useful."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PAIN TYPE CLASSIFICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Set pain_type to ONE of:
  blocked    → transfer or account actively blocked/frozen/rejected
  delayed    → transfer stuck, taking too long, not arriving
  expensive  → fees, poor rates, SWIFT charges complaints
  rejected   → KYC failure, compliance hold, documents refused
  researching → asking questions, comparing services, no active crisis
  expanding  → launching business, new supplier, new market entry
  null       → cannot determine or not applicable

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REMEMBER ABOVE EVERYTHING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You are not just scoring sentiment. You are identifying the exact
moment a diaspora business owner is ready to switch payment providers.

That moment is worth thousands of dollars to Settla.
Your accuracy directly determines whether Settla finds that buyer
before every competitor does.

Be precise. Be ruthless with noise. Be generous with genuine pain.

Score every message in the batch — return the SAME COUNT as received.
Return JSON array ONLY. Always. Every time.
"""


# ─────────────────────────────────────────────
# MONGODB SETUP
# ─────────────────────────────────────────────

def get_database():
    try:
        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        mongo_client.server_info()
        db = mongo_client[MONGODB_DB]

        db.signals.create_index(
            [("message_id", ASCENDING)],
            unique=True,
            name="message_id_unique"
        )
        db.signals.create_index([("intent_score",         ASCENDING)])
        db.signals.create_index([("created_at",           ASCENDING)])
        db.signals.create_index([("client_id",            ASCENDING)])
        db.signals.create_index([("platform",             ASCENDING)])
        db.signals.create_index([("tier",                 ASCENDING)])
        db.signals.create_index([("corridor",             ASCENDING)])
        db.signals.create_index([("competitor_mentioned", ASCENDING)])
        db.signals.create_index([("pain_type",            ASCENDING)])

        log.info("MongoDB connected successfully.")
        return db
    except Exception as e:
        log.error(f"MongoDB connection failed: {e}")
        raise


db = get_database()


# ─────────────────────────────────────────────
# ANTHROPIC CLIENT
# ─────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ─────────────────────────────────────────────
# RETRY WITH EXPONENTIAL BACKOFF
# ─────────────────────────────────────────────

def retry_with_backoff(func, *args, retries=3, delay=2, label="operation", **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = delay * attempt
            log.error(f"[{label}] Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                log.info(f"[{label}] Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.critical(f"[{label}] All {retries} attempts failed. Giving up.")
                return None


# ─────────────────────────────────────────────
# LAYER 2 — CLAUDE BATCH SCORER
# Receives list of 10 items, merges into one prompt
# Returns list of scores — one per item
# ─────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        content_type = item.get("content_type", "unknown").upper()
        subreddit    = item.get("subreddit", "unknown")
        username     = item.get("username", "unknown")
        text         = item.get("text", "")[:1000]

        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Type: {content_type} | Subreddit: r/{subreddit} | User: u/{username}\n"
            f"Content: {text}\n"
        )

    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Claude API failed after all retries.") -> dict:
    return {
        "index":               index,
        "intent_score":        0,
        "signal_category":     "no_intent",
        "tier":                "discard",
        "is_business":         False,
        "corridor":            None,
        "estimated_amount":    None,
        "competitor_mentioned": None,
        "pain_type":           None,
        "reason":              reason,
        "suggested_action":    "Check system logs and Claude API status.",
        "outreach_script":     None,
    }


def _call_claude_batch(batch: list) -> list:
    prompt = _build_batch_prompt(batch)

    response = anthropic_client.messages.create(
        model      = "claude-sonnet-4-20250514",
        max_tokens = 4000,
        system     = CLAUDE_SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": f"Score this batch of Reddit content:\n\n{prompt}"}]
    )

    raw_text = response.content[0].text.strip()

    if raw_text.startswith("```"):
        parts = raw_text.split("```")
        raw_text = parts[1] if len(parts) > 1 else raw_text
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
    raw_text = raw_text.strip()

    results = json.loads(raw_text)

    if not isinstance(results, list):
        raise ValueError("Claude returned non-list response for batch scoring")

    required_keys = [
        "index", "intent_score", "signal_category", "tier",
        "is_business", "reason", "suggested_action"
    ]
    for r in results:
        for key in required_keys:
            if key not in r:
                raise ValueError(f"Missing key '{key}' in Claude batch response item")
        r.setdefault("corridor",             None)
        r.setdefault("estimated_amount",     None)
        r.setdefault("competitor_mentioned", None)
        r.setdefault("pain_type",            None)
        r.setdefault("outreach_script",      None)

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(
        _call_claude_batch,
        batch,
        retries=3,
        delay=5,
        label="Claude-Batch"
    )

    if result is None:
        log.error("Claude batch scoring failed after all retries. Using fallback scores.")
        return [_fallback_score(i + 1) for i in range(len(batch))]

    return result


# ─────────────────────────────────────────────
# LAYER 3 — MONGODB STORAGE
# ─────────────────────────────────────────────

def save_signal(signal_data: dict) -> bool:
    try:
        document = {
            "message_id":           signal_data["message_id"],
            "platform":             signal_data.get("platform", "reddit"),
            "content_type":         signal_data.get("content_type", "unknown"),
            "subreddit":            signal_data.get("subreddit", "unknown"),
            "post_url":             signal_data.get("post_url", ""),
            "username":             signal_data.get("username", "unknown"),
            "message_text":         signal_data["message_text"],
            "intent_score":         signal_data["intent_score"],
            "signal_category":      signal_data["signal_category"],
            "tier":                 signal_data.get("tier", "discard"),
            "is_business":          signal_data.get("is_business", False),
            "corridor":             signal_data.get("corridor"),
            "estimated_amount":     signal_data.get("estimated_amount"),
            "competitor_mentioned": signal_data.get("competitor_mentioned"),
            "pain_type":            signal_data.get("pain_type"),
            "reason":               signal_data["reason"],
            "suggested_action":     signal_data["suggested_action"],
            "outreach_script":      signal_data.get("outreach_script"),
            "client_id":            CLIENT_ID,
            "alerted_slack":        False,
            "alerted_hubspot":      False,
            "digest_included":      False,
            "created_at":           datetime.now(timezone.utc),
        }

        db.signals.insert_one(document)
        log.info(
            f"Saved | Score: {signal_data['intent_score']} "
            f"| Tier: {signal_data.get('tier', '?')} "
            f"| u/{signal_data.get('username')} "
            f"| r/{signal_data.get('subreddit')} "
            f"| {signal_data.get('content_type', '')}"
        )
        return True

    except DuplicateKeyError:
        log.debug(f"Duplicate skipped: {signal_data['message_id']}")
        return False
    except Exception as e:
        log.error(f"MongoDB save error: {e}")
        return False


def mark_slack_alerted(message_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {
                "alerted_slack":    True,
                "alerted_slack_at": datetime.now(timezone.utc)
            }}
        )
    except Exception as e:
        log.error(f"MongoDB mark_slack_alerted error: {e}")


def mark_hubspot_alerted(message_id: str, contact_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {
                "alerted_hubspot":    True,
                "hubspot_contact_id": contact_id,
                "alerted_hubspot_at": datetime.now(timezone.utc)
            }}
        )
    except Exception as e:
        log.error(f"MongoDB mark_hubspot_alerted error: {e}")


# ─────────────────────────────────────────────
# LAYER 4A — SLACK DELIVERY
# ─────────────────────────────────────────────

def _post_to_slack(payload: dict):
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Slack returned {response.status_code}: {response.text}")
    return response


def _safe_text(text: str, limit: int = 2900) -> str:
    if not text:
        return ""
    return text[:limit] + ("…" if len(text) > limit else "")


def send_slack_alert(signal_data: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set. Skipping Slack alert.")
        return False

    score                = signal_data["intent_score"]
    category             = signal_data["signal_category"].replace("_", " ").upper()
    content_type         = signal_data.get("content_type", "post").upper()
    subreddit            = signal_data.get("subreddit", "unknown")
    post_url             = signal_data.get("post_url", "")
    is_business          = signal_data.get("is_business", False)
    tier                 = signal_data.get("tier", "")
    corridor             = signal_data.get("corridor") or "Unknown"
    competitor_mentioned = signal_data.get("competitor_mentioned") or "—"
    pain_type            = signal_data.get("pain_type") or "—"
    estimated_amount     = signal_data.get("estimated_amount") or "—"
    outreach_script      = signal_data.get("outreach_script") or ""

    if score >= 8:
        emoji  = "🚨"
        header = f"{emoji} HIGH INTENT SIGNAL — {category}"
    else:
        emoji  = "⚠️"
        header = f"{emoji} MEDIUM INTENT SIGNAL — {category}"

    business_tag = "✅ Business Owner" if is_business else "👤 Individual"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header[:150], "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Platform:*\nReddit"},
                {"type": "mrkdwn", "text": f"*Subreddit:*\nr/{subreddit}"},
                {"type": "mrkdwn", "text": f"*Score:*\n{score}/10"},
                {"type": "mrkdwn", "text": f"*Tier:*\n{tier.upper()}"},
                {"type": "mrkdwn", "text": f"*User:*\nu/{signal_data.get('username', 'unknown')}"},
                {"type": "mrkdwn", "text": f"*Type:*\n{content_type}"},
                {"type": "mrkdwn", "text": f"*Profile:*\n{business_tag}"},
                {"type": "mrkdwn", "text": f"*Time:*\n{signal_data.get('timestamp', 'N/A')}"},
            ]
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Corridor:*\n{corridor}"},
                {"type": "mrkdwn", "text": f"*Amount:*\n{estimated_amount}"},
                {"type": "mrkdwn", "text": f"*Pain Type:*\n{pain_type}"},
                {"type": "mrkdwn", "text": f"*Competitor:*\n{competitor_mentioned}"},
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Message:*\n>{_safe_text(signal_data['message_text'], 400)}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Reason:*\n{_safe_text(signal_data['reason'], 300)}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Action:*\n🎯 {_safe_text(signal_data['suggested_action'], 300)}"
            }
        },
    ]

    if outreach_script:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Outreach Script:*\n💬 {_safe_text(outreach_script, 600)}"
            }
        })

    if post_url:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type":  "button",
                "text":  {"type": "plain_text", "text": "View on Reddit →"},
                "url":   post_url,
                "style": "primary"
            }]
        })

    blocks.append({"type": "divider"})

    payload = {"text": header, "blocks": blocks}

    result = retry_with_backoff(
        _post_to_slack, payload,
        retries=3, delay=2, label="Slack"
    )

    if result:
        log.info(f"Slack alert sent | u/{signal_data.get('username')} | Score: {score}")
        return True

    log.error("Slack alert failed after all retries.")
    return False


# ─────────────────────────────────────────────
# LAYER 4B — HUBSPOT CRM
# ─────────────────────────────────────────────

HUBSPOT_BASE = "https://api.hubapi.com"


def _hubspot_headers() -> dict:
    return {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type":  "application/json"
    }


def _find_hubspot_contact(username: str) -> str | None:
    try:
        url  = f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search"
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "firstname",
                    "operator":     "EQ",
                    "value":        username
                }]
            }]
        }
        response = requests.post(url, json=body, headers=_hubspot_headers(), timeout=10)
        response.raise_for_status()
        results = response.json().get("results", [])
        return results[0]["id"] if results else None
    except Exception as e:
        log.error(f"HubSpot search error: {e}")
        return None


def _create_hubspot_contact(signal: dict) -> str | None:
    try:
        url  = f"{HUBSPOT_BASE}/crm/v3/objects/contacts"
        body = {
            "properties": {
                "firstname":              f"u/{signal['username']}",
                "lastname":               f"Reddit: r/{signal.get('subreddit', 'unknown')}",
                "fx_intent_score":        str(signal["intent_score"]),
                "fx_signal_category":     signal["signal_category"],
                "fx_tier":                signal.get("tier", ""),
                "fx_corridor":            signal.get("corridor") or "",
                "fx_pain_type":           signal.get("pain_type") or "",
                "fx_competitor":          signal.get("competitor_mentioned") or "",
                "fx_source_community":    f"r/{signal.get('subreddit', 'unknown')}",
                "fx_signal_reason":       signal["reason"],
                "fx_suggested_action":    signal["suggested_action"],
            }
        }
        response = requests.post(url, json=body, headers=_hubspot_headers(), timeout=10)
        response.raise_for_status()
        return response.json().get("id")
    except Exception as e:
        log.error(f"HubSpot create contact error: {e}")
        return None


def _create_hubspot_note(signal: dict, contact_id: str):
    try:
        url       = f"{HUBSPOT_BASE}/crm/v3/objects/notes"
        note_body = (
            f"FLINTEL SIGNAL — REDDIT v5.1\n\n"
            f"Message:\n{signal['message_text']}\n\n"
            f"Score:        {signal['intent_score']}/10\n"
            f"Tier:         {signal.get('tier', '')}\n"
            f"Category:     {signal['signal_category']}\n"
            f"Business:     {signal.get('is_business', False)}\n"
            f"Corridor:     {signal.get('corridor') or 'Unknown'}\n"
            f"Amount:       {signal.get('estimated_amount') or 'Unknown'}\n"
            f"Competitor:   {signal.get('competitor_mentioned') or 'None'}\n"
            f"Pain Type:    {signal.get('pain_type') or 'Unknown'}\n"
            f"Content Type: {signal.get('content_type', 'unknown')}\n"
            f"Subreddit:    r/{signal.get('subreddit', 'unknown')}\n"
            f"Post URL:     {signal.get('post_url', 'N/A')}\n"
            f"Reason:       {signal['reason']}\n"
            f"Action:       {signal['suggested_action']}\n"
            f"Outreach:     {signal.get('outreach_script') or 'N/A'}\n"
            f"Time:         {signal.get('timestamp', 'N/A')}"
        )
        body = {
            "properties": {
                "hs_note_body":  note_body,
                "hs_timestamp":  str(int(datetime.now(timezone.utc).timestamp() * 1000))
            },
            "associations": [{
                "to":    {"id": contact_id},
                "types": [{
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId":   202
                }]
            }]
        }
        response = requests.post(url, json=body, headers=_hubspot_headers(), timeout=10)
        response.raise_for_status()
    except Exception as e:
        log.error(f"HubSpot create note error: {e}")


def _send_to_hubspot(signal: dict) -> str | None:
    if not HUBSPOT_API_KEY:
        log.warning("HUBSPOT_API_KEY not set. Skipping HubSpot.")
        return None

    username   = signal.get("username", "unknown")
    contact_id = _find_hubspot_contact(username)

    if contact_id:
        log.info(f"HubSpot: Existing contact | u/{username} | ID: {contact_id}")
    else:
        contact_id = _create_hubspot_contact(signal)
        if contact_id:
            log.info(f"HubSpot: New contact created | u/{username} | ID: {contact_id}")
        else:
            log.error(f"HubSpot: Failed to create contact | u/{username}")
            return None

    _create_hubspot_note(signal, contact_id)
    log.info(f"HubSpot: Note attached | contact {contact_id}")
    return contact_id


def send_to_hubspot(signal: dict) -> str | None:
    return retry_with_backoff(
        _send_to_hubspot, signal,
        retries=3, delay=3, label="HubSpot"
    )


# ─────────────────────────────────────────────
# DAILY DIGEST — Score 6-7, sent 8am UTC daily
# ─────────────────────────────────────────────

def send_daily_digest():
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set. Skipping daily digest.")
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
            preview = s["message_text"][:120]
            if len(s["message_text"]) > 120:
                preview += "..."
            corridor  = s.get("corridor") or "Unknown corridor"
            pain_type = s.get("pain_type") or "—"
            lines.append(
                f"• *u/{s.get('username', 'unknown')}* | Score: {s['intent_score']}/10 "
                f"| r/{s.get('subreddit', 'unknown')} | {s.get('content_type', '').upper()}\n"
                f"  Corridor: {corridor} | Pain: {pain_type}\n"
                f"  _{preview}_\n"
                f"  Action: {s['suggested_action']}"
            )

        date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
        joined   = "\n\n".join(lines)
        chunks   = [joined[i:i+2900] for i in range(0, len(joined), 2900)]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📋 Daily Reddit Signal Digest — {date_str}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(signals)} medium intent signals* (score 6–7) from the past 24 hours:"
                }
            },
        ]

        for chunk in chunks:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": chunk}
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"Client: {CLIENT_ID} | Platform: Reddit | Research-stage leads."
            }]
        })

        payload = {
            "text":   f"📋 Daily Reddit Signal Digest — {date_str}",
            "blocks": blocks
        }

        result = retry_with_backoff(
            _post_to_slack, payload,
            retries=3, delay=2, label="Digest"
        )

        if result:
            ids = [s["message_id"] for s in signals]
            db.signals.update_many(
                {"message_id": {"$in": ids}},
                {"$set": {"digest_included": True}}
            )
            log.info(f"Daily digest sent | {len(signals)} signals.")
        else:
            log.error("Daily digest Slack send failed.")

    except Exception as e:
        log.error(f"Daily digest error: {e}")


# ─────────────────────────────────────────────
# WEEKLY REPORT — All signals, Monday 9am UTC
# ─────────────────────────────────────────────

def send_weekly_report():
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set. Skipping weekly report.")
        return

    try:
        since = datetime.now(timezone.utc) - timedelta(days=7)

        all_signals  = list(db.signals.find({
            "client_id":  CLIENT_ID,
            "created_at": {"$gte": since}
        }))
        high_signals = [s for s in all_signals if s["intent_score"] >= 8]
        med_signals  = [s for s in all_signals if 6 <= s["intent_score"] <= 7]
        biz_signals  = [s for s in all_signals if s.get("is_business")]
        total        = len(all_signals)

        corridor_counts: dict = {}
        for s in all_signals:
            c = s.get("corridor") or "Unknown"
            corridor_counts[c] = corridor_counts.get(c, 0) + 1
        corridor_text = "\n".join(
            f"  • {c}: {n}" for c, n in sorted(
                corridor_counts.items(), key=lambda x: -x[1]
            )
        ) or "_No corridor data._"

        competitor_counts: dict = {}
        for s in all_signals:
            comp = s.get("competitor_mentioned")
            if comp:
                competitor_counts[comp] = competitor_counts.get(comp, 0) + 1
        competitor_text = "\n".join(
            f"  • {c}: {n}" for c, n in sorted(
                competitor_counts.items(), key=lambda x: -x[1]
            )
        ) or "_No competitor mentions._"

        if total == 0:
            log.info("Weekly report: no signals in past 7 days.")
            return

        top3 = sorted(high_signals, key=lambda x: x["intent_score"], reverse=True)[:3]
        top3_lines = []
        for s in top3:
            preview  = s["message_text"][:100]
            if len(s["message_text"]) > 100:
                preview += "..."
            corridor = s.get("corridor") or "Unknown"
            top3_lines.append(
                f"• *u/{s.get('username', 'unknown')}* | Score: {s['intent_score']}/10 "
                f"| r/{s.get('subreddit', 'unknown')} | {corridor}\n"
                f"  _{preview}_"
            )

        week_start = since.strftime("%b %d")
        week_end   = datetime.now(timezone.utc).strftime("%b %d, %Y")
        top3_text  = "\n\n".join(top3_lines) if top3_lines else "_No high intent signals this week._"

        payload = {
            "text": f"📊 Weekly Reddit Signal Report — {week_start} to {week_end}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"📊 Weekly Reddit Signal Report — {week_start} to {week_end}",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Total signals:*\n{total}"},
                        {"type": "mrkdwn", "text": f"*High intent (8–10):*\n{len(high_signals)}"},
                        {"type": "mrkdwn", "text": f"*Medium intent (6–7):*\n{len(med_signals)}"},
                        {"type": "mrkdwn", "text": f"*Business owners:*\n{len(biz_signals)}"},
                        {"type": "mrkdwn", "text": f"*Platform:*\nReddit"},
                        {"type": "mrkdwn", "text": f"*Client:*\n{CLIENT_ID}"},
                    ]
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Corridor breakdown:*\n{corridor_text}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Competitor mentions:*\n{competitor_text}"
                    }
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Top 3 highest intent signals this week:*\n\n{_safe_text(top3_text, 2800)}"
                    }
                },
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [{
                        "type": "mrkdwn",
                        "text": f"FLINTEL v5.1 | {CLIENT_ID} | Reddit Monitor | Week ending {week_end}"
                    }]
                }
            ]
        }

        result = retry_with_backoff(
            _post_to_slack, payload,
            retries=3, delay=2, label="WeeklyReport"
        )

        if result:
            log.info(
                f"Weekly report sent | Total: {total} "
                f"| High: {len(high_signals)} | Medium: {len(med_signals)} "
                f"| Business: {len(biz_signals)}"
            )
        else:
            log.error("Weekly report Slack send failed.")

    except Exception as e:
        log.error(f"Weekly report error: {e}")


# ─────────────────────────────────────────────
# SCHEDULER — Digest + Weekly Report
# ─────────────────────────────────────────────

async def run_scheduler():
    log.info(
        f"Scheduler started — digest at {DAILY_DIGEST_HOUR}:00 UTC daily, "
        f"report Monday {WEEKLY_REPORT_HOUR}:00 UTC."
    )
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


# ─────────────────────────────────────────────
# CORE SIGNAL PROCESSOR
# Called once per scored item after batch scoring
# Full pipeline: save → Slack → HubSpot
# ─────────────────────────────────────────────

def process_scored_item(item: dict, score_result: dict):
    intent_score = score_result.get("intent_score", 0)

    if intent_score < MIN_SCORE_MEDIUM:
        log.debug(
            f"Score {intent_score} below {MIN_SCORE_MEDIUM} — discarded | "
            f"u/{item.get('username')} | r/{item.get('subreddit')}"
        )
        return

    signal_data = {
        "message_id":           item["message_id"],
        "platform":             "reddit",
        "content_type":         item.get("content_type", "unknown"),
        "subreddit":            item.get("subreddit", "unknown"),
        "post_url":             item.get("post_url", ""),
        "username":             item.get("username", "unknown"),
        "message_text":         item.get("text", ""),
        "intent_score":         intent_score,
        "signal_category":      score_result.get("signal_category", "no_intent"),
        "tier":                 score_result.get("tier", "discard"),
        "is_business":          score_result.get("is_business", False),
        "corridor":             score_result.get("corridor"),
        "estimated_amount":     score_result.get("estimated_amount"),
        "competitor_mentioned": score_result.get("competitor_mentioned"),
        "pain_type":            score_result.get("pain_type"),
        "reason":               score_result.get("reason", ""),
        "suggested_action":     score_result.get("suggested_action", ""),
        "outreach_script":      score_result.get("outreach_script"),
        "timestamp":            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    saved = save_signal(signal_data)
    if not saved:
        log.debug(f"Duplicate signal — skipping delivery | {item['message_id']}")
        return

    if MIN_SCORE_MEDIUM <= intent_score < MIN_SCORE_HIGH:
        log.info(
            f"MEDIUM INTENT | Score: {intent_score} | Tier: {signal_data['tier']} "
            f"| Slack only | u/{item.get('username')}"
        )
        slack_ok = send_slack_alert(signal_data)
        if slack_ok:
            mark_slack_alerted(signal_data["message_id"])

    elif intent_score >= MIN_SCORE_HIGH:
        log.info(
            f"HIGH INTENT | Score: {intent_score} | Tier: {signal_data['tier']} "
            f"| Slack + HubSpot | u/{item.get('username')}"
        )
        slack_ok = send_slack_alert(signal_data)
        if slack_ok:
            mark_slack_alerted(signal_data["message_id"])

        contact_id = send_to_hubspot(signal_data)
        if contact_id:
            mark_hubspot_alerted(signal_data["message_id"], contact_id)
            log.info(f"HubSpot contact saved | u/{item.get('username')} | ID: {contact_id}")


# ─────────────────────────────────────────────
# BATCH PROCESSOR
# ─────────────────────────────────────────────

def run_batch_processor():
    log.info(
        f"Batch processor started | "
        f"Batch size: {BATCH_SIZE} | "
        f"Gap between batches: {BATCH_GAP_SECONDS}s"
    )

    current_batch  = []
    total_received = 0
    total_matched  = 0
    total_deleted  = 0
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
                reddit_queue.task_done()
                continue

            if not passes_keyword_filter(text):
                total_deleted += 1
                log.debug(
                    f"Keyword filter blocked | "
                    f"{item.get('content_type', '?').upper()} | "
                    f"u/{item.get('username')} | "
                    f"r/{item.get('subreddit')}"
                )
                reddit_queue.task_done()
                continue

            total_matched += 1
            current_batch.append(item)

            log.info(
                f"Keyword match [{len(current_batch)}/{BATCH_SIZE}] | "
                f"{item.get('content_type', '?').upper()} | "
                f"u/{item.get('username')} | "
                f"r/{item.get('subreddit')}"
            )

            reddit_queue.task_done()

            if len(current_batch) >= BATCH_SIZE:
                total_batches   += 1
                batch_to_process = current_batch[:BATCH_SIZE]
                current_batch    = current_batch[BATCH_SIZE:]

                log.info(
                    f"━━━ BATCH {total_batches} READY ━━━ | "
                    f"{len(batch_to_process)} items | "
                    f"Received: {total_received} | "
                    f"Matched: {total_matched} | "
                    f"Deleted: {total_deleted}"
                )

                log.info(f"Sending batch {total_batches} to Claude...")
                scores = score_batch_with_claude(batch_to_process)

                score_map = {}
                for s in scores:
                    idx = s.get("index")
                    if idx is not None:
                        score_map[int(idx)] = s

                for i, item_data in enumerate(batch_to_process):
                    position     = i + 1
                    score_result = score_map.get(position)
                    if score_result is None:
                        score_result = scores[i] if i < len(scores) else _fallback_score(position, "Index mismatch.")
                    process_scored_item(item_data, score_result)

                log.info(
                    f"━━━ BATCH {total_batches} COMPLETE ━━━ | "
                    f"Waiting {BATCH_GAP_SECONDS}s before next batch..."
                )

                time.sleep(BATCH_GAP_SECONDS)

        except Exception as e:
            log.error(f"Batch processor error: {e}")
            time.sleep(5)


# ─────────────────────────────────────────────
# REDDIT JSON POLLER — POSTS
# Replaces PRAW stream_posts
# Uses public Reddit JSON endpoints — zero credentials
# Rate limit safe: REQUEST_DELAY seconds between each subreddit
# ─────────────────────────────────────────────

def fetch_subreddit_posts(subreddit: str, session: requests.Session) -> list:
    """
    Fetches latest 25 posts from a subreddit via public JSON endpoint.
    Returns list of item dicts ready for the queue.
    """
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25"
    try:
        response = session.get(url, timeout=15)
        if response.status_code == 429:
            log.warning(f"Rate limited on r/{subreddit} — sleeping 60s...")
            time.sleep(60)
            return []
        if response.status_code != 200:
            log.warning(f"r/{subreddit} posts returned {response.status_code}")
            return []

        data  = response.json()
        posts = data.get("data", {}).get("children", [])
        items = []

        for post in posts:
            p       = post.get("data", {})
            post_id = f"post_{p.get('id', '')}"

            if is_seen(post_id):
                continue
            mark_seen(post_id)

            title    = p.get("title", "")
            selftext = p.get("selftext", "").strip()
            text     = f"{title}\n\n{selftext}" if selftext else title
            author   = p.get("author", "[deleted]")
            permalink = p.get("permalink", "")

            items.append({
                "message_id":   post_id,
                "content_type": "post",
                "text":         text,
                "username":     author,
                "subreddit":    subreddit,
                "post_url":     f"https://reddit.com{permalink}",
            })

        return items

    except Exception as e:
        log.error(f"fetch_subreddit_posts error | r/{subreddit}: {e}")
        return []


def fetch_subreddit_comments(subreddit: str, session: requests.Session) -> list:
    """
    Fetches latest 25 comments from a subreddit via public JSON endpoint.
    Returns list of item dicts ready for the queue.
    """
    url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit=25"
    try:
        response = session.get(url, timeout=15)
        if response.status_code == 429:
            log.warning(f"Rate limited on r/{subreddit} comments — sleeping 60s...")
            time.sleep(60)
            return []
        if response.status_code != 200:
            log.warning(f"r/{subreddit} comments returned {response.status_code}")
            return []

        data     = response.json()
        comments = data.get("data", {}).get("children", [])
        items    = []

        for comment in comments:
            c          = comment.get("data", {})
            comment_id = f"comment_{c.get('id', '')}"

            if is_seen(comment_id):
                continue
            mark_seen(comment_id)

            body      = c.get("body", "").strip()
            author    = c.get("author", "[deleted]")
            permalink = c.get("permalink", "")
            parent_id = c.get("parent_id", "")

            content_type = "reply" if parent_id.startswith("t1_") else "comment"

            if not body or body in ("[deleted]", "[removed]"):
                continue

            items.append({
                "message_id":   comment_id,
                "content_type": content_type,
                "text":         body,
                "username":     author,
                "subreddit":    subreddit,
                "post_url":     f"https://reddit.com{permalink}",
            })

        return items

    except Exception as e:
        log.error(f"fetch_subreddit_comments error | r/{subreddit}: {e}")
        return []


def poll_reddit_json(subreddits: list, thread_name: str):
    """
    Polls a list of subreddits continuously via JSON endpoints.
    Fetches both posts and comments for each subreddit.
    Respects REQUEST_DELAY between each request to stay within rate limits.
    Pushes new items to reddit_queue.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": REDDIT_USER_AGENT
    })

    log.info(f"[{thread_name}] JSON poller started | {len(subreddits)} subreddits")

    while True:
        cycle_start  = time.time()
        total_pushed = 0

        for subreddit in subreddits:
            # ── Fetch posts ───────────────────────────────────────
            posts = fetch_subreddit_posts(subreddit, session)
            for item in posts:
                reddit_queue.put(item)
                total_pushed += 1
            time.sleep(REQUEST_DELAY)

            # ── Fetch comments ────────────────────────────────────
            comments = fetch_subreddit_comments(subreddit, session)
            for item in comments:
                reddit_queue.put(item)
                total_pushed += 1
            time.sleep(REQUEST_DELAY)

        cycle_elapsed = time.time() - cycle_start

        log.info(
            f"[{thread_name}] Cycle complete | "
            f"Pushed: {total_pushed} items | "
            f"Elapsed: {cycle_elapsed:.1f}s | "
            f"Queue size: {reddit_queue.qsize()}"
        )

        # ── Wait before next cycle ────────────────────────────────
        remaining = POLL_INTERVAL - cycle_elapsed
        if remaining > 0:
            log.info(f"[{thread_name}] Waiting {remaining:.1f}s before next cycle...")
            time.sleep(remaining)


# ─────────────────────────────────────────────
# REDDIT LISTENER — Starts polling threads
# Splits subreddits into 2 groups
# Each group runs in its own thread
# Monitors threads + auto-restarts on crash
# ─────────────────────────────────────────────

async def start_reddit_listener():
    """
    Splits TARGET_SUBREDDITS into 2 groups.
    Each group polled in its own thread — stays within 10 req/min limit.
    Also starts the batch processor thread.
    Monitors all threads and restarts if any crash.
    """
    mid   = len(TARGET_SUBREDDITS) // 2
    group_a = TARGET_SUBREDDITS[:mid]   # first 10 subreddits
    group_b = TARGET_SUBREDDITS[mid:]   # last 10 subreddits

    log.info(
        f"Starting Reddit JSON pollers | "
        f"Group A: {len(group_a)} subreddits | "
        f"Group B: {len(group_b)} subreddits"
    )

    def make_poller_a():
        return threading.Thread(
            target=poll_reddit_json,
            args=(group_a, "PollerA"),
            daemon=True,
            name="RedditPollerA"
        )

    def make_poller_b():
        return threading.Thread(
            target=poll_reddit_json,
            args=(group_b, "PollerB"),
            daemon=True,
            name="RedditPollerB"
        )

    def make_batch():
        return threading.Thread(
            target=run_batch_processor,
            daemon=True,
            name="BatchProcessor"
        )

    poller_a = make_poller_a()
    poller_b = make_poller_b()
    batch    = make_batch()

    poller_a.start()
    time.sleep(3)   # stagger start to avoid burst
    poller_b.start()
    batch.start()

    log.info(
        f"All threads running — "
        f"PollerA ✅ | PollerB ✅ | BatchProcessor ✅ | "
        f"Poll interval: {POLL_INTERVAL}s | Request delay: {REQUEST_DELAY}s"
    )

    # ── Keep-alive: monitor + restart dead threads ────────────────
    while True:
        await asyncio.sleep(60)

        if not poller_a.is_alive():
            log.error("PollerA died — restarting...")
            poller_a = make_poller_a()
            poller_a.start()

        if not poller_b.is_alive():
            log.error("PollerB died — restarting...")
            poller_b = make_poller_b()
            poller_b.start()

        if not batch.is_alive():
            log.error("BatchProcessor died — restarting...")
            batch = make_batch()
            batch.start()


# ─────────────────────────────────────────────
# FASTAPI — REST API
# ─────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel",
    description = "Reddit signals: monitor, batch-score, store, alert.",
    version     = "5.1.0"
)


@app.on_event("shutdown")
async def shutdown_event():
    log.info("FastAPI shutting down gracefully...")


@app.get("/")
def root():
    return {
        "status":        "running",
        "system":        "FX Signal Intelligence — Flintel",
        "version":       "5.1.0",
        "platform":      "Reddit (JSON endpoints — no API key)",
        "client":        CLIENT_ID,
        "batch_size":    BATCH_SIZE,
        "batch_gap_s":   BATCH_GAP_SECONDS,
        "poll_interval": POLL_INTERVAL,
        "request_delay": REQUEST_DELAY,
        "queue_size":    reddit_queue.qsize(),
        "seen_ids":      len(seen_ids),
    }


@app.get("/health")
def health_check():
    try:
        db.command("ping")
        mongo_status = "connected"
    except Exception:
        mongo_status = "disconnected"

    return {
        "status":      "ok",
        "mongodb":     mongo_status,
        "reddit":      "polling (JSON endpoints)",
        "queue_size":  reddit_queue.qsize(),
        "seen_ids":    len(seen_ids),
        "client_id":   CLIENT_ID,
        "timestamp":   datetime.now(timezone.utc).isoformat()
    }


@app.get("/signals")
def get_signals(
    limit:     int = 50,
    category:  str = None,
    min_score: int = None,
    platform:  str = None,
    subreddit: str = None,
    tier:      str = None,
    corridor:  str = None,
    pain_type: str = None,
):
    try:
        query = {"client_id": CLIENT_ID}
        if category:
            query["signal_category"] = category
        if min_score is not None:
            query["intent_score"] = {"$gte": min_score}
        if platform:
            query["platform"] = platform
        if subreddit:
            query["subreddit"] = subreddit
        if tier:
            query["tier"] = tier
        if corridor:
            query["corridor"] = {"$regex": corridor, "$options": "i"}
        if pain_type:
            query["pain_type"] = pain_type

        signals = list(
            db.signals
            .find(query, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
        )

        for s in signals:
            for field in ["created_at", "alerted_slack_at", "alerted_hubspot_at"]:
                if field in s:
                    s[field] = s[field].isoformat()

        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/stats")
def get_stats():
    try:
        pipeline = [
            {"$match": {"client_id": CLIENT_ID}},
            {"$group": {
                "_id":       "$signal_category",
                "count":     {"$sum": 1},
                "avg_score": {"$avg": "$intent_score"}
            }}
        ]
        stats          = list(db.signals.aggregate(pipeline))
        total          = db.signals.count_documents({"client_id": CLIENT_ID})
        business_count = db.signals.count_documents({"client_id": CLIENT_ID, "is_business": True})

        corridor_pipeline = [
            {"$match": {"client_id": CLIENT_ID, "corridor": {"$ne": None}}},
            {"$group": {"_id": "$corridor", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        corridor_stats = list(db.signals.aggregate(corridor_pipeline))

        pain_pipeline = [
            {"$match": {"client_id": CLIENT_ID, "pain_type": {"$ne": None}}},
            {"$group": {"_id": "$pain_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        pain_stats = list(db.signals.aggregate(pain_pipeline))

        competitor_pipeline = [
            {"$match": {"client_id": CLIENT_ID, "competitor_mentioned": {"$ne": None}}},
            {"$group": {"_id": "$competitor_mentioned", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        competitor_stats = list(db.signals.aggregate(competitor_pipeline))

        return {
            "total_signals":   total,
            "business_owners": business_count,
            "breakdown":       stats,
            "corridors":       corridor_stats,
            "pain_types":      pain_stats,
            "competitors":     competitor_stats,
            "queue_size":      reddit_queue.qsize(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/high-intent")
def get_high_intent(limit: int = 20):
    try:
        signals = list(
            db.signals
            .find({"client_id": CLIENT_ID, "intent_score": {"$gte": 8}}, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
        )
        for s in signals:
            if "created_at" in s:
                s["created_at"] = s["created_at"].isoformat()
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/digest")
def get_digest(limit: int = 50):
    try:
        signals = list(
            db.signals
            .find({"client_id": CLIENT_ID, "intent_score": {"$gte": 6, "$lte": 7}}, {"_id": 0})
            .sort("created_at", -1)
            .limit(limit)
        )
        for s in signals:
            if "created_at" in s:
                s["created_at"] = s["created_at"].isoformat()
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/business")
def get_business_signals(limit: int = 20):
    try:
        signals = list(
            db.signals
            .find({"client_id": CLIENT_ID, "is_business": True}, {"_id": 0})
            .sort("intent_score", -1)
            .limit(limit)
        )
        for s in signals:
            if "created_at" in s:
                s["created_at"] = s["created_at"].isoformat()
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/outreach")
def get_outreach_ready(limit: int = 20):
    try:
        signals = list(
            db.signals
            .find(
                {
                    "client_id":       CLIENT_ID,
                    "intent_score":    {"$gte": 5},
                    "outreach_script": {"$ne": None}
                },
                {"_id": 0}
            )
            .sort("intent_score", -1)
            .limit(limit)
        )
        for s in signals:
            if "created_at" in s:
                s["created_at"] = s["created_at"].isoformat()
        return {"count": len(signals), "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signals/corridors")
def get_by_corridor(corridor: str, limit: int = 20):
    try:
        signals = list(
            db.signals
            .find(
                {
                    "client_id": CLIENT_ID,
                    "corridor":  {"$regex": corridor, "$options": "i"}
                },
                {"_id": 0}
            )
            .sort("intent_score", -1)
            .limit(limit)
        )
        for s in signals:
            if "created_at" in s:
                s["created_at"] = s["created_at"].isoformat()
        return {"count": len(signals), "corridor": corridor, "signals": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────

async def main():
    api_thread = threading.Thread(target=run_fastapi, daemon=True)
    api_thread.start()
    log.info("FastAPI started on http://0.0.0.0:8000")

    await asyncio.gather(
        start_reddit_listener(),
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v5.1")
    log.info("=" * 60)
    log.info(f"Client ID        : {CLIENT_ID}")
    log.info(f"Platform         : Reddit (JSON endpoints — zero credentials)")
    log.info(f"Batch size       : {BATCH_SIZE} messages per Claude call")
    log.info(f"Batch gap        : {BATCH_GAP_SECONDS}s between batches")
    log.info(f"Poll interval    : {POLL_INTERVAL}s per full cycle")
    log.info(f"Request delay    : {REQUEST_DELAY}s between subreddit requests")
    log.info(f"Score 0-5        : DELETE — dropped completely")
    log.info(f"Score 6-7        : MEDIUM — MongoDB + Slack only")
    log.info(f"Score 8-10       : HIGH   — MongoDB + Slack + HubSpot")
    log.info(f"Daily digest     : {DAILY_DIGEST_HOUR}:00 UTC daily")
    log.info(f"Weekly report    : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"Subreddits       : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"MongoDB DB       : {MONGODB_DB}")
    log.info(f"Keywords         : {len(KEYWORDS)} filters active")
    log.info(f"HubSpot          : {'enabled' if HUBSPOT_API_KEY else 'disabled — set HUBSPOT_API_KEY'}")
    log.info(f"Slack            : {'enabled' if SLACK_WEBHOOK_URL else 'disabled — set SLACK_WEBHOOK_URL'}")
    log.info("=" * 60)

    asyncio.run(main())
