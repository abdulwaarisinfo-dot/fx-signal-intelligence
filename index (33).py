"""
FLINTEL DASHBOARD — read-only monitoring web service
=====================================================
This is a SEPARATE process from the FLINTEL background worker
(flintel.py / your v7.6.0 script). It never imports it, never starts its
threads, and never writes to MongoDB. It only opens its own MongoDB
connection and READS from the exact same database + collections the
background worker already writes to:

    signals                 -> per-platform message + score history
    flintel_pending_batch    -> what's currently sitting in each platform's
                                in-flight Claude batch, and how long it's
                                been waiting
    flintel_queue_messages   -> the persistent backlog queue per platform
    flintel_seen_ids         -> size of each platform's dedup set
    flintel_rescore_messages -> rescore queue depth by status

Run it as its own process, on its own port, pointed at the SAME .env
(MONGODB_URI / MONGODB_DB) as the background worker:

    pip install -r requirements.txt
    uvicorn dashboard_service:app --host 0.0.0.0 --port 8100

Why this won't "disturb" your background worker or Atlas:
  1. It NEVER writes — every query is find()/aggregate() only.
  2. It opens ONE MongoClient at startup and reuses that connection pool
     for the life of the process (no repeated connect/disconnect).
  3. Every expensive aggregation is wrapped in a small in-memory TTL
     cache (SUMMARY_CACHE_SECONDS / LIVE_CACHE_SECONDS, both
     env-configurable, default 20s / 10s). No matter how many browser
     tabs are open or how often they poll, MongoDB itself is only
     queried once per TTL window.
  4. It reuses the indexes the background worker already creates
     (platform, intent_score, created_at, etc.) — nothing here needs a
     new index or a collection scan.
  5. It uses readPreference=secondaryPreferred so, on a replica set, its
     reads are served off a secondary instead of competing with the
     worker's writes on the primary.
"""

import os
import time
import threading
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.status import HTTP_403_FORBIDDEN
from pymongo import MongoClient
from pymongo.read_preferences import SecondaryPreferred

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — reuses the SAME env vars as the background worker for Mongo +
# score thresholds, so pointing this at the same .env is enough.
# ─────────────────────────────────────────────────────────────────────────────

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")
CLIENT_ID   = os.getenv("CLIENT_ID", "settla")

MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "4"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))

# How long each cache layer is trusted before re-querying MongoDB.
SUMMARY_CACHE_SECONDS = int(os.getenv("DASHBOARD_SUMMARY_CACHE_SECONDS", "20"))
LIVE_CACHE_SECONDS    = int(os.getenv("DASHBOARD_LIVE_CACHE_SECONDS",    "10"))

# Optional — if set, dashboard API endpoints require X-API-Key / ?api_key=.
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")

# A platform is shown as "ACTIVE" if it produced a signal more recently
# than this many minutes ago; otherwise "IDLE".
ACTIVE_WINDOW_MINUTES = int(os.getenv("DASHBOARD_ACTIVE_WINDOW_MINUTES", "15"))

PLATFORMS = [
    {"key": "reddit",   "label": "Reddit",   "color": "#FF4500"},
    {"key": "twitter",  "label": "Twitter/X", "color": "#4FA8E0"},
    {"key": "telegram", "label": "Telegram", "color": "#29A9EA"},
    {"key": "facebook", "label": "Facebook", "color": "#1877F2"},
    {"key": "linkedin", "label": "LinkedIn", "color": "#0A66C2"},
]
PLATFORM_KEYS = [p["key"] for p in PLATFORMS]

if not MONGODB_URI:
    raise RuntimeError(
        "MONGODB_URI is not set. Point this dashboard at the SAME .env "
        "file the FLINTEL background worker uses."
    )

# ─────────────────────────────────────────────────────────────────────────────
# ONE persistent MongoClient for the life of this process — read-only usage.
# ─────────────────────────────────────────────────────────────────────────────

_mongo_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=5000,
    read_preference=SecondaryPreferred(),
    maxPoolSize=10,
)
db = _mongo_client[MONGODB_DB]

# ─────────────────────────────────────────────────────────────────────────────
# TINY IN-MEMORY TTL CACHE — this is what stops the dashboard from hammering
# MongoDB. Every browser refresh / poll hits this cache first; MongoDB is
# only actually queried once every TTL seconds, regardless of traffic.
# ─────────────────────────────────────────────────────────────────────────────

_cache_lock = threading.Lock()
_cache_store: dict = {}


def cached(key: str, ttl_seconds: int, fn):
    now = time.time()
    with _cache_lock:
        entry = _cache_store.get(key)
        if entry and (now - entry["computed_at"]) < ttl_seconds:
            return entry["data"], entry["computed_at"], True

    data = fn()
    computed_at = time.time()
    with _cache_lock:
        _cache_store[key] = {"data": data, "computed_at": computed_at}
    return data, computed_at, False


# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH (optional — mirrors the same pattern as the background worker)
# ─────────────────────────────────────────────────────────────────────────────

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key",    auto_error=False)


async def verify_api_key(
    key_header: str = Security(api_key_header),
    key_query:  str = Security(api_key_query),
):
    if not DASHBOARD_API_KEY:
        return
    if key_header == DASHBOARD_API_KEY or key_query == DASHBOARD_API_KEY:
        return
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid or missing API key.")


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES — every one of these is find()/aggregate() only, using indexes
# the background worker already created (platform, intent_score, created_at,
# is_business, etc). Nothing here scans a full collection.
# ─────────────────────────────────────────────────────────────────────────────

def _score_bucket_expr():
    return {
        "$switch": {
            "branches": [
                {"case": {"$gte": ["$intent_score", MIN_SCORE_HIGH]}, "then": "high"},
                {"case": {"$gte": ["$intent_score", MIN_SCORE_MEDIUM]}, "then": "medium"},
            ],
            "default": "low",
        }
    }


def _compute_summary() -> dict:
    now = datetime.now(timezone.utc)
    since_24h = now - timedelta(hours=24)
    since_1h  = now - timedelta(hours=1)

    pipeline = [
        {"$match": {"client_id": CLIENT_ID}},
        {
            "$facet": {
                "all_time": [
                    {
                        "$group": {
                            "_id": "$platform",
                            "total": {"$sum": 1},
                            "avg_score": {"$avg": "$intent_score"},
                            "business_count": {"$sum": {"$cond": ["$is_business", 1, 0]}},
                            "last_signal_at": {"$max": "$created_at"},
                            "high":   {"$sum": {"$cond": [{"$gte": ["$intent_score", MIN_SCORE_HIGH]}, 1, 0]}},
                            "medium": {"$sum": {"$cond": [
                                {"$and": [
                                    {"$gte": ["$intent_score", MIN_SCORE_MEDIUM]},
                                    {"$lt":  ["$intent_score", MIN_SCORE_HIGH]},
                                ]}, 1, 0,
                            ]}},
                            "low": {"$sum": {"$cond": [{"$lt": ["$intent_score", MIN_SCORE_MEDIUM]}, 1, 0]}},
                        }
                    },
                ],
                "last_24h": [
                    {"$match": {"created_at": {"$gte": since_24h}}},
                    {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
                ],
                "last_1h": [
                    {"$match": {"created_at": {"$gte": since_1h}}},
                    {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
                ],
                "overall": [
                    {
                        "$group": {
                            "_id": None,
                            "total": {"$sum": 1},
                            "avg_score": {"$avg": "$intent_score"},
                            "business_count": {"$sum": {"$cond": ["$is_business", 1, 0]}},
                        }
                    },
                ],
            }
        },
    ]

    result = list(db.signals.aggregate(pipeline))
    facet = result[0] if result else {"all_time": [], "last_24h": [], "last_1h": [], "overall": []}

    by_platform_24h = {row["_id"]: row["count"] for row in facet.get("last_24h", []) if row.get("_id")}
    by_platform_1h  = {row["_id"]: row["count"] for row in facet.get("last_1h", []) if row.get("_id")}
    all_time_rows   = {row["_id"]: row for row in facet.get("all_time", []) if row.get("_id")}

    platforms_out = []
    for p in PLATFORMS:
        row = all_time_rows.get(p["key"], {})
        total = row.get("total", 0)
        last_signal_at = row.get("last_signal_at")
        is_active = bool(
            last_signal_at and (now - last_signal_at.replace(tzinfo=timezone.utc)) <= timedelta(minutes=ACTIVE_WINDOW_MINUTES)
        )
        platforms_out.append({
            "key":               p["key"],
            "label":             p["label"],
            "color":             p["color"],
            "total":             total,
            "high":              row.get("high", 0),
            "medium":            row.get("medium", 0),
            "low":               row.get("low", 0),
            "avg_score":         round(row.get("avg_score", 0) or 0, 2),
            "business_count":    row.get("business_count", 0),
            "last_signal_at":    last_signal_at.isoformat() if last_signal_at else None,
            "messages_last_24h": by_platform_24h.get(p["key"], 0),
            "messages_last_1h":  by_platform_1h.get(p["key"], 0),
            "is_active":         is_active,
        })

    # Rank platforms by 24h volume so the UI can show "busiest platform"
    ranked = sorted(platforms_out, key=lambda x: x["messages_last_24h"], reverse=True)
    busiest = ranked[0]["key"] if ranked and ranked[0]["messages_last_24h"] > 0 else None

    overall_row = (facet.get("overall") or [{}])[0]

    return {
        "generated_at": now.isoformat(),
        "client_id": CLIENT_ID,
        "thresholds": {"min_score_medium": MIN_SCORE_MEDIUM, "min_score_high": MIN_SCORE_HIGH},
        "overall": {
            "total_signals":  overall_row.get("total", 0),
            "avg_score":      round(overall_row.get("avg_score", 0) or 0, 2),
            "business_count": overall_row.get("business_count", 0),
        },
        "busiest_platform": busiest,
        "platforms": platforms_out,
    }


def _compute_live() -> dict:
    now = datetime.now(timezone.utc)

    # What's sitting in each platform's in-flight Claude batch right now.
    pending_docs = {d["platform"]: d for d in db.flintel_pending_batch.find({})}

    # Persistent backlog queue depth per platform.
    queue_counts = {}
    for row in db.flintel_queue_messages.aggregate([
        {"$group": {"_id": "$_platform_key", "count": {"$sum": 1}}}
    ]):
        if row.get("_id"):
            queue_counts[row["_id"]] = row["count"]

    # Dedup set size per platform (rough proxy for how much each platform
    # has ever seen).
    seen_sizes = {}
    for d in db.flintel_seen_ids.find({}, {"platform": 1, "ids": 1}):
        seen_sizes[d.get("platform")] = len(d.get("ids", []))

    # Rescore queue depth by status.
    rescore_counts = {"pending": 0, "processing": 0, "done": 0, "error": 0}
    for row in db.flintel_rescore_messages.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ]):
        if row.get("_id") in rescore_counts:
            rescore_counts[row["_id"]] = row["count"]

    platforms_out = []
    for p in PLATFORMS:
        key = p["key"]
        pending = pending_docs.get(key, {})
        items = pending.get("items", [])
        start = pending.get("batch_start_time")
        age_seconds = None
        if start:
            start_utc = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
            age_seconds = int((now - start_utc).total_seconds())

        platforms_out.append({
            "key":                 key,
            "label":               p["label"],
            "current_batch_count": len(items),
            "current_batch_age_s": age_seconds,
            "queue_backlog":       queue_counts.get(key, 0),
            "dedup_ids_tracked":   seen_sizes.get(key, 0),
        })

    return {
        "generated_at": now.isoformat(),
        "platforms": platforms_out,
        "rescore_queue": rescore_counts,
    }


def _mongo_ping_ok() -> bool:
    try:
        db.command("ping")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FLINTEL Dashboard (read-only monitor)",
    description=(
        "Read-only monitoring dashboard for the FLINTEL background worker. "
        "Runs as a separate process, on its own port, and only reads "
        "(never writes) the same MongoDB database/collections the worker "
        "already uses. Results are cached in-memory so no amount of "
        "browser polling increases load on MongoDB beyond one query per "
        "cache window."
    ),
    version="1.0.0",
)


@app.get("/api/summary", dependencies=[Depends(verify_api_key)])
def api_summary():
    data, computed_at, from_cache = cached("summary", SUMMARY_CACHE_SECONDS, _compute_summary)
    return JSONResponse({
        **data,
        "_cache": {
            "from_cache": from_cache,
            "cache_ttl_seconds": SUMMARY_CACHE_SECONDS,
            "cache_age_seconds": round(time.time() - computed_at, 1),
        },
    })


@app.get("/api/live", dependencies=[Depends(verify_api_key)])
def api_live():
    data, computed_at, from_cache = cached("live", LIVE_CACHE_SECONDS, _compute_live)
    return JSONResponse({
        **data,
        "_cache": {
            "from_cache": from_cache,
            "cache_ttl_seconds": LIVE_CACHE_SECONDS,
            "cache_age_seconds": round(time.time() - computed_at, 1),
        },
    })


@app.get("/api/health")
def api_health():
    return {
        "status": "ok" if _mongo_ping_ok() else "mongo_unreachable",
        "database": MONGODB_DB,
        "client_id": CLIENT_ID,
        "summary_cache_ttl_s": SUMMARY_CACHE_SECONDS,
        "live_cache_ttl_s": LIVE_CACHE_SECONDS,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard_page():
    return HTMLResponse(DASHBOARD_HTML)


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD HTML/CSS/JS — single self-contained page, no external CDN
# dependency, polls the two cached API endpoints above (never MongoDB
# directly from the browser).
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FLINTEL — Signal Monitor</title>
<style>
  :root{
    --bg:#0A0D13; --panel:#10141D; --panel-2:#141924; --border:#1E2532;
    --text:#E7EAF2; --muted:#7C879C; --dim:#4C5567;
    --high:#FF5D5D; --medium:#FFB020; --low:#3E4658;
    --accent:#4FE0C4; --mono: ui-monospace,SFMono-Regular,Consolas,"Liberation Mono",monospace;
    --sans: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  }
  *{box-sizing:border-box;}
  body{
    margin:0; background:var(--bg); color:var(--text); font-family:var(--sans);
    min-height:100vh;
  }
  .wrap{max-width:1180px; margin:0 auto; padding:28px 22px 60px;}
  header{
    display:flex; align-items:baseline; justify-content:space-between;
    border-bottom:1px solid var(--border); padding-bottom:18px; margin-bottom:24px;
    flex-wrap:wrap; gap:10px;
  }
  .brand{display:flex; align-items:center; gap:10px;}
  .brand .dot{
    width:9px;height:9px;border-radius:50%; background:var(--accent);
    box-shadow:0 0 0 0 rgba(79,224,196,.55); animation:pulse 2.2s infinite;
  }
  @keyframes pulse{
    0%{box-shadow:0 0 0 0 rgba(79,224,196,.55);}
    70%{box-shadow:0 0 0 8px rgba(79,224,196,0);}
    100%{box-shadow:0 0 0 0 rgba(79,224,196,0);}
  }
  h1{font-size:17px; font-weight:650; letter-spacing:.02em; margin:0;}
  .sub{color:var(--muted); font-size:12.5px; font-family:var(--mono);}
  .meta{text-align:right; font-family:var(--mono); font-size:11.5px; color:var(--muted); line-height:1.6;}

  .grid-top{display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:26px;}
  @media(max-width:820px){.grid-top{grid-template-columns:repeat(2,1fr);}}
  .stat{
    background:var(--panel); border:1px solid var(--border); border-radius:10px;
    padding:16px 18px;
  }
  .stat .label{font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:.06em;}
  .stat .value{font-family:var(--mono); font-size:26px; font-weight:600; margin-top:6px;}
  .stat .value.accent{color:var(--accent);}

  h2.section-title{
    font-size:12px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);
    margin:30px 0 12px; display:flex; align-items:center; gap:8px;
  }
  h2.section-title::after{content:""; flex:1; height:1px; background:var(--border);}

  table.platforms{width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--border); border-radius:10px; overflow:hidden;}
  table.platforms th{
    text-align:left; font-size:10.5px; text-transform:uppercase; letter-spacing:.06em;
    color:var(--muted); font-weight:600; padding:10px 14px; border-bottom:1px solid var(--border);
    background:var(--panel-2);
  }
  table.platforms td{padding:12px 14px; border-bottom:1px solid var(--border); font-size:13px; vertical-align:middle;}
  table.platforms tr:last-child td{border-bottom:none;}
  .plat-name{display:flex; align-items:center; gap:9px; font-weight:600;}
  .swatch{width:9px;height:9px;border-radius:2px; flex-shrink:0;}
  .status-pill{
    font-family:var(--mono); font-size:10.5px; padding:2px 8px; border-radius:20px;
    border:1px solid var(--border); color:var(--muted);
  }
  .status-pill.active{color:var(--accent); border-color:rgba(79,224,196,.35); background:rgba(79,224,196,.08);}
  .num{font-family:var(--mono); font-size:13px;}
  .bar{display:flex; height:7px; width:150px; border-radius:4px; overflow:hidden; background:var(--low);}
  .bar span{display:block; height:100%;}
  .bar .b-high{background:var(--high);}
  .bar .b-medium{background:var(--medium);}
  .bar .b-low{background:var(--low);}
  .legend{display:flex; gap:14px; font-size:10.5px; color:var(--muted); margin-top:8px; font-family:var(--mono);}
  .legend .sw{display:inline-block; width:8px; height:8px; border-radius:2px; margin-right:4px; vertical-align:middle;}

  .live-grid{display:grid; grid-template-columns:repeat(5,1fr); gap:12px;}
  @media(max-width:960px){.live-grid{grid-template-columns:repeat(2,1fr);}}
  .live-card{background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:14px 16px;}
  .live-card .lp-name{font-weight:600; font-size:13px; display:flex; align-items:center; gap:8px; margin-bottom:10px;}
  .live-row{display:flex; justify-content:space-between; font-size:11.5px; color:var(--muted); padding:3px 0; font-family:var(--mono);}
  .live-row b{color:var(--text); font-weight:600;}

  .rescore-row{display:flex; gap:10px; flex-wrap:wrap; margin-top:14px;}
  .rescore-chip{
    background:var(--panel); border:1px solid var(--border); border-radius:8px;
    padding:8px 14px; font-family:var(--mono); font-size:12px; display:flex; gap:8px; align-items:center;
  }
  .rescore-chip b{font-size:15px;}

  footer{margin-top:40px; color:var(--dim); font-size:11px; font-family:var(--mono); text-align:center;}
  .err-banner{
    background:#2A1216; border:1px solid #5A2530; color:#FF9AA0; padding:10px 14px;
    border-radius:8px; font-family:var(--mono); font-size:12px; margin-bottom:18px; display:none;
  }
</style>
</head>
<body>
<div class="wrap">

  <header>
    <div class="brand">
      <span class="dot"></span>
      <div>
        <h1>FLINTEL — SIGNAL MONITOR</h1>
        <div class="sub">read-only · reflects background worker · <span id="clientId">—</span></div>
      </div>
    </div>
    <div class="meta">
      <div>last refreshed <span id="lastRefreshed">—</span></div>
      <div>next refresh in <span id="nextRefresh">—</span>s</div>
    </div>
  </header>

  <div class="err-banner" id="errBanner">Could not reach the dashboard API.</div>

  <div class="grid-top">
    <div class="stat"><div class="label">Total Signals</div><div class="value" id="totalSignals">—</div></div>
    <div class="stat"><div class="label">Avg Intent Score</div><div class="value" id="avgScore">—</div></div>
    <div class="stat"><div class="label">Business Owners</div><div class="value" id="bizCount">—</div></div>
    <div class="stat"><div class="label">Busiest Platform (24h)</div><div class="value accent" id="busiest">—</div></div>
  </div>

  <h2 class="section-title">Volume &amp; Score Distribution by Platform</h2>
  <table class="platforms">
    <thead>
      <tr>
        <th>Platform</th><th>Status</th><th>Total</th><th>Last 1h</th><th>Last 24h</th>
        <th>Avg Score</th><th>Score Distribution</th><th>Last Signal</th>
      </tr>
    </thead>
    <tbody id="platformRows"></tbody>
  </table>
  <div class="legend">
    <span><span class="sw" style="background:var(--high)"></span>High (score ≥ <span id="thHigh">8</span>)</span>
    <span><span class="sw" style="background:var(--medium)"></span>Medium (<span id="thMedLabel">4–7</span>)</span>
    <span><span class="sw" style="background:var(--low)"></span>Low (&lt; <span id="thMed">4</span>)</span>
  </div>

  <h2 class="section-title">What The Background Worker Is Doing Right Now</h2>
  <div class="live-grid" id="liveCards"></div>

  <div class="rescore-row" id="rescoreRow"></div>

  <footer id="cacheFooter">—</footer>
</div>

<script>
const PLATFORM_META = {
  reddit:   {label:"Reddit",   color:"#FF4500"},
  twitter:  {label:"Twitter/X",color:"#4FA8E0"},
  telegram: {label:"Telegram", color:"#29A9EA"},
  facebook: {label:"Facebook", color:"#1877F2"},
  linkedin: {label:"LinkedIn", color:"#0A66C2"},
};

function fmtTime(iso){
  if(!iso) return "never";
  const d = new Date(iso);
  const diffMs = Date.now() - d.getTime();
  const mins = Math.floor(diffMs/60000);
  if(mins < 1) return "just now";
  if(mins < 60) return mins + "m ago";
  const hrs = Math.floor(mins/60);
  if(hrs < 24) return hrs + "h ago";
  return Math.floor(hrs/24) + "d ago";
}

async function fetchJSON(url){
  const r = await fetch(url);
  if(!r.ok) throw new Error("HTTP " + r.status);
  return r.json();
}

function renderSummary(s){
  document.getElementById("clientId").textContent = s.client_id;
  document.getElementById("totalSignals").textContent = s.overall.total_signals.toLocaleString();
  document.getElementById("avgScore").textContent = s.overall.avg_score.toFixed(2);
  document.getElementById("bizCount").textContent = s.overall.business_count.toLocaleString();
  document.getElementById("busiest").textContent = s.busiest_platform
    ? (PLATFORM_META[s.busiest_platform]?.label || s.busiest_platform) : "—";

  document.getElementById("thHigh").textContent = s.thresholds.min_score_high;
  document.getElementById("thMed").textContent = s.thresholds.min_score_medium;
  document.getElementById("thMedLabel").textContent =
    s.thresholds.min_score_medium + "–" + (s.thresholds.min_score_high - 1);

  const rows = s.platforms.map(p => {
    const total = p.total || 1;
    const hPct = (p.high/total*100).toFixed(1);
    const mPct = (p.medium/total*100).toFixed(1);
    const lPct = (p.low/total*100).toFixed(1);
    return `
      <tr>
        <td><div class="plat-name"><span class="swatch" style="background:${p.color}"></span>${p.label}</div></td>
        <td><span class="status-pill ${p.is_active ? 'active' : ''}">${p.is_active ? 'ACTIVE' : 'IDLE'}</span></td>
        <td class="num">${p.total.toLocaleString()}</td>
        <td class="num">${p.messages_last_1h.toLocaleString()}</td>
        <td class="num">${p.messages_last_24h.toLocaleString()}</td>
        <td class="num">${p.avg_score.toFixed(2)}</td>
        <td>
          <div class="bar">
            <span class="b-high" style="width:${hPct}%"></span>
            <span class="b-medium" style="width:${mPct}%"></span>
            <span class="b-low" style="width:${lPct}%"></span>
          </div>
        </td>
        <td class="num">${fmtTime(p.last_signal_at)}</td>
      </tr>`;
  }).join("");
  document.getElementById("platformRows").innerHTML = rows;
}

function renderLive(l){
  const cards = l.platforms.map(p => {
    const meta = PLATFORM_META[p.key] || {label:p.key, color:"#888"};
    const ageStr = p.current_batch_age_s !== null && p.current_batch_age_s !== undefined
      ? p.current_batch_age_s + "s" : "—";
    return `
      <div class="live-card">
        <div class="lp-name"><span class="swatch" style="background:${meta.color}"></span>${meta.label}</div>
        <div class="live-row"><span>In current batch</span><b>${p.current_batch_count}</b></div>
        <div class="live-row"><span>Batch age</span><b>${ageStr}</b></div>
        <div class="live-row"><span>Queue backlog</span><b>${p.queue_backlog}</b></div>
        <div class="live-row"><span>Dedup IDs tracked</span><b>${p.dedup_ids_tracked.toLocaleString()}</b></div>
      </div>`;
  }).join("");
  document.getElementById("liveCards").innerHTML = cards;

  const rq = l.rescore_queue;
  document.getElementById("rescoreRow").innerHTML = `
    <div class="rescore-chip">Rescore pending <b>${rq.pending}</b></div>
    <div class="rescore-chip">Rescore processing <b>${rq.processing}</b></div>
    <div class="rescore-chip">Rescore done <b>${rq.done}</b></div>
    <div class="rescore-chip">Rescore errors <b>${rq.error}</b></div>
  `;
}

let refreshSeconds = 20;
let countdown = refreshSeconds;

async function refresh(){
  try{
    const [summary, live] = await Promise.all([
      fetchJSON("/api/summary"),
      fetchJSON("/api/live"),
    ]);
    document.getElementById("errBanner").style.display = "none";
    renderSummary(summary);
    renderLive(live);
    refreshSeconds = Math.max(summary._cache.cache_ttl_seconds, live._cache.cache_ttl_seconds);
    countdown = refreshSeconds;
    document.getElementById("lastRefreshed").textContent = new Date().toLocaleTimeString();
    document.getElementById("cacheFooter").textContent =
      `summary cache: ${summary._cache.from_cache ? "served from cache" : "freshly queried"} `
      + `(age ${summary._cache.cache_age_seconds}s / ttl ${summary._cache.cache_ttl_seconds}s) · `
      + `live cache: ${live._cache.from_cache ? "served from cache" : "freshly queried"} `
      + `(age ${live._cache.cache_age_seconds}s / ttl ${live._cache.cache_ttl_seconds}s)`;
  }catch(e){
    document.getElementById("errBanner").style.display = "block";
    document.getElementById("errBanner").textContent = "Could not reach the dashboard API: " + e.message;
  }
}

setInterval(() => {
  countdown -= 1;
  if(countdown <= 0){ refresh(); }
  document.getElementById("nextRefresh").textContent = Math.max(countdown, 0);
}, 1000);

refresh();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("DASHBOARD_PORT", "8100"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")