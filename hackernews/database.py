"""
hackernews/database.py
======================
Hacker News ke liye MongoDB #2 (db1) ka sara access yahin hai.

Is file ka kaam:
  - Lazy MongoClient (MONGODB1_URI / MONGODB1_DB) banana aur reuse karna.
  - `flintel_search_jobs` se HN-targeted jobs (aur unke keywords) padhna.
  - Incremental fetch state (`flintel_hn_state`) padhna / likhna.
  - `flintel_service_status` mein "hackernews_worker" ka heartbeat likhna.

Is file ka kaam NAHI:
  - `flintel_signals` mein save karna. Wo `_save_signal()` karta hai, jo fetcher
    ke zariye function argument ke tor par pass hota hai.

Rules:
  - Import time par koi connection nahi banta.
  - Koi function raise nahi karta: har failure par log.warning aur safe default
    (None / [] / silent return), taake poller/worker thread kabhi crash na ho.
  - `flintel_service` se koi import nahi (circular import se bachne ke liye).
"""

import logging
import os
import threading
from datetime import datetime, timezone

from pymongo import MongoClient

from . import config

log = logging.getLogger("flintel-fetch")


# ---------------------------------------------------------------------------
# Lazy MongoClient (MongoDB #2)
# ---------------------------------------------------------------------------

# Pehli call par banega, phir reuse hoga. Lock isliye ke multiple threads ek saath
# pehli call karein to sirf ek client bane.
_client = None
_client_lock = threading.Lock()

# State collection ka unique index sirf ek baar ensure hota hai (lazily).
_state_index_ensured = False
_state_index_lock = threading.Lock()


def _get_client():
    """
    Lazy MongoClient wapas karta hai (MONGODB1_URI se), ya connect fail hone par None.

    - Import time par connect nahi hota; pehli call par hota hai.
    - serverSelectionTimeoutMS=5000 (5 second) taake DB down ho to worker atke nahi.
    - Fail hone par client cache NAHI hota, isliye agli call par dobara koshish hogi.
    - Kabhi raise nahi karta.
    """
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:
            return _client

        uri = os.getenv("MONGODB1_URI", "").strip()
        if not uri:
            log.warning("HN database: MONGODB1_URI is not set, Mongo #2 unavailable")
            return None

        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # MongoClient lazy hota hai; ping se asli connection check hota hai.
            client.admin.command("ping")
            _client = client
            log.info("HN database: connected to MongoDB #2")
            return _client
        except Exception as e:
            log.warning("HN database: could not connect to MongoDB #2: %s", e)
            return None


def get_jobs_db():
    """
    MongoDB #2 ka database object (MONGODB1_DB, default "flintel_bot") wapas karta hai,
    ya connect na ho sake to None. Kabhi raise nahi karta.
    """
    try:
        client = _get_client()
        if client is None:
            return None
        return client[os.getenv("MONGODB1_DB", "flintel_bot")]
    except Exception as e:
        log.warning("HN database: get_jobs_db failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Jobs (flintel_search_jobs)
# ---------------------------------------------------------------------------

def _normalize_keywords(raw) -> list:
    """
    Job document ke `keywords` field ko saaf list of strings mein badalta hai.

    List ho to har item strip hota hai; comma-separated string ho to split hoti hai.
    Khali / non-string items hata diye jate hain. Kabhi raise nahi karta.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        raw = raw.split(",")
    if not isinstance(raw, (list, tuple, set)):
        return []
    cleaned = []
    for item in raw:
        if isinstance(item, str):
            item = item.strip()
            if item:
                cleaned.append(item)
    return cleaned


def load_hackernews_jobs() -> list:
    """
    `flintel_search_jobs` se saare jobs padhta hai (status ki parwah kiye baghair,
    `_load_all_jobs_keyword_map()` ki tarah) aur sirf wo rakhta hai jo Hacker News
    ko target karte hain.

    Projection: topic_key, keywords, targeting_platform.

    Filter:
      - config.job_targets_hackernews(targeting_platform) True ho
        (yaani "all" ya "hackernews"; "reddit" / "x_twitter" nahi), aur
      - topic_key maujood ho aur keywords khali na hon.

    Returns:
        list of {"topic_key", "keywords", "targeting_platform"}.
        DB error par [] + log.warning. Kabhi raise nahi karta.

    Note: process_job ke andar to single job milta hai; ye function future
    always-on use ya debugging ke liye hai.
    """
    try:
        db = get_jobs_db()
        if db is None:
            return []

        cursor = db["flintel_search_jobs"].find(
            {},
            {"_id": 0, "topic_key": 1, "keywords": 1, "targeting_platform": 1},
        )

        jobs = []
        for doc in cursor:
            topic_key = doc.get("topic_key")
            targeting_platform = doc.get("targeting_platform")

            if not topic_key:
                continue
            if not config.job_targets_hackernews(targeting_platform):
                continue

            keywords = _normalize_keywords(doc.get("keywords"))
            if not keywords:
                continue

            jobs.append(
                {
                    "topic_key": topic_key,
                    "keywords": keywords,
                    "targeting_platform": targeting_platform,
                }
            )
        return jobs
    except Exception as e:
        log.warning("HN database: load_hackernews_jobs failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Incremental fetch state (flintel_hn_state)
# ---------------------------------------------------------------------------

def _get_state_collection():
    """
    `flintel_hn_state` collection wapas karta hai (ya None), aur pehli baar
    (topic_key, keyword, kind) par unique index ensure karta hai.

    Index banane mein fail ho to sirf log.warning hota hai; collection phir bhi
    wapas aata hai taake state read/write chalta rahe. Kabhi raise nahi karta.
    """
    global _state_index_ensured
    try:
        db = get_jobs_db()
        if db is None:
            return None

        coll = db[config.HN_STATE_COLLECTION]

        if not _state_index_ensured:
            with _state_index_lock:
                if not _state_index_ensured:
                    try:
                        coll.create_index(
                            [("topic_key", 1), ("keyword", 1), ("kind", 1)],
                            unique=True,
                            name="uniq_topic_keyword_kind",
                        )
                        _state_index_ensured = True
                    except Exception as e:
                        log.warning(
                            "HN database: could not ensure index on %s: %s",
                            config.HN_STATE_COLLECTION,
                            e,
                        )
        return coll
    except Exception as e:
        log.warning("HN database: state collection unavailable: %s", e)
        return None


def get_last_seen_ts(topic_key, keyword, kind):
    """
    Is (topic_key, keyword, kind) ke liye ab tak ka sab se naya dekha hua
    `created_at_i` (unix seconds) wapas karta hai.

    kind: "story" ya "comment".

    Returns:
        int agar state maujood ho, warna None (state nahi mila, ya DB error).
        None milne par caller LOOKBACK_DAYS wale cutoff se fetch karega.
        Kabhi raise nahi karta.
    """
    try:
        coll = _get_state_collection()
        if coll is None:
            return None

        doc = coll.find_one(
            {"topic_key": topic_key, "keyword": keyword, "kind": kind},
            {"_id": 0, "last_created_at_i": 1},
        )
        if not doc:
            return None

        value = doc.get("last_created_at_i")
        if value is None:
            return None
        return int(value)
    except Exception as e:
        log.warning(
            "HN database: get_last_seen_ts failed (topic=%s keyword=%r kind=%s): %s",
            topic_key,
            keyword,
            kind,
            e,
        )
        return None


def set_last_seen_ts(topic_key, keyword, kind, ts):
    """
    (topic_key, keyword, kind) ka last-seen timestamp upsert karta hai.

    `$max` use hota hai, yaani sirf tab update hota hai jab naya `ts` purane se
    bada ho; chhota ya barabar ts purani value ko kabhi peeche nahi karta.

    Fail hone par sirf log.warning (silent); fetch agli baar bhi LOOKBACK_DAYS
    cutoff se chal sakta hai. Kabhi raise nahi karta.
    """
    try:
        coll = _get_state_collection()
        if coll is None:
            return

        ts = int(ts)
        coll.update_one(
            {"topic_key": topic_key, "keyword": keyword, "kind": kind},
            {
                "$max": {"last_created_at_i": ts},
                "$set": {"updated_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )
    except Exception as e:
        log.warning(
            "HN database: set_last_seen_ts failed (topic=%s keyword=%r kind=%s): %s",
            topic_key,
            keyword,
            kind,
            e,
        )


# ---------------------------------------------------------------------------
# Service heartbeat (flintel_service_status)
# ---------------------------------------------------------------------------

def set_hn_service_status(running: bool):
    """
    `flintel_service_status` mein HN worker ka status upsert karta hai:
        {"service": config.HN_SERVICE_NAME, "running": running, "updated_at": now(UTC)}

    Sirf "hackernews_worker" wala document touch hota hai; baqi services ke
    documents ko koi asar nahi. Kabhi raise nahi karta.
    """
    try:
        db = get_jobs_db()
        if db is None:
            return

        db["flintel_service_status"].update_one(
            {"service": config.HN_SERVICE_NAME},
            {
                "$set": {
                    "service": config.HN_SERVICE_NAME,
                    "running": bool(running),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
    except Exception as e:
        log.warning("HN database: set_hn_service_status failed: %s", e)
