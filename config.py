"""
hackernews/config.py
====================
Hacker News integration ki saari settings yahin rehti hain.

Is file mein SIRF settings, constants aur chhote helpers hain:
  - Koi network call nahi.
  - Koi DB call nahi.
  - flintel_service se koi import nahi (circular import se bachne ke liye).

Keywords yahan hardcode NAHI hote; wo MongoDB #2 ki `flintel_search_jobs`
collection se aate hain (database.py / worker ka kaam).
"""

import logging
import os

from dotenv import load_dotenv

log = logging.getLogger("flintel-fetch")

# Module import par ek baar .env load karo taake neeche ke constants
# (HN_API_URL, HN_RESULTS_PER_QUERY, ...) .env ki values uthayein.
load_dotenv()


# ---------------------------------------------------------------------------
# Chhote env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: bool) -> bool:
    """
    Env variable ko bool mein parse karta hai (flintel_service wale helper ki copy).

    Accepted (case-insensitive):
      true  : "true", "1", "yes", "on"
      false : "false", "0", "no", "off"
    Variable missing ho ya value samajh na aaye to `default` wapas aata hai.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


def _env_int(name: str, default: int) -> int:
    """
    Env variable ko int mein parse karta hai. Galat value ho to default use hota hai
    (taake import ke waqt koi exception na aaye aur service crash na ho).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (ValueError, TypeError):
        log.warning("Invalid int for %s=%r, using default %s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    """
    Env variable ko float mein parse karta hai. Galat value ho to default use hota hai.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except (ValueError, TypeError):
        log.warning("Invalid float for %s=%r, using default %s", name, raw, default)
        return default


# ---------------------------------------------------------------------------
# Live-checked switches
# Har call par .env dobara padhi jati hai (override=True), isliye in switches ko
# badalne ke liye service restart karne ki zaroorat nahi.
# ---------------------------------------------------------------------------

def is_hackernews_enabled() -> bool:
    """
    Master switch: Hacker News ka poora fetch on/off.
    Env: HACKERNEWS_ENABLED (default True).
    False ho to HN worker kuch fetch nahi karega.
    """
    load_dotenv(override=True)
    return _env_bool("HACKERNEWS_ENABLED", True)


def is_hn_comments_enabled() -> bool:
    """
    HN comments (platform "hackernews_comment") fetch karne ka switch.
    Env: HN_FETCH_COMMENTS (default True).
    """
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_COMMENTS", True)


def is_hn_stories_enabled() -> bool:
    """
    HN stories (platform "hackernews") fetch karne ka switch.
    Env: HN_FETCH_STORIES (default True).
    """
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_STORIES", True)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Algolia HN Search API ka endpoint (date ke hisaab se sorted results).
# Free hai, API key nahi chahiye.
HN_API_URL = os.getenv("HN_API_URL", "https://hn.algolia.com/api/v1/search_by_date")

# Har keyword/query par Algolia se kitne results maangne hain (hitsPerPage).
# Default 50; Algolia ki limit ke hisaab se maximum 1000 par clamp (kam az kam 1).
HN_RESULTS_PER_QUERY = max(1, min(_env_int("HN_RESULTS_PER_QUERY", 50), 1000))

# Ek HTTP request ka timeout (seconds). Is se zyada lagey to request fail
# maani jayegi (log hoga, worker chalta rahega).
HN_REQUEST_TIMEOUT = _env_int("HN_REQUEST_TIMEOUT", 15)

# Do keywords ke darmiyan intezar (seconds), taake Algolia par rate-limit /
# burst traffic na ho.
HN_KEYWORD_GAP_SECONDS = _env_float("HN_KEYWORD_GAP_SECONDS", 1.0)

# Kitne din purani posts/comments tak dekhna hai. Default wahi LOOKBACK_DAYS
# jo baqi service use karti hai (180), alag se HN ke liye override nahi karna ho to.
HN_LOOKBACK_DAYS = _env_int("HN_LOOKBACK_DAYS", _env_int("LOOKBACK_DAYS", 180))

# True ho to multi-word keyword double quotes mein bheja jata hai (exact phrase
# match), warna Algolia words ko alag-alag match karta hai.
HN_EXACT_PHRASE = _env_bool("HN_EXACT_PHRASE", False)

# Algolia ko bheji jane wali User-Agent header (identify karne ke liye).
HN_USER_AGENT = os.getenv(
    "HN_USER_AGENT", "flintel-fetch-service/1.0 (contact: ops@example.com)"
)

# `flintel_signals` mein save hone wala platform naam: HN stories ke liye.
HN_PLATFORM_STORY = "hackernews"

# `flintel_signals` mein save hone wala platform naam: HN comments ke liye.
HN_PLATFORM_COMMENT = "hackernews_comment"

# Job ke `targeting_platform` field ki wo value jo sirf Hacker News ko target karti hai.
HN_TARGETING_VALUE = "hackernews"

# Mongo collection jahan HN ka state (jaise last-fetched checkpoint) rakha jayega.
# Isey database.py use karega.
HN_STATE_COLLECTION = "flintel_hn_state"

# Heartbeat mein HN worker ka naam.
HN_SERVICE_NAME = "hackernews_worker"


# ---------------------------------------------------------------------------
# Job targeting rule
# ---------------------------------------------------------------------------

def job_targets_hackernews(targeting_platform) -> bool:
    """
    Batata hai ke diye gaye job ke `targeting_platform` ke hisaab se Hacker News chalna chahiye ya nahi.

    Rule (sab platforms ke liye same):
      - None / empty / whitespace  -> "all" samjha jata hai -> True
      - "all"                      -> True  (Reddit + Twitter + HN teeno)
      - "hackernews"               -> True  (sirf HN)
      - baqi sab ("reddit", "x_twitter", ...) -> False

    Value pehle .strip().lower() hoti hai. Koi env switch is function mein nahi hai.
    """
    value = str(targeting_platform).strip().lower() if targeting_platform else ""
    if not value:
        value = "all"
    return value == "all" or value == HN_TARGETING_VALUE
