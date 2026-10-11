"""
tests/test_hackernews.py
========================
Hacker News integration ke hermetic tests (pytest).

  - Koi real network nahi: `requests.get` monkeypatch hota hai.
  - Koi real Mongo nahi: `hackernews.database` ke functions fake hote hain.
  - `.env` ka asar nahi: `config.load_dotenv` no-op kar diya jata hai, taake
    developer ki local .env live-checked switches ko override na kare.
  - `flintel_service` yahan import NAHI hota (import par Mongo se connect karta hai).

Chalane ke liye (project root se):
    pytest tests/test_hackernews.py -v
"""

import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from hackernews import config, database, fetcher  # noqa: E402

# Asli (unpatched) functions — test_7 inhi ko test karta hai, jabke autouse
# fixture baqi tests mein inhe fake kar deta hai.
_REAL_GET_LAST_SEEN = database.get_last_seen_ts
_REAL_SET_LAST_SEEN = database.set_last_seen_ts

CUTOFF = datetime(2020, 1, 1, tzinfo=timezone.utc)

STORY_HIT = {
    "objectID": "123",
    "title": "Show HN: Nike tracker",
    "story_text": "<p>Hello &amp; <i>world</i></p>",
    "author": "bob",
    "created_at_i": 1700000000,
    "points": 42,
    "num_comments": 7,
    "url": "https://example.com/nike",
}

COMMENT_HIT = {
    "objectID": "456",
    "comment_text": '<p>I use <a href="https://x.test">Nike</a> shoes</p><p>great &amp; light</p>',
    "author": "alice",
    "created_at_i": 1700000100,
    "story_id": 123,
}


# ─────────────────────────────────────────────────────────────────────────────
# Fakes / fixtures
# ─────────────────────────────────────────────────────────────────────────────

class FakeResp:
    def __init__(self, payload=None, ok=True, json_exc=None):
        self._payload = payload
        self._ok = ok
        self._json_exc = json_exc

    def raise_for_status(self):
        if not self._ok:
            raise requests.HTTPError("500 Server Error")

    def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._payload


@pytest.fixture(autouse=True)
def hermetic(monkeypatch):
    """Har test ke liye: .env ignore, gap 0, switches default (on), database fake."""
    monkeypatch.setattr(config, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(config, "HN_KEYWORD_GAP_SECONDS", 0)
    monkeypatch.setattr(config, "HN_EXACT_PHRASE", False)
    for var in ("HACKERNEWS_ENABLED", "HN_FETCH_STORIES", "HN_FETCH_COMMENTS"):
        monkeypatch.delenv(var, raising=False)
    # Strict match default (ON) par rahe; real env mein HN_STRICT_MATCH ho to test na bigde.
    # Jis test ke fake hits mein keyword nahi hota wo isay khud "false" karta hai.
    monkeypatch.delenv("HN_STRICT_MATCH", raising=False)

    state = SimpleNamespace(seen={}, set_calls=[])
    monkeypatch.setattr(
        fetcher.database, "get_last_seen_ts",
        lambda topic_key, keyword, kind: state.seen.get((topic_key, keyword, kind)),
    )
    monkeypatch.setattr(
        fetcher.database, "set_last_seen_ts",
        lambda topic_key, keyword, kind, ts: state.set_calls.append((topic_key, keyword, kind, ts)),
    )
    return state


@pytest.fixture
def api(monkeypatch):
    """Fake Algolia: har requests.get call record hoti hai."""
    state = SimpleNamespace(
        calls=[],
        hits={"story": [STORY_HIT], "comment": [COMMENT_HIT]},
        error=None,
        fail_query=None,
    )

    def fake_get(url, params=None, headers=None, timeout=None):
        params = dict(params or {})
        state.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        if state.error is not None:
            raise state.error
        if state.fail_query is not None and params.get("query") == state.fail_query:
            raise requests.Timeout("simulated timeout")
        return FakeResp({"hits": state.hits.get(params.get("tags"), [])})

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    return state


def _job(targeting_platform="all", keywords=("nike",)):
    return {"topic_key": "nike", "keywords": list(keywords), "targeting_platform": targeting_platform}


# ─────────────────────────────────────────────────────────────────────────────
# test_1 — story hit -> post dict
# ─────────────────────────────────────────────────────────────────────────────

def test_1_story_hit_converted_to_post_dict(api):
    posts = fetcher.fetch_hackernews_search("nike", "story", 1690000000)

    assert len(posts) == 1
    p = posts[0]
    assert p["id"] == "123"
    assert p["title"] == "Show HN: Nike tracker"
    assert p["selftext"] == "Hello & world"            # HTML stripped + unescaped
    assert p["author"] == "bob"
    assert p["subreddit"] == ""
    assert p["post_url"] == "https://news.ycombinator.com/item?id=123"
    assert p["created_utc"] == 1700000000.0
    assert p["score"] == 42
    assert p["num_comments"] == 7
    assert p["hn_type"] == "story"
    assert p["external_url"] == "https://example.com/nike"

    # Request shape
    call = api.calls[0]
    assert call["url"] == config.HN_API_URL
    assert call["params"]["query"] == "nike"
    assert call["params"]["tags"] == "story"
    assert call["params"]["hitsPerPage"] == config.HN_RESULTS_PER_QUERY
    assert call["params"]["numericFilters"] == "created_at_i>1690000000"
    assert call["headers"]["User-Agent"] == config.HN_USER_AGENT
    assert call["timeout"] == config.HN_REQUEST_TIMEOUT


def test_1b_no_since_ts_means_no_numeric_filter(api):
    fetcher.fetch_hackernews_search("nike", "story", None)
    assert "numericFilters" not in api.calls[0]["params"]


def test_1c_exact_phrase_quotes_multi_word_keyword(api, monkeypatch):
    monkeypatch.setattr(config, "HN_EXACT_PHRASE", True)
    fetcher.fetch_hackernews_search("air max", "story", None)
    fetcher.fetch_hackernews_search("nike", "story", None)
    assert api.calls[0]["params"]["query"] == '"air max"'
    assert api.calls[1]["params"]["query"] == "nike"   # single word: no quotes


# ─────────────────────────────────────────────────────────────────────────────
# test_2 — comment hit
# ─────────────────────────────────────────────────────────────────────────────

def test_2_comment_hit_strips_html_and_title_is_text(api):
    posts = fetcher.fetch_hackernews_search("nike", "comment", None)

    assert len(posts) == 1
    p = posts[0]
    assert p["id"] == "456"
    assert p["title"] == "I use Nike shoes great & light"
    assert "<" not in p["title"] and ">" not in p["title"]
    assert p["selftext"] == ""
    assert p["hn_type"] == "comment"
    assert p["hn_story_id"] == 123
    assert p["post_url"] == "https://news.ycombinator.com/item?id=456"
    assert p["score"] == 0
    assert p["num_comments"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# test_3 — API failure -> [] (never raises)
# ─────────────────────────────────────────────────────────────────────────────

def test_3_timeout_returns_empty_list(api):
    api.error = requests.Timeout("timed out")
    assert fetcher.fetch_hackernews_search("nike", "story", None) == []


def test_3b_http_error_returns_empty_list(monkeypatch):
    monkeypatch.setattr(fetcher.requests, "get", lambda *a, **k: FakeResp(ok=False))
    assert fetcher.fetch_hackernews_search("nike", "story", None) == []


def test_3c_bad_json_returns_empty_list(monkeypatch):
    monkeypatch.setattr(
        fetcher.requests, "get", lambda *a, **k: FakeResp(json_exc=ValueError("not json"))
    )
    assert fetcher.fetch_hackernews_search("nike", "comment", None) == []


def test_3d_hits_without_id_or_text_are_skipped(monkeypatch):
    payload = {"hits": [
        {"objectID": "", "title": "no id"},
        {"objectID": "9", "title": None, "story_text": None},       # story with no text at all
        {"objectID": "10", "title": "good one", "created_at_i": 1700000000},
    ]}
    monkeypatch.setattr(fetcher.requests, "get", lambda *a, **k: FakeResp(payload))
    posts = fetcher.fetch_hackernews_search("x", "story", None)
    assert [p["id"] for p in posts] == ["10"]


# ─────────────────────────────────────────────────────────────────────────────
# test_4 — job_targets_hackernews
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "value, expected",
    [
        ("hackernews", True),
        ("all", True),
        (None, True),          # None -> "all"
        ("", True),            # empty -> "all"
        ("reddit", False),
        ("x_twitter", False),
        (" ALL ", True),       # strip + lower
        (" HackerNews ", True),
    ],
)
def test_4_job_targets_hackernews(value, expected):
    assert config.job_targets_hackernews(value) is expected


# ─────────────────────────────────────────────────────────────────────────────
# test_5 — save_fn platforms + master switch
# ─────────────────────────────────────────────────────────────────────────────

def test_5_save_fn_called_with_story_and_comment_platforms(api, hermetic):
    saved = []

    def save_fn(topic_key, keyword, platform, post):
        saved.append((topic_key, keyword, platform, post["id"]))
        return True

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 2
    assert {s[2] for s in saved} == {"hackernews", "hackernews_comment"}
    assert ("nike", "nike", "hackernews", "123") in saved
    assert ("nike", "nike", "hackernews_comment", "456") in saved
    # Last-seen state: har kind ke max created_at_i par update hua.
    assert ("nike", "nike", "story", 1700000000) in hermetic.set_calls
    assert ("nike", "nike", "comment", 1700000100) in hermetic.set_calls


def test_5b_disabled_returns_zero_without_api_call(api, monkeypatch):
    monkeypatch.setenv("HACKERNEWS_ENABLED", "false")
    saved = []
    count = fetcher.run_hackernews_for_job(
        _job("all"), lambda *a: saved.append(a) or True, CUTOFF
    )
    assert count == 0
    assert api.calls == []
    assert saved == []


def test_5c_save_fn_returning_false_is_not_counted(api):
    count = fetcher.run_hackernews_for_job(_job("all"), lambda *a: False, CUTOFF)
    assert count == 0
    assert len(api.calls) == 2   # API phir bhi chali (story + comment)


def test_5d_stories_and_comments_switches(api, monkeypatch):
    monkeypatch.setenv("HN_FETCH_COMMENTS", "false")
    fetcher.run_hackernews_for_job(_job("all"), lambda *a: True, CUTOFF)
    assert [c["params"]["tags"] for c in api.calls] == ["story"]

    api.calls.clear()
    monkeypatch.setenv("HN_FETCH_COMMENTS", "true")
    monkeypatch.setenv("HN_FETCH_STORIES", "false")
    fetcher.run_hackernews_for_job(_job("all"), lambda *a: True, CUTOFF)
    assert [c["params"]["tags"] for c in api.calls] == ["comment"]


def test_5e_uses_saved_since_ts_else_cutoff(api, hermetic):
    hermetic.seen[("nike", "nike", "story")] = 1699999999
    fetcher.run_hackernews_for_job(_job("all"), lambda *a: True, CUTOFF)

    by_kind = {c["params"]["tags"]: c["params"] for c in api.calls}
    assert by_kind["story"]["numericFilters"] == "created_at_i>1699999999"                 # saved state
    assert by_kind["comment"]["numericFilters"] == f"created_at_i>{int(CUTOFF.timestamp())}"  # fallback


# ─────────────────────────────────────────────────────────────────────────────
# test_6 — ek keyword fail ho to baqi chalein
# ─────────────────────────────────────────────────────────────────────────────

def test_6_one_keyword_raising_does_not_stop_others(monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "false")   # fake hit mein keyword nahi hota

    def fake_search(keyword, kind, since_ts):
        if keyword == "boom":
            raise RuntimeError("unexpected failure")
        return [{
            "id": f"{keyword}-{kind}", "title": "t", "selftext": "", "author": "a",
            "subreddit": "", "post_url": "u", "created_utc": 1700000000.0,
            "score": 0, "num_comments": 0, "hn_type": kind,
        }]

    monkeypatch.setattr(fetcher, "fetch_hackernews_search", fake_search)

    seen_keywords = []

    def save_fn(topic_key, keyword, platform, post):
        seen_keywords.append(keyword)
        return True

    count = fetcher.run_hackernews_for_job(
        _job("all", keywords=("a", "boom", "c")), save_fn, CUTOFF
    )

    assert count == 4                                  # a + c, har ek ke story & comment
    assert set(seen_keywords) == {"a", "c"}


def test_6b_http_failure_on_one_keyword_does_not_stop_others(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "false")   # fake hits mein keyword nahi hota

    api.fail_query = "boom"
    saved_keywords = []
    count = fetcher.run_hackernews_for_job(
        _job("all", keywords=("a", "boom", "c")),
        lambda t, k, p, post: saved_keywords.append(k) or True,
        CUTOFF,
    )
    assert count == 4
    assert set(saved_keywords) == {"a", "c"}


def test_6c_save_fn_raising_never_propagates(api, hermetic):
    def bad_save(*a):
        raise RuntimeError("db exploded")

    count = fetcher.run_hackernews_for_job(_job("all"), bad_save, CUTOFF)
    assert count == 0
    assert hermetic.set_calls == []     # save fail hua, to state aage nahi badha


# ─────────────────────────────────────────────────────────────────────────────
# test_7 — set_last_seen_ts ($max) aur DB down
# ─────────────────────────────────────────────────────────────────────────────

class FakeStateColl:
    """Mongo update_one ka chhota sa emulation: $max, $set, upsert."""

    def __init__(self):
        self.docs = {}
        self.operators_used = set()

    @staticmethod
    def _key(flt):
        return (flt["topic_key"], flt["keyword"], flt["kind"])

    def update_one(self, flt, update, upsert=False):
        self.operators_used.update(update.keys())
        key = self._key(flt)
        doc = self.docs.get(key)
        if doc is None:
            if not upsert:
                return
            doc = {}
            self.docs[key] = doc
        for field, value in update.get("$max", {}).items():
            if field not in doc or value > doc[field]:
                doc[field] = value
        for field, value in update.get("$set", {}).items():
            doc[field] = value

    def find_one(self, flt, projection=None):
        doc = self.docs.get(self._key(flt))
        return dict(doc) if doc is not None else None


def test_7_set_last_seen_ts_keeps_only_larger_ts(monkeypatch):
    coll = FakeStateColl()
    monkeypatch.setattr(database, "_get_state_collection", lambda: coll)

    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") is None     # state nahi

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 100)
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") == 100

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 50)                # chhota -> ignore
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") == 100

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 100)               # barabar -> same
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") == 100

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 200)               # bada -> update
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") == 200

    # kind alag ho to alag state
    assert _REAL_GET_LAST_SEEN("nike", "nike", "comment") is None
    assert "$max" in coll.operators_used


def test_7b_db_down_never_raises(monkeypatch):
    monkeypatch.setattr(database, "_get_state_collection", lambda: None)
    assert _REAL_SET_LAST_SEEN("nike", "nike", "story", 100) is None
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") is None


def test_7c_collection_errors_never_raise(monkeypatch):
    class BoomColl:
        def update_one(self, *a, **k):
            raise RuntimeError("write failed")

        def find_one(self, *a, **k):
            raise RuntimeError("read failed")

    monkeypatch.setattr(database, "_get_state_collection", lambda: BoomColl())
    assert _REAL_SET_LAST_SEEN("nike", "nike", "story", 100) is None
    assert _REAL_GET_LAST_SEEN("nike", "nike", "story") is None


def test_7d_no_mongo_uri_means_no_db_and_no_crash(monkeypatch):
    monkeypatch.delenv("MONGODB1_URI", raising=False)
    monkeypatch.setattr(database, "_client", None)
    assert database.get_jobs_db() is None
    assert database.load_hackernews_jobs() == []
    assert database.set_hn_service_status(True) is None


# ─────────────────────────────────────────────────────────────────────────────
# test_8 — targeting_platform ka rule run_hackernews_for_job mein
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("targeting_platform", ["reddit", "x_twitter"])
def test_8_non_hn_jobs_make_no_api_call(api, targeting_platform):
    saved = []
    count = fetcher.run_hackernews_for_job(
        _job(targeting_platform), lambda *a: saved.append(a) or True, CUTOFF
    )
    assert count == 0
    assert api.calls == []
    assert saved == []


@pytest.mark.parametrize("targeting_platform", ["hackernews", "all", None, ""])
def test_8_hn_jobs_make_api_calls(api, targeting_platform):
    count = fetcher.run_hackernews_for_job(
        _job(targeting_platform), lambda *a: True, CUTOFF
    )
    assert len(api.calls) > 0
    assert count == 2


# ─────────────────────────────────────────────────────────────────────────────
# test_9..test_14 — strict local keyword match (HN_STRICT_MATCH)
# ─────────────────────────────────────────────────────────────────────────────

# Algolia ne ye item sirf URL / author ki wajah se diya: title/text mein "nike" nahi.
URL_ONLY_STORY_HIT = {
    "objectID": "700",
    "title": "Some unrelated launch",
    "story_text": None,
    "author": "nike_fan",
    "created_at_i": 1700000200,
    "points": 1,
    "num_comments": 0,
    "url": "https://example.com/nike-shoes",
}

URL_ONLY_COMMENT_HIT = {
    "objectID": "701",
    "comment_text": "<p>totally unrelated words</p>",
    "author": "nike",
    "created_at_i": 1700000300,
    "story_id": 1,
}


def _collect():
    saved = []

    def save_fn(topic_key, keyword, platform, post):
        saved.append((keyword, platform, post["id"]))
        return True

    return saved, save_fn


def test_9_strict_on_skips_story_when_keyword_only_in_url(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    api.hits = {"story": [URL_ONLY_STORY_HIT], "comment": []}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 0
    assert saved == []                      # save_fn call hi nahi hua


def test_13_strict_on_applies_to_comments_too(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    api.hits = {"story": [], "comment": [URL_ONLY_COMMENT_HIT]}   # sirf author "nike" hai
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 0
    assert saved == []


def test_9c_strict_match_defaults_to_true(monkeypatch):
    monkeypatch.delenv("HN_STRICT_MATCH", raising=False)
    assert config.is_hn_strict_match_enabled() is True
    monkeypatch.setenv("HN_STRICT_MATCH", "false")
    assert config.is_hn_strict_match_enabled() is False


def test_9d_keyword_in_post_helper():
    assert fetcher._keyword_in_post("nike", {"title": "NIKE news", "selftext": ""}) is True
    assert fetcher._keyword_in_post(" Nike ", {"title": "x", "selftext": "about nIkE"}) is True
    assert fetcher._keyword_in_post("nike", {"title": "adidas", "selftext": ""}) is False
    assert fetcher._keyword_in_post("air max", {"title": "my air max pair", "selftext": ""}) is True


def test_10_strict_on_keeps_story_with_keyword_in_other_case(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    hit = dict(STORY_HIT, objectID="800", title="NIKE announces something", story_text=None)
    api.hits = {"story": [hit], "comment": []}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 1
    assert saved == [("nike", "hackernews", "800")]


def test_13b_strict_on_keeps_comment_with_keyword_in_other_case(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    hit = dict(COMMENT_HIT, objectID="801", comment_text="<p>I love NIKE shoes</p>")
    api.hits = {"story": [], "comment": [hit]}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 1
    assert saved == [("nike", "hackernews_comment", "801")]


def test_11_strict_off_sends_every_hit_to_save_fn(api, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "false")
    api.hits = {"story": [STORY_HIT, URL_ONLY_STORY_HIT], "comment": [URL_ONLY_COMMENT_HIT]}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 3
    assert {s[2] for s in saved} == {"123", "700", "701"}


def test_12_checkpoint_uses_max_ts_of_all_fetched_posts(api, hermetic, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    # Matched story ka ts (1700000000) skipped story (1700000200) se chhota hai.
    api.hits = {"story": [STORY_HIT, URL_ONLY_STORY_HIT], "comment": [URL_ONLY_COMMENT_HIT]}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 1                                    # sirf matched story save hui
    assert [s[2] for s in saved] == ["123"]
    # Checkpoint skipped posts ko bhi gin kar max par gaya:
    assert ("nike", "nike", "story", 1700000200) in hermetic.set_calls
    # Comment kind mein sab skip hua, phir bhi checkpoint aage badha:
    assert ("nike", "nike", "comment", 1700000300) in hermetic.set_calls


def test_12b_checkpoint_advances_even_if_every_post_skipped(api, hermetic, monkeypatch):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")
    api.hits = {"story": [URL_ONLY_STORY_HIT], "comment": []}
    saved, save_fn = _collect()

    count = fetcher.run_hackernews_for_job(_job("all"), save_fn, CUTOFF)

    assert count == 0 and saved == []
    assert hermetic.set_calls == [("nike", "nike", "story", 1700000200)]


def test_14_skip_count_is_logged_only_when_positive(api, monkeypatch, caplog):
    monkeypatch.setenv("HN_STRICT_MATCH", "true")

    api.hits = {"story": [STORY_HIT, URL_ONLY_STORY_HIT], "comment": []}
    with caplog.at_level("INFO", logger="flintel-fetch"):
        fetcher.run_hackernews_for_job(_job("all"), lambda *a: True, CUTOFF)
    assert "[HACKERNEWS] strict match skipped 1 | keyword='nike' | kind=story" in caplog.text

    caplog.clear()
    api.hits = {"story": [STORY_HIT], "comment": []}      # sab match -> koi skip nahi
    with caplog.at_level("INFO", logger="flintel-fetch"):
        fetcher.run_hackernews_for_job(_job("all"), lambda *a: True, CUTOFF)
    assert "strict match skipped" not in caplog.text
