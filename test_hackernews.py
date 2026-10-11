"""Tests for the Hacker News module (hackernews/).
Everything is faked: no real network, no real Mongo. requests.get is
monkeypatched and database.* functions are replaced with fakes."""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hackernews import config, database, fetcher  # noqa: E402
from hackernews.fetcher import fetch_hackernews_search, run_hackernews_for_job  # noqa: E402


# Real functions captured BEFORE the autouse fixture stubs them (test 7
# exercises the real implementations against fake collections).
_REAL_SET_LAST_SEEN = database.set_last_seen_ts
_REAL_GET_LAST_SEEN = database.get_last_seen_ts

# ───────────────────────── fakes / helpers ─────────────────────────

class FakeResponse:
    def __init__(self, payload=None, status=200):
        self._payload = payload if payload is not None else {"hits": []}
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


STORY_HIT = {
    "objectID": "111",
    "title": "Show HN: Nike tracker",
    "story_text": "<p>Hello <i>world</i> &amp; friends</p>",
    "author": "alice",
    "points": 42,
    "num_comments": 7,
    "created_at_i": 1700000000,
    "url": "https://example.com",
    "story_id": 111,
}

COMMENT_HIT = {
    "objectID": "222",
    "comment_text": "<p>I love <a href=\"https://x.com\">Nike</a> shoes</p><p>so much</p>",
    "author": "bob",
    "created_at_i": 1700000100,
    "story_id": 111,
}


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch):
    """Defaults for every test: HN on, no sleeping, no real DB state."""
    monkeypatch.setenv("HACKERNEWS_ENABLED", "true")
    monkeypatch.setenv("HN_FETCH_STORIES", "true")
    monkeypatch.setenv("HN_FETCH_COMMENTS", "true")
    monkeypatch.setattr(config, "HN_KEYWORD_GAP_SECONDS", 0)
    monkeypatch.setattr(config, "HN_EXACT_PHRASE", False)
    monkeypatch.setattr(database, "get_last_seen_ts", lambda *a, **k: None)
    monkeypatch.setattr(database, "set_last_seen_ts", lambda *a, **k: None)

    def _no_network(*a, **k):
        raise AssertionError("real network call attempted")

    monkeypatch.setattr(fetcher.requests, "get", _no_network)


def _cutoff():
    return datetime(2020, 1, 1, tzinfo=timezone.utc)


def _job(platform="all", keywords=("nike",)):
    return {"topic_key": "nike", "keywords": list(keywords), "targeting_platform": platform}


class SaveRecorder:
    def __init__(self):
        self.calls = []

    def __call__(self, topic_key, keyword, platform, post):
        self.calls.append((topic_key, keyword, platform, post))
        return True


# ───────────────────────── test 1 ─────────────────────────

def test_1_story_hit_converted_to_post_dict(monkeypatch):
    monkeypatch.setattr(
        fetcher.requests, "get", lambda *a, **k: FakeResponse({"hits": [STORY_HIT]})
    )
    posts = fetch_hackernews_search("nike", "story", None)
    assert len(posts) == 1
    p = posts[0]
    assert p["id"] == "111"
    assert p["title"] == "Show HN: Nike tracker"
    assert p["selftext"] == "Hello world & friends"  # HTML stripped, entity unescaped
    assert p["post_url"] == "https://news.ycombinator.com/item?id=111"
    assert p["created_utc"] == 1700000000.0
    assert p["score"] == 42
    assert p["num_comments"] == 7
    assert p["hn_type"] == "story"
    assert p["author"] == "alice"


# ───────────────────────── test 2 ─────────────────────────

def test_2_comment_hit_strips_html_and_uses_text_as_title(monkeypatch):
    monkeypatch.setattr(
        fetcher.requests, "get", lambda *a, **k: FakeResponse({"hits": [COMMENT_HIT]})
    )
    posts = fetch_hackernews_search("nike", "comment", None)
    assert len(posts) == 1
    p = posts[0]
    assert "<" not in p["title"] and ">" not in p["title"]
    assert p["title"] == "I love Nike shoes so much"
    assert p["selftext"] == ""
    assert p["hn_type"] == "comment"
    assert p["post_url"].endswith("id=222")


# ───────────────────────── test 3 ─────────────────────────

@pytest.mark.parametrize(
    "boom",
    [
        TimeoutError("timed out"),
        ConnectionError("down"),
        RuntimeError("weird"),
    ],
)
def test_3_api_exception_returns_empty_list(monkeypatch, boom):
    def raiser(*a, **k):
        raise boom

    monkeypatch.setattr(fetcher.requests, "get", raiser)
    assert fetch_hackernews_search("nike", "story", None) == []


def test_3_http_error_status_returns_empty_list(monkeypatch):
    monkeypatch.setattr(fetcher.requests, "get", lambda *a, **k: FakeResponse(status=500))
    assert fetch_hackernews_search("nike", "story", None) == []


# ───────────────────────── test 4 ─────────────────────────

@pytest.mark.parametrize(
    "value,expected",
    [
        ("hackernews", True),
        ("all", True),
        (None, True),
        ("", True),
        ("reddit", False),
        ("x_twitter", False),
        (" ALL ", True),
    ],
)
def test_4_job_targets_hackernews(value, expected):
    assert config.job_targets_hackernews(value) is expected


# ───────────────────────── test 5 ─────────────────────────

def test_5_run_calls_save_fn_with_story_and_comment_platforms(monkeypatch):
    def fake_get(url, params=None, **k):
        hit = STORY_HIT if params["tags"] == "story" else COMMENT_HIT
        return FakeResponse({"hits": [hit]})

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    save = SaveRecorder()

    saved = run_hackernews_for_job(_job("all"), save, _cutoff())

    platforms = sorted(c[2] for c in save.calls)
    assert platforms == ["hackernews", "hackernews_comment"]
    assert saved == 2
    for topic_key, keyword, _platform, _post in save.calls:
        assert topic_key == "nike"
        assert keyword == "nike"


def test_5_disabled_returns_zero_without_api_call(monkeypatch):
    monkeypatch.setenv("HACKERNEWS_ENABLED", "false")
    save = SaveRecorder()
    # autouse fixture's requests.get raises AssertionError if it is hit
    assert run_hackernews_for_job(_job("all"), save, _cutoff()) == 0
    assert save.calls == []


# ───────────────────────── test 6 ─────────────────────────

def test_6_failing_keyword_does_not_stop_the_rest(monkeypatch):
    seen_queries = []

    def fake_get(url, params=None, **k):
        seen_queries.append(params["query"])
        if params["query"] == "bad":
            raise TimeoutError("boom")
        return FakeResponse({"hits": [STORY_HIT]})

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    monkeypatch.setenv("HN_FETCH_COMMENTS", "false")
    save = SaveRecorder()

    saved = run_hackernews_for_job(_job("all", ("bad", "good", "fine")), save, _cutoff())

    assert seen_queries == ["bad", "good", "fine"]
    assert [c[1] for c in save.calls] == ["good", "fine"]
    assert saved == 2


def test_6_exception_inside_keyword_processing_is_isolated(monkeypatch):
    """Even a non-fetch failure (e.g. state lookup blowing up) for one
    keyword must not abort the remaining keywords."""
    def flaky_state(topic_key, keyword, kind):
        if keyword == "bad":
            raise RuntimeError("state exploded")
        return None

    monkeypatch.setattr(database, "get_last_seen_ts", flaky_state)
    monkeypatch.setattr(
        fetcher.requests, "get", lambda *a, **k: FakeResponse({"hits": [STORY_HIT]})
    )
    monkeypatch.setenv("HN_FETCH_COMMENTS", "false")
    save = SaveRecorder()

    saved = run_hackernews_for_job(_job("all", ("bad", "good")), save, _cutoff())

    assert [c[1] for c in save.calls] == ["good"]
    assert saved == 1


# ───────────────────────── test 7 ─────────────────────────

class FakeStateCollection:
    """Emulates update_one(..., {"$max": ..., "$set": ...}, upsert=True)
    with real $max semantics."""

    def __init__(self):
        self.docs = {}

    def update_one(self, flt, update, upsert=False):
        key = (flt["topic_key"], flt["keyword"], flt["kind"])
        doc = self.docs.get(key)
        if doc is None:
            if not upsert:
                return
            doc = {}
            self.docs[key] = doc
        for field, val in update.get("$max", {}).items():
            if field not in doc or val > doc[field]:
                doc[field] = val
        doc.update(update.get("$set", {}))


def test_7_set_last_seen_ts_keeps_only_the_larger_ts(monkeypatch):
    coll = FakeStateCollection()
    monkeypatch.setattr(database, "_get_state_collection", lambda: coll)

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 100)
    _REAL_SET_LAST_SEEN("nike", "nike", "story", 50)   # smaller -> ignored
    _REAL_SET_LAST_SEEN("nike", "nike", "story", 100)  # equal -> unchanged
    assert coll.docs[("nike", "nike", "story")]["last_created_at_i"] == 100

    _REAL_SET_LAST_SEEN("nike", "nike", "story", 250)  # bigger -> wins
    assert coll.docs[("nike", "nike", "story")]["last_created_at_i"] == 250

    # separate kind keeps its own checkpoint
    _REAL_SET_LAST_SEEN("nike", "nike", "comment", 7)
    assert coll.docs[("nike", "nike", "comment")]["last_created_at_i"] == 7


def test_7_set_last_seen_ts_uses_max_operator(monkeypatch):
    captured = {}

    class Capture:
        def update_one(self, flt, update, upsert=False):
            captured["update"] = update
            captured["upsert"] = upsert

    monkeypatch.setattr(database, "_get_state_collection", lambda: Capture())
    _REAL_SET_LAST_SEEN("nike", "nike", "story", 123)
    assert captured["update"]["$max"] == {"last_created_at_i": 123}
    assert captured["upsert"] is True


def test_7_set_last_seen_ts_does_not_raise_when_db_down(monkeypatch):
    # state collection unavailable
    monkeypatch.setattr(database, "_get_state_collection", lambda: None)
    _REAL_SET_LAST_SEEN("nike", "nike", "story", 1)  # must not raise

    # collection present but write explodes
    class Broken:
        def update_one(self, *a, **k):
            raise RuntimeError("mongo down")

    monkeypatch.setattr(database, "_get_state_collection", lambda: Broken())
    _REAL_SET_LAST_SEEN("nike", "nike", "story", 1)  # must not raise

    # no Mongo URI at all -> get_jobs_db() returns None, still no raise
    monkeypatch.setattr(database, "_client", None)
    monkeypatch.delenv("MONGODB1_URI", raising=False)
    _REAL_GET_LAST_SEEN("nike", "nike", "story")  # must not raise either


# ───────────────────────── test 8 ─────────────────────────

class CountingGet:
    def __init__(self):
        self.calls = 0

    def __call__(self, url, params=None, **k):
        self.calls += 1
        return FakeResponse({"hits": [STORY_HIT]})


@pytest.mark.parametrize("platform", ["reddit", "x_twitter"])
def test_8_non_hn_targets_return_zero_without_api_call(monkeypatch, platform):
    getter = CountingGet()
    monkeypatch.setattr(fetcher.requests, "get", getter)
    save = SaveRecorder()

    assert run_hackernews_for_job(_job(platform), save, _cutoff()) == 0
    assert getter.calls == 0
    assert save.calls == []


@pytest.mark.parametrize("platform", ["hackernews", "all"])
def test_8_hn_targets_hit_the_api(monkeypatch, platform):
    getter = CountingGet()
    monkeypatch.setattr(fetcher.requests, "get", getter)
    monkeypatch.setenv("HN_FETCH_COMMENTS", "false")
    save = SaveRecorder()

    saved = run_hackernews_for_job(_job(platform), save, _cutoff())
    assert getter.calls >= 1
    assert saved == 1
    assert save.calls[0][2] == "hackernews"
