"""
tests/test_hn_comments.py
=========================
HN story ke keyword-matched comments (hn_comments) ke hermetic tests.

  - Koi real network / Mongo / MySQL / OpenAI nahi: requests.get, Mongo, MySQL
    aur generate_embedding sab fake hain.
  - .env ka asar nahi: load_dotenv no-op.
  - Existing test files ko chheda nahi gaya.

Chalane ke liye (project root se):
    pytest tests/test_hn_comments.py -v
"""

import base64
import copy
import importlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pymongo  # noqa: E402
import pymysql  # noqa: E402
from pymongo.errors import DuplicateKeyError  # noqa: E402

from hackernews import config as hn_cfg, fetcher  # noqa: E402

DIM = 1536
T0 = 1_760_000_000  # a fixed "now-ish" unix time for comments


# ═════════════════════════════════════════════════════════════════════════════
# PART 1 — fetch_story_comments (hackernews/fetcher.py)
# ═════════════════════════════════════════════════════════════════════════════

class FakeResp:
    def __init__(self, payload=None, ok=True, json_exc=None):
        self._payload, self._ok, self._json_exc = payload, ok, json_exc

    def raise_for_status(self):
        if not self._ok:
            raise requests.HTTPError("500 Server Error")

    def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._payload


def hit(oid, text, ts, author="alice", parent=123):
    return {"objectID": str(oid), "comment_text": text, "created_at_i": ts,
            "author": author, "parent_id": parent}


@pytest.fixture
def http(monkeypatch):
    """requests.get fake: pages = list of payloads, returned in call order."""
    monkeypatch.setattr(hn_cfg, "load_dotenv", lambda *a, **k: None)
    monkeypatch.delenv("HN_STRICT_MATCH", raising=False)
    monkeypatch.setattr(hn_cfg, "HN_EXACT_PHRASE", False)
    state = SimpleNamespace(calls=[], pages=[])

    def fake_get(url, params=None, headers=None, timeout=None):
        state.calls.append({"url": url, "params": dict(params), "headers": headers, "timeout": timeout})
        idx = len(state.calls) - 1
        page = state.pages[min(idx, len(state.pages) - 1)]
        if isinstance(page, Exception):
            raise page
        return page

    monkeypatch.setattr(fetcher.requests, "get", fake_get)
    return state


def test_f1_request_shape(http):
    http.pages = [FakeResp({"hits": [], "nbPages": 1})]
    assert fetcher.fetch_story_comments(123, "nike") == []
    c = http.calls[0]
    assert c["url"] == hn_cfg.HN_API_URL and "search_by_date" in c["url"]
    assert c["params"]["query"] == "nike"
    assert c["params"]["tags"] == "comment,story_123"
    assert c["params"]["hitsPerPage"] == 1000
    assert "numericFilters" not in c["params"]
    assert c["headers"] == {"User-Agent": hn_cfg.HN_USER_AGENT}
    assert c["timeout"] == hn_cfg.HN_REQUEST_TIMEOUT


def test_f2_since_ts_uses_gte(http):
    http.pages = [FakeResp({"hits": [], "nbPages": 1})]
    fetcher.fetch_story_comments("123", "nike", since_ts=1700000000)
    assert http.calls[0]["params"]["numericFilters"] == "created_at_i>=1700000000"


def test_f3_exact_phrase_quoting(http, monkeypatch):
    http.pages = [FakeResp({"hits": [], "nbPages": 1})]
    monkeypatch.setattr(hn_cfg, "HN_EXACT_PHRASE", True)
    fetcher.fetch_story_comments("123", "air jordan")
    fetcher.fetch_story_comments("123", "nike")
    assert http.calls[0]["params"]["query"] == '"air jordan"'
    assert http.calls[1]["params"]["query"] == "nike"  # single word: no quotes


def test_f4_pagination_all_pages_oldest_first_flat(http):
    http.pages = [
        FakeResp({"nbPages": 3, "hits": [hit(3, "<p>nike three</p>", T0 + 30), hit(2, "nike two", T0 + 20)]}),
        FakeResp({"nbPages": 3, "hits": [hit(1, "nike one", T0 + 10, parent=2)]}),
        FakeResp({"nbPages": 3, "hits": [hit(0, "Nike zero", T0)]}),
    ]
    out = fetcher.fetch_story_comments("123", "nike")
    assert [p["params"]["page"] for p in http.calls] == [0, 1, 2]
    assert [c["comment_id"] for c in out] == ["0", "1", "2", "3"]  # oldest first, all depths flat
    c1 = out[1]
    assert c1["author"] == "alice" and c1["text"] == "nike one" and c1["parent_id"] == "2"
    assert c1["created_utc"] == datetime.fromtimestamp(T0 + 10, tz=timezone.utc)
    assert c1["created_utc"].tzinfo is not None
    assert out[3]["text"] == "nike three"  # HTML stripped


def test_f5_keyword_filter_strict_and_non_strict(http, monkeypatch):
    http.pages = [FakeResp({"nbPages": 1, "hits": [
        hit(1, "I love NIKE shoes", T0),
        hit(2, "adidas only", T0 + 1),
    ]})]
    out = fetcher.fetch_story_comments("123", "nike")  # strict (default on), case-insensitive
    assert [c["comment_id"] for c in out] == ["1"]

    monkeypatch.setenv("HN_STRICT_MATCH", "false")
    http.calls.clear()
    out = fetcher.fetch_story_comments("123", "nike")
    assert [c["comment_id"] for c in out] == ["1", "2"]


def test_f6_empty_deleted_dead_skipped(http):
    http.pages = [FakeResp({"nbPages": 1, "hits": [
        hit(1, None, T0),
        hit(2, "", T0 + 1),
        hit(3, "<p> </p>", T0 + 2),
        hit(4, "[dead]", T0 + 3),
        hit(5, "[deleted]", T0 + 4),
        {"comment_text": "nike but no id", "created_at_i": T0 + 5},
        {"objectID": "7", "comment_text": "nike but no timestamp"},
        hit(6, "nike real one", T0 + 6),
    ]})]
    out = fetcher.fetch_story_comments("123", "nike")
    assert [c["comment_id"] for c in out] == ["6"]


def test_f7_overlapping_pages_deduped(http):
    http.pages = [
        FakeResp({"nbPages": 2, "hits": [hit(1, "nike a", T0)]}),
        FakeResp({"nbPages": 2, "hits": [hit(1, "nike a", T0), hit(2, "nike b", T0 + 1)]}),
    ]
    out = fetcher.fetch_story_comments("123", "nike")
    assert [c["comment_id"] for c in out] == ["1", "2"]


@pytest.mark.parametrize("bad", [
    requests.ConnectionError("down"),
    FakeResp(ok=False),
    FakeResp(json_exc=ValueError("bad json")),
])
def test_f8_failure_returns_empty_never_raises(http, bad):
    http.pages = [bad]
    assert fetcher.fetch_story_comments("123", "nike") == []


def test_f9_later_page_failure_returns_empty_not_partial(http):
    """Partial result ke saath since_ts aage badh kar purane comments miss ho sakte the."""
    http.pages = [
        FakeResp({"nbPages": 2, "hits": [hit(2, "nike b", T0 + 1)]}),
        requests.ConnectionError("page 2 died"),
    ]
    assert fetcher.fetch_story_comments("123", "nike") == []


def test_f10_blank_inputs_no_request(http):
    assert fetcher.fetch_story_comments("", "nike") == []
    assert fetcher.fetch_story_comments("123", "  ") == []
    assert http.calls == []


# ═════════════════════════════════════════════════════════════════════════════
# PART 2 — fakes for flintel_service
# ═════════════════════════════════════════════════════════════════════════════

class FakeCollection:
    """Mongo `flintel_signals` fake: find_one / find / insert_one / update_one with the
    exact operators the HN code uses ($ne on array path, $elemMatch, $push, $set with `$`)."""

    def __init__(self):
        self.docs = []
        self.update_calls = 0
        self.insert_calls = 0
        self._n = 0

    @staticmethod
    def _match(doc, flt):
        for k, v in flt.items():
            if k == "hn_comments.comment_id":
                ids = [c.get("comment_id") for c in (doc.get("hn_comments") or [])]
                if isinstance(v, dict) and "$ne" in v:
                    if v["$ne"] in ids:
                        return False
                    continue
                return False
            if isinstance(v, dict) and "$elemMatch" in v:
                em = v["$elemMatch"]
                if not any(all(e.get(ek) == ev for ek, ev in em.items()) for e in (doc.get(k) or [])):
                    return False
                continue
            if isinstance(v, dict) and "$gte" in v:
                if doc.get(k) is None or doc.get(k) < v["$gte"]:
                    return False
                continue
            if doc.get(k) != v:
                return False
        return True

    def find_one(self, flt, projection=None):
        for d in self.docs:
            if self._match(d, flt):
                return copy.deepcopy(d)
        return None

    def find(self, flt, projection=None):
        return [copy.deepcopy(d) for d in self.docs if self._match(d, flt)]

    def insert_one(self, doc):
        self.insert_calls += 1
        if any(d["message_id"] == doc["message_id"] for d in self.docs):
            raise DuplicateKeyError("dup")
        self._n += 1
        doc["_id"] = self._n
        self.docs.append(copy.deepcopy(doc))

    def update_one(self, flt, upd, upsert=False):
        self.update_calls += 1
        for d in self.docs:
            if not self._match(d, flt):
                continue
            if "$push" in upd:
                for k, v in upd["$push"].items():
                    d.setdefault(k, [])
                    d[k].append(copy.deepcopy(v))
            if "$set" in upd:
                # Real Mongo positional `$` ek baar resolve hota hai (update se pehle).
                positions = {}
                for k in upd["$set"]:
                    if ".$." in k:
                        arr = k.split(".$.")[0]
                        if arr not in positions:
                            em = flt[arr]["$elemMatch"]
                            positions[arr] = next(
                                i for i, e in enumerate(d[arr])
                                if all(e.get(ek) == ev for ek, ev in em.items())
                            )
                for k, v in upd["$set"].items():
                    if ".$." in k:
                        arr, field = k.split(".$.")
                        d[arr][positions[arr]][field] = copy.deepcopy(v)
                    else:
                        d[k] = v
            return SimpleNamespace(matched_count=1, modified_count=1)
        return SimpleNamespace(matched_count=0, modified_count=0)


class FakeMongoDb:
    def __init__(self):
        self.flintel_signals = FakeCollection()


class FakeMySQLStore:
    def __init__(self, has_hn_col=True):
        self.rows = {}
        self.has_hn_col = has_hn_col
        self.alters = []
        self.update_calls = 0
        self.statements = []


class FakeCursor:
    def __init__(self, store):
        self.s = store
        self._res = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        q = " ".join(sql.split()).upper()
        self.s.statements.append(q)
        mod = sys.modules["flintel_service"]
        if q.startswith("CREATE TABLE"):
            pass
        elif q.startswith("SELECT COUNT(*) FROM INFORMATION_SCHEMA"):
            self._res = [(1 if self.s.has_hn_col else 0,)]
        elif q.startswith("ALTER TABLE FLINTEL_SIGNALS ADD COLUMN HN_COMMENTS JSON NULL"):
            self.s.alters.append(q)
            self.s.has_hn_col = True
        elif q.startswith("SELECT 1 FROM FLINTEL_SIGNALS"):
            self._res = [(1,)] if params[0] in self.s.rows else []
        elif q.startswith("INSERT INTO"):
            row = dict(zip(mod._MYSQL_COLUMNS, params))
            if row["message_id"] in self.s.rows:
                raise pymysql.err.IntegrityError(1062, "Duplicate entry")
            row["hn_comments"] = None
            self.s.rows[row["message_id"]] = row
        elif q.startswith("SELECT HN_COMMENTS FROM FLINTEL_SIGNALS WHERE MESSAGE_ID=%S"):
            r = self.s.rows.get(params[0])
            self._res = [(r["hn_comments"],)] if r else []
        elif q.startswith("UPDATE FLINTEL_SIGNALS SET HN_COMMENTS"):
            payload, mid = params
            self.s.update_calls += 1
            self.s.rows[mid]["hn_comments"] = payload
        elif q.startswith("SELECT MESSAGE_ID, TOPIC_KEY, SEARCH_KEYWORD"):
            platform, cutoff = params
            self._res = [
                (r["message_id"], r["topic_key"], r["search_keyword"])
                for r in self.s.rows.values()
                if r["platform"] == platform and r["created_utc"] >= cutoff
            ]
        else:  # pragma: no cover
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self._res[0] if self._res else None

    def fetchall(self):
        return list(self._res)


class FakeConn:
    def __init__(self, store):
        self.s = store

    def ping(self, reconnect=True):
        pass

    def cursor(self):
        return FakeCursor(self.s)

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def vec_for(seed: float):
    return [seed + (i % 7) * 0.01 for i in range(DIM)]


@pytest.fixture
def fs(monkeypatch):
    monkeypatch.setattr(pymongo, "MongoClient", lambda *a, **k: MagicMock())
    sys.modules.pop("flintel_service", None)
    mod = importlib.import_module("flintel_service")

    monkeypatch.setattr(mod, "load_dotenv", lambda *a, **k: False)
    monkeypatch.setattr(mod.hn_config, "load_dotenv", lambda *a, **k: None)

    mod.db = FakeMongoDb()
    mod.db1 = MagicMock()

    store = FakeMySQLStore()
    monkeypatch.setattr(mod, "_mysql_connect", lambda: FakeConn(store))
    mod._mysql_local = threading.local()
    mod._mysql_table_ready = False
    mod._mysql_hn_col_ready = False
    mod._mysql_hn_col_last_try = 0.0
    mod._last_no_sink_warn = 0.0
    mod.store = store

    # embeddings: every call returns a distinct vector, counted
    emb = SimpleNamespace(calls=[], enabled=True)

    def fake_embed(text):
        emb.calls.append(text)
        return vec_for(len(emb.calls) / 100.0)

    monkeypatch.setattr(mod, "generate_embedding", fake_embed)
    monkeypatch.setattr(mod, "_is_embedding_enabled", lambda: emb.enabled)
    monkeypatch.setattr(mod, "EMBEDDING_BACKFILL_GAP_SECONDS", 0.0)
    mod.emb = emb

    # fetch_story_comments fake: records args, returns mod.fetch_result (or raises)
    fetch = SimpleNamespace(calls=[], result=[], raises=None)

    def fake_fetch(story_id, keyword, since_ts=None):
        fetch.calls.append((str(story_id), keyword, since_ts))
        if fetch.raises:
            raise fetch.raises
        return list(fetch.result)

    monkeypatch.setattr(mod, "fetch_story_comments", fake_fetch)
    mod.fetch = fetch

    monkeypatch.setattr(mod.hn_config, "HN_KEYWORD_GAP_SECONDS", 0.0)
    monkeypatch.setattr(mod, "HN_MAX_COMMENTS_PER_STORY", 200)
    for name in ("MONGODB_DATA", "MYSQL_DATA", "HN_NESTED_COMMENTS_ENABLED", "HACKERNEWS_ENABLED"):
        monkeypatch.delenv(name, raising=False)

    yield mod
    sys.modules.pop("flintel_service", None)


def sinks(monkeypatch, mongo=True, mysql=False):
    monkeypatch.setenv("MONGODB_DATA", str(mongo).lower())
    monkeypatch.setenv("MYSQL_DATA", str(mysql).lower())


def story(sid="100", ts=None, title="Show HN: Nike tracker", **kw):
    p = {"id": sid, "title": title, "selftext": "", "author": "bob", "subreddit": "",
         "post_url": f"https://news.ycombinator.com/item?id={sid}",
         "created_utc": float(ts if ts is not None else time.time()), "score": 5, "num_comments": 3}
    p.update(kw)
    return p


def comment(cid, text=None, ts=T0, author="alice"):
    return {"comment_id": str(cid), "author": author, "text": text or f"nike comment {cid}",
            "created_utc": datetime.fromtimestamp(ts, tz=timezone.utc), "parent_id": "100"}


def mongo_doc(fs, sid="100"):
    return fs.db.flintel_signals.find_one({"message_id": f"hackernews_{sid}"})


def mysql_comments(fs, sid="100"):
    raw = fs.store.rows[f"hackernews_{sid}"]["hn_comments"]
    return None if raw is None else json.loads(raw)


def mysql_vec(fs, entry):
    return fs._blob_to_embedding(base64.b64decode(entry["embedding"]))


# ═════════════════════════════════════════════════════════════════════════════
# PART 3 — storage, hook, embeddings, retry
# ═════════════════════════════════════════════════════════════════════════════

def test_s1_field_absent_until_first_matching_comment(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    fs.fetch.result = []  # koi matching comment nahi
    assert fs._save_signal("nike", "nike", "hackernews", story()) is True
    assert "hn_comments" not in mongo_doc(fs)              # Mongo: ABSENT
    assert fs.store.rows["hackernews_100"]["hn_comments"] is None  # MySQL: NULL
    assert fs.emb.calls == [fs.emb.calls[0]]  # sirf post ka apna embedding (1 call)
    assert fs.emb.calls[0].startswith("Show HN")  # comments ka text nahi


def test_s2_new_comment_one_embedding_both_sinks_story_text_untouched(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    fs.fetch.result = [comment(1, "I like <nike> a lot", ts=T0)]
    assert fs._save_signal("nike", "nike", "hackernews", story(selftext="body text")) is True

    assert fs.fetch.calls == [("100", "nike", None)]  # matched_keyword pass hua, since_ts None
    assert fs.emb.calls == ["Show HN: Nike tracker\n\nbody text", "I like <nike> a lot"]  # post + 1 comment

    m = mongo_doc(fs)["hn_comments"]
    assert len(m) == 1
    assert m[0]["comment_id"] == "1" and m[0]["author"] == "alice"
    assert m[0]["created_utc"] == datetime.fromtimestamp(T0, tz=timezone.utc)
    assert isinstance(m[0]["embedding"], list) and len(m[0]["embedding"]) == DIM
    assert m[0]["embedding_dim"] == DIM and m[0]["embedding_model"] == fs.EMBEDDING_MODEL

    sq = mysql_comments(fs)
    assert len(sq) == 1 and sq[0]["comment_id"] == "1"
    assert sq[0]["created_utc"] == datetime.fromtimestamp(T0, tz=timezone.utc).isoformat()
    assert isinstance(sq[0]["embedding"], str)                      # base64 string
    assert len(base64.b64decode(sq[0]["embedding"])) == DIM * 4     # float32 LE bytes
    assert sq[0]["embedding_dim"] == DIM
    assert mysql_vec(fs, sq[0]) == pytest.approx(m[0]["embedding"], rel=1e-6)  # ek hi vector

    # story ka `text` sirf post ka text
    assert mongo_doc(fs)["text"] == "Show HN: Nike tracker\n\nbody text"
    assert fs.store.rows["hackernews_100"]["text"] == "Show HN: Nike tracker\n\nbody text"
    assert "nike comment" not in mongo_doc(fs)["text"]


def test_s3_hook_only_on_new_hn_story(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    fs.fetch.result = []
    # reddit / hackernews_comment / twitter par hook nahi
    fs._save_signal("nike", "nike", "reddit", story(sid="r1"))
    fs._save_signal("nike", "nike", "hackernews_comment", story(sid="c1"))
    assert fs.fetch.calls == []
    # nayi HN story par ek baar
    assert fs._save_signal("nike", "nike", "hackernews", story()) is True
    assert len(fs.fetch.calls) == 1
    n_embed = len(fs.emb.calls)
    # already-saved story: poll cycle mein koi extra Algolia / OpenAI call nahi
    for _ in range(3):
        assert fs._save_signal("nike", "nike", "hackernews", story()) is False
    assert len(fs.fetch.calls) == 1 and len(fs.emb.calls) == n_embed


def test_s4_dedupe_on_comment_id_already_embedded_never_reembedded(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    mid = "hackernews_100"
    fs.fetch.result = [comment(1, ts=T0), comment(2, ts=T0 + 10)]
    fs._save_signal("nike", "nike", "hackernews", story())
    base = len(fs.emb.calls)  # post + 2 comments
    assert base == 3

    # overlap: >= since_ts ki wajah se Algolia wahi comments phir deta hai (+ ek naya)
    fs.fetch.result = [comment(2, ts=T0 + 10), comment(3, ts=T0 + 20)]
    res = fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert res["new"] == 1
    assert len(fs.emb.calls) == base + 1           # sirf comment 3
    assert fs.emb.calls[-1] == "nike comment 3"
    assert fs.fetch.calls[-1][2] == T0 + 10        # since_ts = max stored created_utc
    assert [c["comment_id"] for c in mongo_doc(fs)["hn_comments"]] == ["1", "2", "3"]
    assert [c["comment_id"] for c in mysql_comments(fs)] == ["1", "2", "3"]

    # bilkul wahi list dobara -> koi embedding call / koi write nahi
    n_embed, m_upd, s_upd = len(fs.emb.calls), fs.db.flintel_signals.update_calls, fs.store.update_calls
    res = fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert res == {"new": 0, "embedded": 0, "retried": 0, "filled": 0}
    assert (len(fs.emb.calls), fs.db.flintel_signals.update_calls, fs.store.update_calls) == (n_embed, m_upd, s_upd)


def test_s5_retry_only_comments_with_none_embedding(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    mid = "hackernews_100"
    fs.fetch.result = [comment(1, ts=T0)]
    fs._save_signal("nike", "nike", "hackernews", story())        # post + c1 embedded
    # c2 embedding disabled hone par aata hai -> None store hota hai
    fs.emb.enabled = False
    fs.fetch.result = [comment(1, ts=T0), comment(2, ts=T0 + 10)]
    fs._sync_hn_story_comments("nike", mid, "100", "nike")
    m = {c["comment_id"]: c for c in mongo_doc(fs)["hn_comments"]}
    assert m["2"]["embedding"] is None and m["2"]["embedding_dim"] is None
    assert {c["comment_id"]: c for c in mysql_comments(fs)}["2"]["embedding"] is None
    c1_vec = list(m["1"]["embedding"])

    # embeddings wapas on -> sirf c2 retry hota hai
    fs.emb.enabled = True
    fs.emb.calls.clear()
    fs.fetch.result = []
    res = fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert fs.emb.calls == ["nike comment 2"]
    assert res["retried"] == 1 and res["new"] == 0
    m = {c["comment_id"]: c for c in mongo_doc(fs)["hn_comments"]}
    assert isinstance(m["2"]["embedding"], list) and m["2"]["embedding_dim"] == DIM
    assert m["1"]["embedding"] == c1_vec                      # c1 ko chhua nahi
    assert len(mongo_doc(fs)["hn_comments"]) == 2            # duplicate entry nahi bani
    sq = {c["comment_id"]: c for c in mysql_comments(fs)}
    assert mysql_vec(fs, sq["2"]) == pytest.approx(m["2"]["embedding"], rel=1e-6)

    # ab dobara chalao -> koi call nahi
    fs.emb.calls.clear()
    fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert fs.emb.calls == []


def test_s6_failed_embedding_stays_none_and_is_retried_next_time(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    mid = "hackernews_100"
    fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    monkeypatch.setattr(fs, "generate_embedding", lambda t: None)   # OpenAI fail
    fs.fetch.result = [comment(1)]
    fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert mongo_doc(fs)["hn_comments"][0]["embedding"] is None     # save nahi roka
    calls = []
    monkeypatch.setattr(fs, "generate_embedding", lambda t: calls.append(t) or vec_for(0.5))
    fs.fetch.result = []
    fs._sync_hn_story_comments("nike", mid, "100", "nike")
    assert calls == ["nike comment 1"]
    assert isinstance(mongo_doc(fs)["hn_comments"][0]["embedding"], list)


def test_s7_gap_between_embedding_calls(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    fs._save_signal("nike", "nike", "hackernews", story())
    monkeypatch.setattr(fs, "EMBEDDING_BACKFILL_GAP_SECONDS", 0.5)
    sleeps = []
    monkeypatch.setattr(fs.time, "sleep", lambda s: sleeps.append(s))
    fs.fetch.result = [comment(1, ts=T0), comment(2, ts=T0 + 1), comment(3, ts=T0 + 2)]
    fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike")
    assert sleeps == [0.5, 0.5]  # 3 calls ke beech 2 gaps


def test_s8_cap_stops_at_limit_without_crash(fs, monkeypatch, caplog):
    sinks(monkeypatch, True, True)
    monkeypatch.setattr(fs, "HN_MAX_COMMENTS_PER_STORY", 3)
    fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    fs.emb.calls.clear()
    fs.fetch.result = [comment(i, ts=T0 + i) for i in range(1, 6)]   # 5 aaye, cap 3
    with caplog.at_level("INFO"):
        res = fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike")
    assert res["new"] == 3 and len(fs.emb.calls) == 3
    assert [c["comment_id"] for c in mongo_doc(fs)["hn_comments"]] == ["1", "2", "3"]
    assert len(mysql_comments(fs)) == 3
    assert any("cap" in r.getMessage() and r.levelname == "INFO" for r in caplog.records)

    # cap par: na Algolia call, na embedding, na write
    fs.fetch.calls.clear(); fs.emb.calls.clear()
    m_upd, s_upd = fs.db.flintel_signals.update_calls, fs.store.update_calls
    fs.fetch.result = [comment(9, ts=T0 + 99)]
    res = fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike")
    assert res["new"] == 0 and fs.fetch.calls == [] and fs.emb.calls == []
    assert (fs.db.flintel_signals.update_calls, fs.store.update_calls) == (m_upd, s_upd)


def test_s9_default_cap_is_200(fs):
    assert fs.HN_MAX_COMMENTS_PER_STORY == 200
    assert fs.HN_COMMENT_REFRESH_INTERVAL_SECONDS == 7200
    assert fs.HN_COMMENT_TRACK_DAYS == 14


def test_s10_nested_comments_disabled_skips_hook_and_sync(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "False")
    fs.fetch.result = [comment(1)]
    assert fs._save_signal("nike", "nike", "hackernews", story()) is True   # story save normal
    assert fs.fetch.calls == []
    assert len(fs.emb.calls) == 1                                           # sirf post ka embedding
    assert "hn_comments" not in mongo_doc(fs)
    assert fs.store.rows["hackernews_100"]["hn_comments"] is None
    assert fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike") == \
        {"new": 0, "embedded": 0, "retried": 0, "filled": 0}
    assert fs.fetch.calls == []


def test_s11_second_sink_enabled_later_copies_vector_no_new_call(fs, monkeypatch):
    sinks(monkeypatch, True, False)                       # pehle sirf Mongo
    fs.fetch.result = [comment(1, ts=T0)]
    fs._save_signal("nike", "nike", "hackernews", story())
    mongo_vec = mongo_doc(fs)["hn_comments"][0]["embedding"]

    # MySQL baad mein on; story wahan bhi aati hai (hook band, taake fill alag se dikhe)
    sinks(monkeypatch, True, True)
    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "false")
    fs.emb.calls.clear()
    fs._save_signal("nike", "nike", "hackernews", story())
    assert fs.store.rows["hackernews_100"]["hn_comments"] is None
    assert fs.emb.calls == []                              # post ka vector Mongo se reuse hua

    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "true")
    fs.fetch.result = []
    res = fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike")
    assert fs.emb.calls == []                              # koi naya OpenAI call nahi
    assert res["filled"] == 1 and res["new"] == 0
    sq = mysql_comments(fs)
    assert len(sq) == 1 and mysql_vec(fs, sq[0]) == pytest.approx(mongo_vec, rel=1e-6)


def test_s11b_hook_fills_new_sink_automatically_without_embedding(fs, monkeypatch):
    """MySQL on hone ke baad story us mein insert ho to hook khud Mongo ke stored
    comments (aur unke vectors) MySQL mein copy kar deta hai — naya OpenAI call nahi."""
    sinks(monkeypatch, True, False)
    fs.fetch.result = [comment(1, ts=T0)]
    fs._save_signal("nike", "nike", "hackernews", story())
    sinks(monkeypatch, True, True)
    fs.emb.calls.clear(); fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    assert fs.emb.calls == []
    assert len(mysql_comments(fs)) == 1


def test_s12_sync_never_raises_on_fetch_error(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    fs._save_signal("nike", "nike", "hackernews", story())
    fs.fetch.raises = RuntimeError("algolia exploded")
    assert fs._sync_hn_story_comments("nike", "hackernews_100", "100", "nike")["new"] == 0


# ═════════════════════════════════════════════════════════════════════════════
# PART 4 — atomic sink writes
# ═════════════════════════════════════════════════════════════════════════════

def test_a1_mongo_push_is_conditional_on_comment_id(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    rec = {"comment_id": "1", "author": "a", "text": "nike x",
           "created_utc": datetime.fromtimestamp(T0, tz=timezone.utc), "embedding": vec_for(0.1)}
    assert fs._hn_append_mongo("hackernews_100", rec) is True      # absent field par array bana
    assert fs._hn_append_mongo("hackernews_100", rec) is False     # $ne filter match nahi karta
    assert len(mongo_doc(fs)["hn_comments"]) == 1


def test_a2_set_embedding_never_overwrites_existing(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    rec = {"comment_id": "1", "author": "a", "text": "nike x",
           "created_utc": datetime.fromtimestamp(T0, tz=timezone.utc), "embedding": vec_for(0.1)}
    fs._hn_append_mongo("hackernews_100", rec); fs._hn_append_mysql("hackernews_100", rec)
    assert fs._hn_set_embedding_mongo("hackernews_100", "1", vec_for(0.9)) is False
    assert fs._hn_set_embedding_mysql("hackernews_100", "1", vec_for(0.9)) is False
    assert mongo_doc(fs)["hn_comments"][0]["embedding"] == vec_for(0.1)
    assert mysql_vec(fs, mysql_comments(fs)[0]) == pytest.approx(vec_for(0.1), rel=1e-6)


def test_a3_mysql_append_dedupes_and_never_writes_when_unchanged(fs, monkeypatch):
    sinks(monkeypatch, False, True)
    fs.fetch.result = []
    fs._save_signal("nike", "nike", "hackernews", story())
    assert fs.store.update_calls == 0
    rec = {"comment_id": "1", "author": "a", "text": "nike 😀 café",
           "created_utc": datetime.fromtimestamp(T0, tz=timezone.utc), "embedding": None}
    assert fs._hn_append_mysql("hackernews_100", rec) is True
    assert fs._hn_append_mysql("hackernews_100", rec) is False
    assert fs.store.update_calls == 1
    assert mysql_comments(fs)[0]["text"] == "nike 😀 café"
    assert mysql_comments(fs)[0]["embedding"] is None
    assert any(s.endswith("FOR UPDATE") for s in fs.store.statements)


# ═════════════════════════════════════════════════════════════════════════════
# PART 5 — MySQL column migration (guarded ALTER, never DROP)
# ═════════════════════════════════════════════════════════════════════════════

def test_m1_alter_runs_once_when_column_missing(fs, monkeypatch):
    sinks(monkeypatch, False, True)
    fs.store.has_hn_col = False
    fs._save_signal("nike", "nike", "reddit", story(sid="r1"))
    fs._save_signal("nike", "nike", "reddit", story(sid="r2"))
    assert len(fs.store.alters) == 1
    assert "HN_COMMENTS JSON NULL" in fs.store.alters[0]
    assert not any("DROP" in s for s in fs.store.statements)
    assert "hn_comments" not in fs._MYSQL_INSERT_SQL      # reddit/twitter INSERT unchanged
    assert fs._MYSQL_COLUMNS[-1] == "reddit_comments" and "hn_comments" not in fs._MYSQL_COLUMNS


def test_m2_no_alter_when_column_exists(fs, monkeypatch):
    sinks(monkeypatch, False, True)
    fs.store.has_hn_col = True
    fs._save_signal("nike", "nike", "reddit", story(sid="r1"))
    assert fs.store.alters == []


def test_m3_alter_failure_does_not_break_saves(fs, monkeypatch, caplog):
    sinks(monkeypatch, False, True)
    fs.store.has_hn_col = False

    def boom(conn):
        raise RuntimeError("no ALTER privilege")

    monkeypatch.setattr(fs, "_ensure_hn_comments_column", boom)
    with caplog.at_level("WARNING"):
        assert fs._save_signal("nike", "nike", "reddit", story(sid="r1")) is True
    assert "reddit_r1" in fs.store.rows
    assert any("hn_comments column" in r.getMessage() for r in caplog.records)


# ═════════════════════════════════════════════════════════════════════════════
# PART 6 — 2-hourly refresher
# ═════════════════════════════════════════════════════════════════════════════

class _Stop(Exception):
    pass


def seed_story(fs, monkeypatch, sid="100", ts=None, kw="nike", platform="hackernews"):
    """Story ko hook ke bina save karo (comments baad mein refresher se aayein)."""
    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "false")
    fs._save_signal("nike", kw, platform, story(sid=sid, ts=ts))
    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "true")


def test_r1_no_new_comment_means_no_embedding_and_no_write(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    seed_story(fs, monkeypatch)
    fs.fetch.result = []
    fs.emb.calls.clear()
    m_upd, s_upd = fs.db.flintel_signals.update_calls, fs.store.update_calls
    summary = fs._run_hn_refresh_cycle()
    assert summary["stories"] == 1 and summary["new"] == 0
    assert fs.fetch.calls == [("100", "nike", None)]       # story ka apna search_keyword
    assert fs.emb.calls == []
    assert (fs.db.flintel_signals.update_calls, fs.store.update_calls) == (m_upd, s_upd)


def test_r2_new_comment_one_embedding_both_sinks(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    seed_story(fs, monkeypatch)
    fs.emb.calls.clear()
    fs.fetch.result = [comment(1, ts=T0)]
    summary = fs._run_hn_refresh_cycle()
    assert summary["new"] == 1
    assert fs.emb.calls == ["nike comment 1"]
    assert len(mongo_doc(fs)["hn_comments"]) == 1 and len(mysql_comments(fs)) == 1

    # agla cycle, wahi comment phir aaye (overlap) + koi naya nahi
    fs.emb.calls.clear(); fs.fetch.calls.clear()
    summary = fs._run_hn_refresh_cycle()
    assert summary["new"] == 0 and fs.emb.calls == []
    assert fs.fetch.calls[0][2] == T0                       # since_ts = max stored created_utc


def test_r3_targets_only_recent_hn_stories_mongo_and_mysql(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    now = time.time()
    seed_story(fs, monkeypatch, sid="1", ts=now - 3600)                      # recent HN
    seed_story(fs, monkeypatch, sid="2", ts=now - 20 * 86400)                # 14 din se purani
    seed_story(fs, monkeypatch, sid="3", ts=now - 3600, platform="reddit")   # reddit
    seed_story(fs, monkeypatch, sid="4", ts=now - 3600, platform="hackernews_comment")
    t = fs._hn_refresh_targets()
    assert [x["message_id"] for x in t] == ["hackernews_1"]
    assert t[0]["story_id"] == "1" and t[0]["search_keyword"] == "nike" and t[0]["topic_key"] == "nike"

    sinks(monkeypatch, False, True)                                          # Mongo off -> MySQL se
    t = fs._hn_refresh_targets()
    assert [x["message_id"] for x in t] == ["hackernews_1"]

    monkeypatch.setattr(fs, "HN_COMMENT_TRACK_DAYS", 30)
    assert sorted(x["message_id"] for x in fs._hn_refresh_targets()) == ["hackernews_1", "hackernews_2"]


def test_r4_one_story_failing_does_not_stop_the_rest(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    seed_story(fs, monkeypatch, sid="1")
    seed_story(fs, monkeypatch, sid="2")
    real_sync = fs._sync_hn_story_comments

    def flaky(topic_key, message_id, story_id, keyword):
        if story_id == "1":
            raise RuntimeError("boom")
        return real_sync(topic_key, message_id, story_id, keyword)

    monkeypatch.setattr(fs, "_sync_hn_story_comments", flaky)
    fs.fetch.result = [comment(7)]
    summary = fs._run_hn_refresh_cycle()
    assert summary["stories"] == 2 and summary["new"] == 1
    assert len(mongo_doc(fs, "2")["hn_comments"]) == 1


def test_r5_gap_between_stories(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    for sid in ("1", "2", "3"):
        seed_story(fs, monkeypatch, sid=sid)
    monkeypatch.setattr(fs.hn_config, "HN_KEYWORD_GAP_SECONDS", 2.5)
    sleeps = []
    monkeypatch.setattr(fs.time, "sleep", lambda s: sleeps.append(s))
    fs._run_hn_refresh_cycle()
    assert sleeps == [2.5, 2.5]                              # 3 stories ke beech 2 gaps


def test_r6_retry_happens_in_refresher_for_none_embeddings(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    seed_story(fs, monkeypatch)
    fs.emb.enabled = False
    fs.fetch.result = [comment(1, ts=T0)]
    fs._run_hn_refresh_cycle()                               # c1 None embedding ke saath store
    assert mongo_doc(fs)["hn_comments"][0]["embedding"] is None
    fs.emb.enabled = True
    fs.emb.calls.clear(); fs.fetch.result = []
    summary = fs._run_hn_refresh_cycle()
    assert summary["retried"] == 1 and fs.emb.calls == ["nike comment 1"]
    assert isinstance(mongo_doc(fs)["hn_comments"][0]["embedding"], list)


def _run_one_loop_iteration(fs, monkeypatch):
    """run_hn_comment_refresher ko ek cycle ke baad (pehle sleep par) rok do."""
    def stop(_secs):
        raise _Stop()
    monkeypatch.setattr(fs.time, "sleep", stop)
    with pytest.raises(_Stop):
        fs.run_hn_comment_refresher()


def test_r7_nested_disabled_refresher_does_nothing(fs, monkeypatch):
    sinks(monkeypatch, True, True)
    seed_story(fs, monkeypatch)
    monkeypatch.setenv("HN_NESTED_COMMENTS_ENABLED", "False")
    fs.fetch.result = [comment(1)]
    fs.emb.calls.clear()
    monkeypatch.setattr(fs, "_hn_refresh_targets", lambda: pytest.fail("targets list nahi honi chahiye"))
    _run_one_loop_iteration(fs, monkeypatch)
    assert fs.fetch.calls == [] and fs.emb.calls == []
    assert "hn_comments" not in mongo_doc(fs)


def test_r8_hackernews_disabled_refresher_does_nothing(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    seed_story(fs, monkeypatch)
    monkeypatch.setenv("HACKERNEWS_ENABLED", "false")
    fs.fetch.result = [comment(1)]
    monkeypatch.setattr(fs, "_hn_refresh_targets", lambda: pytest.fail("targets list nahi honi chahiye"))
    _run_one_loop_iteration(fs, monkeypatch)
    assert fs.fetch.calls == []


def test_r9_enabled_loop_runs_first_cycle_immediately(fs, monkeypatch):
    sinks(monkeypatch, True, False)
    seed_story(fs, monkeypatch)
    fs.fetch.result = [comment(1)]
    _run_one_loop_iteration(fs, monkeypatch)                 # pehli cycle sleep se pehle chal gayi
    assert len(mongo_doc(fs)["hn_comments"]) == 1


def test_r10_thread_started_in_start_all(fs, monkeypatch):
    started = []

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None, name=None):
            self.target, self.name = target, name

        def start(self):
            started.append((self.name, self.target))

        def join(self):
            pass

    monkeypatch.setattr(fs.threading, "Thread", FakeThread)
    monkeypatch.setattr(fs, "start_worker_pool", lambda: [])
    fs.start_all()
    names = {n: t for n, t in started}
    assert names["RedditPoller"] is fs.run_reddit_poller
    assert names["HNCommentRefresher"] is fs.run_hn_comment_refresher
