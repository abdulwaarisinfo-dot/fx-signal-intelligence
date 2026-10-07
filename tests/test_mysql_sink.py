"""Tests for the MySQL sink + MONGODB_DATA / MYSQL_DATA switches.
Everything is faked: no real Mongo, MySQL or OpenAI is touched."""
import importlib
import json
import logging
import struct
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pymongo  # noqa: E402
import pymysql  # noqa: E402
from pymongo.errors import DuplicateKeyError  # noqa: E402


# ───────────────────────── fakes ─────────────────────────

class FakeCollection:
    def __init__(self):
        self.docs = []
        self.insert_calls = 0
        self.find_calls = 0
        self.fail = False
        self._n = 0

    @staticmethod
    def _match(doc, flt):
        return all(doc.get(k) == v for k, v in flt.items())

    def find_one(self, flt, projection=None):
        self.find_calls += 1
        if self.fail:
            raise RuntimeError("mongo down")
        for d in self.docs:
            if self._match(d, flt):
                return dict(d)
        return None

    def insert_one(self, doc):
        self.insert_calls += 1
        if self.fail:
            raise RuntimeError("mongo down")
        if any(d["message_id"] == doc["message_id"] for d in self.docs):
            raise DuplicateKeyError("dup")
        self._n += 1
        doc["_id"] = self._n
        self.docs.append(doc)

    def update_one(self, flt, upd):
        for d in self.docs:
            if self._match(d, flt):
                d.update(upd["$set"])
                return


class FakeMongoDb:
    def __init__(self):
        self.flintel_signals = FakeCollection()


class FakeMySQLStore:
    def __init__(self):
        self.rows = {}
        self.created = False
        self.fail_connect = False
        self.connect_calls = 0
        self.insert_calls = 0


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
        mod = sys.modules["flintel_service"]
        if q.startswith("CREATE TABLE"):
            self.s.created = True
        elif q.startswith("SELECT 1 FROM FLINTEL_SIGNALS"):
            self._res = [(1,)] if params[0] in self.s.rows else []
        elif q.startswith("INSERT INTO"):
            self.s.insert_calls += 1
            row = dict(zip(mod._MYSQL_COLUMNS, params))
            if row["message_id"] in self.s.rows:
                raise pymysql.err.IntegrityError(1062, "Duplicate entry")
            self.s.rows[row["message_id"]] = row
        elif q.startswith("SELECT REDDIT_COMMENTS"):
            topic, mid = params
            r = self.s.rows.get(mid)
            self._res = [(r["reddit_comments"],)] if r and r["topic_key"] == topic else []
        elif q.startswith("UPDATE FLINTEL_SIGNALS SET REDDIT_COMMENTS"):
            payload, topic, mid = params
            self.s.rows[mid]["reddit_comments"] = payload
        else:  # pragma: no cover
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self._res[0] if self._res else None


class FakeConn:
    def __init__(self, store):
        self.s = store
        self.closed = False

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
        self.closed = True


# ───────────────────────── fixtures ─────────────────────────

@pytest.fixture
def fs(monkeypatch):
    monkeypatch.setattr(pymongo, "MongoClient", lambda *a, **k: MagicMock())
    sys.modules.pop("flintel_service", None)
    mod = importlib.import_module("flintel_service")

    # hermetic: a real .env must never override monkeypatched env vars
    monkeypatch.setattr(mod, "load_dotenv", lambda *a, **k: False)

    mod.db = FakeMongoDb()
    mod.db1 = MagicMock()

    mod._real_mysql_connect = mod._mysql_connect
    store = FakeMySQLStore()

    def fake_connect():
        store.connect_calls += 1
        if store.fail_connect:
            raise pymysql.err.OperationalError(2003, "Can't connect to MySQL server")
        return FakeConn(store)

    monkeypatch.setattr(mod, "_mysql_connect", fake_connect)
    mod._mysql_local = threading.local()
    mod._mysql_table_ready = False
    mod._last_no_sink_warn = 0.0
    mod.store = store

    calls = {"n": 0}

    def fake_embed(text):
        calls["n"] += 1
        return [0.1 * ((i % 7) + 1) for i in range(1536)]

    monkeypatch.setattr(mod, "generate_embedding", fake_embed)
    monkeypatch.setattr(mod, "_is_embedding_enabled", lambda: True)
    mod.embed_calls = calls

    monkeypatch.delenv("MONGODB_DATA", raising=False)
    monkeypatch.delenv("MYSQL_DATA", raising=False)
    yield mod
    sys.modules.pop("flintel_service", None)


def flags(monkeypatch, mongo, mysql):
    monkeypatch.setenv("MONGODB_DATA", str(mongo).lower())
    monkeypatch.setenv("MYSQL_DATA", str(mysql).lower())


def make_post(pid="abc", title="Nike shoes are great", **kw):
    p = {
        "id": pid, "title": title, "selftext": "", "author": "u1",
        "subreddit": "sneakers",
        "post_url": f"https://www.reddit.com/r/sneakers/comments/{pid}/x/",
        "created_utc": 1_760_000_000.0, "score": 0, "num_comments": 0,
    }
    p.update(kw)
    return p


# ───────────────────────── tests ─────────────────────────

def test_1_default_only_mongo(fs):
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert len(fs.db.flintel_signals.docs) == 1
    assert fs.db.flintel_signals.docs[0]["reddit_comments"] == 0
    assert fs.store.connect_calls == 0 and fs.store.rows == {}


def test_2_only_mysql(fs, monkeypatch):
    flags(monkeypatch, False, True)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert fs.db.flintel_signals.insert_calls == 0
    assert "reddit_abc" in fs.store.rows
    assert fs.store.rows["reddit_abc"]["reddit_comments"] is None  # NULL, not 0
    assert fs.store.created is True


def test_3_both_one_embedding_call(fs, monkeypatch):
    flags(monkeypatch, True, True)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert fs.embed_calls["n"] == 1
    assert len(fs.db.flintel_signals.docs) == 1
    row = fs.store.rows["reddit_abc"]
    assert len(row["embedding"]) == 6144 and row["embedding_dim"] == 1536
    assert row["embedding_model"] == fs.EMBEDDING_MODEL


def test_4_both_false(fs, monkeypatch, caplog):
    flags(monkeypatch, False, False)
    with caplog.at_level(logging.WARNING):
        assert fs._save_signal("nike", "nike", "reddit", make_post()) is False
    assert any("dono false" in r.getMessage() for r in caplog.records)
    assert fs.embed_calls["n"] == 0
    assert fs.db.flintel_signals.insert_calls == 0 and fs.store.insert_calls == 0


def test_5_duplicate_zero_embedding_calls(fs, monkeypatch):
    flags(monkeypatch, True, True)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert fs.embed_calls["n"] == 1
    for _ in range(5):  # poller re-fetching same RSS window
        assert fs._save_signal("nike", "nike", "reddit", make_post()) is False
    assert fs.embed_calls["n"] == 1  # still 1: duplicates never embed
    assert fs.db.flintel_signals.insert_calls == 1 and fs.store.insert_calls == 1


def test_6_mongo_has_it_mysql_new_reuses_embedding(fs, monkeypatch):
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True  # mongo only
    assert fs.embed_calls["n"] == 1
    mongo_emb = fs.db.flintel_signals.docs[0]["embedding"]

    flags(monkeypatch, True, True)  # MySQL switched on later
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert fs.embed_calls["n"] == 1  # reused, no new OpenAI call
    assert fs.db.flintel_signals.insert_calls == 1
    assert fs.store.insert_calls == 1
    blob_vals = fs._blob_to_embedding(fs.store.rows["reddit_abc"]["embedding"])
    assert blob_vals == pytest.approx(mongo_emb, rel=1e-6)


def test_7_mysql_fail_mongo_still_saved(fs, monkeypatch, caplog):
    flags(monkeypatch, True, True)
    fs.store.fail_connect = True
    with caplog.at_level(logging.ERROR):
        assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert len(fs.db.flintel_signals.docs) == 1
    assert any("[MYSQL]" in r.getMessage() for r in caplog.records)
    # MySQL comes back -> next save reconnects by itself
    fs.store.fail_connect = False
    assert fs._save_signal("nike", "nike", "reddit", make_post(pid="def")) is True
    assert "reddit_def" in fs.store.rows


def test_8_mongo_fail_mysql_still_saved(fs, monkeypatch, caplog):
    flags(monkeypatch, True, True)
    fs.db.flintel_signals.fail = True
    with caplog.at_level(logging.ERROR):
        assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert "reddit_abc" in fs.store.rows
    assert any("[MONGO]" in r.getMessage() for r in caplog.records)


def test_9_blob_roundtrip_and_bad_length(fs):
    vec = [((i * 37) % 1000) / 997.0 - 0.5 for i in range(1536)]
    blob = fs._embedding_to_blob(vec)
    assert len(blob) == 6144
    expected = list(struct.unpack("<1536f", struct.pack("<1536f", *vec)))
    assert fs._blob_to_embedding(blob) == expected
    with pytest.raises(ValueError):
        fs._embedding_to_blob(vec[:1000])
    with pytest.raises(ValueError):
        fs._blob_to_embedding(blob[:-4])


def test_9b_wrong_dim_saves_row_with_null_embedding(fs, monkeypatch):
    flags(monkeypatch, False, True)
    monkeypatch.setattr(fs, "generate_embedding", lambda t: [0.5] * 100)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    row = fs.store.rows["reddit_abc"]
    assert row["embedding"] is None and row["embedding_dim"] is None


def test_10_embedding_none_gives_null_columns(fs, monkeypatch):
    flags(monkeypatch, False, True)
    monkeypatch.setattr(fs, "_is_embedding_enabled", lambda: False)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    row = fs.store.rows["reddit_abc"]
    assert row["embedding"] is None
    assert row["embedding_dim"] is None and row["embedding_model"] is None
    assert fs._embedding_to_blob(None) is None and fs._embedding_to_blob([]) is None


def test_11_emoji_and_non_ascii(fs, monkeypatch):
    flags(monkeypatch, True, True)
    post = make_post(title="Nike 🔥👟 café — 日本語 مرحبا", selftext="ça va? 😀")
    assert fs._save_signal("nike", "nike", "reddit", post) is True
    row = fs.store.rows["reddit_abc"]
    assert "🔥" in row["text"] and "日本語" in row["text"] and "😀" in row["text"]


def test_11b_connect_uses_utf8mb4_autocommit(fs, monkeypatch):
    captured = {}
    monkeypatch.setattr(fs.pymysql, "connect", lambda **kw: captured.update(kw) or object())
    fs._real_mysql_connect()
    assert captured["charset"] == "utf8mb4" and captured["autocommit"] is True
    assert captured["connect_timeout"] == fs.MYSQL_CONNECT_TIMEOUT
    assert captured["read_timeout"] == fs.MYSQL_READ_TIMEOUT
    assert captured["write_timeout"] == fs.MYSQL_WRITE_TIMEOUT


def test_12_comment_attach_mysql(fs, monkeypatch):
    flags(monkeypatch, False, True)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    ts = 1_760_000_100.0
    assert fs._attach_comment_to_post("nike", "nike", "abc", "first nike comment 😀", ts) is True
    stored = json.loads(fs.store.rows["reddit_abc"]["reddit_comments"])
    assert len(stored) == 1 and stored[0]["text"] == "first nike comment 😀"
    assert fs._attach_comment_to_post("nike", "nike", "abc", "second comment", ts) is True
    assert len(json.loads(fs.store.rows["reddit_abc"]["reddit_comments"])) == 2
    # same text again -> not duplicated
    assert fs._attach_comment_to_post("nike", "nike", "abc", "second comment", ts) is False
    assert len(json.loads(fs.store.rows["reddit_abc"]["reddit_comments"])) == 2
    # parent not stored -> skipped
    assert fs._attach_comment_to_post("nike", "nike", "zzz", "orphan", ts) is False


def test_12b_comment_attach_mongo_still_works(fs):
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    assert fs._attach_comment_to_post("nike", "nike", "abc", "c1", 1_760_000_100.0) is True
    assert fs._attach_comment_to_post("nike", "nike", "abc", "c1", 1_760_000_100.0) is False
    assert len(fs.db.flintel_signals.docs[0]["reddit_comments"]) == 1


def test_13_live_toggle_without_restart(fs, monkeypatch):
    flags(monkeypatch, True, False)
    assert fs._save_signal("nike", "nike", "reddit", make_post(pid="a1")) is True
    assert fs.store.rows == {}
    flags(monkeypatch, True, True)  # flipped live
    assert fs._save_signal("nike", "nike", "reddit", make_post(pid="a2")) is True
    assert "reddit_a2" in fs.store.rows
    flags(monkeypatch, False, True)
    n_mongo = fs.db.flintel_signals.insert_calls
    assert fs._save_signal("nike", "nike", "reddit", make_post(pid="a3")) is True
    assert fs.db.flintel_signals.insert_calls == n_mongo  # mongo off now


def test_14_connection_per_thread(fs):
    conns = {}

    def grab(name):
        conns[name] = fs._get_mysql_conn()

    t1 = threading.Thread(target=grab, args=("t1",))
    t2 = threading.Thread(target=grab, args=("t2",))
    t1.start(); t1.join(); t2.start(); t2.join()
    assert conns["t1"] is not conns["t2"]
    assert fs._get_mysql_conn() is fs._get_mysql_conn()  # same thread reuses


def test_15_datetime_is_utc_naive(fs, monkeypatch):
    flags(monkeypatch, False, True)
    fs._save_signal("nike", "nike", "reddit", make_post())
    row = fs.store.rows["reddit_abc"]
    assert row["created_utc"].tzinfo is None and row["fetched_at"].tzinfo is None


def test_16_race_duplicate_on_insert_is_caught(fs, monkeypatch):
    """Two threads both pass the existence check; the loser hits MySQL 1062 /
    Mongo DuplicateKeyError on insert and must return False, not crash."""
    flags(monkeypatch, True, True)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is True
    monkeypatch.setattr(fs, "_mysql_post_exists", lambda mid: False)
    monkeypatch.setattr(fs.db.flintel_signals, "find_one", lambda *a, **k: None)
    assert fs._save_signal("nike", "nike", "reddit", make_post()) is False
    assert fs.db.flintel_signals.insert_calls == 2 and fs.store.insert_calls == 2
