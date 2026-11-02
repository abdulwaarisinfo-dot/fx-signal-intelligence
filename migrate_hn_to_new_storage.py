"""
migrate_hn_to_new_storage.py  (OPTIONAL, one-time)
==================================================
Purane Hacker News data ko `flintel_signals` se naye ghar mein copy karta hai:

    Mongo : flintel_signals  ->  hacker_news_signals   (same DB)
    MySQL : flintel_signals  ->  hacker_news           (same database)

Sirf platform "hackernews" / "hackernews_comment" wali rows copy hoti hain.
Reddit / Twitter ko koi touch nahi hota.

Default = sirf COPY (purana data wahin rehta hai, dobara chalana safe hai:
duplicates skip hote hain). Purana data hatana ho to verify karne ke baad
`--delete-old` ke saath chalayein.

Usage:
    python migrate_hn_to_new_storage.py                # copy (Mongo + MySQL, jo env mein configured ho)
    python migrate_hn_to_new_storage.py --mongo-only
    python migrate_hn_to_new_storage.py --mysql-only
    python migrate_hn_to_new_storage.py --delete-old   # copy + purani HN rows hata do

Env: MONGODB_URI, MONGODB_DB, MYSQL_HOST/PORT/USER/PASSWORD/DATABASE (+ MYSQL_SSL, MYSQL_SSL_CA),
     HN_MONGO_COLLECTION, HN_MYSQL_TABLE (defaults: hacker_news_signals / hacker_news).
"""

import argparse
import os
import re
import sys

from dotenv import load_dotenv

load_dotenv()

HN_PLATFORMS = ["hackernews", "hackernews_comment"]
HN_MONGO_COLLECTION = os.getenv("HN_MONGO_COLLECTION", "hacker_news_signals").strip() or "hacker_news_signals"
HN_MYSQL_TABLE = os.getenv("HN_MYSQL_TABLE", "hacker_news").strip() or "hacker_news"
if not re.fullmatch(r"[A-Za-z0-9_]+", HN_MYSQL_TABLE):
    HN_MYSQL_TABLE = "hacker_news"

CREATE_HN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {HN_MYSQL_TABLE} (
    id               BIGINT        NOT NULL AUTO_INCREMENT,
    message_id       VARCHAR(191)  NOT NULL,
    topic_key        VARCHAR(255)  NOT NULL,
    search_keyword   VARCHAR(255)  NULL,
    platform         VARCHAR(32)   NOT NULL,
    username         VARCHAR(255)  NULL,
    title            MEDIUMTEXT    NULL,
    text             MEDIUMTEXT    NULL,
    post_url         VARCHAR(1024) NULL,
    score            INT           DEFAULT 0,
    num_comments     INT           DEFAULT 0,
    created_utc      DATETIME(3)   NOT NULL,
    fetched_at       DATETIME(3)   NOT NULL,
    embedding        BLOB          NULL,
    embedding_dim    SMALLINT      NULL,
    embedding_model  VARCHAR(64)   NULL,
    hn_comments      JSON          NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_hn_message_id (message_id),
    KEY idx_hn_topic_key (topic_key),
    KEY idx_hn_search_keyword (search_keyword),
    KEY idx_hn_created_utc (created_utc),
    KEY idx_hn_fetched_at (fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

COPY_COLS = [
    "message_id", "topic_key", "search_keyword", "platform", "username", "title", "text",
    "post_url", "score", "num_comments", "created_utc", "fetched_at",
    "embedding", "embedding_dim", "embedding_model",
]


def migrate_mongo(delete_old: bool) -> None:
    from pymongo import ASCENDING, MongoClient

    uri = os.getenv("MONGODB_URI", "").strip()
    if not uri:
        print("[MONGO] MONGODB_URI not set -> skipped")
        return
    db = MongoClient(uri, serverSelectionTimeoutMS=5000)[os.getenv("MONGODB_DB", "flintel_bot")]
    src, dst = db["flintel_signals"], db[HN_MONGO_COLLECTION]
    dst.create_index([("message_id", ASCENDING)], unique=True, name="hn_signals_message_id_unique")
    dst.create_index([("topic_key", ASCENDING)], name="hn_signals_topic_key")
    dst.create_index([("search_keyword", ASCENDING)], name="hn_signals_search_keyword")
    dst.create_index([("created_utc", ASCENDING)], name="hn_signals_created_utc")

    flt = {"platform": {"$in": HN_PLATFORMS}}
    scanned = inserted = 0
    for doc in src.find(flt):
        scanned += 1
        doc.pop("_id", None)
        doc.pop("subreddit", None)        # Reddit-only fields
        doc.pop("reddit_comments", None)
        res = dst.update_one({"message_id": doc["message_id"]}, {"$setOnInsert": doc}, upsert=True)
        if res.upserted_id is not None:
            inserted += 1
    print(f"[MONGO] scanned={scanned} newly_copied={inserted} -> {HN_MONGO_COLLECTION}")

    if delete_old:
        ids = [d["message_id"] for d in src.find(flt, {"message_id": 1})]
        missing = [m for m in ids if dst.count_documents({"message_id": m}, limit=1) == 0]
        if missing:
            print(f"[MONGO] NOT deleting old rows: {len(missing)} not present in {HN_MONGO_COLLECTION}")
            return
        res = src.delete_many(flt)
        print(f"[MONGO] deleted {res.deleted_count} HN docs from flintel_signals")


def migrate_mysql(delete_old: bool) -> None:
    import pymysql

    host = os.getenv("MYSQL_HOST", "").strip()
    if not host:
        print("[MYSQL] MYSQL_HOST not set -> skipped")
        return
    kwargs = dict(
        host=host, port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", ""), password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", ""), charset="utf8mb4", autocommit=True,
    )
    if os.getenv("MYSQL_SSL", "").strip().lower() in ("1", "true", "yes", "on"):
        ca = os.getenv("MYSQL_SSL_CA", "").strip()
        kwargs["ssl"] = {"ca": ca} if ca else {}
    conn = pymysql.connect(**kwargs)
    ph = ", ".join(["%s"] * len(HN_PLATFORMS))
    cols = ", ".join(f"`{c}`" for c in COPY_COLS)
    with conn.cursor() as cur:
        cur.execute(CREATE_HN_TABLE_SQL)
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = 'flintel_signals' AND COLUMN_NAME = 'hn_comments'"
        )
        has_hn_col = bool(cur.fetchone()[0])
        hn_expr = "`hn_comments`" if has_hn_col else "NULL"
        cur.execute(
            f"INSERT IGNORE INTO {HN_MYSQL_TABLE} ({cols}, `hn_comments`) "
            f"SELECT {cols}, {hn_expr} FROM flintel_signals WHERE platform IN ({ph})",
            HN_PLATFORMS,
        )
        print(f"[MYSQL] copied {cur.rowcount} new rows -> {HN_MYSQL_TABLE}")
        if delete_old:
            cur.execute(
                f"SELECT COUNT(*) FROM flintel_signals s LEFT JOIN {HN_MYSQL_TABLE} h "
                f"ON h.message_id = s.message_id WHERE s.platform IN ({ph}) AND h.message_id IS NULL",
                HN_PLATFORMS,
            )
            missing = cur.fetchone()[0]
            if missing:
                print(f"[MYSQL] NOT deleting old rows: {missing} not present in {HN_MYSQL_TABLE}")
            else:
                cur.execute(f"DELETE FROM flintel_signals WHERE platform IN ({ph})", HN_PLATFORMS)
                print(f"[MYSQL] deleted {cur.rowcount} HN rows from flintel_signals")
    conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mongo-only", action="store_true")
    ap.add_argument("--mysql-only", action="store_true")
    ap.add_argument("--delete-old", action="store_true",
                    help="copy ke baad purani HN rows flintel_signals se hata do (sirf agar sab copy ho chuki hon)")
    a = ap.parse_args()
    if not a.mysql_only:
        migrate_mongo(a.delete_old)
    if not a.mongo_only:
        migrate_mysql(a.delete_old)
    return 0


if __name__ == "__main__":
    sys.exit(main())
