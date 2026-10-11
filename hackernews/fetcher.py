"""
hackernews/fetcher.py
=====================
Hacker News ka asli kaam yahin hota hai:

  1. Algolia HN Search API (search_by_date) se keyword ke hisaab se stories /
     comments dhoondna.
  2. Har hit ko existing post-dict shape mein badalna (taake `_save_signal()`
     bina kisi change ke use samajh sake).
  3. Har post ko `save_fn` (yaani `_save_signal`) ko dena. Save, dedupe aur
     embedding sab `_save_signal` ke andar pehle jaise hota hai.

Rules:
  - `flintel_service` se koi import nahi. `_save_signal` function argument
    (`save_fn`) ke tor par pass hota hai.
  - Koi function raise nahi karta: har failure par log.warning / log.error
    aur aage chalo, taake poller/worker thread crash na ho.
  - Keywords kahin hardcode nahi; wo job dict se aate hain (jo MongoDB #2 ki
    `flintel_search_jobs` se aata hai).
"""

import html
import logging
import re
import time
from datetime import datetime, timezone

import requests

from . import config, database

log = logging.getLogger("flintel-fetch")


# Har HN item ka permanent link (post_url) isi se banta hai.
_HN_ITEM_URL = "https://news.ycombinator.com/item?id={}"


def _strip_html(s: str) -> str:
    """
    HN ke HTML text ko plain text mein badalta hai.

    HN comments mein <p>, <a>, <i>, <pre> jaise tags aur &#x27; jaisi entities
    hoti hain. Pehle entities unescape hoti hain, phir tags space se replace hote
    hain, phir extra whitespace collapse karke strip kiya jata hai.
    """
    text = re.sub(r"<[^>]+>", " ", html.unescape(s or ""))
    return re.sub(r"\s+", " ", text).strip()


def _hit_to_post(hit: dict, kind: str):
    """
    Algolia ke ek hit ko existing post-dict shape mein convert karta hai.

    Story ke liye `title` hi message ka main hissa hai (selftext optional);
    comment mein title nahi hota, isliye comment ka text hi "title" mein jata hai.

    Returns:
        post dict, ya None agar hit ka objectID ya final text khali ho (skip).
    """
    object_id = str(hit.get("objectID") or "").strip()
    if not object_id:
        return None

    if kind == "comment":
        title = _strip_html(hit.get("comment_text"))
        selftext = ""
        if not title:
            return None
    else:
        title = hit.get("title") or ""
        selftext = _strip_html(hit.get("story_text"))
        if not title and not selftext:
            return None

    created_utc = None
    if hit.get("created_at_i") is not None:
        try:
            created_utc = float(hit["created_at_i"])
        except (ValueError, TypeError):
            created_utc = None

    return {
        "id": object_id,
        "title": title,
        "selftext": selftext,
        "author": hit.get("author") or "unknown",
        "subreddit": "",
        "post_url": _HN_ITEM_URL.format(object_id),
        "created_utc": created_utc,
        "score": hit.get("points") or 0,
        "num_comments": hit.get("num_comments") or 0,
        "hn_type": kind,
        # Neeche ki do keys extra hain; save par ignore ho jayengi.
        "external_url": hit.get("url") or "",
        "hn_story_id": hit.get("story_id"),
    }


def fetch_hackernews_search(keyword: str, kind: str, since_ts) -> list:
    """
    Algolia HN API se ek keyword ke liye stories ya comments fetch karta hai.

    Args:
        keyword:  search term (job ke keywords se aata hai).
        kind:     "story" ya "comment" (Algolia ke `tags` param mein jata hai).
        since_ts: unix seconds (int) ya None. Diya ho to sirf is se naye items
                  (`created_at_i > since_ts`) maange jate hain.

    Returns:
        list of post dicts (existing shape). Koi bhi failure par [] return hota
        hai aur log.warning likhi jati hai; ye function kabhi raise nahi karta.
    """
    try:
        query = keyword
        if config.HN_EXACT_PHRASE and " " in keyword.strip():
            query = f'"{keyword.strip()}"'

        params = {
            "query": query,
            "tags": kind,
            "hitsPerPage": config.HN_RESULTS_PER_QUERY,
        }
        if since_ts is not None:
            params["numericFilters"] = f"created_at_i>{int(since_ts)}"

        resp = requests.get(
            config.HN_API_URL,
            params=params,
            headers={"User-Agent": config.HN_USER_AGENT},
            timeout=config.HN_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        hits = (resp.json() or {}).get("hits") or []

        posts = []
        for hit in hits:
            try:
                post = _hit_to_post(hit, kind)
            except Exception as hit_exc:
                log.warning(
                    f"[HACKERNEWS] bad hit skipped | keyword='{keyword}' | kind={kind} | {hit_exc}"
                )
                continue
            if post is not None:
                posts.append(post)
        return posts

    except Exception as exc:
        log.warning(f"[HACKERNEWS] search failed | keyword='{keyword}' | kind={kind} | {exc}")
        return []


def run_hackernews_for_job(job: dict, save_fn, cutoff: datetime) -> int:
    """
    Ek job ke saare keywords ke liye Hacker News chalata hai aur matched
    messages `save_fn` ke zariye save karwata hai.

    Args:
        job:     {"topic_key", "keywords", "targeting_platform", ...}.
        save_fn: wahi `_save_signal(topic_key, matched_keyword, platform, post)`
                 (argument se aata hai; hum sirf call karte hain, badalte nahi).
        cutoff:  LOOKBACK_DAYS wala cutoff datetime. Is se purani posts skip hoti
                 hain, aur jahan last-seen state nahi hota wahan fetch isi se shuru hota hai.

    Flow:
        - HN disabled ho ya job "reddit" / "x_twitter" ka ho -> koi API call
          kiye baghair 0 return.
        - Har keyword x har enabled kind (story / comment): since_ts nikalo,
          fetch karo, cutoff se purani skip karo, `save_fn` se save karo,
          last-seen state update karo, phir keywords ke beech gap lo.

    Returns:
        int: kitne naye messages save hue. Kabhi raise nahi karta.
    """
    topic_key = None
    total_saved = 0
    try:
        job = job or {}
        topic_key = job.get("topic_key")
        targeting_platform = job.get("targeting_platform")

        if not config.is_hackernews_enabled():
            log.info(f"[HACKERNEWS] SKIP | topic_key={topic_key} | reason=HACKERNEWS_ENABLED is off")
            return 0
        if not config.job_targets_hackernews(targeting_platform):
            log.info(
                f"[HACKERNEWS] SKIP | topic_key={topic_key} | "
                f"reason=job targeting_platform='{targeting_platform}' does not include hackernews"
            )
            return 0

        keywords = job.get("keywords") or []
        if isinstance(keywords, str):
            keywords = keywords.split(",")
        keywords = [k.strip() for k in keywords if isinstance(k, str) and k.strip()]

        kinds = []
        if config.is_hn_stories_enabled():
            kinds.append("story")
        if config.is_hn_comments_enabled():
            kinds.append("comment")

        log.info(f"[HACKERNEWS] START | topic_key={topic_key} | keywords={len(keywords)}")

        # Naive datetime ko UTC maana jata hai, taake timestamp() local timezone se na bigde.
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        cutoff_ts = cutoff.timestamp()

        for keyword in keywords:
            try:
                for kind in kinds:
                    since_ts = database.get_last_seen_ts(topic_key, keyword, kind)
                    if since_ts is None:
                        since_ts = int(cutoff_ts)

                    posts = fetch_hackernews_search(keyword, kind, since_ts)
                    platform = (
                        config.HN_PLATFORM_COMMENT if kind == "comment" else config.HN_PLATFORM_STORY
                    )

                    max_ts = None
                    save_failed = False
                    for post in posts:
                        created = post.get("created_utc")
                        if created is not None:
                            if created < cutoff_ts:
                                continue
                            if max_ts is None or created > max_ts:
                                max_ts = created
                        try:
                            if save_fn(topic_key, keyword, platform, post):
                                total_saved += 1
                        except Exception as save_exc:
                            # Is batch ka state aage nahi badhayenge, taake agli baar dobara
                            # koshish ho (dedupe pehle se saved posts ko sambhal leta hai).
                            save_failed = True
                            log.warning(
                                f"[HACKERNEWS] save failed | topic_key={topic_key} | "
                                f"keyword='{keyword}' | kind={kind} | id={post.get('id')} | {save_exc}"
                            )

                    if max_ts is not None and not save_failed:
                        database.set_last_seen_ts(topic_key, keyword, kind, int(max_ts))
            except Exception as kw_exc:
                log.warning(
                    f"[HACKERNEWS] keyword failed | topic_key={topic_key} | keyword='{keyword}' | {kw_exc}"
                )

            # Har keyword ke baad gap (Algolia par burst traffic se bachne ke liye).
            try:
                if config.HN_KEYWORD_GAP_SECONDS > 0:
                    time.sleep(config.HN_KEYWORD_GAP_SECONDS)
            except Exception:
                pass

        log.info(f"[HACKERNEWS] DONE | topic_key={topic_key} | new_messages_saved={total_saved}")
        return total_saved

    except Exception as exc:
        log.error(f"[HACKERNEWS] job failed | topic_key={topic_key} | {exc}")
        return total_saved
