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


def _keyword_in_post(keyword: str, post: dict) -> bool:
    """
    Local keyword match: kya keyword post ke title/selftext mein maujood hai?

    Case-insensitive substring match, bilkul Reddit ke `_match_any_keyword`
    jaisa. Comment ke liye `title` mein hi comment ka text hota hai, isliye
    comments par bhi wahi check lagta hai. Algolia kabhi URL / author / typo
    tolerance ki wajah se bhi item deta hai; ye check un items ko pakadta hai.
    """
    text = f"{post.get('title','')} {post.get('selftext','')}".lower()
    return keyword.strip().lower() in text


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
                    strict_skipped = 0
                    for post in posts:
                        created = post.get("created_utc")
                        if created is not None:
                            if created < cutoff_ts:
                                continue
                            if max_ts is None or created > max_ts:
                                max_ts = created
                        # Strict local match: max_ts upar hi update ho chuka hai, yaani skip hui
                        # posts bhi checkpoint mein ginti hain (agli baar dobara fetch nahi hongi).
                        if config.is_hn_strict_match_enabled() and not _keyword_in_post(keyword, post):
                            strict_skipped += 1
                            continue
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

                    if strict_skipped > 0:
                        log.info(
                            f"[HACKERNEWS] strict match skipped {strict_skipped} | "
                            f"keyword='{keyword}' | kind={kind}"
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


# ---------------------------------------------------------------------------
# Story ke keyword-matched comments (additive; upar ka koi code change nahi)
# ---------------------------------------------------------------------------

# Algolia ek query par max 1000 hits deta hai; page loop phir bhi nbPages tak chalta hai.
_HN_COMMENTS_PER_PAGE = 1000

# HN ke deleted/dead comments kabhi kabhi ye placeholder text ke saath aate hain.
_HN_DEAD_TEXTS = {"[dead]", "[deleted]", "[flagged]"}


def fetch_story_comments(story_id, keyword: str, since_ts=None) -> list:
    """
    Ek HN story ke woh comments (saare depths, flat list) laata hai jin mein
    `keyword` aata hai.

    Args:
        story_id: HN story ka objectID (str ya int).
        keyword:  search term. HN_EXACT_PHRASE on ho aur keyword multi-word ho
                  to quote hota hai (fetch_hackernews_search jaisi hi logic).
        since_ts: unix seconds ya None. Diya ho to `created_at_i>=since_ts`
                  (>= jaan boojh kar: overlap safe hai kyunke dedupe comment_id
                  par hota hai).

    Returns:
        list of dicts, oldest first:
        {"comment_id", "author", "text", "created_utc" (UTC datetime), "parent_id"}

        Khali / deleted / dead text skip hota hai. HN_STRICT_MATCH on ho to
        keyword comment text mein case-insensitive substring bhi hona chahiye.

    Failure par (koi bhi page fail ho) [] return hota hai aur log.warning
    likhi jati hai. Partial result jaan boojh kar return nahi hota: Algolia
    newest-first deta hai, aur partial list se caller ka since_ts aage badh
    kar purane comments hamesha ke liye miss ho sakte the. Kabhi raise nahi karta.
    """
    try:
        keyword = (keyword or "").strip()
        story_id = str(story_id or "").strip()
        if not keyword or not story_id:
            return []

        query = keyword
        if config.HN_EXACT_PHRASE and " " in keyword:
            query = f'"{keyword}"'

        params = {
            "query": query,
            "tags": f"comment,story_{story_id}",
            "hitsPerPage": _HN_COMMENTS_PER_PAGE,
        }
        if since_ts is not None:
            params["numericFilters"] = f"created_at_i>={int(since_ts)}"

        strict = config.is_hn_strict_match_enabled()

        comments = {}  # comment_id -> dict (pages ke overlap par dobara nahi judta)
        page = 0
        nb_pages = 1
        while page < nb_pages:
            params["page"] = page
            resp = requests.get(
                config.HN_API_URL,
                params=params,
                headers={"User-Agent": config.HN_USER_AGENT},
                timeout=config.HN_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json() or {}
            try:
                nb_pages = int(data.get("nbPages") or 0)
            except (ValueError, TypeError):
                nb_pages = 0

            for hit in data.get("hits") or []:
                try:
                    comment = _hit_to_story_comment(hit, keyword, strict)
                except Exception as hit_exc:
                    log.warning(
                        f"[HACKERNEWS] bad comment hit skipped | story_id={story_id} | {hit_exc}"
                    )
                    continue
                if comment is not None:
                    comments.setdefault(comment["comment_id"], comment)
            page += 1

        return sorted(comments.values(), key=lambda c: (c["created_utc"], c["comment_id"]))

    except Exception as exc:
        log.warning(
            f"[HACKERNEWS] story comments fetch failed | story_id={story_id} | "
            f"keyword='{keyword}' | {exc}"
        )
        return []


def _hit_to_story_comment(hit: dict, keyword: str, strict: bool):
    """Algolia comment hit -> story-comment dict, ya None (skip)."""
    comment_id = str(hit.get("objectID") or "").strip()
    if not comment_id:
        return None

    text = _strip_html(hit.get("comment_text"))
    if not text or text.lower() in _HN_DEAD_TEXTS:
        return None

    # Strict local match: wahi existing _keyword_in_post (comment text "title" mein jata hai).
    if strict and not _keyword_in_post(keyword, {"title": text, "selftext": ""}):
        return None

    created_at_i = hit.get("created_at_i")
    if created_at_i is None:
        return None
    created_utc = datetime.fromtimestamp(float(created_at_i), tz=timezone.utc)

    parent_id = hit.get("parent_id")
    return {
        "comment_id": comment_id,
        "author": hit.get("author") or "unknown",
        "text": text,
        "created_utc": created_utc,
        "parent_id": str(parent_id) if parent_id is not None else None,
    }
