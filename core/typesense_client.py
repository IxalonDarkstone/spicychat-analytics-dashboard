import json
import logging
import time
from typing import Dict, List, Any

import requests

from .config import (
    TYPESENSE_KEY,
    TYPESENSE_SEARCH_ENDPOINT,
    FILTERED_CACHE,
    UNFILTERED_CACHE,
)
from .logging_utils import safe_log
from .helpers import rating_to_pct


# ------------------ Typesense client + trending ------------------

def multi_search_request(payload: dict) -> dict:
    """
    Wrapper for Typesense's multi_search endpoint using your public API key.
    Guaranteed to always return a dict (or {}), never None.
    """
    headers = {
        "X-TYPESENSE-API-KEY": TYPESENSE_KEY,
        "Content-Type": "application/json",
    }

    for attempt in range(3):
        try:
            response = requests.post(
                TYPESENSE_SEARCH_ENDPOINT,
                headers=headers,
                data=json.dumps(payload),
                timeout=25,
            )
            response.raise_for_status()

            try:
                data = response.json()
                return data if isinstance(data, dict) else {}
            except Exception as e:
                logging.error(
                    f"[Typesense] Invalid JSON response: {e}. "
                    f"Text: {response.text[:500]}"
                )
                return {}

        except requests.exceptions.HTTPError as e:
            if getattr(e, "response", None) and e.response.status_code == 429:
                time.sleep(2**attempt)
            else:
                time.sleep(2)
        except requests.exceptions.RequestException:
            time.sleep(2)
        except Exception:
            time.sleep(2)

    logging.error("[Typesense] All attempts failed → using empty fallback {}.")
    return {}


def fetch_typesense_tags_for_bot_ids(bot_ids: List[str]) -> Dict[str, List[str]]:
    """
    Fetch tags for specific bot IDs from Typesense, regardless of rank/top480.
    Returns: { "bot_id": ["tag1", "tag2", ...] }
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return {}

    tag_map: Dict[str, List[str]] = {}
    CHUNK = 80

    for i in range(0, len(bot_ids), CHUNK):
        chunk = bot_ids[i:i + CHUNK]
        ids_json = json.dumps(chunk)

        payload = {
            "searches": [{
                "collection": "public_characters_alias",
                "q": "*",
                "query_by": "name,title,tags,character_id",
                "filter_by": f"character_id:={ids_json}",
                "include_fields": "character_id,tags",
                "per_page": len(chunk),
                "page": 1,
                "highlight_fields": "none",
                "enable_highlight_v1": False,
            }]
        }

        result = multi_search_request(payload)
        results = (result or {}).get("results", [])
        hits = results[0].get("hits", []) if results else []

        for h in hits:
            doc = (h or {}).get("document") or {}
            cid = str(doc.get("character_id") or "")
            if cid:
                tag_map[cid] = doc.get("tags") or []

    safe_log(f"Tags: fetched tags for {len(tag_map)} / {len(bot_ids)} bot_ids from Typesense")
    return tag_map


def fetch_typesense_ratings_for_bot_ids(bot_ids: List[str]) -> Dict[str, float | None]:
    """
    Fetch rating_score for specific bot IDs from Typesense.
    Returns: { "bot_id": float|None }
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return {}

    rating_map: Dict[str, float | None] = {}
    CHUNK = 80

    for i in range(0, len(bot_ids), CHUNK):
        chunk = bot_ids[i:i + CHUNK]
        ids_json = json.dumps(chunk)

        payload = {
            "searches": [{
                "collection": "public_characters_alias",
                "q": "*",
                "query_by": "name,title,tags,character_id",
                "filter_by": f"character_id:={ids_json}",
                "include_fields": "character_id,rating_score",
                "per_page": len(chunk),
                "page": 1,
                "highlight_fields": "none",
                "enable_highlight_v1": False,
            }]
        }

        result = multi_search_request(payload)
        results = (result or {}).get("results", [])
        hits = results[0].get("hits", []) if results else []

        for h in hits:
            doc = (h or {}).get("document") or {}
            cid = str(doc.get("character_id") or "")
            rs = doc.get("rating_score", None)
            if cid:
                try:
                    rating_map[cid] = float(rs) if rs is not None else None
                except Exception:
                    rating_map[cid] = None

    safe_log(f"Ratings: fetched ratings for {len(rating_map)} / {len(bot_ids)} bot_ids from Typesense")
    return rating_map


def fetch_typesense_top_bots(max_pages: int = 10, use_cache: bool = True, filter_female_nsfw: bool = True) -> Dict[str, dict]:
    """
    Fetch Top Bots from Typesense.
    Supports:
      - filtered mode (Female + NSFW only)
      - unfiltered mode (all STANDARD spicychat characters)
    """
    cache_file = FILTERED_CACHE if filter_female_nsfw else UNFILTERED_CACHE

    # ----- CACHE READ -----
    if use_cache and cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            if isinstance(cached, list) and all("character_id" in b for b in cached):
                safe_log(f"Loaded {len(cached)} bots from cache: {cache_file}")
                return {str(b["character_id"]): b for b in cached}
        except Exception as e:
            safe_log(f"Failed reading cached Typesense results: {e}")

    safe_log("Fetching fresh Top Bots from Typesense...")
    ALL_RESULTS: List[dict] = []
    page = 1
    per_page = 48

    if filter_female_nsfw:
        filter_clause = (
            "application_ids:spicychat && tags:![Step-Family] && "
            "creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c',"
            "'kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] "
            "&& type:STANDARD && tags:[\"Female\"] && tags:[\"NSFW\"]"
        )
    else:
        filter_clause = (
            "application_ids:spicychat && tags:![Step-Family] && "
            "creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c',"
            "'kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] "
            "&& type:STANDARD"
        )

    while page <= max_pages:
        payload = {
            "searches": [{
                "query_by": "name,title,tags,creator_username,character_id,type",
                "include_fields": (
                    "name,title,tags,creator_username,character_id,"
                    "avatar_is_nsfw,avatar_url,visibility,definition_visible,"
                    "num_messages,token_count,rating_score,lora_status,"
                    "creator_user_id,is_nsfw,type,sub_characters_count,"
                    "group_size_category,num_messages_24h"
                ),
                "use_cache": True,
                "highlight_fields": "none",
                "enable_highlight_v1": False,
                "sort_by": "num_messages_24h:desc",
                "collection": "public_characters_alias",
                "q": "*",
                "facet_by": "definition_size_category,group_size_category,tags,translated_languages",
                "filter_by": filter_clause,
                "max_facet_values": 100,
                "page": page,
                "per_page": per_page,
            }]
        }

        result = multi_search_request(payload)
        results_page = (result or {}).get("results", [])
        hits = results_page[0].get("hits", []) if results_page else []

        if not hits:
            break

        safe_log(f"Page {page}: Fetched {len(hits)} hits")

        for obj in hits:
            doc: dict = obj.get("document") or {}
            cid = str(doc.get("character_id") or "").strip()
            if not cid:
                continue

            rank = len(ALL_RESULTS) + 1

            bot = {
                "character_id": cid,
                "name": (doc.get("name") or "").strip(),
                "title": doc.get("title") or "",
                "num_messages": doc.get("num_messages", 0) or 0,
                "num_messages_24h": doc.get("num_messages_24h", 0) or 0,
                "avatar_url": doc.get("avatar_url") or "",
                "creator_username": doc.get("creator_username") or "",
                "creator_user_id": doc.get("creator_user_id") or "",
                "tags": doc.get("tags", []) or [],
                "is_nsfw": bool(doc.get("is_nsfw", False)),
                "link": f"https://spicychat.ai/chat/{cid}",
                "page": page,
                "rank": rank,
                "rating_score": doc.get("rating_score", None),
                "rating_pct": rating_to_pct(doc.get("rating_score", None)),
            }
            ALL_RESULTS.append(bot)

        if len(hits) < per_page:
            break

        page += 1

    # ----- CACHE WRITE -----
    if ALL_RESULTS:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(ALL_RESULTS, indent=2), encoding="utf-8")
            safe_log(f"Saved {len(ALL_RESULTS)} bots to Typesense cache: {cache_file}")
        except Exception as e:
            safe_log(f"Failed writing Typesense cache: {e}")

    return {str(b["character_id"]): b for b in ALL_RESULTS}


def get_typesense_tag_map() -> Dict[str, List[str]]:
    """
    Build a tag map from the UNFILTERED cached (or live) Typesense top bots.
    Returns: { bot_id: [tags...] }
    """
    ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=True, filter_female_nsfw=False)
    if not ts_map:
        safe_log("Tags: unfiltered TS cache empty — fetching live once to build tag_map")
        ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=False, filter_female_nsfw=False)

    tag_map: Dict[str, List[str]] = {}
    for cid, bot in (ts_map or {}).items():
        tags = bot.get("tags") or []
        if tags:
            tag_map[str(cid)] = tags

    safe_log(f"Tags: built tag_map for {len(tag_map)} bots")
    return tag_map
import json

def fetch_typesense_created_at_for_bot_ids(
    bot_ids,
    collections=("public_characters", "public_characters_alias"),
):
    """
    Fetch created_at for specific bot IDs by direct ID lookup.
    This is NOT tied to top-480 / trending caches.

    Returns:
        { bot_id: raw_created_at }
    """
    bot_ids = [str(x) for x in (bot_ids or []) if x]
    if not bot_ids:
        safe_log("[TS created_at] no bot_ids provided")
        return {}

    out = {}
    CHUNK = 80

    safe_log(f"[TS created_at] lookup start for {len(bot_ids)} bot_ids")

    for coll in collections:
        remaining = [bid for bid in bot_ids if bid not in out]
        if not remaining:
            break

        safe_log(
            f"[TS created_at] trying collection='{coll}' "
            f"for {len(remaining)} remaining ids"
        )

        for i in range(0, len(remaining), CHUNK):
            chunk = remaining[i:i + CHUNK]
            ids_json = json.dumps(chunk)

            payload = {
                "searches": [{
                    "collection": coll,
                    "q": "*",
                    "query_by": "character_id",
                    "filter_by": f"character_id:={ids_json}",
                    "include_fields": "character_id,created_at",
                    "per_page": len(chunk),
                    "page": 1,
                    "highlight_fields": "none",
                    "enable_highlight_v1": False,
                }]
            }

            result = multi_search_request(payload)
            results = (result or {}).get("results", [])
            hits = results[0].get("hits", []) if results else []

            safe_log(
                f"[TS created_at] coll='{coll}' "
                f"chunk={i//CHUNK + 1} "
                f"ids={len(chunk)} hits={len(hits)}"
            )

            # Log schema once per collection
            if hits and not out:
                doc0 = (hits[0] or {}).get("document") or {}
                safe_log(
                    f"[TS created_at] coll='{coll}' document keys={list(doc0.keys())}"
                )

            for h in hits:
                doc = (h or {}).get("document") or {}
                cid = str(doc.get("character_id") or "").strip()
                if not cid:
                    continue

                ca = doc.get("created_at")
                if ca:
                    out[cid] = ca

        safe_log(
            f"[TS created_at] coll='{coll}' "
            f"accumulated={len(out)}"
        )

    safe_log(
        f"[TS created_at] lookup complete: "
        f"found {len(out)} / {len(bot_ids)}"
    )
    return out
