import logging
import sqlite3
from datetime import datetime

import pandas as pd

from .config import DATABASE, CDT, ALLOWED_FIELDS
from .logging_utils import safe_log
from .fs_utils import ensure_dirs, set_last_snapshot_time
from .db import (
    init_db,
    save_cached_tag_map,
    save_cached_rating_map,
    save_rank_history_for_date,
    save_rating_history_for_date,
)
from .auth import ensure_fresh_kinde_token, test_auth_credentials
from .api_capture import capture_payloads
from .helpers import (
    flatten_items,
    get_num_messages,
    get_id,
    get_created_at,
    get_name,
    get_title,
    get_avatar_url,
)
from .typesense_client import (
    fetch_typesense_top_bots,
    fetch_typesense_tags_for_bot_ids,
    fetch_typesense_ratings_for_bot_ids,
)
from .authors_service import refresh_tracked_authors_snapshot

# If you want these globals for UI status, keep them here.
AUTH_REQUIRED = False
LAST_SNAPSHOT_DATE = None


def sanitize_rows(rows):
    return [{k: r.get(k, "") for k in ALLOWED_FIELDS} for r in rows]


def take_snapshot(args=None, verbose=True):
    global AUTH_REQUIRED, LAST_SNAPSHOT_DATE

    ensure_dirs()
    init_db()

    snapshot_time = datetime.now(tz=CDT)
    stamp = snapshot_time.strftime("%Y-%m-%d")
    safe_log(f"Starting snapshot for {stamp} in CDT")

    bearer_token, guest_userid = ensure_fresh_kinde_token()
    if not bearer_token or not guest_userid:
        safe_log("Snapshot aborted — auth required.")
        AUTH_REQUIRED = True
        return str(DATABASE)

    # Double-check (useful when recapture returns something stale)
    if not test_auth_credentials(bearer_token, guest_userid):
        safe_log("Credentials invalid after refresh/recapture — marking auth required.")
        AUTH_REQUIRED = True
        return str(DATABASE)

    AUTH_REQUIRED = False

    # Capture payload
    try:
        payloads = capture_payloads(bearer_token, guest_userid)
    except RuntimeError as e:
        logging.warning(f"No payloads captured: {e}. Marking auth required.")
        AUTH_REQUIRED = True
        return str(DATABASE)

    if not payloads:
        logging.warning("Snapshot: no payloads found.")
        return str(DATABASE)

    # Flatten and clean
    items = []
    for pl in payloads:
        flatten_items(pl, items)

    rows = []
    seen = set()
    for d in items:
        num = get_num_messages(d)
        bot_id = get_id(d)
        if num is None or not bot_id:
            logging.debug(f"Skipping item due to missing num_messages or bot_id: {d}")
            continue

        created_at = get_created_at(d)
        if created_at:
            try:
                created_at = pd.Timestamp(created_at, tz="UTC").tz_convert(CDT).isoformat()
            except Exception:
                pass

        row = {
            "date": stamp,
            "bot_id": bot_id,
            "bot_name": get_name(d),
            "bot_title": get_title(d),
            "num_messages": num,
            "creator_user_id": str(d.get("creator_user_id") or ""),
            "created_at": created_at,
            "avatar_url": get_avatar_url(d),
        }

        if bot_id in seen:
            continue

        seen.add(bot_id)
        rows.append(row)

    rows_clean = sanitize_rows(rows)

    # Write to DB
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM bots WHERE date = ?", (stamp,))
        for row in rows_clean:
            c.execute(
                """
                INSERT INTO bots (
                    date, bot_id, bot_name, bot_title, num_messages,
                    creator_user_id, created_at, avatar_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["date"],
                    row["bot_id"],
                    row["bot_name"],
                    row["bot_title"],
                    row["num_messages"],
                    row["creator_user_id"],
                    row["created_at"],
                    row["avatar_url"],
                ),
            )
        conn.commit()

    set_last_snapshot_time()
    safe_log("Last snapshot time updated.")

    if verbose:
        safe_log(f"Snapshot saved for {len(rows_clean)} bots to {DATABASE}")

    # Refresh Typesense trending cache (top 480)
    try:
        safe_log("Refreshing Typesense trending cache (top 480 bots)")
        ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=False, filter_female_nsfw=True)
        if not isinstance(ts_map, dict):
            ts_map = {}
        safe_log(f"Typesense trending cache updated successfully ({len(ts_map)} entries)")
    except Exception as e:
        logging.error(f"Failed to refresh Typesense trending cache: {e}")
        ts_map = {}

    # Save rank history
    try:
        save_rank_history_for_date(stamp, ts_map)
    except Exception as e:
        logging.error(f"Error saving rank history: {e}")

    # Cache tags for "My Chatbots"
    try:
        your_ids = [str(r["bot_id"]) for r in rows_clean]
        tag_map = fetch_typesense_tags_for_bot_ids(your_ids)
        save_cached_tag_map(tag_map)
        safe_log(f"Cached tags for {len(tag_map)} bots (My Chatbots)")
    except Exception as e:
        safe_log(f"Tag caching failed: {e}")

    # Cache ratings for "My Chatbots" + rating history
    try:
        your_ids = [str(r["bot_id"]) for r in rows_clean]
        rating_map = fetch_typesense_ratings_for_bot_ids(your_ids)
        save_cached_rating_map(rating_map)
        save_rating_history_for_date(stamp, rating_map)
        safe_log(f"Cached ratings for {len(rating_map)} bots (My Chatbots)")
    except Exception as e:
        safe_log(f"Rating caching failed: {e}")

    # Track Authors (snapshot cache)
    try:
        refresh_tracked_authors_snapshot(stamp)
    except Exception as e:
        safe_log(f"Author Tracker snapshot failed: {e}")

    LAST_SNAPSHOT_DATE = snapshot_time.isoformat()
    safe_log(f"Snapshot complete at {LAST_SNAPSHOT_DATE}")
    return str(DATABASE)
