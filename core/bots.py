import logging
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from .config import DATABASE, ALLOWED_FIELDS, CDT, AVATAR_BASE_URL
from .logging_utils import safe_log
from .helpers import fmt_commas, fmt_delta_commas, rating_to_pct
from .db import init_db, load_cached_rating_map, load_cached_tag_map


# ------------------ Load + compute deltas ------------------
def load_history_df() -> pd.DataFrame:
    """
    Load all rows from bots table and normalize:
    - date as date
    - num_messages as int
    - created_at as timezone-aware CDT
    """
    init_db()

    try:
        with sqlite3.connect(DATABASE) as conn:
            df = pd.read_sql_query("SELECT * FROM bots", conn)
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return pd.DataFrame(columns=ALLOWED_FIELDS)

    if df.empty:
        safe_log("Database is empty. Returning empty DataFrame.")
        return pd.DataFrame(columns=ALLOWED_FIELDS)

    # Ensure all expected columns exist
    for col in ALLOWED_FIELDS:
        if col not in df.columns:
            df[col] = ""
            logging.warning(f"Missing column {col} in database, filled with empty values")

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    invalid_dates = df["date"].isna().sum()
    if invalid_dates > 0:
        logging.warning(f"Dropping {invalid_dates} rows with invalid/missing date")
        df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.date

    # num_messages
    df["num_messages"] = (
        pd.to_numeric(df["num_messages"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # created_at
    if "created_at" in df.columns and pd.notnull(df["created_at"]).any():
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce").dt.tz_convert(CDT)
    else:
        df["created_at"] = pd.NaT

    return df


def compute_deltas(df_raw: pd.DataFrame, timeframe="All") -> pd.DataFrame:
    """
    Compute daily deltas for each bot and apply timeframe filter.
    """
    if df_raw.empty:
        safe_log("No data to compute deltas.")
        return pd.DataFrame(
            columns=[
                "date", "bot_id", "bot_name", "bot_title",
                "num_messages", "daily_messages", "created_at", "avatar_url",
            ]
        )

    df = df_raw.sort_values(["bot_id", "date"]).copy()
    df["daily_messages"] = df.groupby("bot_id")["num_messages"].diff().fillna(0).astype(int)
    df.loc[df["daily_messages"] < 0, "daily_messages"] = 0

    today = datetime.now().date()
    if timeframe == "7day":
        df = df[df["date"] >= (today - timedelta(days=7))]
    elif timeframe == "30day":
        df = df[df["date"] >= (today - timedelta(days=30))]
    elif timeframe == "current_month":
        df = df[df["date"] >= today.replace(day=1)]
        # first-day-of-month adjustment (kept simple; your old logic can be re-added if needed)

    return df


# ------------------ Dashboard bots data ------------------
def normalize_avatar_url(url: str) -> str:
    """
    Convert relative avatar paths to absolute CDN URLs.
    """
    url = (url or "").strip()
    if not url:
        return ""

    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/avatars/"):
        return f"{AVATAR_BASE_URL}/{url[len('/avatars/'):]}"
    if url.startswith("avatars/"):
        return f"{AVATAR_BASE_URL}/{url[len('avatars/'):]}"
    if url.startswith("/"):
        return f"https://spicychat.ai{url}"
    return url


def get_bots_data(timeframe="All", sort_by="delta", sort_asc=False, created_after="All", tags="", q=""):
    """
    Load snapshot history from DB, compute deltas for the given timeframe,
    and build the list of bots for the dashboard.

    Ranks are loaded from bot_rank_history for the latest date in the timeframe.
    Tags/ratings are loaded from caches (not from Typesense here).
    """
    df_raw = load_history_df()
    dfc = compute_deltas(df_raw, timeframe)

    if dfc.empty:
        return [], [], 0, None

    # Optional: filter by created_after
    if created_after != "All":
        now = datetime.now(tz=CDT)
        cutoff = None
        if created_after == "7day":
            cutoff = now - timedelta(days=7)
        elif created_after == "30day":
            cutoff = now - timedelta(days=30)
        elif created_after == "current_month":
            cutoff = now.replace(day=1)
        if cutoff is not None:
            dfc = dfc[dfc["created_at"] >= cutoff]

    if dfc.empty:
        return [], [], 0, None

    latest_date = sorted(dfc["date"].unique())[-1]
    today_df = dfc[dfc["date"] == latest_date].copy()

    bot_ids = [str(x) for x in today_df["bot_id"].tolist()]
    tag_map = load_cached_tag_map(bot_ids)
    rating_map = load_cached_rating_map(bot_ids)

    # Totals history for the totals table
    totals_df = (
        dfc.groupby("date", as_index=False)
        .agg({"num_messages": "sum", "daily_messages": "sum"})
        .sort_values("date", ascending=False)
    )

    totals = []
    for _, row in totals_df.iterrows():
        totals.append({
            "date": str(row["date"]),
            "total": int(row["num_messages"]),
            "total_fmt": fmt_commas(row["num_messages"]),
            "daily": int(row["daily_messages"]),
            "daily_fmt": fmt_delta_commas(row["daily_messages"]),
        })

    # Load ranks for this date
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT bot_id, rank FROM bot_rank_history WHERE date = ?", (str(latest_date),))
    rank_rows = cur.fetchall()
    conn.close()

    rank_by_bot = {str(bot_id): (rank or 0) for (bot_id, rank) in rank_rows}

    bots = []
    for _, row in today_df.iterrows():
        bot_id = str(row["bot_id"])
        total = int(row["num_messages"])
        delta = int(row.get("daily_messages", 0))

        avatar_url = normalize_avatar_url(row.get("avatar_url") or "") or f"{AVATAR_BASE_URL}/default-avatar.png"

        created_at_str = ""
        if pd.notnull(row.get("created_at")):
            try:
                created_at_str = row["created_at"].strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                created_at_str = str(row["created_at"])

        r = rank_by_bot.get(bot_id, 0)
        if r and 1 <= r <= 480:
            rank_val = r
            rank_tier = "top240" if r <= 240 else "top480"
        else:
            rank_val = None
            rank_tier = None

        rating_val = rating_map.get(bot_id, None)

        bots.append({
            "bot_id": bot_id,
            "name": row["bot_name"],
            "title": row["bot_title"],
            "total": total,
            "total_fmt": fmt_commas(total),
            "delta": delta,
            "delta_fmt": fmt_delta_commas(delta),
            "created_at": created_at_str,
            "link": f"https://spicychat.ai/chat/{bot_id}",
            "avatar_url": avatar_url,
            "rank": rank_val,
            "rank_tier": rank_tier,
            "tags": tag_map.get(bot_id, []),
            "rating": rating_val,
            "rating_pct": rating_to_pct(rating_val),
        })

    # --- Tag filter (AND logic) ---
    required_tags = [t.strip().lower() for t in (tags or "").split(",") if t.strip()]
    if required_tags:
        def has_all_tags(bot):
            bot_tags = [str(t).lower() for t in (bot.get("tags") or [])]
            return all(t in bot_tags for t in required_tags)
        bots = [b for b in bots if has_all_tags(b)]

    # --- Search filter (Name/Title/Tags) ---
    q_norm = (q or "").strip().lower()
    if q_norm:
        def matches_search(bot):
            name = (bot.get("name") or "").lower()
            title = (bot.get("title") or "").lower()
            tags_blob = " ".join([str(t).lower() for t in (bot.get("tags") or [])])
            return (q_norm in name) or (q_norm in title) or (q_norm in tags_blob)
        bots = [b for b in bots if matches_search(b)]

    # Sorting
    reverse = not sort_asc
    if sort_by == "name":
        bots.sort(key=lambda b: (b["name"] or "").lower(), reverse=reverse)
    elif sort_by == "total":
        bots.sort(key=lambda b: b["total"], reverse=reverse)
    elif sort_by == "created_at":
        bots.sort(key=lambda b: b["created_at"] or "", reverse=reverse)
    else:
        bots.sort(key=lambda b: b["delta"], reverse=reverse)

    total_messages = int(totals_df["num_messages"].iloc[0]) if not totals_df.empty else 0
    return bots, totals, total_messages, latest_date
