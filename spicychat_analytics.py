import os
import sys
import re
import time
import argparse
import logging
import shutil
import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, jsonify
import sqlite3
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive Agg backend
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
from scipy.interpolate import make_interp_spline
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse
import hashlib
import pytz
import threading
import subprocess

# ------------------ Config ------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DATABASE = DATA_DIR / "spicychat.db"
AUTH_FILE = DATA_DIR / "auth_credentials.json"
API_URL = "https://prod.nd-api.com/v2/users/characters?switch=T1"
MY_BOTS_URL = "https://spicychat.ai/my-chatbots"
CHARTS_DIR = BASE_DIR / "charts"
STATIC_CHARTS_DIR = BASE_DIR / "static/charts"
CHART_TIMEOUT = 300  # Timeout in seconds for chart generation
AVATAR_BASE_URL = "https://cdn.nd-api.com/avatars"
# Global flag indicating whether authentication is required
AUTH_REQUIRED = False
SNAPSHOT_THREAD_STARTED = False
# Timestamp of the last snapshot taken
LAST_SNAPSHOT_DATE = None


# ------------------ Typesense / Trending config ------------------
TYPESENSE_HOST = "https://etmzpxgvnid370fyp.a1.typesense.net"
TYPESENSE_KEY = "STHKtT6jrC5z1IozTJHIeSN4qN9oL1s3"  # Public read key used by web UI
TYPESENSE_SEARCH_ENDPOINT = f"{TYPESENSE_HOST}/multi_search"
TRENDS_CACHE = DATA_DIR / "public_bots_home_all.json"  # Cached Typesense results

ALLOWED_FIELDS = [
    "date", "bot_id", "bot_name", "bot_title",
    "num_messages", "creator_user_id", "created_at", "avatar_url"
]

# Set CDT timezone
CDT = pytz.timezone('America/Chicago')

# ------------------ Flask App ------------------
app = Flask(__name__, template_folder="templates", static_folder="static")

# Prevent endpoint redefinition
_routes_defined = False

# ------------------ Logging ------------------
def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGS_DIR / "spicychat.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Ensure console can handle UTF-8
    if sys.stdout.encoding != 'utf-8':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception as e:
            logging.warning(f"Failed to reconfigure stdout to UTC-8: {e}")

def safe_log(message):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode('ascii', errors='replace').decode('ascii'))

# ------------------ Utils ------------------
def set_last_snapshot_time():
    ts = datetime.now(CDT).strftime("%Y-%m-%d %I:%M %p")
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        c.execute("REPLACE INTO metadata (key, value) VALUES ('last_snapshot', ?)", (ts,))
        conn.commit()

def get_last_snapshot_time():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        # Ensure metadata table exists
        c.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()

        row = c.execute("SELECT value FROM metadata WHERE key='last_snapshot'").fetchone()
        return row[0] if row else None


def get_bots_data(timeframe="All", sort_by="delta", sort_asc=False, created_after="All"):
    """
    Load snapshot history from DB, compute deltas for the given timeframe,
    and build the list of bots for the dashboard.

    Ranks are loaded from bot_rank_history for the latest date in the
    timeframe – no direct Typesense calls here anymore.
    """
    import sqlite3

    df_raw = load_history_df()
    dfc = compute_deltas(df_raw, timeframe)

    if dfc.empty:
        return [], [], 0, None

    # Optional: filter by created_after (keep your existing logic if different)
    if created_after != "All":
        now = datetime.now(tz=CDT)

        if created_after == "7day":
            cutoff = now - timedelta(days=7)
        elif created_after == "30day":
            cutoff = now - timedelta(days=30)
        elif created_after == "current_month":
            cutoff = now.replace(day=1)
        else:
            cutoff = None

        if cutoff:
            dfc = dfc[dfc["created_at"] >= cutoff]

    if dfc.empty:
        return [], [], 0, None

    # Latest date in this timeframe = "current snapshot"
    latest_date = sorted(dfc["date"].unique())[-1]
    today = dfc[dfc["date"] == latest_date].copy()

    # Totals history (for totals table + main charts)
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

    # --- NEW: load ranks for this date from bot_rank_history (no Typesense) ---
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "SELECT bot_id, rank FROM bot_rank_history WHERE date = ?",
        (str(latest_date),)
    )
    rank_rows = cur.fetchall()
    conn.close()

    rank_by_bot = {str(bot_id): (rank or 0) for (bot_id, rank) in rank_rows}

    bots = []
    for _, row in today.iterrows():
        bot_id = str(row["bot_id"])
        total = int(row["num_messages"])
        delta = int(row.get("daily_messages", 0))

        # avatar URL as before
        avatar_raw = row.get("avatar_url") or ""
        if avatar_raw:
            filename = str(avatar_raw).split("/")[-1]
            avatar_url = f"{AVATAR_BASE_URL}/{filename}"
        else:
            avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

        created_at_str = ""
        if pd.notnull(row.get("created_at")):
            try:
                created_at_str = row["created_at"].strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                created_at_str = str(row["created_at"])

        # rank info from DB
        r = rank_by_bot.get(bot_id, 0)
        if r and 1 <= r <= 480:
            if r <= 240:
                rank_tier = "top240"
            else:
                rank_tier = "top480"
            rank_val = r
        else:
            rank_val = None
            rank_tier = None

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
            "rank": rank_val,        # used for tags & bot detail
            "rank_tier": rank_tier,  # "top240" / "top480" / None
        })

    # Sorting behavior
    reverse = not sort_asc
    if sort_by == "name":
        bots.sort(key=lambda b: b["name"].lower(), reverse=reverse)
    elif sort_by == "total":
        bots.sort(key=lambda b: b["total"], reverse=reverse)
    elif sort_by == "created_at":
        bots.sort(key=lambda b: b["created_at"] or "", reverse=reverse)
    else:
        # default: sort by delta
        bots.sort(key=lambda b: b["delta"], reverse=reverse)

    total_messages = int(totals_df["num_messages"].iloc[0]) if not totals_df.empty else 0

    return bots, totals, total_messages, latest_date

def coerce_int(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    m = re.search(r'\d+', str(x))
    return int(m.group(0).replace(",", "")) if m else None

def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    STATIC_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    if not os.path.exists(STATIC_CHARTS_DIR):
        try:
            os.symlink(CHARTS_DIR, STATIC_CHARTS_DIR, target_is_directory=True)
            safe_log(f"Created symlink {STATIC_CHARTS_DIR} -> {CHARTS_DIR}")
        except OSError as e:
            logging.warning(f"Could not create symlink {STATIC_CHARTS_DIR}: {e}. Copying files instead.")
            for chart in CHARTS_DIR.glob("*.png"):
                shutil.copy(chart, STATIC_CHARTS_DIR / chart.name)
                safe_log(f"Copied {chart} to {STATIC_CHARTS_DIR}")

def clear_charts():
    try:
        for chart in CHARTS_DIR.glob("*.png"):
            chart.unlink()
            safe_log(f"Deleted chart {chart}")
        for chart in STATIC_CHARTS_DIR.glob("*.png"):
            chart.unlink()
            safe_log(f"Deleted static chart {STATIC_CHARTS_DIR / chart.name}")
    except Exception as e:
        logging.warning(f"Error clearing charts: {e}")

def pick(d, *keys, default=""):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            return d[k]
    return default

def get_name(d): return pick(d, "name", "characterName", "displayName", "botTitle", default="")
def get_title(d): return pick(d, "title", "botTitle", "description", default="")
def get_id(d): return pick(d, "id", "slug", "uuid", "characterId", "_id", default="")
def get_created_at(d): return pick(d, "createdAt", "created_at", default="")
def get_avatar_url(d): return pick(d, "avatarUrl", "avatar_url", default="")

def get_num_messages(d):
    if "num_messages" in d and d["num_messages"] is not None:
        return coerce_int(d["num_messages"])
    for k in ("messageCount", "message_count", "messages", "interactions", "numMessages"):
        if k in d and d[k] is not None:
            return coerce_int(d[k])
    for path in (("stats", "messageCount"), ("stats", "messages"),
                 ("usage", "messages"), ("metrics", "messages"), ("analytics", "messages")):
        cur, ok = d, True
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok:
            return coerce_int(cur)
    return None

def flatten_items(obj, out):
    if isinstance(obj, dict):
        if any(k in obj for k in ("name", "title", "characterName", "displayName", "botTitle")):
            out.append(obj)
        for v in obj.values():
            flatten_items(v, out)
    elif isinstance(obj, list):
        for it in obj:
            flatten_items(it, out)

# ------------------ Token Capture ------------------
def load_auth_credentials():
    if AUTH_FILE.exists():
        try:
            with open(AUTH_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                return data.get("bearer_token"), data.get("guest_userid")
        except Exception as e:
            logging.warning(f"Error loading auth credentials from {AUTH_FILE}: {e}")
    return None, None

def load_bearer_token():
    """Return (bearer_token, guest_userid) by reusing existing load_auth_credentials."""
    return load_auth_credentials()

def save_auth_credentials(bearer_token, guest_userid):
    try:
        with open(AUTH_FILE, "w", encoding='utf-8') as f:
            json.dump({"bearer_token": bearer_token, "guest_userid": guest_userid}, f)
        safe_log(f"Saved auth credentials to {AUTH_FILE}")
    except Exception as e:
        logging.error(f"Error saving auth credentials to {AUTH_FILE}: {e}")

def test_auth_credentials(bearer_token, guest_userid):
    if not bearer_token or not guest_userid:
        return False
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://spicychat.ai",
        "Referer": "https://spicychat.ai/my-chatbots",
        "x-app-id": "spicychat",
        "x-country": "US",
        "x-guest-userid": guest_userid
    }
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, (dict, list)) and data:
            safe_log("Existing auth credentials are valid")
            return True
        logging.warning("API response is empty or invalid")
        return False
    except requests.exceptions.RequestException as e:
        logging.warning(f"Auth credentials test failed: {e}")
        return False

def capture_auth_credentials(wait_rounds=18):
    # Try existing credentials first
    bearer_token, guest_userid = load_auth_credentials()
    if test_auth_credentials(bearer_token, guest_userid):
        return bearer_token, guest_userid

    # If credentials are invalid or missing, prompt for manual login
    bearer_token = None
    guest_userid = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Non-headless for manual login
        ctx = browser.new_context()
        page = ctx.new_page()

        def on_request(req):
            nonlocal bearer_token, guest_userid
            try:
                if "/v2/users/characters" in urlparse(req.url).path:
                    headers = req.headers
                    if "authorization" in headers and headers["authorization"].startswith("Bearer "):
                        bearer_token = headers["authorization"].replace("Bearer ", "")
                        safe_log("Captured bearer token")
                    if "x-guest-userid" in headers:
                        guest_userid = headers["x-guest-userid"]
                        safe_log("Captured x-guest-userid")
            except Exception as e:
                logging.warning(f"Error processing request: {e}")

        page.on("request", on_request)
        try:
            print("Please log in to SpicyChat using Google Sign-In in the opened browser window.")
            print("After logging in, navigate to 'My Chatbots' and press Enter in this terminal to continue.")
            page.goto("https://spicychat.ai", timeout=45000)
            input("Press Enter when you have logged in and navigated to My Chatbots...")
            page.goto(MY_BOTS_URL, timeout=45000)
            for _ in range(wait_rounds):
                page.wait_for_load_state("networkidle", timeout=15000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.4)
            if not bearer_token or not guest_userid:
                page.reload(timeout=45000)
                page.wait_for_load_state("networkidle", timeout=15000)
                time.sleep(1.0)
        except Exception as e:
            logging.error(f"Error capturing auth credentials: {e}")
        finally:
            ctx.close()
            browser.close()

    if not bearer_token or not guest_userid:
        logging.error("Failed to capture bearer token or guest user ID")
        raise RuntimeError("Failed to capture auth credentials")

    save_auth_credentials(bearer_token, guest_userid)
    return bearer_token, guest_userid

# ------------------ Database ------------------
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        
        # Check if table exists and add avatar_url if missing
        c.execute("PRAGMA table_info(bots)")
        columns = {row[1] for row in c.fetchall()}
        if "avatar_url" not in columns:
            c.execute("ALTER TABLE bots ADD COLUMN avatar_url TEXT")
            safe_log("Added avatar_url column to bots table")
        c.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                date TEXT,
                bot_id TEXT,
                bot_name TEXT,
                bot_title TEXT,
                num_messages INTEGER,
                creator_user_id TEXT,
                created_at TEXT,
                avatar_url TEXT,
                PRIMARY KEY (date, bot_id)
            )
        """)

        # Rank history per bot per day
        c.execute("""
            CREATE TABLE IF NOT EXISTS bot_rank_history (
                date TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                rank INTEGER,
                PRIMARY KEY (date, bot_id)
            )
        """)

        # NEW: number of your bots in Typesense top 480 per day
        c.execute("""
            CREATE TABLE IF NOT EXISTS top480_history (
                date TEXT PRIMARY KEY,
                count INTEGER
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS top240_history (
                date TEXT PRIMARY KEY,
                count INTEGER
            )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        conn.commit()


        safe_log(f"Initialized or updated SQLite database at {DATABASE}")
# ------------------ Typesense Client Wrapper ------------------
def multi_search_request(payload):
    """
    Wrapper for Typesense's multi_search endpoint using your public API key.
    """
    headers = {
        "X-TYPESENSE-API-KEY": TYPESENSE_KEY,
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(
            TYPESENSE_SEARCH_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        safe_log(f"Typesense request failed: {e}")
        raise

def save_rank_history_for_date(stamp, ts_map):
    """
    Save rank info for all bots (top 480) on a given date.
    """
    import sqlite3

    # Always fetch fresh top-480 filtered list (Female+NSFW)
    try:
        ts_map = fetch_typesense_top_bots(
            max_pages=10,
            use_cache=True,
            filter_female_nsfw=True
        )
    except Exception as e:
        safe_log(f"Rank history: Failed to fetch Typesense data: {e}")
        return

    if not ts_map:
        safe_log(f"No Typesense data available to save rank history for {stamp}")
        return

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    rows = []
    for cid, info in ts_map.items():
        try:
            rank = int(info.get("rank") or 0)
        except:
            continue

        if 1 <= rank <= 480:
            rows.append((stamp, cid, rank))

    cur.executemany(
        "INSERT OR REPLACE INTO bot_rank_history (date, bot_id, rank) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

    safe_log(f"Saved {len(rows)} rank history rows for {stamp}")


# ------------------ Capture ------------------
def capture_payloads(bearer_token, guest_userid, max_retries=3, delay=5):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://spicychat.ai",
        "Referer": "https://spicychat.ai/my-chatbots",
        "x-app-id": "spicychat",
        "x-country": "US",
        "x-guest-userid": guest_userid
    }

    for attempt in range(max_retries):
        try:
            safe_log(f"Attempt {attempt + 1}/{max_retries} to fetch {API_URL}")
            response = requests.get(API_URL, headers=headers, timeout=10)
            response.raise_for_status()
            logging.debug(f"Response status: {response.status_code}")
            logging.debug(f"Response headers: {response.headers}")
            logging.debug(f"Raw response (first 1000 chars): {response.text[:1000]}")

            if not response.text.strip():
                logging.warning("Empty response received from API")
                return []

            try:
                data = response.json()
                bots = data.get("data", []) if isinstance(data, dict) else data
                safe_log(f"Captured payload from {API_URL}: {len(bots)} items")
                logging.debug(f"Payload content: {bots}")
                return [bots]
            except requests.exceptions.JSONDecodeError as e:
                logging.error(f"Invalid JSON response from {API_URL}: {e}, response={response.text[:1000]}")
                if response.text.startswith("<!DOCTYPE html") or "<html" in response.text:
                    logging.error("Received HTML response. Likely redirected to login page. Check BEARER_TOKEN, GUEST_USERID, or API_URL.")
                raise

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error fetching {API_URL}: {e}, status={response.status_code}, text={response.text[:1000]}")
            if response.status_code == 401:
                logging.error("Authentication failed. Verify BEARER_TOKEN or GUEST_USERID.")
                raise
            elif response.status_code == 403:
                logging.error("Access forbidden. Check token permissions or rate limits.")
            elif response.status_code == 429:
                logging.warning(f"Rate limited on attempt {attempt + 1}. Retrying after {delay * (2 ** attempt)} seconds...")
                time.sleep(delay * (2 ** attempt))
                continue
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error fetching {API_URL}: {e}")
            if attempt < max_retries - 1:
                logging.warning(f"Retrying after {delay * (2 ** attempt)} seconds...")
                time.sleep(delay * (2 ** attempt))
                continue
            raise

    logging.error(f"Failed to fetch {API_URL} after {max_retries} attempts")
    raise RuntimeError(f"Failed to capture payloads after {max_retries} attempts")

# ------------------ Typesense fetch (Trending) ------------------
def fetch_typesense_top_bots(max_pages=10, use_cache=True, filter_female_nsfw=True):
    """
    Fetch Top Bots from Typesense.
    Supports filtered mode (Female + NSFW only) and unfiltered mode (all tags).
    """
    import json
    import pathlib

    # Separate caches for filtered and unfiltered trending
    if filter_female_nsfw:
        TRENDS_CACHE = pathlib.Path("data/ts_filtered_480.json")
    else:
        TRENDS_CACHE = pathlib.Path("data/ts_unfiltered_480.json")

    # --------------------
    # CACHE READ
    # --------------------
    if use_cache and TRENDS_CACHE.exists():
        try:
            with open(TRENDS_CACHE, "r", encoding="utf-8") as f:
                cached = json.load(f)

            # If cached data contains tags and already matches our data model,
            # return immediately.
            return {b["character_id"]: b for b in cached if "character_id" in b}
        except Exception as e:
            safe_log(f"Failed reading cached Typesense results: {e}")

    # --------------------
    # CACHE WAS MISSING OR INVALID → FULL FETCH
    # --------------------
    safe_log("Fetching fresh Top Bots from Typesense...")

    ALL_RESULTS = []

    # Set up pagination
    page = 1
    per_page = 48  # Typesense usually returns 48 per page

    # -------------------------
    # Build the filter clause
    # -------------------------
    if filter_female_nsfw:
        filter_clause = (
            "application_ids:spicychat && tags:![Step-Family] && "
            "creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c','kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] "
            "&& type:STANDARD && tags:[`Female`] && tags:[`NSFW`]"
        )
    else:
        filter_clause = (
            "application_ids:spicychat && tags:![Step-Family] && "
            "creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c','kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] "
            "&& type:STANDARD"
        )

    # --------------------
    # Loop pages
    # --------------------
    while page <= max_pages:
        payload = {
            "searches": [{
                "query_by": "name,title,tags,creator_username,character_id,type",
                "include_fields": (
                    "name,title,tags,creator_username,character_id,avatar_is_nsfw,avatar_url,"
                    "visibility,definition_visible,num_messages,token_count,rating_score,lora_status,"
                    "creator_user_id,is_nsfw,type,sub_characters_count,group_size_category,"
                    "num_messages_24h"
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
        results_page = result.get("results", [])
        if not results_page:
            break

        hits = results_page[0].get("hits", [])
        if not hits:
            break

        # --------------------
        # Build bot list
        # --------------------
        for obj in hits:
            doc = obj.get("document")
            if not doc:
                continue

            cid = doc.get("character_id")
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
                "tags": doc.get("tags", []) or [],      # <-- IMPORTANT!
                "is_nsfw": bool(doc.get("is_nsfw", False)),
                "link": f"https://spicychat.ai/chat/{cid}",
                "page": page,
                "rank": rank,
            }

            ALL_RESULTS.append(bot)

        page += 1

    # --------------------
    # CACHE WRITE
    # --------------------
    try:
        TRENDS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with open(TRENDS_CACHE, "w", encoding="utf-8") as f:
            json.dump(ALL_RESULTS, f, indent=2)
        safe_log(f"Saved {len(ALL_RESULTS)} bots to Typesense cache.")
    except Exception as e:
        safe_log(f"Failed writing Typesense cache: {e}")

    # Return as dict keyed by character_id
    return {b["character_id"]: b for b in ALL_RESULTS}


# ------------------ Snapshot ------------------
def sanitize_rows(rows):
    return [{k: r.get(k, "") for k in ALLOWED_FIELDS} for r in rows]

def take_snapshot(args, verbose=True):
    ensure_dirs()
    init_db()

    # Full timestamp for UI
    snapshot_time = datetime.now(tz=CDT)
    stamp = snapshot_time.strftime("%Y-%m-%d")
    safe_log(f"Starting snapshot for {stamp} in CDT")

    try:
        bearer_token, guest_userid = capture_auth_credentials()
        payloads = capture_payloads(bearer_token, guest_userid)
    except RuntimeError as e:
        logging.warning(f"No payloads captured: {e}. Proceeding with empty dataset.")
        payloads = []

    if not payloads:
        logging.warning("No payloads captured from API. Saving empty snapshot.")
        return DATABASE

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
            created_at = pd.Timestamp(created_at, tz="UTC").tz_convert(CDT).isoformat()
        row = {
            "date": stamp,
            "bot_id": bot_id,
            "bot_name": get_name(d),
            "bot_title": get_title(d),
            "num_messages": num,
            "creator_user_id": str(d.get("creator_user_id") or ""),
            "created_at": created_at,
            "avatar_url": get_avatar_url(d)
        }
        if bot_id in seen:
            logging.debug(f"Skipping duplicate bot_id: {bot_id}")
            continue
        seen.add(bot_id)
        rows.append(row)

    rows_clean = sanitize_rows(rows)
    logging.debug(f"Sanitized rows: {len(rows_clean)}")

    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("DELETE FROM bots WHERE date = ?", (stamp,))
        for row in rows_clean:
            c.execute("""
                INSERT INTO bots (date, bot_id, bot_name, bot_title, num_messages, creator_user_id, created_at, avatar_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["date"], row["bot_id"], row["bot_name"],
                row["bot_title"], row["num_messages"], row["creator_user_id"],
                row["created_at"], row["avatar_url"]
            ))
        conn.commit()
        
        set_last_snapshot_time()
        safe_log("Last snapshot time updated.")
        
    if verbose:
        safe_log(f"Snapshot saved for {len(rows_clean)} bots to {DATABASE}")
    

    # --- Refresh Typesense trending cache automatically ---
    try:
        safe_log("Refreshing Typesense trending cache (top 480 bots)")
        ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=False)
        safe_log(f"Typesense trending cache updated successfully ({len(ts_map)} entries)")

    except Exception as e:
        logging.error(f"Failed to refresh Typesense trending cache: {e}")
        ts_map = {}

    # ------------------------------------------------
    # Save rank history
    # ------------------------------------------------
    try:
        save_rank_history_for_date(stamp, ts_map)
    except Exception as e:
        logging.error(f"Error saving rank history: {e}")

    # ------------------------------------------------
    # Save Top-240 and Top-480 history (corrected logic)
    # ------------------------------------------------
    try:
        your_bot_ids = {row["bot_id"] for row in rows_clean}
        count_top240 = 0
        count_top480 = 0

        for cid, info in ts_map.items():
            if cid not in your_bot_ids:
                continue

            raw_rank = info.get("rank")
            try:
                rank = int(raw_rank)
            except:
                continue

            if rank < 1:
                continue

            if rank <= 480:
                count_top480 += 1
                if rank <= 240:
                    count_top240 += 1

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()

        cur.execute("""
            INSERT OR REPLACE INTO top480_history (date, count)
            VALUES (?, ?)
        """, (stamp, count_top480))

        cur.execute("""
            INSERT OR REPLACE INTO top240_history (date, count)
            VALUES (?, ?)
        """, (stamp, count_top240))

        conn.commit()
        conn.close()

    except Exception as e:
        logging.error(f"Error saving top-level histories: {e}")

    # ------------------------------------------------
    # FINAL STEP — Set last snapshot timestamp
    # ------------------------------------------------
    LAST_SNAPSHOT_DATE = snapshot_time.isoformat()
    safe_log(f"Snapshot complete at {LAST_SNAPSHOT_DATE}")

    return DATABASE

# ------------------ Load & consolidate ------------------
def load_history_df() -> pd.DataFrame:
    if not DATABASE.exists():
        safe_log(f"No database found at {DATABASE}. Returning empty DataFrame.")
        return pd.DataFrame(columns=ALLOWED_FIELDS)
    
    try:
        with sqlite3.connect(DATABASE) as conn:
            df = pd.read_sql_query("SELECT * FROM bots", conn)
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return pd.DataFrame(columns=ALLOWED_FIELDS)

    if df.empty:
        safe_log("Database is empty. Returning empty DataFrame.")
        return pd.DataFrame(columns=ALLOWED_FIELDS)

    for col in ALLOWED_FIELDS:
        if col not in df.columns:
            df[col] = ""
            logging.warning(f"Missing column {col} in database, filled with empty values")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["num_messages"] = pd.to_numeric(df["num_messages"], errors="coerce").fillna(0).astype(int)
    if not df.empty and "created_at" in df.columns and pd.notnull(df["created_at"]).any():
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True).dt.tz_convert(CDT)
    else:
        df["created_at"] = pd.NaT
    safe_log(f"Loaded {len(df)} rows from database: {list(df.columns)}")
    logging.debug(f"DataFrame head: {df.head().to_string()}")
    logging.debug(f"Available dates: {sorted(df['date'].unique())}")
    return df

def compute_deltas(df_raw: pd.DataFrame, timeframe="All") -> pd.DataFrame:
    if df_raw.empty:
        safe_log("No data to compute deltas.")
        return pd.DataFrame(columns=["date", "bot_id", "bot_name", "bot_title", "num_messages", "daily_messages", "created_at", "avatar_url"])

    # Compute deltas on full dataset
    df = df_raw.sort_values(["bot_id", "date"])
    df["daily_messages"] = df.groupby("bot_id")["num_messages"].diff().fillna(0).astype(int)

    decreases = df[df["daily_messages"] < 0].copy()
    if not decreases.empty:
        logging.warning("Detected decreases in total message counts:")
        for _, row in decreases.iterrows():
            logging.warning(
                f"Bot ID: {row['bot_id']}, Name: {row['bot_name']}, Date: {row['date']}, "
                f"Messages: {row['num_messages']}, Previous: {row['num_messages'] - row['daily_messages']}, "
                f"Delta: {row['daily_messages']}"
            )
        logging.warning("Please review the code and data source for inconsistencies.")

    df.loc[df["daily_messages"] < 0, "daily_messages"] = 0
    logging.debug(f"Computed DataFrame with deltas: {df[['bot_id', 'date', 'num_messages', 'daily_messages']].to_string()}")

    # Apply timeframe filter
    today = datetime.now().date()
    if timeframe == "7day":
        start_date = today - timedelta(days=7)
        df = df[df["date"] >= start_date]
    elif timeframe == "30day":
        start_date = today - timedelta(days=30)
        df = df[df["date"] >= start_date]
    elif timeframe == "current_month":
        current_month_start = today.replace(day=1)
        df = df[df["date"] >= current_month_start]
        # Adjust delta for the first day of the month
        for bot_id in df["bot_id"].unique():
            bot_data = df[df["bot_id"] == bot_id]
            first_day = bot_data["date"].min()
            if first_day == current_month_start and not bot_data.empty:
                prev_month_end = current_month_start - timedelta(days=1)
                prev_data = df_raw[(df_raw["bot_id"] == bot_id) & (df_raw["date"] <= prev_month_end)]
                prev_num = prev_data["num_messages"].max() if not prev_data.empty else 0
                first_day_row = bot_data[bot_data["date"] == first_day].index[0]
                df.loc[first_day_row, "daily_messages"] = int(bot_data.iloc[0]["num_messages"] - prev_num)
    # "All" timeframe uses all data, no filtering

    logging.debug(f"DataFrame after timeframe filter ({timeframe}): {df[['bot_id', 'date', 'num_messages', 'daily_messages']].to_string()}")
    if df.empty:
        logging.warning(f"No data available after applying timeframe filter: {timeframe}")

    return df

# ------------------ Plot helpers ------------------
def set_thousands_axis(ax):
    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))

def plot_line(dates, values, title, out_png, ylabel="Messages"):
    safe_log(f"Generating plot: {out_png}")
    logging.debug(f"Dates: {list(dates) if isinstance(dates, pd.Series) else dates}")
    logging.debug(f"Values: {list(values) if isinstance(dates, pd.Series) else values}")

    # Handle single data point case
    if len(dates) == 1:
        fig = plt.figure()
        try:
            plt.plot([0], values, marker='o', linestyle='none')
            plt.title(title)
            plt.xlabel("Date")
            plt.ylabel(ylabel)
            ax = plt.gca()
            set_thousands_axis(ax)
            ax.set_xticks([0])
            ax.set_xticklabels([str(dates.iloc[0])], rotation=45, ha="right")
            plt.tight_layout()
            out_png = Path(out_png).resolve()
            out_png.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(out_png, dpi=144)
            safe_log(f"Saved single-point plot to {out_png}")
            static_path = STATIC_CHARTS_DIR / out_png.name
            shutil.copy(out_png, static_path)
            safe_log(f"Copied single-point plot to {static_path}")
        except Exception as e:
            logging.error(f"Error generating single-point plot {out_png}: {e}")
        finally:
            plt.close(fig)
        return

    # Handle multiple data points
    fig = plt.figure()
    try:
        dates_numeric = np.arange(len(dates))
        values = np.array(values, dtype=float)
        spline = make_interp_spline(dates_numeric, values, k=3 if len(dates) >= 4 else 1)
        dates_smooth = np.linspace(dates_numeric.min(), dates_numeric.max(), 300)
        values_smooth = spline(dates_smooth)
        plt.plot(dates_smooth, values_smooth, linewidth=2)
        plt.scatter(dates_numeric, values, marker="o")
        plt.title(title)
        plt.xlabel("Date")
        plt.ylabel(ylabel)
        ax = plt.gca()
        set_thousands_axis(ax)
        ax.set_xticks(dates_numeric)
        ax.set_xticklabels(dates, rotation=45, ha="right")
        plt.tight_layout()
        out_png = Path(out_png).resolve()
        out_png.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_png, dpi=144)
        safe_log(f"Saved plot to {out_png}")
        static_path = STATIC_CHARTS_DIR / out_png.name
        shutil.copy(out_png, static_path)
        safe_log(f"Copied plot to {static_path}")
    except Exception as e:
        logging.error(f"Error generating plot {out_png}: {e}")
    finally:
        plt.close(fig)

# ------------------ Charts ------------------ 
def build_charts(verbose=True, timeframes=["7day", "30day", "current_month", "All"], updated_bot_ids=None):
    ensure_dirs()
    df_raw = load_history_df()
    if df_raw.empty:
        if verbose:
            safe_log("History is empty. Nothing to chart.")
        return

    start_time = time.time()
    safe_log(f"Starting chart generation for timeframes: {', '.join(timeframes)}")

    # Generate main page charts for all specified timeframes
    for timeframe in timeframes:
        dfc = compute_deltas(df_raw, timeframe)
        if dfc.empty:
            logging.warning(f"Computed DataFrame is empty for timeframe: {timeframe}. No charts will be generated.")
            continue

        totals = dfc.groupby("date", as_index=False).agg({"num_messages": "sum", "daily_messages": "sum"})
        logging.debug(f"Totals DataFrame for timeframe {timeframe}: {totals.to_string()}")

        total_chart = CHARTS_DIR / f"total_messages_{timeframe.replace(' ', '_')}.png"
        delta_chart = CHARTS_DIR / f"total_daily_changes_{timeframe.replace(' ', '_')}.png"
        if not total_chart.exists() or not delta_chart.exists() or (updated_bot_ids and df_raw[df_raw["bot_id"].isin(updated_bot_ids)]["date"].max() == datetime.now().date()):
            if not totals.empty and not totals["date"].empty and not totals["num_messages"].empty:
                safe_log(f"Generating global charts with {len(totals)} data points for timeframe: {timeframe}")
                plot_line(totals["date"], totals["num_messages"],
                          f"Total Messages ({timeframe})", total_chart)
                plot_line(totals["date"], totals["daily_messages"],
                          f"Total Daily Message Changes ({timeframe})", delta_chart, ylabel="Message Changes")
            else:
                logging.warning(f"No valid data for global charts with timeframe: {timeframe}")

        if time.time() - start_time > CHART_TIMEOUT:
            logging.error(f"Chart generation exceeded timeout of {CHART_TIMEOUT} seconds for timeframe: {timeframe}")
            return

    # Generate bot detail charts only for "All" timeframe and updated bots
    if "All" in timeframes and updated_bot_ids:
        df_all = compute_deltas(df_raw, "All")
        bot_ids_to_update = updated_bot_ids
        total_bots = len(bot_ids_to_update)
        for i, bot_id in enumerate(bot_ids_to_update, 1):
            sub = df_all[df_all["bot_id"] == bot_id].sort_values("date")
            name = sub["bot_name"].iloc[-1] if not sub.empty else "Unknown"
            logging.debug(f"Bot {bot_id} ({name}) data: {sub[['date', 'num_messages', 'daily_messages']].to_string()}")
            if not sub["date"].empty and not sub["num_messages"].empty:
                safe_log(f"Generating charts for bot {bot_id} ({name}) [{i}/{total_bots}]")
                out_total = CHARTS_DIR / f"bot_{bot_id}_total.png"
                out_delta = CHARTS_DIR / f"bot_{bot_id}_delta.png"
                plot_line(sub["date"], sub["num_messages"], f"{name} — Total Messages (All)", out_total)
                plot_line(sub["date"], sub["daily_messages"], f"{name} — Daily Message Changes (All)", out_delta, ylabel="Message Changes")
            else:
                logging.warning(f"No valid data for bot {bot_id} ({name}) charts")

            if time.time() - start_time > CHART_TIMEOUT:
                logging.error(f"Chart generation exceeded timeout of {CHART_TIMEOUT} seconds for bot: {bot_id}")
                return

    if verbose:
        safe_log(f"Charts written to {CHARTS_DIR}\\*.png and copied to {STATIC_CHARTS_DIR}")
    safe_log(f"Chart generation completed in {time.time() - start_time:.2f} seconds")

# ------------------ Formatters ------------------ 
def fmt_commas(n):
    try:
        return f"{int(n):,}"
    except:
        return ""

def fmt_delta_commas(n):
    try:
        n = int(n)
        sign = "+" if n >= 0 else "-"
        return f"{sign}{abs(n):,}"
    except:
        return ""

# ------------------ Routes ------------------ 
def define_routes():
    global _routes_defined
    if _routes_defined:
        return

    @app.route("/")
    def index():
        chart_sort_by = request.args.get("chart_sort_by", "7day")
        chart_sort_asc = request.args.get("chart_sort_asc", "false") == "true"
        sort_by = request.args.get("sort_by", "delta")
        sort_asc = request.args.get("sort_asc", "false") == "true"
        created_after = request.args.get("created_after", "All")
        timeframe = request.args.get("timeframe", "All")  # Default to All for bots
        safe_log(f"Index route: chart_sort_by={chart_sort_by}, chart_sort_asc={chart_sort_asc}, sort_by={sort_by}, sort_asc={sort_asc}, created_after={created_after}, timeframe={timeframe}")

        last_7_days = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        last_30_days = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        current_month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')

        df_raw = load_history_df()
        if df_raw.empty:
            safe_log("Rendering index with no data.")
            return render_template(
                "index.html",
                latest="No data",
                total_messages=0,
                total_bots=0,
                bots=[],
                totals=[],
                sort_by=sort_by,
                sort_asc=sort_asc,
                chart_sort_by=chart_sort_by,
                chart_sort_asc=chart_sort_asc,
                created_after=created_after,
                last_7_days=last_7_days,
                last_30_days=last_30_days,
                current_month_start=current_month_start,
                timeframe=timeframe
            )

        dfc = compute_deltas(df_raw, chart_sort_by if chart_sort_by in ["7day", "30day", "current_month", "All"] else "7day")
        all_dates = sorted(dfc["date"].unique(), reverse=True)
        today_date = datetime.now().date().strftime("%Y-%m-%d")
        latest = all_dates[0] if all_dates else today_date
        # if str(latest) != today_date:
        #     logging.warning(f"Latest date {latest} is not today ({today_date}). Forcing snapshot.")
        #     try:
        #         class DummyArgs:
        #             def __init__(self):
        #                 self.no_snapshot = False
        #                 self.no_charts = False
        #                 self.port = 5000
        #         dummy_args = DummyArgs()
        #         take_snapshot(dummy_args)
        #         df_raw = load_history_df()
        #         dfc = compute_deltas(df_raw, chart_sort_by if chart_sort_by in ["7day", "30day", "current_month", "All"] else "7day")
        #         all_dates = sorted(dfc["date"].unique(), reverse=True)
        #         latest = all_dates[0] if all_dates else today_date
        #     except Exception as e:
        #         logging.error(f"Error forcing snapshot: {e}")

        totals = dfc.groupby("date", as_index=False).agg({"num_messages": "sum", "daily_messages": "sum"})
        if chart_sort_asc:
            totals = totals.sort_values("date")
        else:
            totals = totals.sort_values("date", ascending=False)
        total_messages = int(totals.loc[totals["date"] == latest, "num_messages"].iloc[0]) if not totals.empty else 0
        total_bots = len(dfc[dfc["date"] == latest]["bot_id"].unique()) if not dfc.empty else 0
        totals_data = [
            {
                "date": str(row["date"]),
                "total": int(row["num_messages"]),
                "total_fmt": fmt_commas(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "daily_fmt": fmt_delta_commas(row["daily_messages"])
            } for _, row in totals.iterrows()
        ]
        logging.debug(f"Totals data for rendering: {totals_data}")

        # try:
        #     bots, total_messages, total_bots = get_bots_data(sort_by, sort_asc, created_after, timeframe)
        #     logging.debug(f"Retrieved {len(bots)} bots, total_messages={total_messages}, total_bots={total_bots}")
        # except Exception as e:
        #     logging.error(f"Error in get_bots_data: {e}")
        #     bots, total_messages, total_bots = [], 0, 0
        try:
            bots, totals_list, total_messages, latest_date_from_bots = get_bots_data(
                timeframe=timeframe,
                sort_by=sort_by,
                sort_asc=sort_asc,
                created_after=created_after
            )
            total_bots = len(bots)
        except Exception as e:
            logging.error(f"Error in get_bots_data: {e}")
            bots, totals_list, total_messages, total_bots, latest_date_from_bots = [], [], 0, 0, None
            
        last_snapshot = get_last_snapshot_time()
    
        safe_log(f"Rendering index for {latest} with {len(bots)} bots")
        return render_template(
            "index.html",
            latest=str(latest),
            total_messages=fmt_commas(total_messages),
            total_bots=total_bots,
            totals=totals_data,
            last_snapshot=last_snapshot,
            bots=bots,
            sort_by=sort_by,
            sort_asc=sort_asc,
            chart_sort_by=chart_sort_by,
            chart_sort_asc=chart_sort_asc,
            created_after=created_after,
            last_7_days=last_7_days,
            last_30_days=last_30_days,
            current_month_start=current_month_start,
            timeframe=timeframe
        )

    @app.route("/api/snapshot_status")
    def api_snapshot_status():
        global AUTH_REQUIRED, LAST_SNAPSHOT_DATE

        return jsonify({
            "auth_required": AUTH_REQUIRED,
            "last_snapshot": LAST_SNAPSHOT_DATE
        })
    
    @app.route("/api/totals")
    def api_totals():
        import sqlite3

        timeframe = request.args.get("timeframe", "7day")
        init_db()  # ensure tables exist

        df_raw = load_history_df()
        dfc = compute_deltas(df_raw, timeframe)

        if dfc.empty:
            return jsonify({"points": []})

        totals = (
            dfc.groupby("date", as_index=False)
            .agg({"num_messages": "sum", "daily_messages": "sum"})
            .sort_values("date")
        )

        # --- Load top480 and top240 history ---
        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()

        # top 10 pages (1–10 → top 480)
        try:
            cur.execute("SELECT date, count FROM top480_history")
            top480_rows = cur.fetchall()
        except sqlite3.OperationalError:
            top480_rows = []

        # top 5 pages (1–5 → top 240)
        try:
            cur.execute("SELECT date, count FROM top240_history")
            top240_rows = cur.fetchall()
        except sqlite3.OperationalError:
            top240_rows = []

        conn.close()

        top480_by_date = {d: c for (d, c) in top480_rows}
        top240_by_date = {d: c for (d, c) in top240_rows}

        # --- Build timeline points ---
        points = []
        for _, row in totals.iterrows():
            date_str = str(row["date"])
            points.append({
                "date": date_str,
                "total": int(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "top480": top480_by_date.get(date_str, 0),
                "top240": top240_by_date.get(date_str, 0),  # <<<<<< REQUIRED
            })
            
        # --- Timeframe totals (for KPI) ---
        tf_total = 0
        tf_bots = 0

        # Determine cutoff date
        today = datetime.now().date()

        if timeframe == "7day":
            cutoff = today - timedelta(days=7)
        elif timeframe == "30day":
            cutoff = today - timedelta(days=30)
        elif timeframe == "current_month":
            cutoff = today.replace(day=1)
        else:
            cutoff = None  # All time

        # Compute timeframe total messages
        if points:
            tf_total = points[-1]["total"] - points[0]["total"]

        # Compute bots created in timeframe
        if cutoff:
            tf_bots = dfc[dfc["created_at"].dt.date >= cutoff]["bot_id"].nunique()
        else:
            tf_bots = dfc["bot_id"].nunique()


        return jsonify({
            "points": points,
            "tf_total": tf_total,
            "tf_bots": tf_bots
        })
        
        @app.route("/api/bot/<bot_id>/history")
        def api_bot_history(bot_id):
            timeframe = request.args.get("timeframe", "All")
            df_raw = load_history_df()
            dfc = compute_deltas(df_raw, timeframe)
            sub = dfc[dfc["bot_id"] == bot_id].sort_values("date")
            points = [
                {
                    "date": str(row["date"]),
                    "total": int(row["num_messages"]),
                    "daily": int(row["daily_messages"]),
                }
                for _, row in sub.iterrows()
            ]
            return jsonify({"bot_id": bot_id, "points": points})

@app.route("/api/bot/<bot_id>/history")
def api_bot_history(bot_id):
    import sqlite3

    timeframe = request.args.get("timeframe", "All")
    df_raw = load_history_df()
    dfc = compute_deltas(df_raw, timeframe)

    # All rows for this bot in timeframe
    sub = dfc[dfc["bot_id"] == bot_id].sort_values("date")

    # Join in rank history from bot_rank_history
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "SELECT date, rank FROM bot_rank_history WHERE bot_id = ?",
        (bot_id,)
    )
    rank_rows = cur.fetchall()
    conn.close()

    rank_by_date = {date_str: (rank or 0) for (date_str, rank) in rank_rows}

    points = []
    for _, row in sub.iterrows():
        date_str = str(row["date"])
        r = rank_by_date.get(date_str, 0)

        # Convert rank → page number:
        # 1–48     → 1
        # 49–96    → 2
        # ...
        # 433–480  → 10
        # >480 or no rank → 11  (meaning ">10")
        if r and 1 <= r <= 480:
            page = (r - 1) // 48 + 1   # pages 1–10
        else:
            page = 11                  # ranks 481+ or missing → ">10"

        points.append({
            "date": date_str,
            "total": int(row["num_messages"]),
            "daily": int(row.get("daily_messages", 0)),
            "rank": r if r else None,
            "page": page,
        })

    return jsonify({"bot_id": bot_id, "points": points})


@app.route("/bot/<bot_id>")
def bot_detail(bot_id):
    import sqlite3

    timeframe = request.args.get("timeframe", "All")

    df_raw = load_history_df()
    dfc = compute_deltas(df_raw, timeframe)

    # All rows for this bot in timeframe
    bot_rows = dfc[dfc["bot_id"] == bot_id].sort_values("date")
    if bot_rows.empty:
        logging.warning(f"Bot {bot_id} not found in DB for timeframe {timeframe}")
        return render_template("bot.html", bot=None, history=[], timeframe=timeframe)

    # Latest row for summary
    latest = bot_rows.iloc[-1]

    # Build avatar URL
    avatar_raw = latest.get("avatar_url") or ""
    if avatar_raw:
        filename = str(avatar_raw).split("/")[-1]
        avatar_url = f"{AVATAR_BASE_URL}/{filename}"
    else:
        avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

    # Format created_at
    created_at_str = ""
    if pd.notnull(latest.get("created_at")):
        try:
            created_at_str = latest["created_at"].strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            created_at_str = str(latest["created_at"])

    bot_data = {
        "bot_id": latest["bot_id"],
        "name": latest["bot_name"],
        "title": latest["bot_title"],
        "total": int(latest["num_messages"]),
        "total_fmt": fmt_commas(latest["num_messages"]),
        "delta": int(latest.get("daily_messages", 0)),
        "delta_fmt": fmt_delta_commas(int(latest.get("daily_messages", 0))),
        "created_at": created_at_str,
        "link": f"https://spicychat.ai/chat/{latest['bot_id']}",
        "avatar_url": avatar_url,
    }

    # ---------- NEW: load rank history for this bot ----------
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "SELECT date, rank FROM bot_rank_history WHERE bot_id = ?",
        (bot_id,)
    )
    rank_rows = cur.fetchall()
    conn.close()

    # Create dict: "YYYY-MM-DD" → rank
    rank_by_date = {date_str: (rank or 0) for (date_str, rank) in rank_rows}

    # ---------- Build history table rows ----------
    history_rows = bot_rows.sort_values("date", ascending=False)

    history = []
    for _, row in history_rows.iterrows():
        # Format created_at for table
        created_row = ""
        if pd.notnull(row.get("created_at")):
            try:
                created_row = row["created_at"].strftime("%Y-%m-%d")
            except Exception:
                created_row = str(row["created_at"])

        daily = int(row.get("daily_messages", 0))
        date_str = str(row["date"])

        # Get rank for that date (if available)
        r = rank_by_date.get(date_str, 0)
        rank_val = r if r and r <= 480 else None

        history.append({
            "date": date_str,
            "total": int(row["num_messages"]),
            "total_fmt": fmt_commas(row["num_messages"]),
            "daily": daily,
            "daily_fmt": fmt_delta_commas(daily),
            "created_at": created_row,
            "rank": rank_val,     # ← NEW
        })

    return render_template("bot.html", bot=bot_data, history=history, timeframe=timeframe)

    # @app.route("/bots")
    # def bots_table():
    #     sort_by = request.args.get("sort_by", "delta")
    #     sort_asc = request.args.get("sort_asc", "false") == "true"
    #     created_after = request.args.get("created_after", "All")
    #     timeframe = request.args.get("timeframe", "All")  # Not used for bot table, kept for consistency
    #     safe_log(f"Bots table route: sort_by={sort_by}, sort_asc={sort_asc}, created_after={created_after}, timeframe={timeframe}")

    #     bots, _, _ = get_bots_data(sort_by, sort_asc, created_after, timeframe)
    #     try:
    #         return render_template("bots_table.html", bots=bots, sort_by=sort_by, sort_asc=sort_asc, created_after=created_after, timeframe=timeframe)
    #     except Exception as e:
    #         logging.error(f"Error rendering bots_table template: {e}")
    #         raise

    # @app.route("/bot/<bot_id>")
    # def bot_detail(bot_id):
    #     timeframe = "All"  # Use "All" to include all historical data
    #     df_raw = load_history_df()
    #     dfc = compute_deltas(df_raw, timeframe)

    #     bot = dfc[dfc["bot_id"] == bot_id]
    #     if bot.empty:
    #         logging.warning(f"Bot {bot_id} not found in database")
    #         return render_template("bot.html", bot=None, history=[], timeframe=timeframe)

    #     # Compute delta for the latest row if not present
    #     row = bot.iloc[-1].copy()
    #     if "delta" not in bot.columns:
    #         all_dates = sorted(df_raw[df_raw["bot_id"] == bot_id]["date"].unique(), reverse=True)
    #         if len(all_dates) >= 2:
    #             prev_date = all_dates[1]
    #             prev_row = df_raw[(df_raw["bot_id"] == bot_id) & (df_raw["date"] == prev_date)]
    #             prev_num = int(prev_row["num_messages"].iloc[0]) if not prev_row.empty else 0
    #             row["delta"] = int(row["num_messages"] - prev_num)
    #         else:
    #             row["delta"] = 0

    #     bot_data = {
    #         "bot_id": row["bot_id"],
    #         "name": row["bot_name"],
    #         "title": row["bot_title"],
    #         "total": int(row["num_messages"]),
    #         "total_fmt": fmt_commas(row["num_messages"]),

    #         "delta": int(row.get("delta", 0)),
    #         "delta_fmt": fmt_delta_commas(row.get("delta", 0)),
    #         "created_at": row["created_at"].strftime("%Y-%m-%d") if pd.notnull(row["created_at"]) else "",
    #         "link": f"https://spicychat.ai/chat/{row['bot_id']}",
    #         "avatar_url": f"{AVATAR_BASE_URL}/{row['avatar_url'].split('/')[-1]}" if row["avatar_url"] else f"{AVATAR_BASE_URL}/default-avatar.png"
    #     }
    #     logging.debug(f"Bot data avatar_url: {bot_data['avatar_url']}")

    #     # Use the delta-computed DataFrame and sort by date descending explicitly
    #     history_df = dfc[dfc["bot_id"] == bot_id].sort_values("date", ascending=False)
    #     history = [
    #         {
    #             "date": str(row["date"]),
    #             "total": int(row["num_messages"]),
    #             "total_fmt": fmt_commas(row["num_messages"]),
    #             "daily": int(row["daily_messages"]) if "daily_messages" in row else 0,
    #             "daily_fmt": fmt_delta_commas(row["daily_messages"]) if "daily_messages" in row else "+0",
    #             "created_at": row["created_at"].strftime("%Y-%m-%d") if pd.notnull(row["created_at"]) else ""
    #         } for _, row in history_df.iterrows()
    #     ]

    #     safe_log(f"Rendering bot detail for {bot_id}: {bot_data['name']} with {len(history)} history entries")
    #     return render_template("bot.html", bot=bot_data, history=history, timeframe=timeframe)

@app.route("/take-snapshot", methods=["POST"])
def take_snapshot_route():
    safe_log("Received request to take snapshot")
    try:
        # Create a dummy args object with default values
        class DummyArgs:
            def __init__(self):
                self.no_snapshot = False
                self.no_charts = False
                self.port = 5000
        dummy_args = DummyArgs()
        take_snapshot(dummy_args, verbose=True)
        safe_log("Snapshot and charts updated successfully")
        sort_by = request.args.get("sort_by", "delta")
        sort_asc = request.args.get("sort_asc", "false")
        created_after = request.args.get("created_after", "All")
        timeframe = request.args.get("timeframe", "All")
        chart_sort_by = request.args.get("chart_sort_by", "7day")
        chart_sort_asc = request.args.get("chart_sort_asc", "false")
        params = {"sort_by": sort_by, "sort_asc": sort_asc, "timeframe": timeframe, "chart_sort_by": chart_sort_by, "chart_sort_asc": chart_sort_asc, "created_after": created_after}
        return redirect(url_for("index", **params))
    except Exception as e:
        logging.error(f"Error in take_snapshot_route: {e}")
        raise

@app.route("/trending")
def trending():
    """
    Show which of *your* bots appear:
        - in Typesense pages 1–5 (Top 240)  → "Top 5" section
        - in Typesense pages 6–10 (Top 241–480) → "Top 10" section
    Each bot shows its global rank (#1–480) and uses the same avatar CDN
    logic as the main dashboard.
    """
    per_page = 48

    df_raw = load_history_df()

    # Always define this up front so it's available in every return path
    if df_raw.empty:
        my_bots_count = 0
    else:
        my_bots_count = df_raw["bot_id"].nunique()

    bearer_token, guest_userid = capture_auth_credentials()


    # First try cache; if it looks empty, force a live fetch
    ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=True)
    if not ts_map:
        safe_log("Trending: cached Typesense results are empty, forcing fresh fetch without cache")
        ts_map = fetch_typesense_top_bots(max_pages=10,  use_cache=False)

    ts_list = list(ts_map.values())

    # Split into pages 1–5 and 6–10 based on the 'page' field we stored
    top5_segment = [b for b in ts_list if 1 <= int(b.get("page", 0)) <= 5]
    top10_segment = [b for b in ts_list if 6 <= int(b.get("page", 0)) <= 10]

    top5_total = len(top5_segment)
    top10_total = len(top10_segment)

    # If Typesense gave us nothing at all, show a clear error
    if top5_total == 0 and top10_total == 0:
        msg = (
            "Typesense returned 0 bots for pages 1–10. "
            "This usually means your bearer token, Typesense key, or filters are invalid, "
            "or Typesense changed its schema. Try taking a fresh snapshot or deleting "
            "data/public_bots_home_all.json so it can be regenerated."
        )
        safe_log(msg)
        return render_template(
            "trending.html",
            error=msg,
            top5=[],
            top10=[],
            top5_total=top5_total,
            top10_total=top10_total,
            my_bots_count=my_bots_count,
        )

    # Latest snapshot date for "your bots" data
    latest_date = None
    if not df_raw.empty:
        latest_date = sorted(df_raw["date"].unique(), reverse=True)[0]

    # Treat all rows in DB as "your bots" (the API already returns your bots)
    my_bots_df = df_raw
    my_bots = {}
    if not my_bots_df.empty:
        latest_rows = my_bots_df[my_bots_df["date"] == latest_date] if latest_date else my_bots_df
        for _, r in latest_rows.iterrows():
            my_bots[str(r["bot_id"])] = {
                "bot_id": r["bot_id"],
                "bot_name": r["bot_name"],
                "num_messages": int(r["num_messages"]),
                "created_at": (r["created_at"].strftime("%Y-%m-%d") if pd.notnull(r["created_at"]) else ""),
                # optional: we could keep DB avatar here if you want a fallback
                "avatar_url": r.get("avatar_url", ""),
            }

    def build_list(segment):
        results = []
        for info in segment:
            cid = info.get("character_id")
            if not cid:
                continue
            if cid in my_bots:
                # Normalize avatar URL to match main dashboard behavior
                raw = info.get("avatar_url") or my_bots[cid].get("avatar_url", "")
                if raw:
                    filename = raw.split("/")[-1]
                    avatar_url = f"{AVATAR_BASE_URL}/{filename}"
                else:
                    avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

                daily = int(info.get("num_messages_24h") or 0)
                rank = int(info.get("rank") or 0)

                results.append({
                    "bot_id": cid,
                    "name": info.get("name", my_bots[cid]["bot_name"]),
                    "link": info.get("link"),
                    "avatar_url": avatar_url,
                    "total_messages": int(info.get("num_messages") or my_bots[cid]["num_messages"]),
                    "daily_messages": daily,
                    "rank": rank,
                })

        # Sort by rank so #1 is at the top; fallback huge number if rank==0
        results.sort(key=lambda x: x["rank"] or 999_999)
        return results

    top5_my = build_list(top5_segment)
    top10_my = build_list(top10_segment)

    safe_log(
        f"Trending page: TS pages1-5={top5_total}, pages6-10={top10_total}, "
        f"DB bots={my_bots_count}, my top5={len(top5_my)}, my top10={len(top10_my)}"
    )

    return render_template(
        "trending.html",
        error=None,
        top5=top5_my,
        top10=top10_my,
        top5_total=top5_total,
        top10_total=top10_total,
        my_bots_count=my_bots_count,
    )

@app.route("/global-trending")
def global_trending():

    # ---------------------------------------------------------
    # Persistent tab (Creators or Tags)
    # ---------------------------------------------------------
    active_tab = request.args.get("tab", "creators")
    

    # ---------------------------------------------------------
    # FETCH #1 (Filtered Trending):
    # Only Female + NSFW bots shown in trending grid.
    # ---------------------------------------------------------
    ts_map_filtered = fetch_typesense_top_bots(
        max_pages=10,
        use_cache=True,
        filter_female_nsfw=True
    )
    ts_list = list(ts_map_filtered.values())

    # ---------------------------------------------------------
    # FETCH #2 (Unfiltered for TAGS):
    # Sidebar tags must use ALL trending bots, not only filtered.
    # ---------------------------------------------------------
    ts_map_all = fetch_typesense_top_bots(
        max_pages=10,
        use_cache=True,
        filter_female_nsfw=False
    )

    # ---------------------------------------------------------
    # Sorting
    # ---------------------------------------------------------
    sort_field = request.args.get("sort", "rank")
    order = request.args.get("order", "asc")
    reverse = (order == "desc")

    if sort_field == "author":
        ts_list.sort(key=lambda b: (b.get("creator_username") or "").lower(), reverse=reverse)
    elif sort_field == "messages":
        ts_list.sort(key=lambda b: int(b.get("num_messages") or 0), reverse=reverse)
    else:  # rank
        ts_list.sort(key=lambda b: int(b.get("rank") or 999999), reverse=reverse)

    # ---------------------------------------------------------
    # Author and Tag Filtering
    # ---------------------------------------------------------
    author_filter = request.args.get("author")
    tag_filter = request.args.get("tag")

    if author_filter:
        ts_list = [b for b in ts_list if b.get("creator_username") == author_filter]

    if tag_filter:
        ts_list = [b for b in ts_list if tag_filter in (b.get("tags") or [])]

    # ---------------------------------------------------------
    # Pagination
    # ---------------------------------------------------------
    PER_PAGE = 48
    page = int(request.args.get("page", 1))

    total_pages = max((len(ts_list) - 1) // PER_PAGE + 1, 1)
    page = max(1, min(page, total_pages))

    start = (page - 1) * PER_PAGE
    end = start + PER_PAGE

    page_items = []

    # Normalize avatars + slice
    for bot in ts_list[start:end]:
        raw = bot.get("avatar_url", "")
        if raw:
            filename = raw.split("/")[-1]
            bot["avatar_url"] = f"{AVATAR_BASE_URL}/{filename}"
        else:
            bot["avatar_url"] = f"{AVATAR_BASE_URL}/default-avatar.png"
        page_items.append(bot)

    # ---------------------------------------------------------
    # Creator Leaderboard (Filtered dataset ONLY)
    # ---------------------------------------------------------
    creator_counts = {}
    for bot in ts_map_filtered.values():
        creator = bot.get("creator_username", "")
        if creator:
            creator_counts[creator] = creator_counts.get(creator, 0) + 1

    creators_sorted = sorted(
        [{"creator": c, "count": n} for c, n in creator_counts.items()],
        key=lambda x: x["count"],
        reverse=True
    )

    # ---------------------------------------------------------
    # Tag Leaderboard (Unfiltered dataset)
    # ---------------------------------------------------------
    tag_counts = {}
    for bot in ts_map_all.values():
        for t in bot.get("tags", []) or []:
            tag_counts[t] = tag_counts.get(t, 0) + 1

    tags_sorted = sorted(
        [{"tag": t, "count": c} for t, c in tag_counts.items()],
        key=lambda x: x["count"],
        reverse=True
    )

    # ---------------------------------------------------------
    # Render template
    # ---------------------------------------------------------
    return render_template(
        "global_trending.html",
        bots=page_items,
        creators=creators_sorted,
        tags=tags_sorted,
        page=page,
        total_pages=total_pages,
        sort_field=sort_field,
        order=order,
        author_filter=author_filter,
        tag_filter=tag_filter,
        active_tab=active_tab
    )


@app.route("/reauth", methods=["POST"])
def reauth():
    global AUTH_REQUIRED, SNAPSHOT_THREAD_STARTED

    try:
        # Capture credentials via Playwright
        bearer, guest = capture_auth_credentials()

        # Mark auth as valid
        AUTH_REQUIRED = False

        safe_log("Reauthentication successful — running immediate snapshot...")

        # Run a snapshot immediately after auth
        try:
            take_snapshot({})
            safe_log("Snapshot after reauth completed.")
        except Exception as e:
            safe_log(f"Snapshot after reauth failed: {e}")

        return jsonify({"success": True})

    except Exception as e:
        safe_log(f"Reauthentication failed: {e}")
        AUTH_REQUIRED = True
        return jsonify({"success": False, "error": str(e)}), 500


    #_routes_defined = True

# ------------------ CLI ------------------ 
# def main():
#     setup_logging()
#     # Log script content hash for verification
#     with open(__file__, 'rb') as f:
#         script_hash = hashlib.md5(f.read()).hexdigest()
#     safe_log(f"Script hash: {script_hash}")

#     p = argparse.ArgumentParser(description="SpicyChat analytics with SQLite database and Flask dashboard")
#     p.add_argument("--no_snapshot", action="store_true", help="Skip snapshot")
#     p.add_argument("--no_charts", action="store_true", help="Skip chart generation")
#     p.add_argument("--port", type=int, default=5000, help="Flask server port (default 5000)")
#     args = p.parse_args()

#     safe_log(f"Command-line args: no_snapshot={args.no_snapshot}, no_charts={args.no_charts}, port={args.port}")
#     # Verify function availability
#     try:
#         get_bots_data
#         safe_log("get_bots_data function is defined")
#     except NameError:
#         safe_log("get_bots_data function is NOT defined - check script structure")
#     define_routes()  # Define routes only once

#     if not args.no_snapshot:
#         try:
#             take_snapshot(args)
#         except RuntimeError as e:
#             logging.error(f"Snapshot failed: {e}. Continuing to start server.")
#     #if not args.no_charts:
#         #build_charts(timeframes=["7day", "30day", "current_month", "All"])  # Generate charts for all timeframes

#     safe_log(f"Starting Flask server on http://localhost:{args.port}")
#     app.run(host="0.0.0.0", port=args.port, debug=False)

def snapshot_scheduler():
    global AUTH_REQUIRED, RESTARTING

    while True:

        try:
            bearer, guest = load_auth_credentials()

            # If missing or invalid → pause snapshots
            if not test_auth_credentials(bearer, guest):
                AUTH_REQUIRED = True
                safe_log("Snapshot scheduler paused — auth invalid.")
            else:
                AUTH_REQUIRED = False
                safe_log("Auth OK — running hourly snapshot.")
                try:
                    take_snapshot({})
                except Exception as e:
                    safe_log(f"Snapshot failed: {e}")

        except Exception as e:
            safe_log(f"Scheduler error: {e}")

        # Wait 1 hour
        time.sleep(3600)

if __name__ == "__main__":
    setup_logging()
    ensure_dirs()
    init_db()   # <---- ADD THIS


    parser = argparse.ArgumentParser(description="SpicyChat Analytics Dashboard")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args = parser.parse_args()

    
    CURRENT_PORT = args.port

    define_routes()

    if not SNAPSHOT_THREAD_STARTED:
        threading.Thread(target=snapshot_scheduler, daemon=True).start()
        SNAPSHOT_THREAD_STARTED = True

    safe_log(f"Starting server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port)
