# core.py
import os
import sys
import re
import time
import logging
import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
import sqlite3
import numpy as np
import pandas as pd
import pytz
import threading
from playwright.sync_api import sync_playwright
import urllib.parse as urlparse

# ------------------ Config ------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DATABASE = DATA_DIR / "spicychat.db"
AUTH_FILE = DATA_DIR / "auth_credentials.json"

API_URL = "https://prod.nd-api.com/v2/users/characters?switch=T1"
MY_BOTS_URL = "https://spicychat.ai/my-chatbots"

AVATAR_BASE_URL = "https://cdn.nd-api.com/avatars"

# Global flags
AUTH_REQUIRED = False
SNAPSHOT_THREAD_STARTED = False
LAST_SNAPSHOT_DATE = None

# ------------------ Typesense / Trending config ------------------
TYPESENSE_HOST = "https://etmzpxgvnid370fyp.a1.typesense.net"
TYPESENSE_KEY = "STHKtT6jrC5z1IozTJHIeSN4qN9oL1s3"  # Public read key used by web UI
TYPESENSE_SEARCH_ENDPOINT = f"{TYPESENSE_HOST}/multi_search"

# Different cache files for filtered vs unfiltered
FILTERED_CACHE = DATA_DIR / "ts_filtered_480.json"
UNFILTERED_CACHE = DATA_DIR / "ts_unfiltered_480.json"

ALLOWED_FIELDS = [
    "date", "bot_id", "bot_name", "bot_title",
    "num_messages", "creator_user_id", "created_at", "avatar_url"
]

# Set CDT timezone
CDT = pytz.timezone("America/Chicago")

# ------------------ Logging ------------------
def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGS_DIR / "spicychat.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Ensure console can handle UTF-8
    if sys.stdout.encoding != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception as e:
            logging.warning(f"Failed to reconfigure stdout to UTF-8: {e}")


def safe_log(message: str):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode("ascii", errors="replace").decode("ascii"))

# ------------------ Basic filesystem helpers ------------------
def ensure_dirs():
    """Ensure data and logs directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------ Snapshot metadata helpers ------------------
def set_last_snapshot_time():
    ts = datetime.now(CDT).strftime("%Y-%m-%d %I:%M %p")
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.commit()
        c.execute(
            "REPLACE INTO metadata (key, value) VALUES ('last_snapshot', ?)",
            (ts,),
        )
        conn.commit()


def get_last_snapshot_time():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.commit()

        row = c.execute(
            "SELECT value FROM metadata WHERE key='last_snapshot'"
        ).fetchone()
        return row[0] if row else None

# ------------------ Formatting helpers ------------------
def fmt_commas(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return ""


def fmt_delta_commas(n):
    try:
        n = int(n)
        sign = "+" if n >= 0 else "-"
        return f"{sign}{abs(n):,}"
    except Exception:
        return ""
def rating_to_pct(r):
    """Convert rating_score (0-1 or 0-5) into a percent (0-100)."""
    try:
        r = float(r)
    except Exception:
        return None

    if r < 0:
        return None

    # heuristic: <=1 is already ratio; otherwise assume 0-5 stars
    if r <= 1.0:
        pct = r * 100.0
    else:
        pct = (r / 5.0) * 100.0

    # clamp
    pct = max(0.0, min(100.0, pct))
    return pct

# ------------------ Generic data helpers ------------------
def coerce_int(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    m = re.search(r"\d+", str(x))
    return int(m.group(0).replace(",", "")) if m else None


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
    for k in (
        "messageCount",
        "message_count",
        "messages",
        "interactions",
        "numMessages",
    ):
        if k in d and d[k] is not None:
            return coerce_int(d[k])
    for path in (
        ("stats", "messageCount"),
        ("stats", "messages"),
        ("usage", "messages"),
        ("metrics", "messages"),
        ("analytics", "messages"),
    ):
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
        if any(
            k in obj
            for k in ("name", "title", "characterName", "displayName", "botTitle")
        ):
            out.append(obj)
        for v in obj.values():
            flatten_items(v, out)
    elif isinstance(obj, list):
        for it in obj:
            flatten_items(it, out)

# ------------------ Auth & token management ------------------
def load_auth_credentials():
    """
    Returns:
        (bearer_token, guest_userid, refresh_token, expires_at, client_id)
    """
    if AUTH_FILE.exists():
        try:
            with open(AUTH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return (
                    data.get("bearer_token"),
                    data.get("guest_userid"),
                    data.get("refresh_token"),
                    data.get("expires_at"),
                    data.get("client_id"),
                )
        except Exception as e:
            logging.warning(f"Error loading auth credentials: {e}")

    return None, None, None, None, None


def save_auth_credentials(
    bearer_token,
    guest_userid,
    refresh_token=None,
    expires_at=None,
    client_id=None,
):
    try:
        data = {
            "bearer_token": bearer_token,
            "guest_userid": guest_userid,
            "refresh_token": refresh_token,
            "expires_at": expires_at,
            "client_id": client_id,
        }
        with open(AUTH_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
        safe_log(f"Saved auth credentials to {AUTH_FILE}")
    except Exception as e:
        logging.error(f"Error saving auth credentials: {e}")


KINDE_DOMAIN = "gamma.kinde.com"


def get_kinde_client_id():
    """
    Returns the Kinde client_id stored in auth_credentials.json.
    If missing, we cannot refresh tokens → require reauth.
    """
    if AUTH_FILE.exists():
        try:
            with open(AUTH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                cid = data.get("client_id")
                if cid:
                    return cid
        except Exception as e:
            safe_log(f"Error loading client_id: {e}")

    safe_log("No Kinde client_id found. Reauth is required.")
    return None


def refresh_kinde_token(refresh_token, client_id):
    if not client_id:
        safe_log("No client_id available, cannot refresh — reauth required.")
        return None

    url = f"https://{KINDE_DOMAIN}/oauth2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }

    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        safe_log(f"Kinde refresh failed: {e}")
        return None


def test_auth_credentials(bearer_token, guest_userid):
    if not bearer_token or not guest_userid:
        return False
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://spicychat.ai",
        "Referer": "https://spicychat.ai/my-chatbots",
        "x-app-id": "spicychat",
        "x-country": "US",
        "x-guest-userid": guest_userid,
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


def ensure_fresh_kinde_token():
    """
    Ensure we have a fresh access token; refresh via Kinde when possible.
    Returns (bearer_token, guest_userid) or (None, None) if auth is required.
    """
    global AUTH_REQUIRED

    bearer, guest, refresh_token, expires_at, client_id = load_auth_credentials()

    # Missing credentials
    if not bearer or not guest:
        AUTH_REQUIRED = True
        return None, None

    # No expiration known → treat as expired
    if not expires_at:
        AUTH_REQUIRED = True
        return None, None

    # Fresh enough
    if time.time() < (expires_at - 60):
        return bearer, guest

    # Try refreshing
    safe_log("Token expired — attempting Kinde refresh...")
    new_tokens = refresh_kinde_token(refresh_token, client_id)

    if not new_tokens or not new_tokens.get("access_token"):
        safe_log("Kinde refresh failed — auth required.")
        AUTH_REQUIRED = True
        return None, None

    new_bearer = new_tokens["access_token"]
    new_refresh = new_tokens.get("refresh_token", refresh_token)
    new_expires = time.time() + new_tokens.get("expires_in", 3600)

    save_auth_credentials(new_bearer, guest, new_refresh, new_expires)

    AUTH_REQUIRED = False
    safe_log("Kinde token refresh successful.")

    return new_bearer, guest

# ------------------ Capture auth via Playwright ------------------
def capture_auth_credentials(wait_rounds=18):
    """
    Manual auth capture:
    - Launch browser
    - User logs in (including Google)
    - Navigate to My Chatbots
    - Capture bearer + guest_userid from API calls
    - Capture refresh token & client_id from Kinde
    """

    bearer_token, guest_userid, refresh_token, expires_at, client_id = (
        load_auth_credentials()
    )

    # If credentials are valid, reuse them
    if test_auth_credentials(bearer_token, guest_userid):
        return bearer_token, guest_userid, refresh_token, expires_at, client_id

    safe_log("Launching Playwright for manual authentication…")

    bearer_token = None
    guest_userid = None
    discovered_client_id = None
    token_bundle = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context()
        page = ctx.new_page()

        def on_request(req):
            nonlocal bearer_token, guest_userid, discovered_client_id
            url = req.url

            # CLIENT ID (Kinde)
            if "/oauth2/auth" in url:
                parsed = urlparse.urlparse(url)
                q = urlparse.parse_qs(parsed.query)
                if "client_id" in q:
                    discovered_client_id = q["client_id"][0]
                    safe_log(f"Captured client_id = {discovered_client_id}")

            # ACCESS TOKEN + guest_userid from spicychat API calls
            path = urlparse.urlparse(url).path
            if "/v2/users/characters" in path:
                headers = req.headers
                if (
                    "authorization" in headers
                    and headers["authorization"].startswith("Bearer ")
                ):
                    bearer_token = headers["authorization"][7:]
                    safe_log("Captured bearer_token (API)")
                if "x-guest-userid" in headers:
                    guest_userid = headers["x-guest-userid"]
                    safe_log(f"Captured guest_userid = {guest_userid}")

        def on_response(res):
            nonlocal token_bundle
            try:
                if "/oauth2/token" in res.url:
                    data = res.json()
                    token_bundle = data
                    safe_log("Captured Kinde token bundle")
            except Exception:
                pass

        ctx.on("request", on_request)
        ctx.on("response", on_response)

        page.goto("https://spicychat.ai", wait_until="networkidle")
        print("Log into SpicyChat. After logging in, navigate to My Chatbots.")
        input("Press Enter when you are fully logged in and on My Chatbots...")

        for _ in range(wait_rounds):
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # ignore load state errors — page may already be loaded

            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                safe_log("Scroll skipped — page navigated during capture")
                continue

            time.sleep(0.3)


        ctx.close()
        browser.close()

    if not bearer_token or not guest_userid:
        raise RuntimeError("Failed to capture bearer_token or guest_userid")

    access_token = bearer_token
    refresh = token_bundle.get("refresh_token")
    expires = time.time() + token_bundle.get("expires_in", 3600)
    cid = discovered_client_id

    save_auth_credentials(access_token, guest_userid, refresh, expires, cid)

    return access_token, guest_userid, refresh, expires, cid

# ------------------ Database ------------------
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()

        # Main bots table
        c.execute(
            """
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
            """
        )

        #bots_tags table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_tags (
                bot_id TEXT PRIMARY KEY,
                tags_json TEXT,
                updated_at TEXT
            )
            """
        )

        # Back-compat: ensure avatar_url exists for very old DBs
        c.execute("PRAGMA table_info(bots)")
        columns = {row[1] for row in c.fetchall()}
        if "avatar_url" not in columns:
            c.execute("ALTER TABLE bots ADD COLUMN avatar_url TEXT")
            safe_log("Added missing avatar_url column to existing bots table")

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_rank_history (
                date TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                rank INTEGER,
                PRIMARY KEY (date, bot_id)
            )
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS top480_history (
                date TEXT PRIMARY KEY,
                count INTEGER
            )
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS top240_history (
                date TEXT PRIMARY KEY,
                count INTEGER
            )
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        
        # bot_ratings table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_ratings (
                bot_id TEXT PRIMARY KEY,
                rating_score REAL,
                updated_at TEXT
            )
            """
        )
        
        # bot_rating_history table (rating snapshots by date)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_rating_history (
                date TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                rating_score REAL,
                PRIMARY KEY (date, bot_id)
            )
            """
        )

        

        conn.commit()

    safe_log(f"Database initialized/verified at {DATABASE}")

def load_cached_rating_map(bot_ids=None):
    """Return {bot_id(str): rating_score(float|None)} from SQLite cache."""
    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    if bot_ids:
        ids = [str(x) for x in bot_ids]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"SELECT bot_id, rating_score FROM bot_ratings WHERE bot_id IN ({placeholders})",
            ids
        )
    else:
        cur.execute("SELECT bot_id, rating_score FROM bot_ratings")

    rows = cur.fetchall()
    conn.close()

    out = {}
    for bot_id, rating_score in rows:
        try:
            out[str(bot_id)] = float(rating_score) if rating_score is not None else None
        except Exception:
            out[str(bot_id)] = None
    return out


def save_cached_rating_map(rating_map):
    """Upsert {bot_id: rating_score} into SQLite cache."""
    if not rating_map:
        return

    init_db()
    now = datetime.now(tz=CDT).isoformat()
    rows = [(str(k), (float(v) if v is not None else None), now) for k, v in rating_map.items()]

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO bot_ratings (bot_id, rating_score, updated_at) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

# ------------------ Typesense client + trending ------------------
def get_typesense_tag_map():
    # Try unfiltered cache first
    ts_map = fetch_typesense_top_bots(
        max_pages=10, use_cache=True, filter_female_nsfw=False
    )

    # If cache isn't there yet, fetch once live to populate it
    if not ts_map:
        safe_log("Tags: unfiltered TS cache empty — fetching live once to build tag_map")
        ts_map = fetch_typesense_top_bots(
            max_pages=10, use_cache=False, filter_female_nsfw=False
        )

    tag_map = {}
    for cid, bot in (ts_map or {}).items():
        tags = bot.get("tags") or []
        if tags:
            tag_map[str(cid)] = tags

    safe_log(f"Tags: built tag_map for {len(tag_map)} bots")
    return tag_map


def multi_search_request(payload):
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

            safe_log(
                f"[Typesense] Attempt {attempt+1}: "
                f"Status {response.status_code}, "
                f"Content-Type: {response.headers.get('Content-Type', 'N/A')}"
            )
            safe_log(f"[Typesense] Response preview: {response.text[:200]}...")

            try:
                data = response.json()
                if isinstance(data, dict):
                    safe_log(
                        f"[Typesense] Success: dict with "
                        f"{len(data.get('results', []))} result sets"
                    )
                    return data
                else:
                    logging.error(
                        f"[Typesense] Non-dict JSON returned (type: {type(data)}); using empty fallback."
                    )
                    return {}
            except Exception as e:
                logging.error(
                    f"[Typesense] Invalid JSON response: {e}. "
                    f"Text: {response.text[:500]}"
                )
                return {}

        except requests.exceptions.HTTPError as e:
            logging.error(
                f"[Typesense] HTTP error (attempt {attempt+1}): {e}. "
                f"Status: {response.status_code if 'response' in locals() else 'N/A'}"
            )
            if getattr(e, "response", None) and e.response.status_code == 429:
                time.sleep(2**attempt)
            else:
                time.sleep(2)
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"[Typesense] Network/Request error (attempt {attempt+1}): {e}"
            )
            time.sleep(2)
        except Exception as e:
            logging.error(f"[Typesense] Unexpected error (attempt {attempt+1}): {e}")
            time.sleep(2)

    logging.error("[Typesense] All attempts failed → using empty fallback {}.")
    return {}

def fetch_typesense_ratings_for_bot_ids(bot_ids):
    """
    Fetch rating_score for specific bot IDs from Typesense.
    Returns: { "bot_id": float|None }
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return {}

    rating_map = {}

    CHUNK = 80
    for i in range(0, len(bot_ids), CHUNK):
        chunk = bot_ids[i:i+CHUNK]
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

def fetch_typesense_tags_for_bot_ids(bot_ids):
    """
    Fetch tags for specific bot IDs from Typesense, regardless of rank/top480.
    Returns: { "bot_id": ["tag1", "tag2", ...] }
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return {}

    tag_map = {}

    # Chunk to keep filter_by strings reasonable
    CHUNK = 80
    for i in range(0, len(bot_ids), CHUNK):
        chunk = bot_ids[i:i+CHUNK]

        # Typesense expects JSON-string array in filter_by for multi-value match
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
            tags = doc.get("tags") or []
            if cid:
                tag_map[cid] = tags

    safe_log(f"Tags: fetched tags for {len(tag_map)} / {len(bot_ids)} bot_ids from Typesense")
    return tag_map

def fetch_typesense_top_bots(
    max_pages=10, use_cache=True, filter_female_nsfw=True
):
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
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if isinstance(cached, list) and all(
                "character_id" in b for b in cached
            ):
                safe_log(
                    f"Loaded {len(cached)} bots from cache: {cache_file}"
                )
                return {b["character_id"]: b for b in cached}
            else:
                safe_log(
                    f"Cache invalid (not list or missing keys): {cache_file}"
                )
        except Exception as e:
            safe_log(f"Failed reading cached Typesense results: {e}")

    # ----- FULL FETCH -----
    safe_log("Fetching fresh Top Bots from Typesense...")
    ALL_RESULTS = []
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
            "searches": [
                {
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
                    "facet_by": (
                        "definition_size_category,group_size_category,tags,"
                        "translated_languages"
                    ),
                    "filter_by": filter_clause,
                    "max_facet_values": 100,
                    "page": page,
                    "per_page": per_page,
                }
            ]
        }

        result = multi_search_request(payload)

        if not isinstance(result, dict):
            logging.error(
                f"Typesense returned invalid result (type: {type(result)}) for page {page}. Skipping."
            )
            break

        results_page = result.get("results", [])
        if not results_page:
            safe_log(f"No results page for page {page} — stopping.")
            break

        hits = results_page[0].get("hits", []) if len(results_page) > 0 else []
        if not hits:
            safe_log(f"No hits for page {page} — stopping.")
            break

        safe_log(f"Page {page}: Fetched {len(hits)} hits")

        for obj in hits:
            doc = obj.get("document")
            if not isinstance(doc, dict):
                logging.warning(f"Invalid document in hit: {obj}")
                continue

            cid = doc.get("character_id")
            if not cid:
                logging.warning(f"Missing character_id in doc: {doc}")
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

        page += 1

        if len(hits) < per_page:
            safe_log(
                f"Partial page {page-1} ({len(hits)} < {per_page}) — assuming end of results."
            )
            break

    # ----- CACHE WRITE -----
    if ALL_RESULTS:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(ALL_RESULTS, f, indent=2)
            safe_log(
                f"Saved {len(ALL_RESULTS)} bots to Typesense cache: {cache_file}"
            )
        except Exception as e:
            safe_log(f"Failed writing Typesense cache: {e}")
    else:
        safe_log("No results to cache — using empty dict.")

    if not ALL_RESULTS:
        safe_log(
            "WARNING: fetch_typesense_top_bots() produced no results. Returning empty dict."
        )
        return {}

    return {b["character_id"]: b for b in ALL_RESULTS}


def save_rank_history_for_date(stamp, ts_map):
    """
    Save rank info for all bots (top 480) on a given date.
    """
    # Always fetch fresh filtered list for rank history
    try:
        ts_map = fetch_typesense_top_bots(
            max_pages=10, use_cache=True, filter_female_nsfw=True
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
        except Exception:
            continue

        if 1 <= rank <= 480:
            rows.append((stamp, cid, rank))

    cur.executemany(
        "INSERT OR REPLACE INTO bot_rank_history (date, bot_id, rank) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    safe_log(f"Saved {len(rows)} rank history rows for {stamp}")

def save_rating_history_for_date(stamp, rating_map):
    """
    Save rating_score for your bots on a given date.
    rating_map: { bot_id(str): rating_score(float|None) }
    """
    if not rating_map:
        return

    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    rows = []
    for bot_id, score in rating_map.items():
        try:
            score_val = float(score) if score is not None else None
        except Exception:
            score_val = None
        rows.append((stamp, str(bot_id), score_val))

    cur.executemany(
        "INSERT OR REPLACE INTO bot_rating_history (date, bot_id, rating_score) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

    safe_log(f"Saved {len(rows)} rating history rows for {stamp}")

# ------------------ API capture ------------------
def capture_payloads(bearer_token, guest_userid, max_retries=3, delay=5):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://spicychat.ai",
        "Referer": "https://spicychat.ai/my-chatbots",
        "x-app-id": "spicychat",
        "x-country": "US",
        "x-guest-userid": guest_userid,
    }

    for attempt in range(max_retries):
        try:
            safe_log(f"Attempt {attempt + 1}/{max_retries} to fetch {API_URL}")
            response = requests.get(API_URL, headers=headers, timeout=10)
            response.raise_for_status()
            logging.debug(f"Response status: {response.status_code}")
            logging.debug(f"Response headers: {response.headers}")
            logging.debug(
                f"Raw response (first 1000 chars): {response.text[:1000]}"
            )

            if not response.text.strip():
                logging.warning("Empty response received from API")
                return []

            try:
                data = response.json()
                bots = data.get("data", []) if isinstance(data, dict) else data
                safe_log(
                    f"Captured payload from {API_URL}: {len(bots)} items"
                )
                logging.debug(f"Payload content: {bots}")
                return [bots]
            except requests.exceptions.JSONDecodeError as e:
                logging.error(
                    f"Invalid JSON response from {API_URL}: {e}, "
                    f"response={response.text[:1000]}"
                )
                if response.text.startswith("<!DOCTYPE html") or "<html" in response.text:
                    logging.error(
                        "Received HTML response. Likely redirected to login page. "
                        "Check BEARER_TOKEN, GUEST_USERID, or API_URL."
                    )
                raise

        except requests.exceptions.HTTPError as e:
            logging.error(
                f"HTTP error fetching {API_URL}: {e}, "
                f"status={response.status_code}, text={response.text[:1000]}"
            )
            if response.status_code == 401:
                logging.error(
                    "Authentication failed. Verify BEARER_TOKEN or GUEST_USERID."
                )
                raise
            elif response.status_code == 403:
                logging.error(
                    "Access forbidden. Check token permissions or rate limits."
                )
            elif response.status_code == 429:
                logging.warning(
                    f"Rate limited on attempt {attempt + 1}. "
                    f"Retrying after {delay * (2 ** attempt)} seconds..."
                )
                time.sleep(delay * (2**attempt))
                continue
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error fetching {API_URL}: {e}")
            if attempt < max_retries - 1:
                logging.warning(
                    f"Retrying after {delay * (2 ** attempt)} seconds..."
                )
                time.sleep(delay * (2**attempt))
                continue
            raise

    logging.error(
        f"Failed to fetch {API_URL} after {max_retries} attempts"
    )
    raise RuntimeError(
        f"Failed to capture payloads after {max_retries} attempts"
    )

# ------------------ Snapshot logic ------------------
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
        AUTH_REQUIRED = True 
        safe_log("Snapshot aborted — auth required.")
        return DATABASE

    AUTH_REQUIRED = False

    # Capture payload
    try:
        payloads = capture_payloads(bearer_token, guest_userid)
    except RuntimeError as e:
        logging.warning(f"No payloads captured: {e}. Marking auth required.")
        AUTH_REQUIRED = True
        return DATABASE

    if not payloads:
        logging.warning("Snapshot: no payloads found.")
        return DATABASE

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
            logging.debug(
                f"Skipping item due to missing num_messages or bot_id: {d}"
            )
            continue

        created_at = get_created_at(d)
        if created_at:
            created_at = (
                pd.Timestamp(created_at, tz="UTC")
                .tz_convert(CDT)
                .isoformat()
            )

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
            logging.debug(f"Skipping duplicate bot_id: {bot_id}")
            continue

        seen.add(bot_id)
        rows.append(row)

    rows_clean = sanitize_rows(rows)
    logging.debug(f"Sanitized rows: {len(rows_clean)}")

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
        safe_log(
            f"Snapshot saved for {len(rows_clean)} bots to {DATABASE}"
        )

    # Refresh Typesense trending cache (top 480)
    try:
        safe_log("Refreshing Typesense trending cache (top 480 bots)")
        ts_map = fetch_typesense_top_bots(
            max_pages=10, use_cache=False, filter_female_nsfw=True
        )
        if not isinstance(ts_map, dict):
            safe_log("Typesense refresh failed — got invalid ts_map")
            ts_map = {}
        safe_log(
            f"Typesense trending cache updated successfully ({len(ts_map)} entries)"
        )
    except Exception as e:
        logging.error(f"Failed to refresh Typesense trending cache: {e}")
        ts_map = {}

    # Save rank history
    try:
        save_rank_history_for_date(stamp, ts_map)
    except Exception as e:
        logging.error(f"Error saving rank history: {e}")

    # Save Top-240 & Top-480 history for your bots
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
            except Exception:
                continue

            if rank < 1:
                continue

            if rank <= 480:
                count_top480 += 1
                if rank <= 240:
                    count_top240 += 1

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT OR REPLACE INTO top480_history (date, count)
            VALUES (?, ?)
            """,
            (stamp, count_top480),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO top240_history (date, count)
            VALUES (?, ?)
            """,
            (stamp, count_top240),
        )

        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error saving top-level histories: {e}")

    # ----------------------------
    # Cache tags for "My Chatbots"
    # ----------------------------
    try:
        your_ids = [str(r["bot_id"]) for r in rows_clean]
        tag_map = fetch_typesense_tags_for_bot_ids(your_ids)  # your existing function
        save_cached_tag_map(tag_map)
        safe_log(f"Cached tags for {len(tag_map)} bots (My Chatbots)")
    except Exception as e:
        safe_log(f"Tag caching failed: {e}")

    LAST_SNAPSHOT_DATE = snapshot_time.isoformat()
    safe_log(f"Snapshot complete at {LAST_SNAPSHOT_DATE}")


    # ----------------------------
    # Cache ratings for "My Chatbots"
    # ----------------------------
    try:
        your_ids = [str(r["bot_id"]) for r in rows_clean]
        rating_map = fetch_typesense_ratings_for_bot_ids(your_ids)
        save_cached_rating_map(rating_map)
        save_rating_history_for_date(stamp, rating_map)
        safe_log(f"Cached ratings for {len(rating_map)} bots (My Chatbots)")
    except Exception as e:
        safe_log(f"Rating caching failed: {e}")

    return DATABASE
# ------------------ Load + compute deltas ------------------
def load_history_df() -> pd.DataFrame:
    """
    Load all rows from bots table and normalize:
    - date as date
    - num_messages as int
    - created_at as timezone-aware CDT
    """
    if not DATABASE.exists():
        safe_log(
            f"No database found at {DATABASE}. Returning empty DataFrame."
        )
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

    # Ensure all expected columns exist
    for col in ALLOWED_FIELDS:
        if col not in df.columns:
            df[col] = ""
            logging.warning(
                f"Missing column {col} in database, filled with empty values"
            )

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    invalid_dates = df["date"].isna().sum()
    if invalid_dates > 0:
        logging.warning(
            f"Dropping {invalid_dates} rows with invalid/missing date"
        )
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
        df["created_at"] = pd.to_datetime(
            df["created_at"], utc=True, errors="coerce"
        ).dt.tz_convert(CDT)
    else:
        df["created_at"] = pd.NaT

    safe_log(f"Loaded {len(df)} rows from database: {list(df.columns)}")

    valid_dates = [d for d in df["date"].unique() if d is not None]
    logging.debug(f"Available dates: {sorted(valid_dates)}")

    return df


def compute_deltas(df_raw: pd.DataFrame, timeframe="All") -> pd.DataFrame:
    """
    Compute daily deltas for each bot and apply timeframe filter.
    """
    if df_raw.empty:
        safe_log("No data to compute deltas.")
        return pd.DataFrame(
            columns=[
                "date",
                "bot_id",
                "bot_name",
                "bot_title",
                "num_messages",
                "daily_messages",
                "created_at",
                "avatar_url",
            ]
        )

    df = df_raw.sort_values(["bot_id", "date"])
    df["daily_messages"] = (
        df.groupby("bot_id")["num_messages"].diff().fillna(0).astype(int)
    )

    decreases = df[df["daily_messages"] < 0].copy()
    if not decreases.empty:
        logging.warning("Detected decreases in total message counts:")
        for _, row in decreases.iterrows():
            logging.warning(
                f"Bot ID: {row['bot_id']}, Name: {row['bot_name']}, "
                f"Date: {row['date']}, Messages: {row['num_messages']}, "
                f"Delta: {row['daily_messages']}"
            )
        logging.warning(
            "Please review the code and data source for inconsistencies."
        )

    df.loc[df["daily_messages"] < 0, "daily_messages"] = 0
    logging.debug(
        "Computed DataFrame with deltas: "
        f"{df[['bot_id', 'date', 'num_messages', 'daily_messages']].to_string()}"
    )

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
        # Adjust delta for first day of month
        for bot_id in df["bot_id"].unique():
            bot_data = df[df["bot_id"] == bot_id]
            first_day = bot_data["date"].min()
            if first_day == current_month_start and not bot_data.empty:
                prev_month_end = current_month_start - timedelta(days=1)
                prev_data = df_raw[
                    (df_raw["bot_id"] == bot_id)
                    & (df_raw["date"] <= prev_month_end)
                ]
                prev_num = (
                    prev_data["num_messages"].max()
                    if not prev_data.empty
                    else 0
                )
                first_day_idx = bot_data[bot_data["date"] == first_day].index[0]
                df.loc[first_day_idx, "daily_messages"] = int(
                    bot_data.iloc[0]["num_messages"] - prev_num
                )
    # timeframe "All" uses everything

    logging.debug(
        f"DataFrame after timeframe filter ({timeframe}): "
        f"{df[['bot_id', 'date', 'num_messages', 'daily_messages']].to_string()}"
    )
    if df.empty:
        logging.warning(
            f"No data available after applying timeframe filter: {timeframe}"
        )

    return df

# ------------------ Dashboard bots data ------------------
def get_bots_data(timeframe="All", sort_by="delta", sort_asc=False, created_after="All", tags="", q=""):

    """
    Load snapshot history from DB, compute deltas for the given timeframe,
    and build the list of bots for the dashboard.

    Ranks are loaded from bot_rank_history for the latest date in the
    timeframe – no direct Typesense calls here anymore.
    """
    df_raw = load_history_df()
    dfc = compute_deltas(df_raw, timeframe)

    if dfc.empty:
        return [], [], 0, None

    # Optional: filter by created_after
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

    latest_date = sorted(dfc["date"].unique())[-1]
    today = dfc[dfc["date"] == latest_date].copy()
    # Build tag_map for *your* bots (not just top480)
    your_ids = [str(x) for x in today["bot_id"].tolist()]
    tag_map = load_cached_tag_map(today["bot_id"].tolist())
    rating_map = load_cached_rating_map(today["bot_id"].tolist())



    # Totals history for the totals table (the big chart uses /api/totals)
    totals_df = (
        dfc.groupby("date", as_index=False)
        .agg({"num_messages": "sum", "daily_messages": "sum"})
        .sort_values("date", ascending=False)
    )

    totals = []
    for _, row in totals_df.iterrows():
        totals.append(
            {
                "date": str(row["date"]),
                "total": int(row["num_messages"]),
                "total_fmt": fmt_commas(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "daily_fmt": fmt_delta_commas(row["daily_messages"]),
            }
        )

    # Load ranks for this date from bot_rank_history
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "SELECT bot_id, rank FROM bot_rank_history WHERE date = ?",
        (str(latest_date),),
    )
    rank_rows = cur.fetchall()
    conn.close()

    rank_by_bot = {str(bot_id): (rank or 0) for (bot_id, rank) in rank_rows}

    bots = []
    for _, row in today.iterrows():
        bot_id = str(row["bot_id"])
        total = int(row["num_messages"])
        delta = int(row.get("daily_messages", 0))

        avatar_raw = row.get("avatar_url") or ""
        if avatar_raw:
            filename = str(avatar_raw).split("/")[-1]
            avatar_url = f"{AVATAR_BASE_URL}/{filename}"
        else:
            avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

        created_at_str = ""
        if pd.notnull(row.get("created_at")):
            try:
                created_at_str = row["created_at"].strftime(
                    "%Y-%m-%d %H:%M:%S %Z"
                )
            except Exception:
                created_at_str = str(row["created_at"])

        r = rank_by_bot.get(bot_id, 0)
        if r and 1 <= r <= 480:
            rank_val = r
            rank_tier = "top240" if r <= 240 else "top480"
        else:
            rank_val = None
            rank_tier = None

        bots.append(
            {
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
                "tags": tag_map.get(str(bot_id), []),
                "rating": rating_map.get(str(bot_id), None),
                "rating_pct": rating_to_pct(rating_map.get(str(bot_id), None)),

            }
        )
        
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
            tags_list = bot.get("tags") or []
            tags_blob = " ".join([str(t).lower() for t in tags_list])
            return (q_norm in name) or (q_norm in title) or (q_norm in tags_blob)

        bots = [b for b in bots if matches_search(b)]
        
    # Sorting
    reverse = not sort_asc
    if sort_by == "name":
        bots.sort(key=lambda b: b["name"].lower(), reverse=reverse)
    elif sort_by == "total":
        bots.sort(key=lambda b: b["total"], reverse=reverse)
    elif sort_by == "created_at":
        bots.sort(key=lambda b: b["created_at"] or "", reverse=reverse)
    else:
        bots.sort(key=lambda b: b["delta"], reverse=reverse)

    total_messages = (
        int(totals_df["num_messages"].iloc[0]) if not totals_df.empty else 0
    )

    return bots, totals, total_messages, latest_date

def load_cached_tag_map(bot_ids=None):
    """Return {bot_id(str): [tags]} from SQLite cache."""
    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    if bot_ids:
        ids = [str(x) for x in bot_ids]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"SELECT bot_id, tags_json FROM bot_tags WHERE bot_id IN ({placeholders})",
            ids
        )
    else:
        cur.execute("SELECT bot_id, tags_json FROM bot_tags")

    rows = cur.fetchall()
    conn.close()

    out = {}
    for bot_id, tags_json in rows:
        try:
            out[str(bot_id)] = json.loads(tags_json) if tags_json else []
        except Exception:
            out[str(bot_id)] = []
    return out


def save_cached_tag_map(tag_map):
    """Upsert {bot_id: [tags]} into SQLite cache."""
    if not tag_map:
        return

    init_db()
    now = datetime.now(tz=CDT).isoformat()
    rows = [(str(k), json.dumps(v), now) for k, v in tag_map.items()]

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO bot_tags (bot_id, tags_json, updated_at) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

# ------------------ Snapshot scheduler ------------------
def snapshot_scheduler():
    global AUTH_REQUIRED
    
    """
    Runs every hour.
    Attempts to refresh Kinde access token before each snapshot.
    Pauses automatically if auth fails (AUTH_REQUIRED=True).
    Resumes as soon as auth is restored by user reauth.
    """
    safe_log("Snapshot scheduler started (1-hour interval).")

    while True:
        try:
            bearer, guest = ensure_fresh_kinde_token()

            if not bearer or not guest:
                AUTH_REQUIRED = True
                safe_log(
                    "Scheduler paused — auth invalid. Waiting for user reauth."
                )
            else:
                AUTH_REQUIRED = False
                safe_log("Scheduler: auth OK — running hourly snapshot.")
                take_snapshot({})
        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        time.sleep(3600)
