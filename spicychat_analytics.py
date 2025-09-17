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
from flask import Flask, render_template, request, redirect, url_for
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

ALLOWED_FIELDS = [
    "date", "bot_id", "bot_name", "bot_title",
    "num_messages", "creator_user_id", "created_at"
]

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
            logging.warning(f"Failed to reconfigure stdout to UTF-8: {e}")

def safe_log(message):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode('ascii', errors='replace').decode('ascii'))

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

# ------------------ Utils ------------------
NUM_RE = re.compile(r"\d[\d,]*")

def coerce_int(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    m = NUM_RE.search(str(x))
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

def get_name(d): return pick(d, "name", "characterName", "displayName", default="")
def get_title(d): return pick(d, "title", "botTitle", "description", default="")
def get_id(d): return pick(d, "id", "slug", "uuid", "characterId", "_id", default="")
def get_created_at(d): return pick(d, "createdAt", "created_at", default="")

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

# ------------------ Database ------------------
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS bots (
                date TEXT,
                bot_id TEXT,
                bot_name TEXT,
                bot_title TEXT,
                num_messages INTEGER,
                creator_user_id TEXT,
                created_at TEXT,
                PRIMARY KEY (date, bot_id)
            )
        """)
        conn.commit()

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

# ------------------ Snapshot ------------------
def sanitize_rows(rows):
    return [{k: r.get(k, "") for k in ALLOWED_FIELDS} for r in rows]

def take_snapshot(verbose=True):
    ensure_dirs()
    init_db()
    stamp = datetime.now().strftime("%Y-%m-%d")
    safe_log(f"Starting snapshot for {stamp}")

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
        row = {
            "date": stamp,
            "bot_id": bot_id,
            "bot_name": get_name(d),
            "bot_title": get_title(d),
            "num_messages": num,
            "creator_user_id": str(d.get("creator_user_id") or ""),
            "created_at": get_created_at(d)
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
                INSERT INTO bots (date, bot_id, bot_name, bot_title, num_messages, creator_user_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row["date"], row["bot_id"], row["bot_name"],
                row["bot_title"], row["num_messages"], row["creator_user_id"],
                row["created_at"]
            ))
        conn.commit()

    if verbose:
        safe_log(f"Snapshot saved for {len(rows_clean)} bots to {DATABASE}")
    
    # Generate all charts in a single pass
    updated_bot_ids = [row["bot_id"] for row in rows_clean]
    build_charts(verbose=True, timeframes=["7day", "30day", "All"], updated_bot_ids=updated_bot_ids)
    
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
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    safe_log(f"Loaded {len(df)} rows from database: {list(df.columns)}")
    logging.debug(f"DataFrame head: {df.head().to_string()}")
    logging.debug(f"Available dates: {sorted(df['date'].unique())}")
    return df

def compute_deltas(df_raw: pd.DataFrame, timeframe="7day") -> pd.DataFrame:
    if df_raw.empty:
        safe_log("No data to compute deltas.")
        return pd.DataFrame(columns=["date", "bot_id", "bot_name", "bot_title", "num_messages", "daily_messages", "created_at"])

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

    # Apply timeframe filter after computing deltas
    today = datetime.now().date()
    if timeframe == "7day":
        start_date = today - timedelta(days=7)
        df = df[df["date"] >= start_date]
    elif timeframe == "30day":
        start_date = today - timedelta(days=30)
        df = df[df["date"] >= start_date]
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
    if pd.Series(dates).empty or pd.Series(values).empty or len(dates) != len(values):
        logging.error(f"Invalid data for plot {out_png}: {len(dates)} dates, {len(values)} values")
        return
    if len(dates) == 1:
        # Create a minimal plot for a single data point
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
def build_charts(verbose=True, timeframes=["7day", "30day", "All"], updated_bot_ids=None):
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

        total_chart = CHARTS_DIR / f"total_messages_{timeframe}.png"
        delta_chart = CHARTS_DIR / f"daily_changes_{timeframe}.png"
        if not total_chart.exists() or not delta_chart.exists() or (updated_bot_ids and df_raw[df_raw["bot_id"].isin(updated_bot_ids)]["date"].max() == datetime.now().date()):
            if not totals.empty and not totals["date"].empty and not totals["num_messages"].empty:
                safe_log(f"Generating global charts with {len(totals)} data points for timeframe: {timeframe}")
                plot_line(totals["date"], totals["num_messages"],
                          f"Total Messages ({timeframe})", total_chart)
                plot_line(totals["date"], totals["daily_messages"],
                          f"Daily Message Changes ({timeframe})", delta_chart, ylabel="Message Changes")
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
        sort_by = request.args.get("sort_by", "delta")
        sort_asc = request.args.get("sort_asc", "false") == "true"
        created_after = request.args.get("created_after", None)
        timeframe = request.args.get("timeframe", "7day")  # Default to 7 days
        safe_log(f"Index route: sort_by={sort_by}, sort_asc={sort_asc}, created_after={created_after}, timeframe={timeframe}")

        last_7_days = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        last_30_days = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

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
                created_after=created_after,
                last_7_days=last_7_days,
                last_30_days=last_30_days,
                timeframe=timeframe
            )

        dfc = compute_deltas(df_raw, timeframe)
        all_dates = sorted(dfc["date"].unique(), reverse=True)
        today_date = datetime.now().date().strftime("%Y-%m-%d")
        latest = all_dates[0] if all_dates else today_date
        if str(latest) != today_date:
            logging.warning(f"Latest date {latest} is not today ({today_date}). Forcing snapshot.")
            try:
                take_snapshot()
                df_raw = load_history_df()
                dfc = compute_deltas(df_raw, timeframe)
                all_dates = sorted(dfc["date"].unique(), reverse=True)
                latest = all_dates[0] if all_dates else today_date
            except Exception as e:
                logging.error(f"Error forcing snapshot: {e}")

        totals = dfc.groupby("date", as_index=False).agg({"num_messages": "sum", "daily_messages": "sum"})
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

        today = dfc[dfc["date"] == latest].copy() if not dfc.empty else pd.DataFrame(columns=dfc.columns)
        prev = all_dates[1] if len(all_dates) >= 2 else None
        safe_log(f"Selected latest date: {latest}, previous date: {prev}")
        if prev is not None:
            prev_df = dfc[dfc["date"] == prev][["bot_id", "num_messages"]].rename(columns={"num_messages": "prev_num"})
            today = today.merge(prev_df, on="bot_id", how="left")
            today["prev_num"] = pd.to_numeric(today["prev_num"], errors="coerce").fillna(0).astype(int)
            today["delta"] = (today["num_messages"] - today["prev_num"]).astype(int)
        else:
            today["delta"] = 0
        logging.debug(f"Today's data: {today[['bot_id', 'num_messages', 'delta']].to_string()}")

        if created_after:
            try:
                created_after_date = pd.to_datetime(created_after).date()
                today = today[today["created_at"].dt.date >= created_after_date]
                safe_log(f"Filtered bots created after {created_after_date}, {len(today)} bots remain")
            except Exception as e:
                logging.warning(f"Invalid created_after date {created_after}: {e}")

        if sort_by == "total":
            today = today.sort_values("num_messages", ascending=sort_asc)
        elif sort_by == "delta":
            today = today.sort_values(["delta", "num_messages"], ascending=[sort_asc, sort_asc])
        elif sort_by == "name":
            today = today.sort_values("bot_name", ascending=sort_asc, key=lambda x: x.str.lower())
        elif sort_by == "created_at":
            today = today.sort_values("created_at", ascending=sort_asc)

        bots = [
            {
                "bot_id": row["bot_id"],
                "name": row["bot_name"],
                "title": row["bot_title"],
                "total": int(row["num_messages"]),
                "total_fmt": fmt_commas(row["num_messages"]),
                "delta": int(row.get("delta", 0)),
                "delta_fmt": fmt_delta_commas(row.get("delta", 0)),
                "created_at": row["created_at"].strftime("%Y-%m-%d") if pd.notnull(row["created_at"]) else "",
                "link": f"https://spicychat.ai/chat/{row['bot_id']}"
            } for _, row in today.iterrows()
        ]
        logging.debug(f"Bots data for rendering: {len(bots)} bots")

        try:
            safe_log(f"Rendering index for {latest} with {len(bots)} bots")
            return render_template(
                "index.html",
                latest=str(latest),
                total_messages=fmt_commas(total_messages),
                total_bots=fmt_commas(total_bots),
                totals=totals_data,
                bots=bots,
                sort_by=sort_by,
                sort_asc=sort_asc,
                created_after=created_after,
                last_7_days=last_7_days,
                last_30_days=last_30_days,
                timeframe=timeframe
            )
        except Exception as e:
            logging.error(f"Error rendering index template: {e}")
            raise

    @app.route("/bots")
    def bots_table():
        sort_by = request.args.get("sort_by", "delta")
        sort_asc = request.args.get("sort_asc", "false") == "true"
        created_after = request.args.get("created_after", None)
        timeframe = request.args.get("timeframe", "7day")
        safe_log(f"Bots table route: sort_by={sort_by}, sort_asc={sort_asc}, created_after={created_after}, timeframe={timeframe}")

        bots, _, _ = get_bots_data(sort_by, sort_asc, created_after, timeframe)
        try:
            return render_template("bots_table.html", bots=bots, sort_by=sort_by, sort_asc=sort_asc, created_after=created_after, timeframe=timeframe)
        except Exception as e:
            logging.error(f"Error rendering bots_table template: {e}")
            raise

    @app.route("/bot/<bot_id>")
    def bot_detail(bot_id):
        timeframe = "All"  # Always use "All" for bot detail pages
        df_raw = load_history_df()
        dfc = compute_deltas(df_raw, timeframe)

        bot = dfc[dfc["bot_id"] == bot_id].sort_values("date")
        if bot.empty:
            logging.warning(f"Bot {bot_id} not found in database")
            return render_template("bot.html", bot=None, history=[], timeframe=timeframe)

        # Compute delta for the latest row if not present
        row = bot.iloc[-1].copy()
        if "delta" not in bot.columns:
            all_dates = sorted(df_raw[df_raw["bot_id"] == bot_id]["date"].unique(), reverse=True)
            if len(all_dates) >= 2:
                prev_date = all_dates[1]
                prev_row = df_raw[(df_raw["bot_id"] == bot_id) & (df_raw["date"] == prev_date)]
                prev_num = int(prev_row["num_messages"].iloc[0]) if not prev_row.empty else 0
                row["delta"] = int(row["num_messages"] - prev_num)
            else:
                row["delta"] = 0

        bot_data = {
            "bot_id": row["bot_id"],
            "name": row["bot_name"],
            "title": row["bot_title"],
            "total": int(row["num_messages"]),
            "total_fmt": fmt_commas(row["num_messages"]),
            "delta": int(row.get("delta", 0)),
            "delta_fmt": fmt_delta_commas(row.get("delta", 0)),
            "created_at": row["created_at"].strftime("%Y-%m-%d") if pd.notnull(row["created_at"]) else ""
        }

        history = [
            {
                "date": str(row["date"]),
                "total": int(row["num_messages"]),
                "total_fmt": fmt_commas(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "daily_fmt": fmt_delta_commas(row["daily_messages"]),
                "created_at": row["created_at"].strftime("%Y-%m-%d") if pd.notnull(row["created_at"]) else ""
            } for _, row in bot.iterrows()
        ]

        safe_log(f"Rendering bot detail for {bot_id}: {bot_data['name']}")
        return render_template("bot.html", bot=bot_data, history=history, timeframe=timeframe)

    @app.route("/take-snapshot", methods=["POST"])
    def take_snapshot_route():
        safe_log("Received request to take snapshot")
        try:
            take_snapshot(verbose=True)
            safe_log("Snapshot and charts updated successfully")
            sort_by = request.args.get("sort_by", "delta")
            sort_asc = request.args.get("sort_asc", "false")
            created_after = request.args.get("created_after", None)
            timeframe = request.args.get("timeframe", "7day")
            params = {"sort_by": sort_by, "sort_asc": sort_asc, "timeframe": timeframe}
            if created_after:
                params["created_after"] = created_after
            return redirect(url_for("index", **params))
        except Exception as e:
            logging.error(f"Error in take_snapshot_route: {e}")
            raise

    _routes_defined = True

# ------------------ CLI ------------------
def main():
    setup_logging()
    p = argparse.ArgumentParser(description="SpicyChat analytics with SQLite database and Flask dashboard")
    p.add_argument("--no-snapshot", action="store_true", help="Skip snapshot")
    p.add_argument("--no-charts", action="store_true", help="Skip chart generation")
    p.add_argument("--port", type=int, default=5000, help="Flask server port (default 5000)")
    args = p.parse_args()

    define_routes()  # Define routes only once

    if not args.no_snapshot:
        try:
            take_snapshot()
        except RuntimeError as e:
            logging.error(f"Snapshot failed: {e}. Continuing to start server.")
    if not args.no_charts:
        build_charts(timeframes=["7day", "30day", "All"])  # Generate charts for all timeframes

    safe_log(f"Starting Flask server on http://localhost:{args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False)

if __name__ == "__main__":
    main()