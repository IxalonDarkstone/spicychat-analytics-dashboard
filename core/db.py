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
from .config import *
from .logging_utils import safe_log


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
        # tracked_authors table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tracked_authors (
                author TEXT PRIMARY KEY,
                added_at TEXT
            )
            """
        )

        # author_bots table (bots for tracked authors, by snapshot date)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS author_bots (
                date TEXT NOT NULL,
                author TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                bot_name TEXT,
                bot_title TEXT,
                tags_json TEXT,
                avatar_url TEXT,
                created_at TEXT,
                PRIMARY KEY (date, author, bot_id)
            )
            """
        )

        # Back-compat: ensure avatar_url exists for author_bots
        c.execute("PRAGMA table_info(author_bots)")
        cols = [r[1] for r in c.fetchall()]
        if "avatar_url" not in cols:
            c.execute("ALTER TABLE author_bots ADD COLUMN avatar_url TEXT")
            safe_log("Added missing avatar_url column to existing author_bots table")
            
        c.execute("PRAGMA table_info(author_bots)")
        cols = [r[1] for r in c.fetchall()]
        if "created_at" not in cols:
            c.execute("ALTER TABLE author_bots ADD COLUMN created_at TEXT")
            safe_log("Added missing created_at column to existing author_bots table")

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
    """
    Returns dict: { bot_id: rating_score (float|None) }

    If bot_ids is provided (list), only returns those ids.
    """
    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_ratings_cache (
            bot_id TEXT PRIMARY KEY,
            rating_score REAL,
            updated_at TEXT
        )
    """)

    if bot_ids:
        ids = [str(x) for x in bot_ids if x]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"SELECT bot_id, rating_score FROM bot_ratings_cache WHERE bot_id IN ({placeholders})",
            ids
        )
    else:
        cur.execute("SELECT bot_id, rating_score FROM bot_ratings_cache")

    rows = cur.fetchall()
    conn.close()

    out = {}
    for bot_id, score in rows:
        try:
            out[str(bot_id)] = float(score) if score is not None else None
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
    
def save_rank_history_for_date(stamp: str, ts_map: dict):
    """
    Save rank history for one date.
    Expects ts_map as { bot_id: {..., 'rank': int, ...} } OR empty dict.
    Writes to bot_rank_history(date, bot_id, rank).
    """
    if not stamp:
        return

    rows = []
    for bot_id, info in (ts_map or {}).items():
        try:
            rank = int(info.get("rank")) if info and info.get("rank") is not None else None
        except Exception:
            rank = None
        if not bot_id or rank is None:
            continue
        rows.append((stamp, str(bot_id), rank))

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_rank_history (
            date TEXT NOT NULL,
            bot_id TEXT NOT NULL,
            rank INTEGER,
            PRIMARY KEY (date, bot_id)
        )
        """
    )

    # Replace ranks for that day (clean write)
    cur.execute("DELETE FROM bot_rank_history WHERE date = ?", (stamp,))
    if rows:
        cur.executemany(
            "INSERT OR REPLACE INTO bot_rank_history (date, bot_id, rank) VALUES (?, ?, ?)",
            rows
        )

    conn.commit()
    conn.close()
    safe_log(f"Rank history: saved {len(rows)} ranks for {stamp}")


def save_rating_history_for_date(stamp: str, rating_map: dict):
    """
    Save rating history for one date.
    rating_map: { bot_id: float|None }
    Writes to bot_rating_history(date, bot_id, rating_score).
    """
    if not stamp:
        return

    rows = []
    for bot_id, score in (rating_map or {}).items():
        if not bot_id:
            continue
        try:
            score_f = float(score) if score is not None else None
        except Exception:
            score_f = None
        rows.append((stamp, str(bot_id), score_f))

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_rating_history (
            date TEXT NOT NULL,
            bot_id TEXT NOT NULL,
            rating_score REAL,
            PRIMARY KEY (date, bot_id)
        )
        """
    )

    # Replace ratings for that day (clean write)
    cur.execute("DELETE FROM bot_rating_history WHERE date = ?", (stamp,))
    if rows:
        cur.executemany(
            "INSERT OR REPLACE INTO bot_rating_history (date, bot_id, rating_score) VALUES (?, ?, ?)",
            rows
        )

    conn.commit()
    conn.close()
    safe_log(f"Rating history: saved {len(rows)} ratings for {stamp}")


def get_latest_rank_map():
    """
    Returns rank map for the most recent date in bot_rank_history:
      { bot_id: rank }
    """
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date
        FROM bot_rank_history
        ORDER BY date DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return {}

    latest = row[0]
    cur.execute(
        "SELECT bot_id, rank FROM bot_rank_history WHERE date = ?",
        (latest,)
    )
    rows = cur.fetchall()
    conn.close()
    return {str(bot_id): int(rank) for bot_id, rank in rows if bot_id and rank is not None}

def load_cached_tag_map(bot_ids=None):
    """
    Returns dict: { bot_id: [tag1, tag2, ...] }

    If bot_ids is provided (list), only returns those ids.
    """
    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # Ensure table exists (keep your existing table name if different)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_tags_cache (
            bot_id TEXT PRIMARY KEY,
            tags_json TEXT,
            updated_at TEXT
        )
    """)

    if bot_ids:
        ids = [str(x) for x in bot_ids if x]
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"SELECT bot_id, tags_json FROM bot_tags_cache WHERE bot_id IN ({placeholders})",
            ids
        )
    else:
        cur.execute("SELECT bot_id, tags_json FROM bot_tags_cache")

    rows = cur.fetchall()
    conn.close()

    out = {}
    for bot_id, tags_json in rows:
        try:
            out[str(bot_id)] = json.loads(tags_json) if tags_json else []
        except Exception:
            out[str(bot_id)] = []
    return out



def save_cached_tag_map(tag_map: dict):
    """
    tag_map: { bot_id: [tag1, tag2, ...] }
    """
    if not tag_map:
        return

    init_db()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_tags_cache (
            bot_id TEXT PRIMARY KEY,
            tags_json TEXT,
            updated_at TEXT
        )
    """)

    now = datetime.now(tz=CDT).isoformat()
    rows = []
    for bot_id, tags in tag_map.items():
        rows.append((str(bot_id), json.dumps(tags or []), now))

    cur.executemany(
        "INSERT OR REPLACE INTO bot_tags_cache (bot_id, tags_json, updated_at) VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()