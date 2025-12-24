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
