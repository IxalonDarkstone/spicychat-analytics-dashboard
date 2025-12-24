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
BASE_DIR = Path(__file__).resolve().parents[1]
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
