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
