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

