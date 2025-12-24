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
from .fs_utils import ensure_dirs

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
