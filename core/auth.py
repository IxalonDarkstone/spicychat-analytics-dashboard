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
import core

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
    Legacy name kept for compatibility.

    We do NOT use Kinde anymore.
    We only ensure we have a usable SpicyChat bearer token + guest_userid.
    Returns (bearer_token, guest_userid) or (None, None) if auth is required.
    """
    global AUTH_REQUIRED

    bearer, guest, _refresh, _expires, _client_id = load_auth_credentials()

    if not bearer or not guest:
        AUTH_REQUIRED = True
        return None, None

    # Optional: validate right here so scheduler/snapshot can pause cleanly
    if not test_auth_credentials(bearer, guest):
        AUTH_REQUIRED = True
        return None, None

    AUTH_REQUIRED = False
    return bearer, guest

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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(no_viewport=True)
        OVERLAY_JS = r"""
(() => {
function ensureOverlay() {
    if (document.getElementById("sa-auth-overlay")) return;

    const box = document.createElement("div");
    box.id = "sa-auth-overlay";
    box.style.cssText = `
    position: fixed;
    top: 14px;
    left: 14px;
    z-index: 2147483647;
    width: 420px;
    background: rgba(18,18,20,0.96);
    color: #eee;
    border: 1px solid rgba(74,168,255,0.65);
    border-radius: 10px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.45);
    padding: 12px 14px;
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    font-size: 14px;
    line-height: 1.35;
    `;

    box.innerHTML = `
    <div style="display:flex; align-items:center; justify-content:space-between; gap:10px;">
        <div style="font-weight:800; color:#4aa8ff;">🔐 SpicyChat Auth Capture</div>
        <button id="sa-auth-overlay-hide" style="
        background:#4aa8ff; color:#07121f; border:none;
        padding:6px 10px; border-radius:8px; cursor:pointer;
        font-weight:700;
        ">Hide</button>
    </div>

    <div style="margin-top:8px; opacity:0.95;">
        <ol style="margin:8px 0 0 18px; padding:0;">
        <li>Log in (email + code) if prompted.</li>
        <li>Navigate to <b>My Creations</b> / <b>My Chatbots</b>.</li>
        <li>Leave this window open until the app reports success.</li>
        </ol>
    </div>

    <div id="sa-auth-overlay-status" style="margin-top:10px; font-size:13px; opacity:0.85;">
        Tip: if this window opened behind apps, press <b>Alt+Tab</b> and select it.
    </div>
    `;

    document.documentElement.appendChild(box);

    const hideBtn = document.getElementById("sa-auth-overlay-hide");
    if (hideBtn) {
    hideBtn.addEventListener("click", () => {
        box.style.display = "none";
    });
    }
}

// Ensure on initial load + SPAs that swap content.
ensureOverlay();
const obs = new MutationObserver(() => ensureOverlay());
obs.observe(document.documentElement, { childList: true, subtree: true });
})();
"""

        # In capture_auth_credentials():
        page = ctx.new_page()
        page.add_init_script(OVERLAY_JS)

        def on_request(req):
            nonlocal bearer_token, guest_userid
            url = req.url

            
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

    save_auth_credentials(access_token, guest_userid)
    return access_token, guest_userid, None, None, None
