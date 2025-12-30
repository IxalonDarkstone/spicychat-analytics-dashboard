import json
import logging
import time
import urllib.parse as urlparse
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

from .config import AUTH_FILE, API_URL, MY_BOTS_URL
from .logging_utils import safe_log
from .fs_utils import ensure_dirs
import threading
from .logging_utils import safe_log
AUTH_CAPTURE_LOCK = threading.Lock()

# Persisted browser profile for cookies/localStorage
PROFILE_DIR = Path("playwright_profile")

# If you use this flag in UI routes, you can import it.
AUTH_REQUIRED = False


def load_auth_credentials():
    """
    Returns:
        (bearer_token, guest_userid, refresh_token, expires_at, client_id)

    We keep the 5-tuple for backward compatibility, but only the first two matter now.
    """
    ensure_dirs()
    if AUTH_FILE.exists():
        try:
            with open(AUTH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
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


def save_auth_credentials(bearer_token, guest_userid, refresh_token=None, expires_at=None, client_id=None):
    """
    Writes auth_credentials.json.

    We still write the extra keys (as None) so older code that expects them won't break.
    """
    ensure_dirs()
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


def test_auth_credentials(bearer_token, guest_userid) -> bool:
    """
    Validates current credentials by calling the API you already use for snapshots.
    """
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
        resp = requests.get(API_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        ok = isinstance(data, (dict, list)) and bool(data)
        if ok:
            safe_log("Existing auth credentials are valid")
        else:
            logging.warning("API response is empty or invalid")
        return ok
    except requests.exceptions.RequestException as e:
        logging.warning(f"Auth credentials test failed: {e}")
        return False

def ensure_fresh_kinde_token():
    """
    Legacy name kept for compatibility.

    We do NOT use Kinde anymore.
    We only ensure we have a usable SpicyChat bearer token + guest_userid.

    Behavior:
      1) If saved bearer+guest is valid -> return it.
      2) Else try auto recapture using persisted Playwright profile (headless).
      3) Else try auto recapture headful (can surface login UI if truly expired).
      4) Else return (None, None) and set AUTH_REQUIRED=True
    """
    global AUTH_REQUIRED

    bearer, guest, _r, _e, _c = load_auth_credentials()
    if bearer and guest and test_auth_credentials(bearer, guest):
        AUTH_REQUIRED = False
        return bearer, guest

    # Silent attempt
    new_bearer, new_guest = _recapture_token_from_profile(timeout_sec=60, headless=True)
    if new_bearer and new_guest and test_auth_credentials(new_bearer, new_guest):
        save_auth_credentials(new_bearer, new_guest)
        AUTH_REQUIRED = False
        return new_bearer, new_guest

    # Headful fallback
    new_bearer, new_guest = _recapture_token_from_profile(timeout_sec=120, headless=False)
    if new_bearer and new_guest and test_auth_credentials(new_bearer, new_guest):
        save_auth_credentials(new_bearer, new_guest)
        AUTH_REQUIRED = False
        return new_bearer, new_guest

    AUTH_REQUIRED = True
    return None, None


def _recapture_token_from_profile(timeout_sec: int = 60, headless: bool = True):
    """
    Uses persisted cookies/localStorage to silently regain a fresh Bearer token.
    Returns (bearer, guest) or (None, None).
    """
    
    if not PROFILE_DIR.exists():
        safe_log("[Auth] No Playwright profile found; headless recapture cannot work until you log in once interactively.")
        return None, None

    captured = {"bearer": None, "guest": None}

    def maybe_capture(headers: dict):
        auth = headers.get("authorization") or headers.get("Authorization")
        if auth and auth.startswith("Bearer ") and not captured["bearer"]:
            captured["bearer"] = auth[len("Bearer "):].strip()

        guest = (
            headers.get("x-guest-userid")
            or headers.get("X-Guest-Userid")
            or headers.get("X-Guest-UserId")
        )
        if guest and not captured["guest"]:
            captured["guest"] = str(guest).strip()

    def is_relevant_host(host: str) -> bool:
        host = (host or "").lower()
        return (
            "spicychat.ai" in host
            or host.endswith("nd-api.com")
            or "nd-api.com" in host
        )

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            args=["--start-maximized"],
            viewport=None,
        )
        try:
            def on_request(req):
                try:
                    u = urlparse.urlparse(req.url)
                    host = (u.netloc or "").lower()
                    if not is_relevant_host(host):
                        return
                    maybe_capture(req.headers)
                except Exception:
                    return

            ctx.on("request", on_request)

            page = ctx.new_page()
            page.goto(MY_BOTS_URL, wait_until="domcontentloaded")

            # Nudge requests: reload tends to trigger the API calls that carry Authorization
            try:
                page.reload(wait_until="domcontentloaded")
            except Exception:
                pass

            deadline = time.time() + timeout_sec
            last_reload = 0.0

            while time.time() < deadline:
                if captured["bearer"] and captured["guest"]:
                    safe_log("[Auth] Headless token recapture successful.")
                    return captured["bearer"], captured["guest"]

                # periodic reload to force fresh API calls
                now = time.time()
                if now - last_reload > 4.0:
                    last_reload = now
                    try:
                        page.reload(wait_until="domcontentloaded")
                    except Exception:
                        pass

                time.sleep(0.2)

            return None, None
        finally:
            try:
                ctx.close()
            except Exception:
                pass


def capture_auth_credentials(timeout_sec: int = 300):
    """
    Interactive capture (visible browser), Kinde email+code login supported:
    - NO console input
    - NEVER reloads while user is on Kinde login pages
    - Waits for user to return to SpicyChat (MY_BOTS_URL), then forces a few nudges to trigger API calls
    - Captures from both request + response.request headers for reliability
    """
    # Prevent concurrent auth launches with the same persistent profile
    if not AUTH_CAPTURE_LOCK.acquire(blocking=False):
        raise RuntimeError("Auth capture already running (lock held). Try again in a moment.")

    try:
        bearer_token, guest_userid, refresh_token, expires_at, client_id = load_auth_credentials()
        if bearer_token and guest_userid and test_auth_credentials(bearer_token, guest_userid):
            return bearer_token, guest_userid, refresh_token, expires_at, client_id

        safe_log("[Auth] Launching browser for email login (Kinde). Complete login; capture will happen automatically on My Chatbots…")

        captured = {"bearer": None, "guest": None}
        last_relevant = {"url": ""}

        def maybe_capture(headers: dict, note: str = ""):
            auth = headers.get("authorization") or headers.get("Authorization")
            if auth and auth.startswith("Bearer ") and not captured["bearer"]:
                captured["bearer"] = auth[len("Bearer "):].strip()
                safe_log(f"[Auth] Captured bearer token{(' via ' + note) if note else ''}")

            guest = (
                headers.get("x-guest-userid")
                or headers.get("X-Guest-Userid")
                or headers.get("X-Guest-UserId")
            )
            if guest and not captured["guest"]:
                captured["guest"] = str(guest).strip()
                safe_log(f"[Auth] Captured guest_userid={captured['guest']}{(' via ' + note) if note else ''}")

        def is_relevant_host(host: str) -> bool:
            host = (host or "").lower()
            return ("nd-api.com" in host) or ("spicychat.ai" in host)

        def is_kinde_url(url: str) -> bool:
            u = (url or "").lower()
            # Kinde hosted login commonly includes kinde.com or kinde.* domains.
            # Keep it broad so it works even if their hosted domain changes.
            return ("kinde" in u) and (("kinde.com" in u) or ("kinde." in u) or ("auth" in u) or ("login" in u))

        def is_spicychat(url: str) -> bool:
            return "spicychat.ai" in (url or "").lower()

        def is_on_my_bots(url: str) -> bool:
            u = (url or "").lower()
            # Prefer strict match to your actual MY_BOTS_URL when possible
            return is_spicychat(u) and ("my" in u and "bot" in u or u.rstrip("/") == MY_BOTS_URL.rstrip("/"))

        with sync_playwright() as p:
            ctx = p.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=False,
                args=["--start-maximized"],
                viewport=None,
            )

            try:
                def on_request(req):
                    try:
                        u = urlparse.urlparse(req.url)
                        if not is_relevant_host(u.netloc):
                            return
                        last_relevant["url"] = req.url
                        maybe_capture(req.headers, note="request")
                    except Exception:
                        pass

                def on_response(resp):
                    try:
                        req = resp.request
                        u = urlparse.urlparse(req.url)
                        if not is_relevant_host(u.netloc):
                            return
                        last_relevant["url"] = req.url
                        maybe_capture(req.headers, note="response.request")
                    except Exception:
                        pass

                ctx.on("request", on_request)
                ctx.on("response", on_response)

                page = ctx.new_page()
                page.goto(MY_BOTS_URL, wait_until="domcontentloaded")

                deadline = time.time() + timeout_sec
                last_status = 0.0
                nudges_started = False
                nudge_count = 0

                while time.time() < deadline:
                    if captured["bearer"] and captured["guest"]:
                        break

                    try:
                        cur_url = page.url or ""
                    except Exception:
                        cur_url = ""

                    now = time.time()
                    if now - last_status > 8.0:
                        last_status = now
                        safe_log(f"[Auth] Waiting… url={cur_url[:120]} last_relevant={last_relevant['url'][:120]}")

                    # If user is on Kinde login pages, DO NOT interfere at all.
                    if is_kinde_url(cur_url):
                        time.sleep(0.25)
                        continue

                    # If not on SpicyChat yet (still transitioning), wait.
                    if not is_spicychat(cur_url):
                        time.sleep(0.25)
                        continue

                    # Once we're back on SpicyChat, force getting to My Chatbots.
                    if not is_on_my_bots(cur_url):
                        try:
                            page.goto(MY_BOTS_URL, wait_until="domcontentloaded")
                        except Exception:
                            pass
                        time.sleep(0.25)
                        continue

                    # Now we are on/near MY_BOTS_URL — start a few gentle nudges to trigger the API call
                    if not nudges_started:
                        nudges_started = True
                        safe_log("[Auth] Back on My Chatbots. Triggering API calls to capture token…")

                    # Do a limited number of nudges (avoid “refreshing constantly”)
                    if nudge_count < 6:
                        nudge_count += 1
                        try:
                            page.reload(wait_until="domcontentloaded")
                        except Exception:
                            pass

                    time.sleep(0.6)

                if not captured["bearer"] or not captured["guest"]:
                    raise RuntimeError(
                        "Timed out waiting for Bearer token. "
                        "Login may have succeeded, but no nd-api.com request with Authorization was observed."
                    )

            finally:
                try:
                    ctx.close()
                except Exception:
                    pass

        save_auth_credentials(captured["bearer"], captured["guest"])
        safe_log("[Auth] Interactive token capture successful.")
        return captured["bearer"], captured["guest"], None, None, None

    finally:
        AUTH_CAPTURE_LOCK.release()
