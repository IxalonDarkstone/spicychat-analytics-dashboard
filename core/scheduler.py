import time
import logging

from .logging_utils import safe_log
from .auth import ensure_fresh_kinde_token
from .snapshot import take_snapshot


def snapshot_scheduler():
    """
    Runs every hour.
    Attempts to refresh Kinde access token before each snapshot.
    If auth fails, it pauses and tries again next hour.
    """
    safe_log("Snapshot scheduler started (1-hour interval).")

    while True:
        try:
            bearer, guest = ensure_fresh_kinde_token()

            if not bearer or not guest:
                safe_log("Scheduler paused — auth invalid. Waiting for user reauth.")
            else:
                safe_log("Scheduler: auth OK — running hourly snapshot.")
                take_snapshot({})
        except Exception as e:
            logging.error(f"Scheduler error: {e}")

        time.sleep(3600)
