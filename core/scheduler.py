# scheduler.py
import time
import logging

from .logging_utils import safe_log
from .auth import ensure_fresh_kinde_token
from .snapshot import take_snapshot


def snapshot_scheduler(initial_delay_seconds: int = 0):
    """
    Runs every hour.
    Optionally waits `initial_delay_seconds` before the first run to avoid
    double-snapshotting on app startup.
    """
    safe_log("Snapshot scheduler started (1-hour interval).")

    if initial_delay_seconds and initial_delay_seconds > 0:
        safe_log(f"Scheduler initial delay: sleeping {initial_delay_seconds} seconds…")
        time.sleep(initial_delay_seconds)

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
