# spicychat_analytics.py
from flask import Flask
import argparse
import threading
import core
global SNAPSHOT_THREAD_STARTED
    
from core import (
    setup_logging,
    ensure_dirs,
    init_db,
    take_snapshot,
    snapshot_scheduler,
    AUTH_REQUIRED,
    SNAPSHOT_THREAD_STARTED,
    safe_log,
)
from core.auth import load_auth_credentials, save_auth_credentials, test_auth_credentials

from routes_dashboard import register_dashboard_routes
from routes_bots import register_bot_routes
from routes_trending import register_trending_routes
from routes_authors import register_author_routes


app = Flask(__name__, template_folder="templates", static_folder="static")


def create_app():
    register_dashboard_routes(app)
    register_bot_routes(app)
    register_trending_routes(app)
    register_author_routes(app)   # ✅ ADD THIS
    return app


if __name__ == "__main__":
    setup_logging()
    ensure_dirs()
    init_db()

    parser = argparse.ArgumentParser(description="SpicyChat Analytics Dashboard")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument(
        "--no_snapshot",
        action="store_true",
        help="Skip ONLY the automatic snapshot on startup (hourly snapshots still run)",
    )
    args = parser.parse_args()

    CURRENT_PORT = args.port
    NO_SNAPSHOT_MODE = args.no_snapshot

    create_app()

    # Load previous credentials (may be None)
    bearer, guest, refresh_token, expires_at, client_id = load_auth_credentials()

    # Determine whether auth is valid on startup
    if not test_auth_credentials(bearer, guest):
        core.AUTH_REQUIRED = True
        safe_log("Startup auth invalid — snapshots paused until reauth.")
    else:
        core.AUTH_REQUIRED = False
        safe_log("Startup auth valid.")

    # --------------------------------------------------------
    #  STARTUP SNAPSHOT  (the ONLY thing --no_snapshot affects)
    # --------------------------------------------------------
    if NO_SNAPSHOT_MODE:
        safe_log("Skipping startup snapshot (--no_snapshot active).")
    elif AUTH_REQUIRED:
        safe_log("Skipping startup snapshot (auth invalid).")
    else:
        safe_log("Running startup snapshot…")
        try:
            take_snapshot({"manual": True})
        except Exception as e:
            safe_log(f"Startup snapshot failed: {e}")

    # --------------------------------------------------------
    #  HOURLY SNAPSHOT SCHEDULER  (ALWAYS runs, even if --no_snapshot)
    # --------------------------------------------------------

    import threading
    import time

    def start_scheduler_later(delay_seconds: int):
        """Start the scheduler after a delay (used when --no_snapshot is active)."""
        def delayed():
            safe_log(f"Scheduler delayed start: waiting {delay_seconds} seconds…")
            time.sleep(delay_seconds)
            safe_log("Starting scheduler now.")
            snapshot_scheduler()

        threading.Thread(target=delayed, daemon=True).start()


    # Start hourly snapshot thread
    if not SNAPSHOT_THREAD_STARTED:
        if args.no_snapshot:
            # Delay scheduler by 1 hour (3600 seconds)
            start_scheduler_later(3600)
            safe_log("Scheduler will start in 1 hour (--no_snapshot active).")
        else:
            # If we already ran a startup snapshot, delay the scheduler so it doesn't run again immediately.
            initial_delay = 3600  # 1 hour
            threading.Thread(
                target=snapshot_scheduler,
                kwargs={"initial_delay_seconds": initial_delay},
                daemon=True
            ).start()
            safe_log("Hourly snapshot scheduler started (delayed 1 hour after startup snapshot).")


        SNAPSHOT_THREAD_STARTED = True


    # --------------------------------------------------------
    #  RUN FLASK SERVER
    # --------------------------------------------------------
    safe_log(f"Starting server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port)

