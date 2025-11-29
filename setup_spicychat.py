import os
import sys
import shutil
import logging
from pathlib import Path
import sqlite3
import subprocess

# ---------------------------------------------
# Logging
# ---------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/setup.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def log(msg):
    try:
        logging.info(msg)
    except:
        logging.info(msg.encode("ascii", errors="replace").decode("ascii"))


# ---------------------------------------------
# Create folders
# ---------------------------------------------
def setup_directories():
    folders = [
        "data",
        "logs",
        "static",
        "templates"
    ]
    for f in folders:
        Path(f).mkdir(parents=True, exist_ok=True)
        log(f"Ensured directory: {f}")


# ---------------------------------------------
# Full database initialization
# ---------------------------------------------
def initialize_database():

    database = Path("data/spicychat.db")

    log("Initializing SQLite database...")

    conn = sqlite3.connect(database)
    c = conn.cursor()

    # Bots table
    c.execute("""
    CREATE TABLE IF NOT EXISTS bots (
        date TEXT,
        bot_id TEXT,
        bot_name TEXT,
        bot_title TEXT,
        num_messages INTEGER,
        creator_user_id TEXT,
        created_at TEXT,
        avatar_url TEXT,
        PRIMARY KEY(date, bot_id)
    )
    """)

    # Rank history
    c.execute("""
    CREATE TABLE IF NOT EXISTS bot_rank_history (
        date TEXT,
        bot_id TEXT,
        rank INTEGER,
        page INTEGER,
        creator_user_id TEXT
    )
    """)

    # Counts of your bots in trending lists
    c.execute("""
    CREATE TABLE IF NOT EXISTS top240_history (
        date TEXT PRIMARY KEY,
        count INTEGER
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS top480_history (
        date TEXT PRIMARY KEY,
        count INTEGER
    )
    """)

    conn.commit()
    conn.close()

    log("Database initialized successfully!")


# ---------------------------------------------
# Install Python dependencies
# ---------------------------------------------
def install_dependencies():
    required = [
        "flask",
        "pandas",
        "numpy",
        "matplotlib",
        "playwright",
        "requests",
        "pytz",
        "typesense",
        "openpyxl"
    ]

    log("Installing Python packages...")
    for pkg in required:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
        log(f"Installed: {pkg}")

    log("Installing Playwright Chromium browser...")
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])


# ---------------------------------------------
# MAIN
# ---------------------------------------------
def main():
    log("=== SpicyChat Analytics Setup Starting ===")

    setup_directories()

    if "--init-db" in sys.argv:
        initialize_database()
        log("Database-only initialization complete.")
        return

    install_dependencies()
    initialize_database()

    log("Setup complete!")
    log("Run: python spicychat_analytics.py")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"Setup failed: {e}")
        sys.exit(1)
