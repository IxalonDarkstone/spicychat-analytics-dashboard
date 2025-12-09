import os
import sys
import logging
import sqlite3
import subprocess
from pathlib import Path

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
    logging.info(msg)


# ---------------------------------------------
# Create required directories
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
# Initialize SQLite database and tables
# ---------------------------------------------
def initialize_database():
    db_path = Path("data/spicychat.db")
    log("Initializing SQLite database...")

    conn = sqlite3.connect(db_path)
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

    # Rank history (global trending)
    c.execute("""
    CREATE TABLE IF NOT EXISTS bot_rank_history (
        date TEXT,
        bot_id TEXT,
        rank INTEGER,
        page INTEGER,
        creator_user_id TEXT
    )
    """)

    # Trending counts
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
# Install dependencies using requirements.txt
# ---------------------------------------------
def install_dependencies():
    log("Installing Python packages from requirements.txt...")

    if not Path("requirements.txt").exists():
        raise FileNotFoundError("requirements.txt is missing!")

    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    log("Python dependencies installed.")

    log("Installing Playwright Chromium browser…")
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
    log("Playwright installation complete.")


# ---------------------------------------------
# MAIN
# ---------------------------------------------
def main():
    log("=== SpicyChat Analytics Setup Starting ===")

    setup_directories()

    # Allow DB-only mode
    if "--init-db" in sys.argv:
        initialize_database()
        log("Database-only initialization complete.")
        return

    install_dependencies()
    initialize_database()

    log("Setup complete!")
    log("Run the dashboard with: python spicychat_analytics.py")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"Setup failed: {e}")
        sys.exit(1)
