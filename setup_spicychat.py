import os
import sys
import shutil
import logging
from pathlib import Path
import sqlite3
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/setup.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def safe_log(message):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode('ascii', errors='replace').decode('ascii'))

def setup_directories():
    """Create necessary directories."""
    directories = ["data", "logs", "charts", "static/charts"]
    for dir_name in directories:
        dir_path = Path(dir_name)
        dir_path.mkdir(parents=True, exist_ok=True)
        safe_log(f"Created directory: {dir_path}")

def initialize_database():
    """Initialize the SQLite database with the bots table."""
    database_path = Path("data/spicychat.db")
    try:
        with sqlite3.connect(database_path) as conn:
            c = conn.cursor()
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
                    PRIMARY KEY (date, bot_id)
                )
            """)
            conn.commit()
            safe_log(f"Initialized database at {database_path}")
    except sqlite3.Error as e:
        safe_log(f"Database initialization error: {e}")
        raise

def install_dependencies():
    """Install required Python packages."""
    required_packages = ["flask", "pandas", "numpy", "matplotlib", "playwright", "requests", "pytz"]
    try:
        for package in required_packages:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            safe_log(f"Installed package: {package}")
    except subprocess.CalledProcessError as e:
        safe_log(f"Error installing dependencies: {e}")
        raise

def main():
    safe_log("Starting SpicyChat analytics setup...")
    
    # Create directories
    setup_directories()
    
    # Initialize database
    initialize_database()
    
    # Install dependencies
    install_dependencies()
    
    safe_log("Setup completed. Please copy spicychat_analytics.py, templates/, and static/style.css from your existing installation.")
    safe_log("Run 'python spicychat_analytics.py' to start the application.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        safe_log(f"Setup failed: {e}")
        sys.exit(1)