import os
import sys
import subprocess
import logging
from pathlib import Path

# ------------------ Config ------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DATABASE = DATA_DIR / "spicychat.db"
CHARTS_DIR = BASE_DIR / "charts"
STATIC_DIR = BASE_DIR / "static"
STATIC_CHARTS_DIR = STATIC_DIR / "charts"
TEMPLATES_DIR = BASE_DIR / "templates"

# ------------------ Logging ------------------
def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOGS_DIR / "setup_spicychat.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

# ------------------ Setup Functions ------------------
def check_python_version():
    required_version = (3, 8)
    current_version = sys.version_info[:2]
    if current_version < required_version:
        logging.error(f"Python {required_version[0]}.{required_version[1]} or higher is required. Found {current_version[0]}.{current_version[1]}.")
        sys.exit(1)
    logging.info(f"Python version {current_version[0]}.{current_version[1]} is compatible.")

def install_dependencies():
    dependencies = [
        "flask>=2.0.0",
        "requests>=2.26.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "matplotlib>=3.4.0",
        "scipy>=1.7.0",
        "playwright>=1.28.0"
    ]
    logging.info("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        for dep in dependencies:
            subprocess.check_call([sys.executable, "-m", "pip", "install", dep])
        logging.info("All dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to install dependencies: {e}")
        sys.exit(1)

def install_playwright():
    logging.info("Installing Playwright browsers...")
    try:
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        logging.info("Playwright browsers installed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to install Playwright browsers: {e}")
        sys.exit(1)

def create_directories():
    logging.info("Creating required directories...")
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        STATIC_DIR.mkdir(parents=True, exist_ok=True)
        STATIC_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
        if not os.path.exists(STATIC_CHARTS_DIR):
            try:
                os.symlink(CHARTS_DIR, STATIC_CHARTS_DIR, target_is_directory=True)
                logging.info(f"Created symlink {STATIC_CHARTS_DIR} -> {CHARTS_DIR}")
            except OSError as e:
                logging.warning(f"Could not create symlink {STATIC_CHARTS_DIR}: {e}. Using directory copy instead.")
                STATIC_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
        logging.info("All directories created successfully.")
    except Exception as e:
        logging.error(f"Failed to create directories: {e}")
        sys.exit(1)

def verify_files():
    required_files = [
        BASE_DIR / "spicychat_analytics.py",
        TEMPLATES_DIR / "index.html",
        TEMPLATES_DIR / "bots_table.html",
        TEMPLATES_DIR / "bot.html"
    ]
    logging.info("Verifying required files...")
    for file in required_files:
        if not file.exists():
            logging.error(f"Required file {file} is missing.")
            sys.exit(1)
    logging.info("All required files are present.")

def initialize_database():
    logging.info("Initializing SQLite database...")
    try:
        with sqlite3.connect(DATABASE) as conn:
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
                    PRIMARY KEY (date, bot_id)
                )
            """)
            conn.commit()
        logging.info(f"Database initialized at {DATABASE}")
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize database: {e}")
        sys.exit(1)

def main():
    setup_logging()
    logging.info("Starting setup for SpicyChat Analytics Dashboard...")
    
    check_python_version()
    create_directories()
    verify_files()
    install_dependencies()
    install_playwright()
    initialize_database()
    
    logging.info("Setup completed successfully.")
    logging.info("To run the dashboard, execute: python spicychat_analytics.py")
    logging.info("The dashboard will be available at http://localhost:5000")

if __name__ == "__main__":
    main()