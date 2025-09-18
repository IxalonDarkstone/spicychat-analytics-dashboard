import sqlite3
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/clean_database.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def safe_log(message):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode('ascii', errors='replace').decode('ascii'))

def clean_date_from_database(date_to_remove, database_path):
    # Ensure database path exists
    db_path = Path(database_path)
    if not db_path.exists():
        safe_log(f"Database not found at {database_path}. Exiting.")
        return

    # Convert date to string format matching the database (YYYY-MM-DD)
    try:
        date_str = datetime.strptime(date_to_remove, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        safe_log(f"Invalid date format. Use MM/DD/YYYY (e.g., 09/16/2025). Exiting.")
        return

    # Connect to the database
    try:
        with sqlite3.connect(database_path) as conn:
            c = conn.cursor()
            # Count rows to be deleted for confirmation
            c.execute("SELECT COUNT(*) FROM bots WHERE date = ?", (date_str,))
            row_count = c.fetchone()[0]
            if row_count == 0:
                safe_log(f"No data found for date {date_str}. Exiting.")
                return

            # Confirmation prompt
            confirm = input(f"About to delete {row_count} rows from {date_str}. Proceed? (yes/no): ").lower()
            if confirm != "yes":
                safe_log("Deletion cancelled by user.")
                return

            # Delete the data
            c.execute("DELETE FROM bots WHERE date = ?", (date_str,))
            conn.commit()
            safe_log(f"Deleted {c.rowcount} rows from {date_str} in {database_path}")

    except sqlite3.Error as e:
        safe_log(f"Database error: {e}")
        return
    except Exception as e:
        safe_log(f"Unexpected error: {e}")
        return

def main():
    parser = argparse.ArgumentParser(description="Remove data for a specific date from the SpicyChat database.")
    parser.add_argument("date", help="Date to remove in MM/DD/YYYY format (e.g., 09/16/2025)")
    parser.add_argument("--database", default="data/spicychat.db", help="Path to the database file (default: data/spicychat.db)")
    args = parser.parse_args()

    safe_log(f"Starting cleanup for date {args.date} in database {args.database}")
    clean_date_from_database(args.date, args.database)
    safe_log("Cleanup completed.")

if __name__ == "__main__":
    main()