import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import argparse  # Added missing import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/backfill.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def safe_log(message):
    """Log a message, handling unencodable characters."""
    try:
        logging.info(message)
    except UnicodeEncodeError:
        logging.info(message.encode('ascii', errors='replace').decode('ascii'))

def backfill_missing_days(db_path, missing_start='2025-09-28', missing_end='2025-10-03', total_growth=None):
    # Ensure database path exists
    db = Path(db_path)
    if not db.exists():
        safe_log(f"Database not found at {db_path}. Exiting.")
        return

    # Connect to database
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Query the dates around the missing period
    missing_start = datetime.strptime(missing_start, '%Y-%m-%d').date()
    missing_end = datetime.strptime(missing_end, '%Y-%m-%d').date()

    # Get the last date before missing_start
    c.execute("""
        SELECT DISTINCT date FROM bots 
        WHERE date < ? 
        ORDER BY date DESC 
        LIMIT 1
    """, (missing_start,))
    before_date = c.fetchone()
    before_date = before_date[0] if before_date else None

    # Get the first date after missing_end
    c.execute("""
        SELECT DISTINCT date FROM bots 
        WHERE date > ? 
        ORDER BY date ASC 
        LIMIT 1
    """, (missing_end,))
    after_date = c.fetchone()
    after_date = after_date[0] if after_date else None

    safe_log(f"Before date: {before_date}")
    safe_log(f"After date: {after_date}")

    if not before_date or not after_date:
        safe_log("Cannot find before or after dates. Exiting.")
        conn.close()
        return

    # Get total messages before and after
    c.execute("""
        SELECT SUM(num_messages) as total_messages FROM bots 
        WHERE date = ?
    """, (before_date,))
    before_total = c.fetchone()[0] or 0

    c.execute("""
        SELECT SUM(num_messages) as total_messages FROM bots 
        WHERE date = ?
    """, (after_date,))
    after_total = c.fetchone()[0] or 0

    safe_log(f"Before total: {before_total}")
    safe_log(f"After total: {after_total}")

    # Use provided total growth if query fails or doesn't match
    calculated_growth = after_total - before_total
    growth = total_growth if total_growth is not None else calculated_growth
    safe_log(f"Calculated growth: {calculated_growth}, Using: {growth}")

    # Missing days
    missing_days = pd.date_range(start=missing_start, end=missing_end).date.tolist()
    safe_log(f"Missing days: {missing_days}")

    # Average daily growth
    avg_daily = growth / len(missing_days)
    safe_log(f"Average daily: {avg_daily}")

    # Confirmation prompt
    confirm = input(f"About to backfill {len(missing_days)} days for all bots with average growth {avg_daily}. Proceed? (yes/no): ").lower()
    if confirm != "yes":
        safe_log("Backfill cancelled by user.")
        conn.close()
        return

    # Get bots from before date
    bots_before = pd.read_sql_query("""
        SELECT bot_id, bot_name, bot_title, creator_user_id, created_at, avatar_url, num_messages
        FROM bots 
        WHERE date = ?
    """, conn, params=[before_date])

    # Backfill for each missing day
    for i, day in enumerate(missing_days):
        day_str = day.strftime('%Y-%m-%d')
        day_fraction = (i + 1) / len(missing_days)
        daily_total = before_total + growth * day_fraction
        safe_log(f"Backfilling {day_str} with total {daily_total}")

        for _, bot in bots_before.iterrows():
            bot_growth = (bot['num_messages'] / before_total) * growth if before_total > 0 else avg_daily
            bot_daily = bot['num_messages'] + bot_growth * day_fraction
            c.execute("""
                INSERT INTO bots (date, bot_id, bot_name, bot_title, num_messages, creator_user_id, created_at, avatar_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                day_str, bot['bot_id'], bot['bot_name'], bot['bot_title'],
                int(bot_daily), bot['creator_user_id'], bot['created_at'], bot['avatar_url']
            ))

    conn.commit()
    safe_log(f"Backfilled {len(missing_days)} days for {len(bots_before)} bots.")
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Backfill missing days in the SpicyChat database with interpolated data.")
    parser.add_argument("missing_start", help="Start date of missing period in YYYY-MM-DD format (e.g., 2025-09-28)")
    parser.add_argument("missing_end", help="End date of missing period in YYYY-MM-DD format (e.g., 2025-10-03)")
    parser.add_argument("--growth", type=int, help="Total message growth over the period (e.g., 1945736). If not provided, calculated from before/after snapshots.")
    parser.add_argument("--database", default="data/spicychat.db", help="Path to the database file (default: data/spicychat.db)")
    args = parser.parse_args()

    safe_log(f"Starting backfill for {args.missing_start} to {args.missing_end} with growth {args.growth}")
    backfill_missing_days(args.database, args.missing_start, args.missing_end, args.growth)
    safe_log("Backfill completed.")

if __name__ == "__main__":
    main()