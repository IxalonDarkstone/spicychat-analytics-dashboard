# routes_bots.py
from flask import render_template, request, jsonify
import sqlite3
import pandas as pd

from core import (
    load_history_df,
    compute_deltas,
    fmt_commas,
    fmt_delta_commas,
    AVATAR_BASE_URL,
    DATABASE,
    safe_log,
)


def register_bot_routes(app):
    @app.route("/api/bot/<bot_id>/history")
    def api_bot_history(bot_id):
        timeframe = request.args.get("timeframe", "All")
        df_raw = load_history_df()
        dfc = compute_deltas(df_raw, timeframe)
        sub = dfc[dfc["bot_id"] == bot_id].sort_values("date")

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute(
            "SELECT date, rank FROM bot_rank_history WHERE bot_id = ?",
            (bot_id,),
        )
        rank_rows = cur.fetchall()
        conn.close()

        rank_by_date = {date_str: (rank or 0) for (date_str, rank) in rank_rows}

        points = []
        for _, row in sub.iterrows():
            date_str = str(row["date"])
            r = rank_by_date.get(date_str, 0)

            if r and 1 <= r <= 480:
                page = (r - 1) // 48 + 1
            else:
                page = 11

            points.append(
                {
                    "date": date_str,
                    "total": int(row["num_messages"]),
                    "daily": int(row.get("daily_messages", 0)),
                    "rank": r if r else None,
                    "page": page,
                }
            )

        return jsonify({"bot_id": bot_id, "points": points})

    @app.route("/bot/<bot_id>")
    def bot_detail(bot_id):
        timeframe = request.args.get("timeframe", "All")

        df_raw = load_history_df()
        dfc = compute_deltas(df_raw, timeframe)

        bot_rows = dfc[dfc["bot_id"] == bot_id].sort_values("date")
        if bot_rows.empty:
            import logging

            logging.warning(f"Bot {bot_id} not found in DB for timeframe {timeframe}")
            return render_template("bot.html", bot=None, history=[], timeframe=timeframe)

        latest = bot_rows.iloc[-1]

        avatar_raw = latest.get("avatar_url") or ""
        if avatar_raw:
            filename = str(avatar_raw).split("/")[-1]
            avatar_url = f"{AVATAR_BASE_URL}/{filename}"
        else:
            avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

        created_at_str = ""
        if pd.notnull(latest.get("created_at")):
            try:
                created_at_str = latest["created_at"].strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                created_at_str = str(latest["created_at"])

        bot_data = {
            "bot_id": latest["bot_id"],
            "name": latest["bot_name"],
            "title": latest["bot_title"],
            "total": int(latest["num_messages"]),
            "total_fmt": fmt_commas(latest["num_messages"]),
            "delta": int(latest.get("daily_messages", 0)),
            "delta_fmt": fmt_delta_commas(int(latest.get("daily_messages", 0))),
            "created_at": created_at_str,
            "link": f"https://spicychat.ai/chat/{latest['bot_id']}",
            "avatar_url": avatar_url,
        }

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute(
            "SELECT date, rank FROM bot_rank_history WHERE bot_id = ?",
            (bot_id,),
        )
        rank_rows = cur.fetchall()
        conn.close()
        rank_by_date = {date_str: (rank or 0) for (date_str, rank) in rank_rows}

        history_rows = bot_rows.sort_values("date", ascending=False)

        history = []
        for _, row in history_rows.iterrows():
            created_row = ""
            if pd.notnull(row.get("created_at")):
                try:
                    created_row = row["created_at"].strftime("%Y-%m-%d %H:%M:%S %Z")
                except Exception:
                    created_row = str(row["created_at"])

            date_str = str(row["date"])
            r = rank_by_date.get(date_str, 0)
            if r and 1 <= r <= 480:
                page = (r - 1) // 48 + 1
            else:
                page = None

            history.append(
                {
                    "date": date_str,
                    "total": int(row["num_messages"]),
                    "total_fmt": fmt_commas(row["num_messages"]),
                    "daily": int(row.get("daily_messages", 0)),
                    "daily_fmt": fmt_delta_commas(int(row.get("daily_messages", 0))),
                    "created_at": created_row,
                    "rank": r or None,
                    "page": page,
                }
            )

        safe_log(f"Rendering bot detail for {bot_id}: {bot_data['name']} with {len(history)} rows")
        return render_template("bot.html", bot=bot_data, history=history, timeframe=timeframe)
