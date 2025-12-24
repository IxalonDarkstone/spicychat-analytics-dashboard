# routes_dashboard.py
from flask import render_template, request, redirect, url_for, jsonify
from datetime import datetime, timedelta
import sqlite3

from core import (
    fmt_commas, fmt_delta_commas,
    load_history_df,
    compute_deltas,
    get_bots_data,
    init_db,
    take_snapshot,
    AUTH_REQUIRED,
    LAST_SNAPSHOT_DATE,
    safe_log,
    get_last_snapshot_time,
    ensure_author_tables,
    DATABASE,
)


def register_dashboard_routes(app):
    @app.route("/")
    def index():
        chart_sort_by = request.args.get("chart_sort_by", "7day")
        chart_sort_asc = request.args.get("chart_sort_asc", "false") == "true"
        sort_by = request.args.get("sort_by", "delta")
        sort_asc = request.args.get("sort_asc", "false") == "true"
        created_after = request.args.get("created_after", "All")
        timeframe = request.args.get("timeframe", "All")
        tags = request.args.get("tags", "")  # comma-separated
        q = request.args.get("q", "")


        safe_log(
            f"Index route: chart_sort_by={chart_sort_by}, "
            f"chart_sort_asc={chart_sort_asc}, sort_by={sort_by}, "
            f"sort_asc={sort_asc}, created_after={created_after}, timeframe={timeframe}"
        )

        last_7_days = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        last_30_days = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        current_month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")

        df_raw = load_history_df()

        dfc = compute_deltas(
            df_raw,
            chart_sort_by if chart_sort_by in ["7day", "30day", "current_month", "All"] else "7day"
        )

        # Always attempt to load bots
        try:
            bots, totals_list, total_messages, latest_date_from_bots = get_bots_data(
                timeframe=timeframe,
                sort_by=sort_by,
                sort_asc=sort_asc,
                created_after=created_after,
                tags=tags,
                q=q,
            )
            total_bots = len(bots)
        except Exception as e:
            import logging
            logging.error(f"Error in get_bots_data: {e}")
            bots, total_messages, total_bots = [], 0, 0

        # Totals history (safe even if df is empty)
        if dfc.empty:
            totals_data = []
            latest = "No data"
        else:
            all_dates = sorted(dfc["date"].unique(), reverse=True)
            latest = str(all_dates[0])

            totals = (
                dfc.groupby("date", as_index=False)
                    .agg({"num_messages": "sum", "daily_messages": "sum"})
                    .sort_values("date", ascending=False)
            )

            totals_data = [
                {
                    "date": str(row["date"]),
                    "total": int(row["num_messages"]),
                    "total_fmt": fmt_commas(row["num_messages"]),
                    "daily": int(row["daily_messages"]),
                    "daily_fmt": fmt_delta_commas(row["daily_messages"]),
                }
                for _, row in totals.iterrows()
            ]



        # Continue when DF not empty
        dfc = compute_deltas(df_raw, chart_sort_by if chart_sort_by in ["7day", "30day", "current_month", "All"] else "7day")
        all_dates = sorted(dfc["date"].unique(), reverse=True)
        today_date = datetime.now().date()
        available_dates = [d for d in all_dates if d <= today_date]
        latest = available_dates[0] if available_dates else all_dates[0]

        # Build totals history across all dates, sorted descending (today first)
        totals = (
            dfc.groupby("date", as_index=False)
                .agg({"num_messages": "sum", "daily_messages": "sum"})
                .sort_values("date", ascending=False)
        )

        totals_data = [
            {
                "date": str(row["date"]),
                "total": int(row["num_messages"]),
                "total_fmt": fmt_commas(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "daily_fmt": fmt_delta_commas(row["daily_messages"]),
            }
            for _, row in totals.iterrows()
        ]

        try:
            bots, totals_list, total_messages, latest_date_from_bots = get_bots_data(
                timeframe=timeframe,
                sort_by=sort_by,
                sort_asc=sort_asc,
                created_after=created_after,
                tags=tags,
                q=q,
            )
            total_bots = len(bots)
        except Exception as e:
            import logging
            logging.error(f"Error in get_bots_data: {e}")
            bots, totals_list, total_messages, total_bots, latest_date_from_bots = [], [], 0, 0, None

        safe_log(f"Rendering index for {latest} with {len(bots)} bots")
        
        # ----------------------------
        # New bots from tracked authors (notification banner)
        # ----------------------------
        ensure_author_tables()

        author_new = []
        author_new_max = ""

        try:
            cutoff = (datetime.utcnow() - timedelta(days=2)).isoformat()

            conn = sqlite3.connect(DATABASE)
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    m.author,
                    s.bot_id,
                    s.bot_name,
                    s.bot_title,
                    m.first_seen_at
                FROM author_bot_map m
                JOIN bot_static s ON s.bot_id = m.bot_id
                WHERE m.first_seen_at IS NOT NULL
                  AND m.first_seen_at >= ?
                ORDER BY m.first_seen_at DESC
                LIMIT 8
            """, (cutoff,))
            rows = cur.fetchall()
            conn.close()

            for author, bot_id, bot_name, bot_title, first_seen_at in rows:
                author_new.append({
                    "author": author,
                    "bot_id": str(bot_id),
                    "name": bot_name or "",
                    "title": bot_title or "",
                    "first_seen_at": first_seen_at or "",
                    "link": f"https://spicychat.ai/chat/{bot_id}",
                })

            if rows:
                author_new_max = rows[0][4] or ""
        except Exception as e:
            safe_log(f"Author new-bots banner query failed: {e}")

        # ================================
        #  FIX 2 — PASS AUTH + LAST SNAPSHOT
        # ================================
        return render_template(
            "index.html",
            latest=str(latest),
            total_messages=fmt_commas(total_messages),
            total_bots=total_bots,
            totals=totals_data,
            bots=bots,
            sort_by=sort_by,
            sort_asc=sort_asc,
            chart_sort_by=chart_sort_by,
            chart_sort_asc=chart_sort_asc,
            created_after=created_after,
            last_7_days=last_7_days,
            last_30_days=last_30_days,
            current_month_start=current_month_start,
            timeframe=timeframe,
            auth_required=AUTH_REQUIRED,            # ← REQUIRED
            last_snapshot=get_last_snapshot_time(), # ← REQUIRED
            tags=tags,
            q=q,
        )

    @app.route("/take-snapshot", methods=["POST"])
    def take_snapshot_route():
        safe_log("Manual snapshot triggered from dashboard.")

        # Collect UI filter parameters to restore after redirect
        params = {
            "sort_by": request.args.get("sort_by", "delta"),
            "sort_asc": request.args.get("sort_asc", "false"),
            "created_after": request.args.get("created_after", "All"),
            "timeframe": request.args.get("timeframe", "All"),
            "chart_sort_by": request.args.get("chart_sort_by", "7day"),
            "chart_sort_asc": request.args.get("chart_sort_asc", "false"),
        }

        try:
            # Manual snapshots should *always* run, regardless of --no_snapshot
            take_snapshot({"manual": True}, verbose=True)
            safe_log("Manual snapshot completed successfully.")
        except Exception as e:
            safe_log(f"Error during manual snapshot: {e}")

        # Redirect back to dashboard with preserved filters
        return redirect(url_for("index", **params))

    @app.route("/api/snapshot_status")
    def api_snapshot_status():
        from core import AUTH_REQUIRED

        return {
            "auth_required": AUTH_REQUIRED,
            "snapshot_paused": AUTH_REQUIRED
        }
    @app.route("/api/totals")
    def api_totals():
        import sqlite3

        timeframe = request.args.get("timeframe", "7day")
        init_db()  # ensure tables exist

        df_raw = load_history_df()
        dfc = compute_deltas(df_raw, timeframe)

        if dfc.empty:
            return jsonify({"points": []})

        totals = (
            dfc.groupby("date", as_index=False)
            .agg({"num_messages": "sum", "daily_messages": "sum"})
            .sort_values("date")
        )

# --- Load top480 and top240 counts for *your bots* from bot_rank_history ---
        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT
                    r.date,
                    SUM(CASE WHEN r.rank IS NOT NULL AND r.rank <= 480 THEN 1 ELSE 0 END) AS top480,
                    SUM(CASE WHEN r.rank IS NOT NULL AND r.rank <= 240 THEN 1 ELSE 0 END) AS top240
                FROM bot_rank_history r
                JOIN bots b
                ON b.date = r.date
                AND b.bot_id = r.bot_id
                GROUP BY r.date
                ORDER BY r.date
            """)
            rank_counts = cur.fetchall()
        except sqlite3.OperationalError:
            rank_counts = []

        conn.close()

        top480_by_date = {str(d): int(t480 or 0) for (d, t480, t240) in rank_counts}
        top240_by_date = {str(d): int(t240 or 0) for (d, t480, t240) in rank_counts}



        # --- Build timeline points ---
        points = []
        for _, row in totals.iterrows():
            date_str = str(row["date"])
            points.append({
                "date": date_str,
                "total": int(row["num_messages"]),
                "daily": int(row["daily_messages"]),
                "top480": top480_by_date.get(date_str, 0),
                "top240": top240_by_date.get(date_str, 0),  # <<<<<< REQUIRED
            })
            
        # --- Timeframe totals (for KPI) ---
        tf_total = 0
        tf_bots = 0

        # Determine cutoff date
        today = datetime.now().date()

        if timeframe == "7day":
            cutoff = today - timedelta(days=7)
        elif timeframe == "30day":
            cutoff = today - timedelta(days=30)
        elif timeframe == "current_month":
            cutoff = today.replace(day=1)
        else:
            cutoff = None  # All time

        # Compute timeframe total messages
        if points:
            tf_total = points[-1]["total"] - points[0]["total"]

        # Compute bots created in timeframe
        if cutoff:
            tf_bots = dfc[dfc["created_at"].dt.date >= cutoff]["bot_id"].nunique()
        else:
            tf_bots = dfc["bot_id"].nunique()


        return jsonify({
            "points": points,
            "tf_total": tf_total,
            "tf_bots": tf_bots
        })

    @app.route("/reauth", methods=["POST"])
    def reauth():
        from core import (
            capture_auth_credentials,
            save_auth_credentials,
            take_snapshot,
            AUTH_FILE,
        )
        import core
        import os

        safe_log("Reauth requested by user…")

        # ✔ FORCE reauth by deleting saved token BEFORE testing ANYTHING
        try:
            if os.path.exists(AUTH_FILE):
                os.remove(AUTH_FILE)
                safe_log("Deleted saved auth credentials — forcing new login.")
        except Exception as e:
            safe_log(f"Error deleting auth credentials: {e}")

        try:
            # Now this WILL open Playwright because no credentials exist
            access_token, guest_userid, refresh_token, expires_at, client_id = (
                capture_auth_credentials()
            )

            if not access_token or not guest_userid:
                raise RuntimeError("capture_auth_credentials returned incomplete data.")

            save_auth_credentials(
                access_token,
                guest_userid,
                refresh_token,
                expires_at,
                client_id
            )

            core.AUTH_REQUIRED = False
            safe_log("Reauth successful — taking snapshot")
            take_snapshot({"manual": True}, verbose=True)

            return jsonify({"success": True})

        except Exception as e:
            core.AUTH_REQUIRED = True
            safe_log(f"Reauth failed: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
