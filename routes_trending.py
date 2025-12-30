# routes_trending.py
from flask import render_template, request
from core import (
    fetch_typesense_top_bots,
    AVATAR_BASE_URL,
    safe_log,
    load_history_df,
    get_last_snapshot_time,
)
import pandas as pd

def register_trending_routes(app):
    @app.route("/trending")
    def trending():
        """
        Show which of *your* bots appear:
            - in Typesense pages 1–5 (Top 240)  → "Top 5" section
            - in Typesense pages 6–10 (Top 241–480) → "Top 10" section
        Each bot shows its global rank (#1–480) and uses the same avatar CDN
        logic as the main dashboard.
        """
        per_page = 48

        df_raw = load_history_df()

        # Always define this up front so it's available in every return path
        if df_raw.empty:
            my_bots_count = 0
        else:
            my_bots_count = df_raw["bot_id"].nunique()

        # First try cache; if it looks empty, force a live fetch
        ts_map = fetch_typesense_top_bots(max_pages=10, use_cache=True)
        if not ts_map:
            safe_log("Trending: cached Typesense results are empty, forcing fresh fetch without cache")
            ts_map = fetch_typesense_top_bots(max_pages=10,  use_cache=False)

        ts_list = list(ts_map.values())

        # Split into pages 1–5 and 6–10 based on the 'page' field we stored
        top5_segment = [b for b in ts_list if 1 <= int(b.get("page", 0)) <= 5]
        top10_segment = [b for b in ts_list if 6 <= int(b.get("page", 0)) <= 10]

        top5_total = len(top5_segment)
        top10_total = len(top10_segment)

        # If Typesense gave us nothing at all, show a clear error
        if top5_total == 0 and top10_total == 0:
            msg = (
                "Typesense returned 0 bots for pages 1–10. "
                "This usually means your bearer token, Typesense key, or filters are invalid, "
                "or Typesense changed its schema. Try taking a fresh snapshot or deleting "
                "data/public_bots_home_all.json so it can be regenerated."
            )
            safe_log(msg)
            return render_template(
                "trending.html",
                error=msg,
                top5=[],
                top10=[],
                top5_total=top5_total,
                top10_total=top10_total,
                my_bots_count=my_bots_count,
                last_snapshot=get_last_snapshot_time(),
            )

        # Latest snapshot date for "your bots" data
        latest_date = None
        if not df_raw.empty:
            latest_date = sorted(df_raw["date"].unique(), reverse=True)[0]

        # Treat all rows in DB as "your bots" (the API already returns your bots)
        my_bots_df = df_raw
        my_bots = {}
        if not my_bots_df.empty:
            latest_rows = my_bots_df[my_bots_df["date"] == latest_date] if latest_date else my_bots_df
            for _, r in latest_rows.iterrows():
                my_bots[str(r["bot_id"])] = {
                    "bot_id": r["bot_id"],
                    "bot_name": r["bot_name"],
                    "num_messages": int(r["num_messages"]),
                    "created_at": (r["created_at"].strftime("%Y-%m-%d") if pd.notnull(r["created_at"]) else ""),
                    # optional: we could keep DB avatar here if you want a fallback
                    "avatar_url": r.get("avatar_url", ""),
                }

        def build_list(segment):
            results = []
            for info in segment:
                cid = info.get("character_id")
                if not cid:
                    continue
                if cid in my_bots:
                    # Normalize avatar URL to match main dashboard behavior
                    raw = info.get("avatar_url") or my_bots[cid].get("avatar_url", "")
                    if raw:
                        filename = raw.split("/")[-1]
                        avatar_url = f"{AVATAR_BASE_URL}/{filename}"
                    else:
                        avatar_url = f"{AVATAR_BASE_URL}/default-avatar.png"

                    daily = int(info.get("num_messages_24h") or 0)
                    rank = int(info.get("rank") or 0)

                    results.append({
                        "bot_id": cid,
                        "name": info.get("name", my_bots[cid]["bot_name"]),
                        "link": info.get("link"),
                        "avatar_url": avatar_url,
                        "total_messages": int(info.get("num_messages") or my_bots[cid]["num_messages"]),
                        "daily_messages": daily,
                        "rank": rank,
                    })

            # Sort by rank so #1 is at the top; fallback huge number if rank==0
            results.sort(key=lambda x: x["rank"] or 999_999)
            return results

        top5_my = build_list(top5_segment)
        top10_my = build_list(top10_segment)

        safe_log(
            f"Trending page: TS pages1-5={top5_total}, pages6-10={top10_total}, "
            f"DB bots={my_bots_count}, my top5={len(top5_my)}, my top10={len(top10_my)}"
        )

        return render_template(
            "trending.html",
            error=None,
            top5=top5_my,
            top10=top10_my,
            top5_total=top5_total,
            top10_total=top10_total,
            my_bots_count=my_bots_count,
            last_snapshot=get_last_snapshot_time(),
        )

    @app.route("/global-trending")
    def global_trending():
        def _qs(name, default=""):
            return (request.args.get(name) or default).strip()

        sort_field = _qs("sort", "rank").lower()
        order = _qs("order", "asc").lower()
        and_raw = _qs("and", "")
        not_raw = _qs("not", "")
        q = _qs("q", "")
        active_tab = _qs("tab", "creators")
        author_filter = _qs("author", "")
        q = request.args.get("q", "").strip().lower()
        
        # --------------------------------------------
        # Persistent tab (creators or tags)
        # --------------------------------------------
        active_tab = request.args.get("tab", "creators")

        # --------------------------------------------
        # Fetch trending: filtered (female+nsfw) for grid
        # --------------------------------------------
        ts_map_filtered = fetch_typesense_top_bots(
            max_pages=10,
            use_cache=True,
            filter_female_nsfw=True
        )
        ts_list = list(ts_map_filtered.values())

        # --------------------------------------------
        # Fetch unfiltered for TAG sidebar
        # --------------------------------------------
        ts_map_all = fetch_typesense_top_bots(
            max_pages=10,
            use_cache=True,
            filter_female_nsfw=False
        )

        # --------------------------------------------
        # Sorting
        # --------------------------------------------
        sort_field = request.args.get("sort", "rank")
        order = request.args.get("order", "asc")
        reverse = (order == "desc")

        if sort_field == "author":
            ts_list.sort(key=lambda b: (b.get("creator_username") or "").lower(), reverse=reverse)
        elif sort_field == "messages":
            ts_list.sort(key=lambda b: int(b.get("num_messages") or 0), reverse=reverse)
        else:  # rank
            ts_list.sort(key=lambda b: int(b.get("rank") or 999999), reverse=reverse)

        # --------------------------------------------
        # AND / NOT TAG FILTERING
        # --------------------------------------------
        and_raw = request.args.get("and", "")
        not_raw = request.args.get("not", "")

        and_tags = [t.strip().lower() for t in and_raw.split(",") if t.strip()]
        not_tags = [t.strip().lower() for t in not_raw.split(",") if t.strip()]

        def tag_match(bot):
            bot_tags = [t.lower() for t in (bot.get("tags") or [])]

            # AND: must contain all
            for t in and_tags:
                if t not in bot_tags:
                    return False

            # NOT: must contain none
            for t in not_tags:
                if t in bot_tags:
                    return False

            return True

        if and_tags or not_tags:
            ts_list = [b for b in ts_list if tag_match(b)]

        # --------------------------------------------
        # Author filter (as-is)
        # --------------------------------------------
        author_filter = (request.args.get("author") or "").strip()
        if author_filter:
            af = author_filter.lower()
            ts_list = [b for b in ts_list if (b.get("creator_username") or "").strip().lower() == af]

            
        # --- Search filter (Name/Title/Tags) ---
        if q:
            def match(bot):
                name = (bot.get("name") or "").lower()
                title = (bot.get("title") or "").lower()
                tags = bot.get("tags") or []
                tags_blob = " ".join([str(t).lower() for t in tags])
                return (q in name) or (q in title) or (q in tags_blob)

            ts_list = [b for b in ts_list if match(b)]

        # --------------------------------------------
        # Pagination
        # --------------------------------------------
        PER_PAGE = 48
        page = int(request.args.get("page", 1))

        total_pages = max((len(ts_list) - 1) // PER_PAGE + 1, 1)
        page = max(1, min(page, total_pages))

        start = (page - 1) * PER_PAGE
        end = start + PER_PAGE

        page_items = []
        for bot in ts_list[start:end]:
            raw = bot.get("avatar_url", "")
            if raw:
                filename = raw.split("/")[-1]
                bot["avatar_url"] = f"{AVATAR_BASE_URL}/{filename}"
            else:
                bot["avatar_url"] = f"{AVATAR_BASE_URL}/default-avatar.png"
            page_items.append(bot)

        # --------------------------------------------
        # Creator leaderboard (filtered trending only)
        # --------------------------------------------
        creator_counts = {}
        for bot in ts_list:
            creator = bot.get("creator_username", "")
            if creator:
                creator_counts[creator] = creator_counts.get(creator, 0) + 1

        creators_sorted = sorted(
            [{"creator": k, "count": v} for k, v in creator_counts.items()],
            key=lambda x: x["count"],
            reverse=True
        )
        # --------------------------------------------
        # Tag leaderboard (unfiltered trending)
        # --------------------------------------------
        tag_counts = {}
        for bot in ts_list:
            for t in bot.get("tags", []) or []:
                tag_counts[t] = tag_counts.get(t, 0) + 1

        tags_sorted = sorted(
            [{"tag": t, "count": c} for t, c in tag_counts.items()],
            key=lambda x: x["count"], reverse=True
        )

        return render_template(
            "global_trending.html",
            bots=page_items,
            creators=creators_sorted,
            tags=tags_sorted,
            page=page,
            total_pages=total_pages,
            sort_field=sort_field,
            order=order,
            author_filter=author_filter,
            and_raw=and_raw,
            not_raw=not_raw,
            and_tags=and_tags,
            not_tags=not_tags,
            active_tab=active_tab,
            q=q,
            ts_total=len(ts_map_filtered),
            filtered_total=len(ts_list),
            last_snapshot=get_last_snapshot_time(),
        )