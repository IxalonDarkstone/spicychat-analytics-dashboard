# routes_authors.py
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import List, Tuple

from flask import render_template, request, redirect, url_for

from core import (
    safe_log,
    load_history_df,
    get_tracked_authors,
    add_tracked_author,
    remove_tracked_author,
    load_author_bots_for_date,
    refresh_single_author_snapshot,
    ensure_author_tables,
    get_last_snapshot_time,
)
from core.config import DATABASE
from core.authors_service import mark_bot_seen, mark_all_seen, fetch_typesense_bot_ids_by_author

ALL_KEY = "__ALL__"


def _latest_stamp_or_today() -> str:
    df_raw = load_history_df()
    if not df_raw.empty and "date" in df_raw.columns:
        latest_date = sorted(df_raw["date"].unique())[-1]
        return str(latest_date)
    return datetime.now().strftime("%Y-%m-%d")


def _parse_csv_lower(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def _filter_by_and_not_tags(bots: List[dict], and_tags: List[str], not_tags: List[str]) -> List[dict]:
    if not and_tags and not not_tags:
        return bots

    and_set = set([t.lower() for t in and_tags])
    not_set = set([t.lower() for t in not_tags])

    out = []
    for b in bots:
        bt = set([str(t).lower() for t in (b.get("tags") or [])])

        # AND: must contain all
        if and_set and not and_set.issubset(bt):
            continue

        # NOT: must contain none
        if not_set and (bt & not_set):
            continue

        out.append(b)
    return out


def _filter_by_query(bots: List[dict], q: str) -> List[dict]:
    q = (q or "").strip().lower()
    if not q:
        return bots
    out = []
    for b in bots:
        name = (b.get("name") or "").lower()
        title = (b.get("title") or "").lower()
        tags = " ".join([str(t) for t in (b.get("tags") or [])]).lower()
        author = (b.get("author") or "").lower()
        if q in f"{name} {title} {tags} {author}":
            out.append(b)
    return out


def _tag_counts(bots: List[dict]) -> List[dict]:
    from collections import Counter

    c = Counter()
    for b in bots:
        for t in (b.get("tags") or []):
            if t:
                c[str(t)] += 1

    # total descending, then alpha
    items = sorted(c.items(), key=lambda x: (-x[1], x[0].lower()))
    return [{"tag": k, "count": v} for k, v in items]


def _load_all_tracked_bots(latest_stamp: str, authors: List[str]) -> List[dict]:
    all_bots: List[dict] = []
    seen = set()
    for a in authors:
        bots = load_author_bots_for_date(latest_stamp, a)
        for b in bots:
            bot_id = str(b.get("bot_id") or "")
            if not bot_id or bot_id in seen:
                continue
            seen.add(bot_id)
            b2 = dict(b)
            b2["author"] = a
            all_bots.append(b2)
    return all_bots


def _sort_bots(bots: List[dict], sort_field: str, order: str) -> Tuple[str, str]:
    sort_field = (sort_field or "date").strip().lower()
    order = (order or "desc").strip().lower()

    if sort_field not in ("date", "name"):
        sort_field = "date"
    if order not in ("asc", "desc"):
        order = "desc"

    reverse = (order == "desc")

    # Secondary sort first (stable)
    if sort_field == "name":
        bots.sort(key=lambda b: (b.get("name") or "").lower(), reverse=reverse)
    else:
        # date == first_seen_at. Empty string sorts last in desc.
        bots.sort(key=lambda b: (b.get("first_seen_at") or ""), reverse=reverse)

    # Primary pin: unseen/new at the top
    bots.sort(key=lambda b: 0 if b.get("is_new") else 1)

    return sort_field, order


def _redirect_authors_page_from_form():
    author = (request.form.get("author") or "").strip()
    q = (request.form.get("q") or "").strip()
    and_raw = (request.form.get("and") or "").strip()
    not_raw = (request.form.get("not") or "").strip()
    sort_field = (request.form.get("sort") or "").strip()
    order = (request.form.get("order") or "").strip()

    kwargs = {}
    if author:
        kwargs["author"] = author   # ✅ keep selection
    if q:
        kwargs["q"] = q
    if and_raw:
        kwargs["and"] = and_raw
    if not_raw:
        kwargs["not"] = not_raw
    if sort_field:
        kwargs["sort"] = sort_field
    if order:
        kwargs["order"] = order

    return redirect(url_for("authors_page", **kwargs))



def register_author_routes(app):
    @app.route("/authors", methods=["GET"])
    def authors_page():
        authors = get_tracked_authors()

        q = (request.args.get("q") or "").strip()
        and_raw = (request.args.get("and") or "").strip()
        not_raw = (request.args.get("not") or "").strip()
        and_tags = _parse_csv_lower(and_raw)
        not_tags = _parse_csv_lower(not_raw)

        sort_field = (request.args.get("sort") or "date").strip().lower()
        order = (request.args.get("order") or "desc").strip().lower()

        latest_stamp = _latest_stamp_or_today()

        # ✅ RESTORE AUTHOR SELECTION
        selected_author = (request.args.get("author") or ALL_KEY).strip()
        if not selected_author:
            selected_author = ALL_KEY

        if selected_author == ALL_KEY:
            base_bots = _load_all_tracked_bots(latest_stamp, authors) if authors else []
        else:
            base_bots = load_author_bots_for_date(latest_stamp, selected_author)

        # ✅ tags reflect current selection
        tags_list = _tag_counts(base_bots)

        bots = _filter_by_and_not_tags(base_bots, and_tags, not_tags)
        bots = _filter_by_query(bots, q)
        sort_field, order = _sort_bots(bots, sort_field, order)

        add_error = (request.args.get("add_error") or "").strip()
        add_author = (request.args.get("add_author") or "").strip()

        return render_template(
            "authors.html",
            authors=authors,
            selected_author=selected_author,  # ✅ IMPORTANT
            bots=bots,
            latest_stamp=latest_stamp,
            q=q,
            and_raw=and_raw,
            not_raw=not_raw,
            and_tags=and_tags,
            not_tags=not_tags,
            tags_list=tags_list,
            sort_field=sort_field,
            order=order,
            add_error=add_error,
            add_author=add_author,
            last_snapshot=get_last_snapshot_time(),
        )

    @app.get("/go-bot/<bot_id>")
    def go_bot(bot_id):
        mark_bot_seen(bot_id)
        return redirect(f"https://spicychat.ai/chat/{bot_id}")

    @app.post("/authors/mark-all-seen")
    def authors_mark_all_seen():
        author = (request.form.get("author") or "").strip()
        if author and author != ALL_KEY:
            mark_all_seen(author)
        else:
            mark_all_seen(None)
        return _redirect_authors_page_from_form()


    @app.route("/authors/add", methods=["POST"])
    def authors_add():
        author = (request.form.get("author") or "").strip()
        if not author:
            return redirect(url_for("authors_page"))

        # existence check: fetch at most 1 bot id
        try:
            ids = fetch_typesense_bot_ids_by_author(author, max_pages=1, per_page=1)
        except Exception as e:
            safe_log(f"[Author Tracker] add check failed for {author}: {e}")
            ids = []

        if not ids:
            return redirect(
                url_for(
                    "authors_page",
                    add_error="User has 0 bots. Please check spelling.",
                    add_author=author,
                )
            )

        add_tracked_author(author)
        safe_log(f"Author Tracker: added {author}")

        # ✅ Immediately refresh just this author so cards appear right away
        try:
            stamp = _latest_stamp_or_today()
            n = refresh_single_author_snapshot(stamp, author)
            safe_log(f"Author Tracker: auto-refreshed {author} ({n} new) for {stamp}")
        except Exception as e:
            safe_log(f"Author Tracker: auto-refresh failed for {author}: {e}")

        # Optional but nice: select the newly-added author immediately
        return redirect(url_for("authors_page", author=author))

    @app.route("/authors/remove", methods=["POST"])
    def authors_remove():
        author = (request.form.get("author") or "").strip()
        if author:
            remove_tracked_author(author)
            safe_log(f"Author Tracker: removed {author}")
        return redirect(url_for("authors_page"))

    @app.route("/authors/refresh", methods=["POST"])
    def authors_refresh():
        stamp = _latest_stamp_or_today()
        authors = get_tracked_authors()
        for a in authors:
            try:
                n = refresh_single_author_snapshot(stamp, a)
                safe_log(f"Author Tracker: refreshed {a} ({n} new) for {stamp}")
            except Exception as e:
                safe_log(f"Author Tracker: refresh failed for {a}: {e}")

        return _redirect_authors_page_from_form()

    @app.route("/api/author-new-counts", methods=["GET"])
    def api_author_new_counts():
        """
        DB-backed unseen counts:
          unseen = first_seen_at IS NOT NULL AND seen_at IS NULL
        """
        ensure_author_tables()

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()
        cur.execute("""
            SELECT author,
                   SUM(CASE WHEN first_seen_at IS NOT NULL AND (seen_at IS NULL OR seen_at = '') THEN 1 ELSE 0 END) AS unseen
            FROM author_bot_map
            GROUP BY author
        """)
        rows = cur.fetchall()
        conn.close()

        by_author = {r[0]: int(r[1] or 0) for r in rows if r and r[0]}
        total = sum(by_author.values())
        return {"total": total, "by_author": by_author}
