from flask import render_template, request, redirect, url_for
from datetime import datetime
from core import (
    safe_log,
    load_history_df,
    get_tracked_authors,
    add_tracked_author,
    remove_tracked_author,
    load_author_bots_for_date,
    refresh_single_author_snapshot,
    ensure_author_tables,
)
import sqlite3
from core.config import DATABASE
import json

ALL_KEY = "__ALL__"

def _latest_stamp_or_today():
    df_raw = load_history_df()
    if not df_raw.empty and "date" in df_raw.columns:
        latest_date = sorted(df_raw["date"].unique())[-1]
        return str(latest_date)
    return datetime.now().strftime("%Y-%m-%d")

def _parse_tags(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]

def _filter_by_tags(bots, required_tags):
    if not required_tags:
        return bots
    req = set(required_tags)
    out = []
    for b in bots:
        bt = set(b.get("tags") or [])
        if req.issubset(bt):
            out.append(b)
    return out

def _filter_by_query(bots, q: str):
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

def _tag_counts(bots):
    from collections import Counter
    c = Counter()
    for b in bots:
        for t in (b.get("tags") or []):
            if t:
                c[t] += 1
    items = sorted(c.items(), key=lambda x: (-x[1], x[0].lower()))
    return [{"tag": k, "count": v} for k, v in items]

def _load_all_tracked_bots(latest_stamp: str, authors: list[str]):
    all_bots = []
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

def register_author_routes(app):
    @app.route("/authors", methods=["GET"])
    def authors_page():
        authors = get_tracked_authors()

        selected = (request.args.get("author") or "").strip()
        q = (request.args.get("q") or "").strip()
        tags_raw = (request.args.get("tags") or "").strip()
        tags = _parse_tags(tags_raw)

        latest_stamp = _latest_stamp_or_today()

        show_all = (selected == "" or selected == ALL_KEY)
        if show_all:
            selected_author = ALL_KEY
            base_bots = _load_all_tracked_bots(latest_stamp, authors) if authors else []
        else:
            selected_author = selected
            base_bots = load_author_bots_for_date(latest_stamp, selected_author) if selected_author else []

        tags_list = _tag_counts(base_bots)

        bots = _filter_by_tags(base_bots, tags)
        bots = _filter_by_query(bots, q)

        return render_template(
            "authors.html",
            authors=authors,
            selected_author=selected_author,
            bots=bots,
            latest_stamp=latest_stamp,
            q=q,
            tags=tags_raw,
            tags_list=tags_list,
            all_key=ALL_KEY,
        )

    @app.route("/authors/add", methods=["POST"])
    def authors_add():
        author = (request.form.get("author") or "").strip()
        if author:
            add_tracked_author(author)
            safe_log(f"Author Tracker: added {author}")
        return redirect(url_for("authors_page"))

    @app.route("/authors/remove", methods=["POST"])
    def authors_remove():
        author = (request.form.get("author") or "").strip()
        if author:
            remove_tracked_author(author)
            safe_log(f"Author Tracker: removed {author}")
        return redirect(url_for("authors_page"))

    @app.route("/authors/refresh", methods=["POST"])
    def authors_refresh():
        author = (request.form.get("author") or "").strip()
        q = (request.form.get("q") or "").strip()
        tags_raw = (request.form.get("tags") or "").strip()

        stamp = _latest_stamp_or_today()

        if not author or author == ALL_KEY:
            authors = get_tracked_authors()
            for a in authors:
                try:
                    n = refresh_single_author_snapshot(stamp, a)
                    safe_log(f"Author Tracker: refreshed {a} ({n} bots) for {stamp}")
                except Exception as e:
                    safe_log(f"Author Tracker: refresh failed for {a}: {e}")
            return redirect(url_for("authors_page", q=q, tags=tags_raw))

        try:
            n = refresh_single_author_snapshot(stamp, author)
            safe_log(f"Author Tracker: refreshed {author} ({n} bots) for {stamp}")
        except Exception as e:
            safe_log(f"Author Tracker: refresh failed for {author}: {e}")

        return redirect(url_for("authors_page", author=author, q=q, tags=tags_raw))
    
    @app.route("/api/author-new-counts", methods=["GET"])
    def api_author_new_counts():
        """
        Returns counts of newly discovered author bots that are still "unseen".

        Query params:
        seen=<json>  e.g. {"ixalon":"2025-12-24T02:00:00+00:00","other":"..."}
        """
        ensure_author_tables()

        seen_raw = (request.args.get("seen") or "").strip()
        try:
            seen_map = json.loads(seen_raw) if seen_raw else {}
            if not isinstance(seen_map, dict):
                seen_map = {}
        except Exception:
            seen_map = {}

        def seen_for(author: str) -> str:
            v = seen_map.get(author)
            return v if isinstance(v, str) and v else "1970-01-01T00:00:00+00:00"

        conn = sqlite3.connect(DATABASE)
        cur = conn.cursor()

        # Unseen counts by author + each author max unseen stamp
        cur.execute("""
            SELECT author, first_seen_at
            FROM author_bot_map
            WHERE first_seen_at IS NOT NULL
            ORDER BY first_seen_at DESC
        """)
        rows = cur.fetchall() or []
        conn.close()

        by_author = {}
        by_author_max = {}
        total = 0

        for author, first_seen_at in rows:
            if not author or not first_seen_at:
                continue
            cutoff = seen_for(author)
            if first_seen_at > cutoff:
                by_author[author] = by_author.get(author, 0) + 1
                total += 1
                # rows are sorted desc, so first time we see author is the max unseen
                if author not in by_author_max:
                    by_author_max[author] = first_seen_at

        # overall max unseen (optional, handy)
        max_stamp = ""
        if by_author_max:
            max_stamp = max(by_author_max.values())

        return {
            "total": int(total),
            "max_stamp": max_stamp,
            "by_author": by_author,
            "by_author_max": by_author_max,
        }