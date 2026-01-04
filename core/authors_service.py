# core/authors_service.py
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .config import DATABASE
from .logging_utils import safe_log
from .typesense_client import multi_search_request
from .bots import normalize_avatar_url


# ------------------ helpers ------------------

def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ------------------ schema ------------------

def ensure_author_tables():
    conn = sqlite3.connect(DATABASE)
    try:
        cur = conn.cursor()

        # tracked authors
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tracked_authors (
                author TEXT PRIMARY KEY,
                added_at TEXT
            )
        """)

        # static per bot (fetched once globally)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_static (
                bot_id TEXT PRIMARY KEY,
                bot_name TEXT,
                bot_title TEXT,
                tags_json TEXT,
                avatar_url TEXT,
                greeting TEXT,
                created_at TEXT,
                fetched_at TEXT
            )
        """)

        # mapping author -> bot_id (no date dimension; always current catalog)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS author_bot_map (
                author TEXT NOT NULL,
                bot_id TEXT NOT NULL,
                first_seen_at TEXT,
                last_seen_at TEXT,
                seen_at TEXT,
                PRIMARY KEY (author, bot_id)
            )
        """)

        # Back-compat migration: author_bot_map.seen_at
        cur.execute("PRAGMA table_info(author_bot_map)")
        cols = [r[1] for r in cur.fetchall()]
        if "seen_at" not in cols:
            cur.execute("ALTER TABLE author_bot_map ADD COLUMN seen_at TEXT")
            safe_log("[Author Tracker] Migrated author_bot_map: added seen_at")

        # Back-compat migration: bot_static.greeting
        cur.execute("PRAGMA table_info(bot_static)")
        bcols = [r[1] for r in cur.fetchall()]
        if "greeting" not in bcols:
            cur.execute("ALTER TABLE bot_static ADD COLUMN greeting TEXT")
            safe_log("[Author Tracker] Migrated bot_static: added greeting")
            
        # Back-compat migration: bot_static.created_at
        cur.execute("PRAGMA table_info(bot_static)")
        bcols = [r[1] for r in cur.fetchall()]
        if "created_at" not in bcols:
            cur.execute("ALTER TABLE bot_static ADD COLUMN created_at TEXT")
            safe_log("[Author Tracker] Migrated bot_static: added created_at")


        conn.commit()
    finally:
        conn.close()


# ------------------ tracked authors ------------------

def get_tracked_authors() -> List[str]:
    ensure_author_tables()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT author FROM tracked_authors ORDER BY author COLLATE NOCASE")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows if r and r[0]]


def add_tracked_author(author: str) -> bool:
    ensure_author_tables()
    author = (author or "").strip()
    if not author:
        return False
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO tracked_authors (author, added_at) VALUES (?, ?)",
        (author, _utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return True


def remove_tracked_author(author: str) -> bool:
    ensure_author_tables()
    author = (author or "").strip()
    if not author:
        return False
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("DELETE FROM tracked_authors WHERE author = ?", (author,))
    cur.execute("DELETE FROM author_bot_map WHERE author = ?", (author,))
    conn.commit()
    conn.close()
    return True


# ------------------ DB helpers ------------------
def _bot_static_needs_backfill(bot_ids: List[str]) -> List[str]:
    """
    Return subset of bot_ids that are missing from bot_static OR have blank created_at.
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return []

    ensure_author_tables()
    placeholders = ",".join("?" for _ in bot_ids)
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT bot_id, created_at
        FROM bot_static
        WHERE bot_id IN ({placeholders})
        """,
        bot_ids,
    )
    rows = cur.fetchall()
    conn.close()

    existing = {str(r[0]): (r[1] or "") for r in rows if r and r[0]}
    out = []
    for bid in bot_ids:
        if bid not in existing:
            out.append(bid)
        elif not str(existing.get(bid) or "").strip():
            out.append(bid)

    return out

def _author_existing_bot_ids(author: str) -> set:
    ensure_author_tables()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT bot_id FROM author_bot_map WHERE author = ?", (author,))
    rows = cur.fetchall()
    conn.close()
    return {str(r[0]) for r in rows if r and r[0]}


def _bot_static_missing_ids(bot_ids: List[str]) -> List[str]:
    """
    Return subset of bot_ids that are not in bot_static yet.
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return []

    ensure_author_tables()
    placeholders = ",".join("?" for _ in bot_ids)
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        f"SELECT bot_id FROM bot_static WHERE bot_id IN ({placeholders})",
        bot_ids,
    )
    existing = {str(r[0]) for r in cur.fetchall()}
    conn.close()
    return [bid for bid in bot_ids if bid not in existing]


def _upsert_author_map(author: str, bot_ids: List[str], first_seen_at: Optional[str] = None):
    if not bot_ids:
        return
    ensure_author_tables()
    now = _utc_now_iso()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    rows = [(author, bid, first_seen_at, now) for bid in bot_ids]
    cur.executemany(
        """
        INSERT INTO author_bot_map (author, bot_id, first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(author, bot_id) DO UPDATE SET
          last_seen_at=excluded.last_seen_at
        """,
        rows,
    )
    conn.commit()
    conn.close()


def _upsert_bot_static(rows: List[Tuple[str, str, str, str, str, str, str, str]]):
    """
    rows: (bot_id, name, title, tags_json, avatar_url, greeting, created_at, fetched_at)
    Upsert so we can backfill greeting for existing rows.
    """
    if not rows:
        return
    ensure_author_tables()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO bot_static
        (bot_id, bot_name, bot_title, tags_json, avatar_url, greeting, created_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_id) DO UPDATE SET
          bot_name=excluded.bot_name,
          bot_title=excluded.bot_title,
          tags_json=excluded.tags_json,
          avatar_url=excluded.avatar_url,
          greeting=COALESCE(excluded.greeting, bot_static.greeting),
          created_at=COALESCE(bot_static.created_at, excluded.created_at),
          fetched_at=excluded.fetched_at
        """,
        rows,
    )
    conn.commit()
    conn.close()


# ------------------ Typesense: incremental discovery ------------------

def fetch_typesense_bot_ids_by_author(author: str, max_pages: int = 60, per_page: int = 250) -> List[str]:
    """
    Cheap scan: only return character_id list for an author.
    """
    author = (author or "").strip()
    if not author:
        return []

    base_filter = (
        "application_ids:spicychat && tags:![Step-Family] && "
        "creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c',"
        "'kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] "
        "&& type:STANDARD"
    )
    filter_clause = f'{base_filter} && creator_username:="{author}"'

    out: List[str] = []
    page = 1

    while page <= max_pages:
        payload = {
            "searches": [{
                "collection": "public_characters_alias",
                "q": "*",
                "query_by": "creator_username,character_id",
                "filter_by": filter_clause,
                "include_fields": "character_id",
                "per_page": per_page,
                "page": page,
                "highlight_fields": "none",
                "enable_highlight_v1": False,
            }]
        }

        result = multi_search_request(payload)
        results = (result or {}).get("results", [])
        hits = results[0].get("hits", []) if results else []
        if not hits:
            break

        for h in hits:
            doc = (h or {}).get("document") or {}
            cid = str(doc.get("character_id") or "").strip()
            if cid:
                out.append(cid)

        if len(hits) < per_page:
            break
        page += 1

    # de-dupe, preserve order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def fetch_typesense_bot_details_by_ids(bot_ids: List[str]) -> Dict[str, dict]:
    """
    Fetch static fields for bot_ids from public_characters_alias.
    Returns dict keyed by bot_id.
    """
    bot_ids = [str(x) for x in bot_ids if x]
    if not bot_ids:
        return {}

    def _ms_to_iso(ms):
        try:
            ms_int = int(ms)
            return datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            return ""

    out: Dict[str, dict] = {}
    CHUNK = 80

    for i in range(0, len(bot_ids), CHUNK):
        chunk = bot_ids[i:i + CHUNK]
        ids_json = json.dumps(chunk)

        payload = {
            "searches": [{
                "collection": "public_characters_alias",
                "q": "*",
                "query_by": "character_id",
                "filter_by": f"character_id:={ids_json}",
                # ✅ include greeting here
                "include_fields": "character_id,name,title,tags,avatar_url,creator_username,greeting,createdAt",
                "per_page": len(chunk),
                "page": 1,
                "highlight_fields": "none",
                "enable_highlight_v1": False,
            }]
        }

        result = multi_search_request(payload)
        results = (result or {}).get("results", [])
        hits = results[0].get("hits", []) if results else []

        for h in hits:
            doc = (h or {}).get("document") or {}
            cid = str(doc.get("character_id") or "").strip()
            if not cid:
                continue
            
            created_iso = _ms_to_iso(doc.get("createdAt"))

            out[cid] = {
                "bot_id": cid,
                "name": (doc.get("name") or "").strip(),
                "title": doc.get("title") or "",
                "tags": doc.get("tags") or [],
                "avatar_url": normalize_avatar_url(doc.get("avatar_url") or ""),
                "greeting": doc.get("greeting") or "",
                "created_at": created_iso,
            }
            

    return out


# ------------------ public API used by routes/snapshot ------------------

def refresh_single_author_snapshot(stamp: str, author: str) -> int:
    ensure_author_tables()
    author = (author or "").strip()
    if not author:
        return 0

    current_ids = fetch_typesense_bot_ids_by_author(author)
    if not current_ids:
        safe_log(f"[Author Tracker] {author}: no bots found in Typesense.")
        return 0

    existing = _author_existing_bot_ids(author)

    # FIRST TIME: baseline existing catalog (NOT "new"), but still populate bot_static
    if not existing:
        _upsert_author_map(author, current_ids, first_seen_at=None)
        
        # Backfill static fields (including created_at) for any bots missing it
        backfill_ids = _bot_static_needs_backfill(current_ids)
        if backfill_ids:
            details_map = fetch_typesense_bot_details_by_ids(backfill_ids)
            fetched_at = _utc_now_iso()
            rows_to_upsert = []
            for bid in backfill_ids:
                d = details_map.get(bid)
                if not d:
                    continue
                rows_to_upsert.append((
                    bid,
                    d.get("name") or "",
                    d.get("title") or "",
                    json.dumps(d.get("tags") or []),
                    d.get("avatar_url") or "",
                    d.get("greeting") or "",
                    d.get("created_at") or "",
                    fetched_at,
                ))
            _upsert_bot_static(rows_to_upsert)
            safe_log(f"[Author Tracker] {author}: backfilled static for {len(rows_to_upsert)} bots.")

        # baseline: we still want static info (now including greeting)
        details_map = fetch_typesense_bot_details_by_ids(current_ids)
        fetched_at = _utc_now_iso()
        rows_to_upsert = []
        for bid in current_ids:
            d = details_map.get(bid)
            if not d:
                continue
            rows_to_upsert.append((
                bid,
                d.get("name") or "",
                d.get("title") or "",
                json.dumps(d.get("tags") or []),
                d.get("avatar_url") or "",
                d.get("greeting") or "",
                d.get("created_at") or "",
                fetched_at,
            ))
        _upsert_bot_static(rows_to_upsert)

        safe_log(f"[Author Tracker] {author}: baselined {len(current_ids)} bots (not marked new).")
        return 0

    # NORMAL CASE: only truly new IDs get first_seen_at set
    new_ids = [bid for bid in current_ids if bid not in existing]

    # update last_seen for all
    _upsert_author_map(author, current_ids, first_seen_at=None)

    if not new_ids:
        safe_log(f"[Author Tracker] {author}: no new bots.")
        return 0

    # set first_seen_at for new ones
    _upsert_author_map(author, new_ids, first_seen_at=_utc_now_iso())

    # fetch + upsert static for new (and any missing)
    details_map = fetch_typesense_bot_details_by_ids(new_ids)
    fetched_at = _utc_now_iso()
    rows_to_upsert = []
    for bid in new_ids:
        d = details_map.get(bid)
        if not d:
            continue
        rows_to_upsert.append((
            bid,
            d.get("name") or "",
            d.get("title") or "",
            json.dumps(d.get("tags") or []),
            d.get("avatar_url") or "",
            d.get("greeting") or "",
            d.get("created_at") or "",
            fetched_at,
        ))
    _upsert_bot_static(rows_to_upsert)

    safe_log(
        f"[Author Tracker] {author}: added {len(new_ids)} new bots "
        f"({len(rows_to_upsert)} upserted into bot_static)"
    )
    return len(new_ids)

def refresh_tracked_authors_snapshot(stamp: str) -> int:
    """
    Back-compat wrapper used by core/snapshot.py.

    Refreshes all tracked authors using Typesense and returns the total number
    of *new* bots discovered across all authors.
    """
    ensure_author_tables()

    authors = get_tracked_authors()
    if not authors:
        return 0

    total_new = 0
    for a in authors:
        try:
            total_new += int(refresh_single_author_snapshot(stamp, a) or 0)
        except Exception as e:
            safe_log(f"[Author Tracker] Bulk refresh failed for {a}: {e}")

    return total_new

def load_author_bots(author: str) -> List[dict]:
    """
    Load author bots from DB (author_bot_map join bot_static).
    """
    ensure_author_tables()
    author = (author or "").strip()
    if not author:
        return []

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.bot_id, s.bot_name, s.bot_title, s.tags_json, s.avatar_url, s.greeting, s.created_at,
               m.first_seen_at, m.seen_at
        FROM author_bot_map m
        JOIN bot_static s ON s.bot_id = m.bot_id
        WHERE m.author = ?
        """,
        (author,),
    )

    rows = cur.fetchall()
    conn.close()

    out = []
    for bot_id, name, title, tags_json, avatar_url, greeting, created_at, first_seen_at, seen_at in rows:
        try:
            tags = json.loads(tags_json) if tags_json else []
        except Exception:
            tags = []

        from zoneinfo import ZoneInfo

        CHI = ZoneInfo("America/Chicago")
        today_start_utc = datetime.now(CHI).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)

        def _parse_iso(dt_str: str):
            try:
                return datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
            except Exception:
                return None

        created_dt = _parse_iso(created_at or "")
        is_new = bool(created_dt and created_dt >= today_start_utc and not (seen_at or "").strip())


        out.append({
            "bot_id": str(bot_id),
            "name": name or "",
            "title": title or "",
            "tags": tags,
            "avatar_url": normalize_avatar_url(avatar_url or ""),
            "link": f"https://spicychat.ai/chat/{bot_id}",
            "created_at": created_at or "",
            "first_seen_at": first_seen_at,
            "seen_at": seen_at,
            "is_new": is_new,
            # ✅ include greeting so search can match it without extra calls
            "greeting": greeting or "",
        })
    return out


# Back-compat: your routes call this with (stamp, author). We ignore stamp now.
def load_author_bots_for_date(stamp: str, author: str) -> List[dict]:
    return load_author_bots(author)


def get_bot_greeting(bot_id: str) -> str:
    """
    Used by the popup endpoint. Returns greeting from DB, and if missing,
    fetches from Typesense and backfills.
    """
    bot_id = (bot_id or "").strip()
    if not bot_id:
        return ""

    ensure_author_tables()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute("SELECT greeting FROM bot_static WHERE bot_id = ?", (bot_id,))
    row = cur.fetchone()
    conn.close()

    if row and row[0]:
        return row[0] or ""

    # backfill from Typesense
    details = fetch_typesense_bot_details_by_ids([bot_id]).get(bot_id) or {}
    greeting = details.get("greeting") or ""
    if greeting:
        _upsert_bot_static([(
            bot_id,
            details.get("name") or "",
            details.get("title") or "",
            json.dumps(details.get("tags") or []),
            details.get("avatar_url") or "",
            greeting,
            details.get("created_at") or "",
            _utc_now_iso(),
        )])

    return greeting


def mark_bot_seen(bot_id: str):
    bot_id = (bot_id or "").strip()
    if not bot_id:
        return
    ensure_author_tables()
    now = _utc_now_iso()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        "UPDATE author_bot_map SET seen_at = ? WHERE bot_id = ?",
        (now, bot_id),
    )
    conn.commit()
    conn.close()


def mark_all_seen(author: Optional[str] = None):
    ensure_author_tables()
    now = _utc_now_iso()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    if author and author.strip():
        cur.execute(
            "UPDATE author_bot_map SET seen_at = ? WHERE author = ? AND (seen_at IS NULL OR seen_at = '')",
            (now, author.strip()),
        )
    else:
        cur.execute(
            "UPDATE author_bot_map SET seen_at = ? WHERE (seen_at IS NULL OR seen_at = '')",
            (now,),
        )

    conn.commit()
    conn.close()
