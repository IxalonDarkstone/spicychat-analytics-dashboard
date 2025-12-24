import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .config import DATABASE
from .logging_utils import safe_log
from .typesense_client import multi_search_request, fetch_typesense_created_at_for_bot_ids
from .bots import normalize_avatar_url


# ------------------ helpers ------------------

def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()

def normalize_created_at(v) -> str:
    """
    Normalize created_at into ISO string.
    Handles ISO strings or epoch seconds/ms.
    """
    if v is None or v == "":
        return ""
    try:
        if isinstance(v, (int, float)):
            ts = float(v)
            if ts > 10_000_000_000:  # ms
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        s = str(v).strip()
        if not s:
            return ""
        # normalize Z to +00:00 if present
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # if it already looks ISO-ish, keep it
        if "T" in s:
            return s
        # best-effort parse
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return ""


# ------------------ schema ------------------

def ensure_author_tables():
    conn = sqlite3.connect(DATABASE)
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
            PRIMARY KEY (author, bot_id)
        )
    """)

    conn.commit()
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

def _upsert_author_map(author: str, bot_ids: List[str]):
    if not bot_ids:
        return
    ensure_author_tables()
    now = _utc_now_iso()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()

    rows = [(author, bid, now, now) for bid in bot_ids]
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

def _insert_bot_static(rows: List[Tuple[str, str, str, str, str, str, str]]):
    """
    rows: (bot_id, name, title, tags_json, avatar_url, created_at, fetched_at)
    Insert only if bot_id not already present (static).
    """
    if not rows:
        return
    ensure_author_tables()
    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR IGNORE INTO bot_static
        (bot_id, bot_name, bot_title, tags_json, avatar_url, created_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
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
                "include_fields": "character_id,name,title,tags,avatar_url,creator_username",
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

            out[cid] = {
                "bot_id": cid,
                "name": (doc.get("name") or "").strip(),
                "title": doc.get("title") or "",
                "tags": doc.get("tags") or [],
                "avatar_url": normalize_avatar_url(doc.get("avatar_url") or ""),
            }

    return out


# ------------------ public API used by routes/snapshot ------------------

def refresh_single_author_snapshot(stamp: str, author: str) -> int:
    """
    Incremental refresh:
      - Query Typesense to discover current bot ids for author (minimal fields)
      - Compare to DB author_bot_map -> identify NEW ids
      - For NEW ids only:
          - fetch static fields once
          - fetch created_at once (from collection that has it)
          - insert into bot_static (INSERT OR IGNORE)
          - insert into author_bot_map (UPSERT)
    Returns number of NEW bots added for this author.
    """
    ensure_author_tables()
    author = (author or "").strip()
    if not author:
        return 0

    current_ids = fetch_typesense_bot_ids_by_author(author)
    if not current_ids:
        safe_log(f"[Author Tracker] {author}: no bots found in Typesense.")
        return 0

    existing = _author_existing_bot_ids(author)
    new_ids = [bid for bid in current_ids if bid not in existing]

    # always update last_seen for existing + new (cheap)
    _upsert_author_map(author, current_ids)

    if not new_ids:
        safe_log(f"[Author Tracker] {author}: no new bots.")
        return 0

    # if bot already exists in bot_static globally, we do NOT re-fetch static details
    static_missing = _bot_static_missing_ids(new_ids)

    if not static_missing:
        safe_log(f"[Author Tracker] {author}: {len(new_ids)} new for author, but all already in bot_static.")
        return len(new_ids)

    details_map = fetch_typesense_bot_details_by_ids(static_missing)
    created_map = fetch_typesense_created_at_for_bot_ids(static_missing)

    fetched_at = _utc_now_iso()
    rows_to_insert = []

    for bid in static_missing:
        d = details_map.get(bid)
        if not d:
            continue

        ca_raw = created_map.get(bid)
        ca_norm = normalize_created_at(ca_raw) if ca_raw else ""

        rows_to_insert.append((
            bid,
            d.get("name") or "",
            d.get("title") or "",
            json.dumps(d.get("tags") or []),
            d.get("avatar_url") or "",
            ca_norm,
            fetched_at,
        ))

    _insert_bot_static(rows_to_insert)

    safe_log(
        f"[Author Tracker] {author}: added {len(new_ids)} new bots "
        f"({len(rows_to_insert)} inserted into bot_static)"
    )
    return len(new_ids)


def refresh_tracked_authors_snapshot(stamp: str):
    authors = get_tracked_authors()
    if not authors:
        return
    safe_log(f"[Author Tracker] incremental refresh for {len(authors)} tracked authors")
    for a in authors:
        try:
            refresh_single_author_snapshot(stamp, a)
        except Exception as e:
            safe_log(f"[Author Tracker] refresh failed for {a}: {e}")


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
        SELECT s.bot_id, s.bot_name, s.bot_title, s.tags_json, s.avatar_url, s.created_at
        FROM author_bot_map m
        JOIN bot_static s ON s.bot_id = m.bot_id
        WHERE m.author = ?
        """,
        (author,),
    )
    rows = cur.fetchall()
    conn.close()

    out = []
    for bot_id, name, title, tags_json, avatar_url, created_at in rows:
        try:
            tags = json.loads(tags_json) if tags_json else []
        except Exception:
            tags = []

        out.append({
            "bot_id": str(bot_id),
            "name": name or "",
            "title": title or "",
            "tags": tags,
            "avatar_url": normalize_avatar_url(avatar_url or ""),
            "created_at": created_at or "",
            "link": f"https://spicychat.ai/chat/{bot_id}",
        })
    return out


# Back-compat: your routes call this with (stamp, author). We ignore stamp now.
def load_author_bots_for_date(stamp: str, author: str) -> List[dict]:
    return load_author_bots(author)

def backfill_missing_created_at(limit: int = 500):
    """
    One-time backfill: for bots already in bot_static with missing created_at,
    fetch created_at from Typesense and update rows. Not used during normal refresh.
    """
    ensure_author_tables()

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bot_id
        FROM bot_static
        WHERE created_at IS NULL OR created_at = ''
        LIMIT ?
        """,
        (limit,),
    )
    ids = [str(r[0]) for r in cur.fetchall()]
    conn.close()

    if not ids:
        safe_log("[Author Tracker] backfill: no missing created_at rows.")
        return 0

    safe_log(f"[Author Tracker] backfill: fetching created_at for {len(ids)} bots")

    created_map = fetch_typesense_created_at_for_bot_ids(ids)  # <-- will log internally

    conn = sqlite3.connect(DATABASE)
    cur = conn.cursor()
    updated = 0
    for bid, raw in created_map.items():
        ca = normalize_created_at(raw)
        if not ca:
            continue
        cur.execute(
            "UPDATE bot_static SET created_at = ? WHERE bot_id = ? AND (created_at IS NULL OR created_at = '')",
            (ca, bid),
        )
        updated += cur.rowcount

    conn.commit()
    conn.close()

    safe_log(f"[Author Tracker] backfill: updated created_at for {updated} bots")
    return updated
