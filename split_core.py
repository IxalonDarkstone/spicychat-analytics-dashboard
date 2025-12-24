import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
CORE_PY = PROJECT_ROOT / "core.py"
LEGACY = PROJECT_ROOT / "core_legacy.py"
CORE_DIR = PROJECT_ROOT / "core"

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not CORE_PY.exists():
        die(f"ERROR: {CORE_PY} not found. Put this script next to your core.py.")

    # Avoid name conflict: package "core" cannot coexist with "core.py"
    if CORE_DIR.exists():
        die(f"ERROR: {CORE_DIR} already exists. Delete/rename it first.")

    # Backup core.py
    shutil.copy2(CORE_PY, LEGACY)
    print(f"Backed up core.py -> {LEGACY.name}")

    text = CORE_PY.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    # Locate section headers (from your current core.py)
    def find_header(s):
        for i, l in enumerate(lines):
            if l.strip() == s:
                return i
        die(f"ERROR: header not found: {s}")

    i_config = find_header("# ------------------ Config ------------------")
    i_ts_cfg = find_header("# ------------------ Typesense / Trending config ------------------")
    i_logging = find_header("# ------------------ Logging ------------------")
    i_fs = find_header("# ------------------ Basic filesystem helpers ------------------")
    i_meta = find_header("# ------------------ Snapshot metadata helpers ------------------")
    i_fmt = find_header("# ------------------ Formatting helpers ------------------")
    i_generic = find_header("# ------------------ Generic data helpers ------------------")
    i_auth = find_header("# ------------------ Auth & token management ------------------")
    i_pw = find_header("# ------------------ Capture auth via Playwright ------------------")
    i_db = find_header("# ------------------ Database ------------------")
    i_ts = find_header("# ------------------ Typesense client + trending ------------------")
    i_api = find_header("# ------------------ API capture ------------------")
    i_snap = find_header("# ------------------ Snapshot logic ------------------")
    i_df = find_header("# ------------------ Load + compute deltas ------------------")
    i_dash = find_header("# ------------------ Dashboard bots data ------------------")
    i_sched = find_header("# ------------------ Snapshot scheduler ------------------")

    # Top imports are before Config header
    imports_block = "\n".join(lines[:i_config]).strip() + "\n"

    def grab(a, b):
        return "\n".join(lines[a:b]).rstrip() + "\n"

    # Build modules by slicing existing core.py into coherent chunks.
    # Each module will import config/constants via "from .config import *"
    CORE_DIR.mkdir()
    (CORE_DIR / "__init__.py").write_text("", encoding="utf-8")

    def write_module(name: str, content: str):
        (CORE_DIR / name).write_text(content, encoding="utf-8")
        print(f"Wrote core/{name}")

    # config.py: imports + Config + Typesense/Trending config
    config_py = (
        imports_block
        + grab(i_config, i_logging)  # includes typesense/trending config section too
    )
    write_module("config.py", config_py)

    # logging_utils.py: setup_logging + safe_log
    logging_py = (
        imports_block
        + "from .config import *\n\n"
        + grab(i_logging, i_fs)
    )
    write_module("logging_utils.py", logging_py)

    # fs_utils.py: ensure_dirs + snapshot metadata helpers
    fs_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n\n"
        + grab(i_fs, i_fmt)  # includes snapshot metadata helpers too
    )
    write_module("fs_utils.py", fs_py)

    # helpers.py: formatting + generic data helpers
    helpers_py = (
        imports_block
        + "from .config import *\n\n"
        + grab(i_fmt, i_auth)
    )
    write_module("helpers.py", helpers_py)

    # auth.py: auth/token mgmt + playwright capture auth
    auth_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n"
        + "from .fs_utils import ensure_dirs\n\n"
        + grab(i_auth, i_db)  # includes capture auth via playwright section
    )
    write_module("auth.py", auth_py)

    # db.py: DB + tracked authors + cached maps + rank/rating history (all lives here in your core.py)
    db_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n\n"
        + grab(i_db, i_ts)
    )
    write_module("db.py", db_py)

    # typesense_client.py: typesense client + trending fetchers + author fetcher
    typesense_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n\n"
        + grab(i_ts, i_api)
    )
    write_module("typesense_client.py", typesense_py)

    # api_capture.py: capture_payloads helper
    api_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n\n"
        + grab(i_api, i_snap)
    )
    write_module("api_capture.py", api_py)

    # snapshot.py: sanitize_rows + take_snapshot (and snapshot-time caching)
    snapshot_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n"
        + "from .db import init_db\n"
        + "from .typesense_client import (\n"
        + "    fetch_typesense_tags_for_bot_ids,\n"
        + "    fetch_typesense_ratings_for_bot_ids,\n"
        + ")\n"
        + "from .db import (\n"
        + "    save_cached_tag_map,\n"
        + "    save_cached_rating_map,\n"
        + "    refresh_tracked_authors_snapshot,\n"
        + ")\n\n"
        + grab(i_snap, i_df)
    )
    write_module("snapshot.py", snapshot_py)

    # bots.py: load_history_df + compute_deltas + normalize_avatar_url + get_bots_data + cached map loaders
    bots_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n"
        + "from .db import init_db\n\n"
        + grab(i_df, i_sched)
    )
    write_module("bots.py", bots_py)

    # scheduler.py: snapshot_scheduler
    scheduler_py = (
        imports_block
        + "from .config import *\n"
        + "from .logging_utils import safe_log\n"
        + "from .snapshot import take_snapshot\n\n"
        + grab(i_sched, len(lines))
    )
    write_module("scheduler.py", scheduler_py)

    # Now generate __init__.py that re-exports everything your routes expect
    init_py = """\
# core package (auto-generated)
# This file re-exports symbols so existing imports like `from core import ...` continue to work.

from .config import *
from .logging_utils import setup_logging, safe_log
from .fs_utils import ensure_dirs, set_last_snapshot_time, get_last_snapshot_time
from .helpers import *
from .auth import *
from .db import *
from .typesense_client import *
from .api_capture import *
from .snapshot import *
from .bots import *
from .scheduler import *
"""
    (CORE_DIR / "__init__.py").write_text(init_py, encoding="utf-8")
    print("Wrote core/__init__.py")

    # Remove core.py (or rename) to avoid import conflict with package "core"
    CORE_PY.unlink()
    print("Removed core.py (now using core/ package).")
    print("\nDONE ✅")
    print("Next: restart Flask. If anything fails, you can revert by restoring core_legacy.py to core.py.")

if __name__ == "__main__":
    main()
