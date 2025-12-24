# core.py
import os
import sys
import re
import time
import logging
import requests
import json
from pathlib import Path
from datetime import datetime, timedelta
import sqlite3
import numpy as np
import pandas as pd
import pytz
import threading
from playwright.sync_api import sync_playwright
import urllib.parse as urlparse
from .config import *

# ------------------ Formatting helpers ------------------
def fmt_commas(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return ""


def fmt_delta_commas(n):
    try:
        n = int(n)
        sign = "+" if n >= 0 else "-"
        return f"{sign}{abs(n):,}"
    except Exception:
        return ""
def rating_to_pct(r):
    """Convert rating_score (0-1 or 0-5) into a percent (0-100)."""
    try:
        r = float(r)
    except Exception:
        return None

    if r < 0:
        return None

    # heuristic: <=1 is already ratio; otherwise assume 0-5 stars
    if r <= 1.0:
        pct = r * 100.0
    else:
        pct = (r / 5.0) * 100.0

    # clamp
    pct = max(0.0, min(100.0, pct))
    return pct

# ------------------ Generic data helpers ------------------
def coerce_int(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    m = re.search(r"\d+", str(x))
    return int(m.group(0).replace(",", "")) if m else None


def pick(d, *keys, default=""):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            return d[k]
    return default


def get_name(d): return pick(d, "name", "characterName", "displayName", "botTitle", default="")
def get_title(d): return pick(d, "title", "botTitle", "description", default="")
def get_id(d): return pick(d, "id", "slug", "uuid", "characterId", "_id", default="")
def get_created_at(d): return pick(d, "createdAt", "created_at", default="")
def get_avatar_url(d): return pick(d, "avatarUrl", "avatar_url", default="")


def get_num_messages(d):
    if "num_messages" in d and d["num_messages"] is not None:
        return coerce_int(d["num_messages"])
    for k in (
        "messageCount",
        "message_count",
        "messages",
        "interactions",
        "numMessages",
    ):
        if k in d and d[k] is not None:
            return coerce_int(d[k])
    for path in (
        ("stats", "messageCount"),
        ("stats", "messages"),
        ("usage", "messages"),
        ("metrics", "messages"),
        ("analytics", "messages"),
    ):
        cur, ok = d, True
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok:
            return coerce_int(cur)
    return None


def flatten_items(obj, out):
    if isinstance(obj, dict):
        if any(
            k in obj
            for k in ("name", "title", "characterName", "displayName", "botTitle")
        ):
            out.append(obj)
        for v in obj.values():
            flatten_items(v, out)
    elif isinstance(obj, list):
        for it in obj:
            flatten_items(it, out)
