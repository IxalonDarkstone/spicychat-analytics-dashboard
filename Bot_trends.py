# fetch_all_home_bots.py – Uses EXACT website payload + your auth
import requests
import json
from pathlib import Path
import time

# Load your dashboard's bearer token
AUTH_FILE = Path("data/auth_credentials.json")

if not AUTH_FILE.exists():
    print("Error: Run your main dashboard first to create data/auth_credentials.json")
    exit(1)

with open(AUTH_FILE) as f:
    creds = json.load(f)

bearer_token = creds["bearer_token"]

# Typesense endpoint (from your curl)
TYPESENSE_HOST = "https://etmzpxgvnid370fyp.a1.typesense.net"
TYPESENSE_KEY = "STHKtT6jrC5z1IozTJHIeSN4qN9oL1s3"  # Public key from your curl

url = f"{TYPESENSE_HOST}/multi_search"

headers = {
    "Authorization": f"Bearer {bearer_token}",  # Your dashboard auth
    "Content-Type": "text/plain",               # Exact from your curl
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-TYPESENSE-API-KEY": TYPESENSE_KEY         # Public read key
}

def get_page(page_num, per_page=48):
    """Get one page of bots using the exact website payload."""
    payload = {
        "searches": [{
            "query_by": "name,title,tags,creator_username,character_id,type",
            "include_fields": "name,title,tags,creator_username,character_id,avatar_is_nsfw,avatar_url,visibility,definition_visible,num_messages,token_count,rating_score,lora_status,creator_user_id,is_nsfw,type,sub_characters_count,group_size_category",
            "use_cache": True,
            "highlight_fields": "none",
            "enable_highlight_v1": False,
            "sort_by": "num_messages_24h:desc",
            "highlight_full_fields": "name,title,tags,creator_username,character_id,type",
            "collection": "public_characters_alias",
            "q": "*",
            "facet_by": "definition_size_category,group_size_category,tags,translated_languages",
            "filter_by": "application_ids:spicychat && tags:![Step-Family] && creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c','kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] && type:STANDARD && tags:[`Female`] && tags:[`NSFW`]",  # Exact from your curl
            "max_facet_values": 100,
            "page": page_num,
            "per_page": per_page
        }]
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code != 200:
        print(f"Error page {page_num}: {response.status_code}")
        print(response.text[:200])
        return []

    data = response.json()
    hits = data["results"][0].get("hits", [])
    return hits

def fetch_all_home_bots(max_pages=20):  # 20 pages = 960 bots
    all_bots = []
    seen = set()

    print(f"Fetching up to {max_pages} pages (48 bots/page = {max_pages*48} max)...")

    for page_num in range(1, max_pages + 1):
        print(f"Fetching page {page_num}...")
        hits = get_page(page_num)

        if not hits:
            print(f"No bots on page {page_num} → stopping")
            break

        new_bots = 0
        for hit in hits:
            doc = hit["document"]
            name = doc.get("name", "").strip()
            if not name or name in seen:
                continue

            bot = {
                "name": name,
                "title": doc.get("title", ""),
                "num_messages": doc.get("num_messages", 0),
                "num_messages_24h": doc.get("num_messages_24h", 0),
                "avatar_url": doc.get("avatar_url", ""),
                "creator_username": doc.get("creator_username", ""),
                "is_nsfw": doc.get("is_nsfw", False),
                "character_id": doc.get("character_id", ""),
                "link": f"https://spicychat.ai/chat/{doc.get('character_id', '')}"
            }

            all_bots.append(bot)
            seen.add(name)
            new_bots += 1

        print(f"Page {page_num}: +{new_bots} new bots → {len(all_bots)} total")

        # Rate limit
        time.sleep(0.5)

    print(f"\nCOMPLETE! Collected {len(all_bots)} unique bots from home page")
    with open("public_bots_home_all.json", "w", encoding="utf-8") as f:
        json.dump(all_bots, f, indent=2, ensure_ascii=False)
    print("Saved → public_bots_home_all.json")

    # Top 10
    print("\nTop 10 by total messages:")
    top10 = sorted(all_bots, key=lambda x: x["num_messages"], reverse=True)[:10]
    for i, b in enumerate(top10, 1):
        print(f"{i:2}. {b['name']:40} → {b['num_messages']:,} total / {b['num_messages_24h']:,} 24h")

    return all_bots

if __name__ == "__main__":
    fetch_all_home_bots(max_pages=10)  # 10 pages = 480 bots