import requests
import json

TYPESENSE_HOST = "https://etmzpxgvnid370fyp.a1.typesense.net"
TYPESENSE_KEY = "STHKtT6jrC5z1IozTJHIeSN4qN9oL1s3"  # Your public read key
TYPESENSE_SEARCH_ENDPOINT = f"{TYPESENSE_HOST}/multi_search"

payload = {
    "searches": [{
        "query_by": "name,title,tags,creator_username,character_id,type",
        "include_fields": "name,title,tags,creator_username,character_id,avatar_is_nsfw,avatar_url,visibility,definition_visible,num_messages,token_count,rating_score,lora_status,creator_user_id,is_nsfw,type,sub_characters_count,group_size_category,num_messages_24h",
        "use_cache": True,
        "highlight_fields": "none",
        "enable_highlight_v1": False,
        "sort_by": "num_messages_24h:desc",
        "collection": "public_characters_alias",
        "q": "*",
        "facet_by": "definition_size_category,group_size_category,tags,translated_languages",
        "filter_by": "application_ids:spicychat && tags:![Step-Family] && creator_user_id:!['kp:018d4672679e4c0d920ad8349061270c','kp:2f4c9fcbdb0641f3a4b960bfeaf1ea0b'] && type:STANDARD && tags:[`Female`] && tags:[`NSFW`]",
        "max_facet_values": 100,
        "page": 1,
        "per_page": 48,
    }]
}

headers = {
    "X-TYPESENSE-API-KEY": TYPESENSE_KEY,
    "Content-Type": "application/json",
}

response = requests.post(TYPESENSE_SEARCH_ENDPOINT, headers=headers, data=json.dumps(payload), timeout=25)
print(f"Status: {response.status_code}")
print(f"Headers: {dict(response.headers)}")
print(f"Text preview: {response.text[:500]}")

if response.status_code == 200:
    try:
        data = response.json()
        print(f"JSON keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        print(f"Results length: {len(data.get('results', [])) if isinstance(data, dict) else 'N/A'}")
    except Exception as e:
        print(f"JSON decode error: {e}")