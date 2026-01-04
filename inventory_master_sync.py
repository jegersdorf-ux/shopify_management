import os
import json
import base64
import smtplib
import requests
import pdfplumber
import io
import re
import time
import warnings
import traceback
import sys
from datetime import datetime

# --- PYTHON 3.9 COMPATIBILITY PATCH ---
if sys.version_info < (3, 10):
    import importlib.metadata
    if not hasattr(importlib.metadata, 'packages_distributions'):
        importlib.metadata.packages_distributions = lambda: {}

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from github import Github, Auth
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# --- CONFIGURATION & FLAGS ---
DRY_RUN = False        
TEST_MODE = False        
TEST_LIMIT = 20        
RESET_IGNORED_ITEMS = False

# --- TOGGLES ---
ENABLE_MOONSTONE = True
ENABLE_INFINITY = True  
ENABLE_ASMODEE = True

# --- CREDENTIALS (REVERTED TO ENV VARS) ---
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = "2025-10" 

EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')

# Files & Folders
JSON_FILE_PATH = "product_inventory.json"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit"
TARGET_LOCATION_NAME = "Deltona Florida Store"

# Local Files (Working Directory)
SOURCE_FILES = [
    "raw_export_moonstone.json",
    "raw_export_warsenal.json"
]

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# --- FACTION & GAME LOGIC ---
ASMODEE_PREFIXES = ["CHX", "ESS", "FF", "G2", "GG49", "GG3", "GG2", "GGS2", "NEM", "SWA", "SWC", "SWO", "SWD", "SWF", "SWL", "SWU", "USWA", "CPE", "CP", "SWP", "SWQ", "CA"]

KNOWN_FACTIONS = {
    "infinity": ["PanOceania", "Yu Jing", "Ariadna", "Haqqislam", "Nomads", "Combined Army", "Aleph", "Tohaa", "O-12", "JSA", "Mercenaries"],
    "moonstone": ["Commonwealth", "Dominion", "Leshavult", "Shades", "Gnomes", "Fairies"]
}

# ==========================================
#            SHOPIFY HELPERS
# ==========================================

def get_shopify_url():
    if not SHOPIFY_STORE_URL: return None
    clean_url = SHOPIFY_STORE_URL.strip().replace("https://", "").replace("http://", "")
    if "/" in clean_url: clean_url = clean_url.split("/")[0]
    return f"https://{clean_url}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

def get_location_id():
    query = "{ locations(first: 10) { edges { node { id, name } } } }"
    try:
        r = requests.post(get_shopify_url(), json={"query": query}, headers=HEADERS, timeout=10)
        data = r.json()
        for edge in data['data']['locations']['edges']:
            loc = edge['node']
            if TARGET_LOCATION_NAME.lower() in loc['name'].lower():
                return loc['id']
        return data['data']['locations']['edges'][0]['node']['id']
    except: return None

# ==========================================
#          MAPPING & SYNC LOGIC
# ==========================================

def determine_vendor(source_vendor, game_name):
    if "Moonstone" in game_name: return "Goblin King Games"
    if "Infinity" in game_name: return "Corvus Belli"
    return source_vendor

def determine_faction(game_name, source_tags):
    key = "moonstone" if "Moonstone" in game_name else "infinity"
    tags_str = " ".join(source_tags).lower()
    for f in KNOWN_FACTIONS.get(key, []):
        if f.lower() in tags_str: return f
    return ""

def load_combined_source_data():
    combined = {}
    for filename in SOURCE_FILES:
        path = os.path.join(os.getcwd(), filename)
        if not os.path.exists(path): continue
            
        with open(path, 'r', encoding='utf-8') as f:
            products = json.load(f)
            
        game_name = "Moonstone" if "moonstone" in filename else "Infinity"
        for p in products:
            variants = p.get('variants', [])
            image_urls = [img['src'] for img in p.get('images', [])]
            tags_raw = p.get('tags', [])
            source_tags = tags_raw.split(',') if isinstance(tags_raw, str) else tags_raw

            for v in variants:
                sku = v.get('sku')
                if not sku: continue
                sku = sku.strip()
                
                combined[sku] = {
                    "sku": sku,
                    "title": p.get('title'),
                    "vendor": determine_vendor(p.get('vendor'), game_name),
                    "game_name": game_name,
                    "primary_faction": determine_faction(game_name, source_tags),
                    "description": p.get('body_html', ''),
                    "images": image_urls,
                    "source_tags": source_tags,
                    "weight": v.get('grams', 0),
                    "upc": v.get('barcode', ''),
                    "price": v.get('price'),      # Selling Price
                    "cost_price": v.get('price'), # MAPPING: Source Price -> Your Cost
                    "compare_at_price": v.get('compare_at_price'),
                    "release_date": p.get('published_at')
                }
    return combined

# ==========================================
#           SHOPIFY API ACTIONS
# ==========================================

def find_shopify_product(sku):
    query = """
    query($sku: String!) {
      products(first: 1, query: $sku) {
        edges {
          node {
            id
            variants(first: 1) {
              edges {
                node {
                  id
                  sku
                  compareAtPrice
                  inventoryItem { id, unitCost { amount } }
                }
              }
            }
            media(first: 50) { edges { node { id } } }
          }
        }
      }
    }
    """
    try:
        r = requests.post(get_shopify_url(), json={"query": query, "variables": {"sku": f"sku:{sku}"}}, headers=HEADERS)
        node = r.json()['data']['products']['edges'][0]['node']
        v_node = node['variants']['edges'][0]['node']
        if v_node['sku'] == sku:
            return {
                "product_id": node['id'],
                "variant_id": v_node['id'],
                "inventory_item_id": v_node['inventoryItem']['id'],
                "current_compare": v_node.get('compareAtPrice'),
                "current_cost": v_node['inventoryItem'].get('unitCost', {}).get('amount') if v_node['inventoryItem'].get('unitCost') else None,
                "media_ids": [m['node']['id'] for m in node['media']['edges']]
            }
    except: return None

def update_product_sync(ids, item):
    if DRY_RUN: return

    # 1. Update Variant (MSRP & Price)
    if str(item['compare_at_price']) != str(ids['current_compare']):
        mut = """mutation v($i: ProductVariantInput!) { productVariantUpdate(input: $i) { userErrors { message } } }"""
        v_input = {"id": ids['variant_id'], "compareAtPrice": str(item['compare_at_price']), "price": str(item['price'])}
        requests.post(get_shopify_url(), json={"query": mut, "variables": {"i": v_input}}, headers=HEADERS)

    # 2. Update Cost (Mapped from Source Selling Price)
    if str(item['cost_price']) != str(ids['current_cost']):
        mut = """mutation i($id: ID!, $i: InventoryItemInput!) { inventoryItemUpdate(id: $id, input: $i) { userErrors { message } } }"""
        requests.post(get_shopify_url(), json={"query": mut, "variables": {"id": ids['inventory_item_id'], "i": {"cost": str(item['cost_price'])}}}, headers=HEADERS)

    # 3. Overwrite Images (Direct Address Logic)
    if item['images']:
        # Delete old
        if ids['media_ids']:
            del_mut = """mutation pdm($pid: ID!, $mids: [ID!]!) { productDeleteMedia(productId: $pid, mediaIds: $mids) { deletedMediaIds } }"""
            requests.post(get_shopify_url(), json={"query": del_mut, "variables": {"pid": ids['product_id'], "mids": ids['media_ids']}}, headers=HEADERS)
        # Add new
        add_mut = """mutation pcm($pid: ID!, $m: [CreateMediaInput!]!) { productCreateMedia(productId: $pid, media: $m) { media { id } } }"""
        media_input = [{"originalSource": url, "mediaContentType": "IMAGE", "alt": item['title']} for url in item['images']]
        requests.post(get_shopify_url(), json={"query": add_mut, "variables": {"pid": ids['product_id'], "media": media_input}}, headers=HEADERS)

def main():
    print(f"--- MASTER SYNC START: {datetime.now()} ---")
    
    source_data = load_combined_source_data()
    loc_id = get_location_id()
    
    success_count = 0
    for sku, item in source_data.items():
        if TEST_MODE and success_count >= TEST_LIMIT: break
        
        ids = find_shopify_product(sku)
        if ids:
            print(f"> Syncing: {sku}")
            try:
                update_product_sync(ids, item)
                success_count += 1
            except Exception as e:
                print(f"  [!] Error: {e}")
            time.sleep(0.5)

    print(f"--- SYNC COMPLETE. Processed {success_count} items. ---")

if __name__ == "__main__":
    main()
