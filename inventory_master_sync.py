import os
import json
import base64
import requests
import time
import sys
import warnings
import re
from datetime import datetime

# --- PYTHON 3.9 COMPATIBILITY PATCH ---
if sys.version_info < (3, 10):
    import importlib.metadata
    if not hasattr(importlib.metadata, 'packages_distributions'):
        importlib.metadata.packages_distributions = lambda: {}

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ==========================================
#              CONFIGURATION
# ==========================================
DRY_RUN = False        
TEST_MODE = False        
TEST_LIMIT = 20        

# Toggles
ENABLE_MOONSTONE = True
ENABLE_INFINITY = True  
ENABLE_ASMODEE = True

# Credentials (Env Vars)
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = "2025-10" 
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

# Source Resources
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit"
SOURCE_FILES = ["raw_export_moonstone.json", "raw_export_warsenal.json"]

# Headers for REST API
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# --- FACTION & VENDOR LOGIC ---
KNOWN_FACTIONS = {
    "infinity": ["PanOceania", "Yu Jing", "Ariadna", "Haqqislam", "Nomads", "Combined Army", "Aleph", "Tohaa", "O-12", "JSA", "Mercenaries"],
    "moonstone": ["Commonwealth", "Dominion", "Leshavult", "Shades", "Gnomes", "Fairies"]
}

# ==========================================
#             HELPER FUNCTIONS
# ==========================================

def get_shopify_base_url():
    if not SHOPIFY_STORE_URL: return None
    clean_url = SHOPIFY_STORE_URL.strip().replace("https://", "").replace("http://", "").split("/")[0]
    return f"https://{clean_url}/admin/api/{SHOPIFY_API_VERSION}"

def safe_float(val):
    if val is None or val == "": return 0.0
    try:
        return float(str(val).replace(",", "").strip())
    except: return 0.0

def safe_int(val):
    if val is None or val == "": return 0
    try:
        return int(float(str(val).replace(",", "").strip()))
    except: return 0

def determine_vendor(source_vendor, game_name):
    """Preserves your custom vendor logic."""
    if "Moonstone" in game_name: return "Goblin King Games"
    if "Infinity" in game_name: return "Corvus Belli"
    return source_vendor

def determine_faction(game_name, source_tags):
    """Preserves your custom faction logic."""
    key = "moonstone" if "Moonstone" in game_name else "infinity"
    tags_str = " ".join(source_tags).lower()
    for f in KNOWN_FACTIONS.get(key, []):
        if f.lower() in tags_str: return f
    return ""

# ==========================================
#       PHASE 1: FETCH LIVE CATALOG (REST)
# ==========================================

def fetch_live_catalog():
    """
    Fetches ALL products using REST Link-Header pagination.
    Captures IDs, Price, Weight, and Vendor for comparison.
    """
    print("--- PHASE 1: FETCHING LIVE CATALOG (REST) ---")
    catalog = {}
    url = f"{get_shopify_base_url()}/products.json"
    params = {"limit": 250}

    while url:
        try:
            r = requests.get(url, headers=HEADERS, params=params)
            if r.status_code != 200:
                print(f"    [!] Error fetching catalog: {r.status_code} - {r.text}")
                break
            
            data = r.json()
            products = data.get("products", [])
            
            for p in products:
                p_id = p['id']
                p_tags = p['tags'].split(', ') if p['tags'] else []
                vendor = p.get('vendor', '')
                
                for v in p.get('variants', []):
                    sku = v.get('sku')
                    if sku:
                        catalog[sku.strip()] = {
                            "product_id": p_id,
                            "variant_id": v['id'],
                            "inventory_item_id": v['inventory_item_id'],
                            "status": p['status'],
                            "tags": p_tags,
                            "vendor": vendor,
                            "current_price": v.get('price'),
                            "current_compare": v.get('compare_at_price'),
                            "current_weight": v.get('grams', 0),
                            "title": p['title']
                        }

            # Handle Pagination (Link Header Logic)
            link_header = r.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                links = link_header.split(',')
                next_link = [l for l in links if 'rel="next"' in l]
                if next_link:
                    url = next_link[0].split(';')[0].strip('<> ')
                    params = {} # Clear params as they are in the URL now
                else: url = None
            else: url = None
            
            print(f"    Loaded {len(catalog)} variants so far...")
            
        except Exception as e:
            print(f"    [!] Exception: {e}")
            break
            
    return catalog

# ==========================================
#       PHASE 2: LOAD SOURCE DATA
# ==========================================

def get_google_sheet_data():
    """Fetches Asmodee data from Google Sheets."""
    if not GOOGLE_CREDENTIALS_BASE64:
        print("    [!] No Google Credentials found. Skipping Sheet.")
        return []

    try:
        creds_json = base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8')
        creds_dict = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        service = build('sheets', 'v4', credentials=creds)
        sheet_id = SHEET_URL.split("/d/")[1].split("/")[0]
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range="A:Z").execute()
        return result.get('values', [])
    except Exception as e:
        print(f"    [!] Google Sheet Error: {e}")
        return []

def load_combined_source_data():
    print("--- PHASE 2: LOADING SOURCE DATA ---")
    combined = {}
    
    # 1. PROCESS JSON FILES
    for filename in SOURCE_FILES:
        path = os.path.join(os.getcwd(), filename)
        if not os.path.exists(path): continue
        
        # Toggles
        if "moonstone" in filename.lower() and not ENABLE_MOONSTONE: continue
        if "warsenal" in filename.lower() and not ENABLE_INFINITY: continue

        # Logic Flags
        game_name = "Moonstone" if "moonstone" in filename.lower() else "Infinity"
        is_warsenal_file = "warsenal" in filename.lower()
        cost_multiplier = 0.60 if "moonstone" in filename.lower() else 0.50
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                products = json.load(f)
                
            for p in products:
                images = [{"src": img['src']} for img in p.get('images', [])]
                tags_raw = p.get('tags', [])
                source_tags = tags_raw.split(',') if isinstance(tags_raw, str) else tags_raw
                
                final_vendor = determine_vendor(p.get('vendor'), game_name)
                faction = determine_faction(game_name, source_tags)
                
                for v in p.get('variants', []):
                    sku = v.get('sku')
                    if not sku: continue
                    
                    price = safe_float(v.get('price'))
                    compare = safe_float(v.get('compare_at_price'))
                    # Extract Weight from Source JSON
                    weight = safe_int(v.get('grams'))
                    
                    msrp = max(price, compare)
                    
                    combined[sku.strip()] = {
                        "title": p.get('title'),
                        "images": images,
                        "weight": weight,
                        "target_compare": f"{msrp:.2f}",
                        "target_cost": f"{msrp * cost_multiplier:.2f}",
                        "target_vendor": final_vendor,
                        "target_faction": faction,
                        "restrict_to_warsenal": is_warsenal_file
                    }
        except Exception as e:
            print(f"    [!] Error reading {filename}: {e}")

    # 2. PROCESS GOOGLE SHEET (Asmodee)
    if ENABLE_ASMODEE:
        sheet_rows = get_google_sheet_data()
        if sheet_rows:
            headers = [h.lower() for h in sheet_rows[0]]
            try:
                # Map columns (Adjust indices if your sheet changes)
                idx_sku = headers.index('sku') if 'sku' in headers else 0
                idx_title = headers.index('title') if 'title' in headers else 1
                idx_price = headers.index('price') if 'price' in headers else 3
                idx_weight = headers.index('weight') if 'weight' in headers else -1
                
                for row in sheet_rows[1:]:
                    if len(row) <= idx_sku: continue
                    sku = row[idx_sku].strip()
                    if not sku: continue
                    
                    msrp = safe_float(row[idx_price] if len(row) > idx_price else 0)
                    weight = safe_int(row[idx_weight]) if idx_weight != -1 and len(row) > idx_weight else 0
                    
                    # Asmodee Cost Rule: 57%
                    cost = msrp * 0.57
                    
                    combined[sku] = {
                        "title": row[idx_title] if len(row) > idx_title else "Unknown",
                        "images": [], # Sheets usually don't have image lists
                        "weight": weight,
                        "target_compare": f"{msrp:.2f}",
                        "target_cost": f"{cost:.2f}",
                        "target_vendor": "Asmodee",
                        "target_faction": "",
                        "restrict_to_warsenal": False
                    }
            except ValueError:
                print("    [!] Could not map Sheet headers.")

    print(f"    [✓] Loaded {len(combined)} total source items.")
    return combined

# ==========================================
#       PHASE 3: REST API UPDATE
# ==========================================

def update_product_rest(live_data, target_data, sku):
    # --- VENDOR CHECK ---
    if target_data.get('restrict_to_warsenal', False):
        vendor = live_data.get('vendor', '').lower()
        allowed = ["infinity", "warsenal", "corvus belli", "asmodee", "atomic mass", "fantasy flight", "star wars", "marvel"]
        if not any(x in vendor for x in allowed):
            print(f"    [SKIP] Vendor Mismatch for {sku}: {vendor}")
            return

    # --- CHANGE DETECTION ---
    live_cmp = safe_float(live_data['current_compare'])
    tgt_cmp = safe_float(target_data['target_compare'])
    price_changed = (live_cmp != tgt_cmp)
    
    live_wgt = int(live_data['current_weight'])
    tgt_wgt = int(target_data['weight'])
    weight_changed = (live_wgt != tgt_wgt and tgt_wgt > 0)

    # --- TAGS & STATUS ---
    current_tags = live_data['tags']
    new_status = live_data['status']
    
    tags_to_add = ["Tabletop Gaming", "automated price"]
    if target_data['target_faction'] and target_data['target_faction'] not in current_tags:
        tags_to_add.append(target_data['target_faction'])
    
    # Active -> Draft Rule
    if price_changed and live_data['status'] == 'active':
        new_status = 'draft'
        tags_to_add.append("price changed")
    
    final_tags = list(set(current_tags + tags_to_add))
    final_tags_str = ", ".join(final_tags)

    if DRY_RUN:
        print(f"    [DRY] {sku} | Cost: {target_data['target_cost']} | Wgt: {tgt_wgt}")
        return

    try:
        # A. UPDATE VARIANT (Price, Weight)
        v_url = f"{get_shopify_base_url()}/variants/{live_data['variant_id']}.json"
        v_payload = {
            "variant": {
                "id": live_data['variant_id'],
                "compare_at_price": target_data['target_compare']
            }
        }
        if weight_changed:
            v_payload["variant"]["grams"] = tgt_wgt
            
        requests.put(v_url, json=v_payload, headers=HEADERS)

        # B. UPDATE INVENTORY (Cost)
        i_url = f"{get_shopify_base_url()}/inventory_items/{live_data['inventory_item_id']}.json"
        i_payload = {
            "inventory_item": {
                "id": live_data['inventory_item_id'],
                "cost": target_data['target_cost']
            }
        }
        requests.put(i_url, json=i_payload, headers=HEADERS)

        # C. UPDATE PRODUCT (Status, Tags, Vendor, Images)
        p_url = f"{get_shopify_base_url()}/products/{live_data['product_id']}.json"
        p_payload = {
            "product": {
                "id": live_data['product_id'],
                "status": new_status,
                "tags": final_tags_str
            }
        }
        
        # Update Vendor if specific logic applies
        if target_data['target_vendor'] and target_data['target_vendor'] != live_data['vendor']:
            p_payload["product"]["vendor"] = target_data['target_vendor']

        # Overwrite Images (Direct URL)
        if target_data['images']:
            p_payload["product"]["images"] = target_data['images']

        requests.put(p_url, json=p_payload, headers=HEADERS)

        print(f"    [✓] Synced {sku}: Cost {target_data['target_cost']} | Wgt: {tgt_wgt} | Status: {new_status}")

    except Exception as e:
        print(f"    [!] Error updating {sku}: {e}")

# ==========================================
#              MAIN EXECUTION
# ==========================================

def main():
    print(f"--- STARTING MASTER SYNC: {datetime.now()} ---")
    
    # 1. Fetch Live Catalog (REST)
    live_map = fetch_live_catalog()
    if not live_map:
        print("Aborting: Could not fetch live catalog.")
        return

    # 2. Load All Sources
    source_map = load_combined_source_data()
    
    print(f"\n--- PHASE 3: COMPARING {len(source_map)} SOURCE ITEMS VS {len(live_map)} LIVE ITEMS ---")
    
    matched_count = 0
    for sku, target in source_map.items():
        if TEST_MODE and matched_count >= TEST_LIMIT: break
            
        if sku in live_map:
            print(f"> Processing {sku}...")
            update_product_rest(live_map[sku], target, sku)
            matched_count += 1
            time.sleep(0.5) 
            
    print(f"\n--- SYNC COMPLETE. Processed {matched_count} matches. ---")

if __name__ == "__main__":
    main()
