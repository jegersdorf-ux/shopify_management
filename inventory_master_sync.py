import os
import json
import base64
import requests
import time
import sys
import warnings
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# --- GOOGLE DEPENDENCIES ---
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- PYTHON 3.9 COMPATIBILITY ---
if sys.version_info < (3, 10):
    import importlib.metadata
    if not hasattr(importlib.metadata, 'packages_distributions'):
        importlib.metadata.packages_distributions = lambda: {}

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# ==========================================
#              CONFIGURATION
# ==========================================
SHOP_URL = os.environ.get("SHOPIFY_STORE_URL", "the-guillotine-life.myshopify.com")
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2025-10")
TARGET_LOCATION_NAME = "Deltona Florida Store"

SHEET_URL = os.environ.get("SHEET_URL", "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit")
GOOGLE_CREDS_B64 = os.environ.get("GOOGLE_CREDENTIALS_BASE64")

EXTERNAL_SOURCES = {
    "Moonstone": "https://shop.moonstonethegame.com",
    "Warsenal": "https://warsen.al",
    "Asmodee": "https://store.asmodee.com"
}
ASMODEE_CALENDAR_URL = "https://store.asmodee.com/pages/release-calendar"

DRY_RUN = False         
TEST_MODE = False         
TEST_LIMIT = 20         
ENABLE_MOONSTONE = True
ENABLE_WARSENAL = True
ENABLE_ASMODEE = True

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

ASMODEE_PREFIXES = [
    "CHX", "ESS", "FF", "G2", "GG49", "GG3", "GG2", "GGS2", "NEM", 
    "SWA", "SWC", "SWO", "SWD", "SWF", "SWL", "SWU", "USWA", 
    "CPE", "CP", "SWP", "SWQ", "CA"
]

KNOWN_FACTIONS = {
    "infinity": ["PanOceania", "Yu Jing", "Ariadna", "Haqqislam", "Nomads", "Combined Army", "Aleph", "Tohaa", "O-12", "JSA", "Mercenaries"],
    "moonstone": ["Commonwealth", "Dominion", "Leshavult", "Shades", "Gnomes", "Fairies"]
}

SAFE_VENDORS_FOR_UPDATE = [
    "infinity", "warsenal", "corvus belli", "asmodee", 
    "atomic mass", "fantasy flight", "star wars", "marvel"
]

# ==========================================
#              HELPER FUNCTIONS
# ==========================================

def get_shopify_base_url():
    return f"https://{SHOP_URL}/admin/api/{API_VERSION}"

def create_retry_session(retries=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

session = create_retry_session()

def update_status_file(current, total):
    """Writes progress to a local file."""
    try:
        percent = (current / total) * 100 if total > 0 else 0
        with open("sync_progress.txt", "w") as f:
            f.write(f"Timestamp: {datetime.now()}\n")
            f.write(f"Status: Running\n")
            f.write(f"Progress: {current} / {total} Groups Processed ({percent:.1f}%)")
    except Exception:
        pass

def safe_float(val):
    if not val: return 0.0
    clean = str(val).replace("$", "").replace("£", "").replace(",", "").strip()
    match = re.search(r"(\d+(\.\d+)?)", clean)
    return float(match.group(1)) if match else 0.0

def safe_int(val):
    if not val: return 0
    clean = str(val).lower().replace("g", "").replace("lbs", "").replace("oz", "").replace(",", "").strip()
    match = re.search(r"(\d+)", clean)
    return int(match.group(1)) if match else 0

def get_google_creds():
    if not GOOGLE_CREDS_B64: return None
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDS_B64).decode('utf-8'))
    return service_account.Credentials.from_service_account_info(creds_json)

def extract_sheet_id(url):
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None

# ==========================================
#          BUSINESS LOGIC
# ==========================================

def calculate_cost(msrp, vendor_name, source_name=""):
    v_lower = vendor_name.lower()
    s_lower = source_name.lower()
    if "goblin king" in v_lower or "moonstone" in v_lower or "moonstone" in s_lower:
        return msrp * 0.60
    high_margin_group = ["asmodee", "atomic", "fantasy flight", "star wars", "marvel", "crisis protocol"]
    if "asmodee" in s_lower or any(x in v_lower for x in high_margin_group):
        return msrp * 0.57
    return msrp * 0.50

def auto_detect_vendor(sku, provided_vendor=""):
    if provided_vendor: return provided_vendor
    sku_upper = sku.upper()
    for prefix in ASMODEE_PREFIXES:
        if sku_upper.startswith(prefix): return "Asmodee"
    return "Tabletop Game"

def determine_faction(vendor_raw, title, tags_list=[]):
    search_text = (vendor_raw + " " + title + " " + " ".join(tags_list)).lower()
    if "infinity" in search_text or "corvus" in search_text:
        for f in KNOWN_FACTIONS["infinity"]:
            if f.lower() in search_text: return f
    if "moonstone" in search_text or "goblin king" in search_text:
        for f in KNOWN_FACTIONS["moonstone"]:
            if f.lower() in search_text: return f
    return ""

def detect_game_system(vendor_raw, source_name):
    v = vendor_raw.lower()
    s = source_name.lower()
    if "moonstone" in v or "goblin king" in v or "moonstone" in s: return "Moonstone"
    if "infinity" in v or "corvus belli" in v: return "Infinity"
    if "atomic mass" in v or "marvel" in v or "crisis protocol" in v: return "Marvel Crisis Protocol"
    if "star wars" in v or "legion" in v or "shatterpoint" in v: return "Star Wars Tabletop"
    return "Tabletop Game"

# ==========================================
#        PHASE 0: RELEASE DATES
# ==========================================

def fetch_asmodee_release_calendar():
    print("--- PHASE 0: SCRAPING RELEASE CALENDAR ---")
    release_map = {} 
    try:
        r = session.get(ASMODEE_CALENDAR_URL, timeout=20)
        if r.status_code != 200: return {}
        
        lines = r.text.split('\n')
        current_date_str = None
        current_year = datetime.now().year
        today = datetime.now()
        month_map = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12}

        for line in lines:
            clean_line = re.sub(r'<[^>]+>', '', line).strip()
            date_match = re.search(r'^([A-Z][a-z]+)\s+(\d+)(st|nd|rd|th)?', clean_line)
            
            if date_match and date_match.group(1).lower() in month_map:
                month_num = month_map[date_match.group(1).lower()]
                calc_year = current_year
                if today.month > 10 and month_num < 3: calc_year += 1
                elif today.month < 3 and month_num > 10: calc_year -= 1
                try: current_date_str = f"{calc_year}-{month_num:02d}-{int(date_match.group(2)):02d}"
                except: current_date_str = None
                continue
            
            if current_date_str and "Add to cart" not in clean_line and len(clean_line) > 5:
                if clean_line.startswith("- ") or clean_line.startswith("• "):
                    prod_title = clean_line[2:].split("$")[0].strip().split(" - ")[0].strip()
                    if prod_title: release_map[prod_title.lower()] = current_date_str
    except Exception as e:
        print(f"    [!] Error parsing calendar: {e}")
    return release_map

# ==========================================
#        PHASE 1 & 2: DATA FETCHING
# ==========================================

def fetch_external_source(source_name, base_url):
    print(f"    --> Scraping {source_name}...")
    products_found = []
    page = 1
    while True:
        try:
            r = session.get(f"{base_url}/products.json?limit=250&page={page}", timeout=20)
            if r.status_code != 200: break
            batch = r.json().get('products', [])
            if not batch: break 
            products_found.extend(batch)
            page += 1
            time.sleep(0.5)
        except: break
    return products_found

def compile_source_data(release_map):
    print("\n--- PHASE 1 & 2: COMPILING SOURCE DATA ---")
    combined = {} # Key = SKU
    
    # 1. Scrape
    for name, url in EXTERNAL_SOURCES.items():
        if (name=="Moonstone" and not ENABLE_MOONSTONE) or (name=="Warsenal" and not ENABLE_WARSENAL) or (name=="Asmodee" and not ENABLE_ASMODEE): continue
        raw_products = fetch_external_source(name, url)
        
        for p in raw_products:
            raw_vendor = p.get('vendor', '')
            if name == "Moonstone" and not raw_vendor: raw_vendor = "Goblin King Games"
            title = p.get('title', 'Unknown')
            tags_list = str(p.get('tags', [])).split(',')
            faction = determine_faction(raw_vendor, title, tags_list)
            game_system = detect_game_system(raw_vendor, name)
            
            r_date = release_map.get(title.lower())
            if not r_date:
                for k, v in release_map.items():
                    if k in title.lower(): r_date = v; break

            images = [{"src": img['src']} for img in p.get('images', [])]

            for v in p.get('variants', []):
                sku = v.get('sku')
                if not sku: continue
                msrp = max(safe_float(v.get('price')), safe_float(v.get('compare_at_price')))
                
                combined[sku.strip()] = {
                    "sku": sku.strip(),
                    "title": title,
                    "description": p.get('body_html', ''),
                    "product_type": game_system,
                    "images": images,
                    "weight": safe_int(v.get('grams')),
                    "barcode": v.get('barcode', ''),
                    "target_compare": msrp,
                    "target_price": msrp,
                    "target_cost": calculate_cost(msrp, raw_vendor, name),
                    "target_vendor": raw_vendor,
                    "target_faction": faction,
                    "release_date": r_date,
                    "source_origin": f"Scrape-{name}",
                    "option1": v.get('option1'), 
                    "option2": v.get('option2'),
                    "option3": v.get('option3')
                }

    # 2. Sheet
    if GOOGLE_CREDS_B64:
        print("    --> Fetching Google Sheet...")
        try:
            creds = get_google_creds()
            service = build('sheets', 'v4', credentials=creds)
            rows = service.spreadsheets().values().get(spreadsheetId=extract_sheet_id(SHEET_URL), range="A:Z").execute().get('values', [])
            
            if rows:
                headers = [h.lower().strip() for h in rows[0]]
                idx_sku = headers.index('sku')
                idx_title = headers.index('title')
                idx_price = headers.index('price')
                idx_vendor = headers.index('vendor') if 'vendor' in headers else -1
                
                for row in rows[1:]:
                    if len(row) <= idx_sku: continue
                    sku = row[idx_sku].strip()
                    if not sku or sku in combined: continue 
                    
                    if not any(sku.upper().startswith(pre) for pre in ASMODEE_PREFIXES): continue 
                    
                    title = row[idx_title] if len(row) > idx_title else "Unknown"
                    msrp = safe_float(row[idx_price] if len(row) > idx_price else "0")
                    vendor = row[idx_vendor] if idx_vendor != -1 and len(row) > idx_vendor else ""
                    final_vendor = auto_detect_vendor(sku, vendor)
                    
                    combined[sku] = {
                        "sku": sku,
                        "title": title,
                        "description": "",
                        "product_type": "Tabletop Game",
                        "images": [],
                        "weight": 0,
                        "barcode": "",
                        "target_compare": msrp,
                        "target_price": msrp,
                        "target_cost": calculate_cost(msrp, final_vendor, "Sheet"),
                        "target_vendor": final_vendor,
                        "target_faction": determine_faction(final_vendor, title),
                        "release_date": None,
                        "source_origin": "GoogleSheet-Filtered",
                        "option1": "Default Title", 
                        "option2": None, "option3": None
                    }
        except Exception as e: print(f"    [!] Sheet Error: {e}")

    return combined

def group_data_by_title(source_map):
    grouped = {}
    for sku, data in source_map.items():
        t = data['title'].strip()
        if t not in grouped: grouped[t] = []
        grouped[t].append(data)
    return grouped

# ==========================================
#        PHASE 3: LIVE CATALOG
# ==========================================

def fetch_live_catalog():
    print("\n--- PHASE 3: FETCHING LIVE SHOPIFY CATALOG ---")
    live_products_by_title = {} 
    
    for status in ["active", "draft", "archived"]:
        print(f"    --> Status: {status.upper()}...")
        url = f"{get_shopify_base_url()}/products.json"
        params = {"limit": 250, "status": status}
        while url:
            try:
                r = session.get(url, headers=HEADERS, params=params)
                data = r.json()
                for p in data.get("products", []):
                    p_title = p['title'].strip()
                    p_data = {
                        "id": p['id'],
                        "status": p['status'],
                        "tags": p['tags'],
                        "vendor": p['vendor'],
                        "product_type": p['product_type'], # Added for comparison
                        "image_count": len(p.get('images', [])),
                        "variants": {} 
                    }
                    
                    for v in p.get('variants', []):
                        sku = v.get('sku', '').strip()
                        if sku:
                            p_data["variants"][sku] = {
                                "id": v['id'],
                                "inventory_item_id": v['inventory_item_id'],
                                "price": safe_float(v.get('price')),
                                "compare_at": safe_float(v.get('compare_at_price')),
                            }
                    
                    live_products_by_title[p_title] = p_data

                link = r.headers.get('Link')
                if link and 'rel="next"' in link:
                    url = [l for l in link.split(',') if 'rel="next"' in l][0].split(';')[0].strip('<> ')
                    params = {}
                else: url = None
            except: break
    print(f"    [✓] Loaded {len(live_products_by_title)} unique products.")
    return live_products_by_title

# ==========================================
#        PHASE 4: SYNC LOGIC (GROUPED)
# ==========================================

def update_automation_notes(product_id, new_notes):
    """Appends new text lines to the custom.automation_notes list metafield."""
    if not new_notes: return
    
    if DRY_RUN:
        for n in new_notes: print(f"    [DRY] Note for {product_id}: {n}")
        return

    # 1. Fetch existing
    existing_notes = []
    metafield_id = None
    try:
        url = f"{get_shopify_base_url()}/products/{product_id}/metafields.json"
        r = session.get(url, headers=HEADERS)
        metafields = r.json().get('metafields', [])
        for m in metafields:
            if m['namespace'] == 'custom' and m['key'] == 'automation_notes':
                metafield_id = m['id']
                try: existing_notes = json.loads(m['value'])
                except: existing_notes = []
                break
    except: pass

    # 2. Combine and Push
    if not isinstance(existing_notes, list): existing_notes = []
    combined = existing_notes + new_notes
    
    payload = {
        "metafield": {
            "namespace": "custom",
            "key": "automation_notes",
            "value": json.dumps(combined),
            "type": "list.single_line_text_field"
        }
    }
    
    try:
        if metafield_id:
            url = f"{get_shopify_base_url()}/products/{product_id}/metafields/{metafield_id}.json"
            payload['metafield']['id'] = metafield_id
            session.put(url, json=payload, headers=HEADERS)
        else:
            url = f"{get_shopify_base_url()}/products/{product_id}/metafields.json"
            session.post(url, json=payload, headers=HEADERS)
    except Exception as e:
        print(f"    [!] Failed to update notes: {e}")

def sync_product_group(title, variant_list, live_product, location_id):
    # CASE 1: PRODUCT DOES NOT EXIST -> CREATE
    if not live_product:
        if DRY_RUN: print(f"    [DRY] CREATE PRODUCT: {title} ({len(variant_list)} variants)"); return

        base = variant_list[0]
        tags = ["Tabletop Gaming", "Auto Import", f"Source: {base['source_origin']}"]
        if base['target_faction']: tags.append(base['target_faction'])
        
        variants_payload = []
        for v in variant_list:
            variants_payload.append({
                "sku": v['sku'],
                "price": f"{v['target_price']:.2f}",
                "compare_at_price": f"{v['target_compare']:.2f}",
                "barcode": v['barcode'],
                "grams": v['weight'],
                "inventory_management": "shopify",
                "option1": v['option1'], "option2": v['option2'], "option3": v['option3']
            })

        prod_payload = {
            "title": title,
            "vendor": base['target_vendor'],
            "product_type": base['product_type'],
            "status": "draft",
            "tags": ", ".join(tags),
            "body_html": base['description'],
            "images": base['images'],
            "variants": variants_payload,
            "metafields": []
        }
        
        if base['release_date']:
            prod_payload['metafields'].append({
                "namespace": "custom", "key": "release_date", "value": base['release_date'], "type": "date"
            })

        try:
            r = session.post(f"{get_shopify_base_url()}/products.json", json={"product": prod_payload}, headers=HEADERS)
            r.raise_for_status()
            new_prod = r.json()['product']
            
            # Update Costs
            for i, created_v in enumerate(new_prod['variants']):
                source_match = next((x for x in variant_list if x['sku'] == created_v['sku']), None)
                if source_match:
                    session.put(
                        f"{get_shopify_base_url()}/inventory_items/{created_v['inventory_item_id']}.json",
                        json={"inventory_item": {"id": created_v['inventory_item_id'], "cost": f"{source_match['target_cost']:.2f}"}},
                        headers=HEADERS
                    )
                    if location_id:
                        session.post(
                            f"{get_shopify_base_url()}/inventory_levels/connect.json",
                            json={"inventory_item_id": created_v['inventory_item_id'], "location_id": location_id, "relocate_if_necessary": True},
                            headers=HEADERS
                        )
            print(f"    [+] Created Product: {title} ({len(variant_list)} variants)")
        except Exception as e:
            print(f"    [!] Error Creating {title}: {e}")
        return

    # CASE 2: PRODUCT EXISTS
    
    # --- COMPARISON LOGIC ---
    notes_to_add = []
    ts = datetime.now().strftime('%Y-%m-%d')
    source_base = variant_list[0]
    
    # Compare Vendor
    if live_product['vendor'] != source_base['target_vendor']:
        notes_to_add.append(f"[{ts}] Vendor Diff: Live '{live_product['vendor']}' vs Source '{source_base['target_vendor']}'")
        
    # Compare Type
    if live_product['product_type'] != source_base['product_type']:
        notes_to_add.append(f"[{ts}] Type Diff: Live '{live_product['product_type']}' vs Source '{source_base['product_type']}'")
        
    # Compare Prices (Variant Level)
    for v_data in variant_list:
        sku = v_data['sku']
        if sku in live_product['variants']:
            live_v = live_product['variants'][sku]
            # Use strict float comparison (assuming standard 2 decimal places)
            if abs(live_v['price'] - v_data['target_price']) > 0.01:
                notes_to_add.append(f"[{ts}] Price Diff ({sku}): Live {live_v['price']} vs Source {v_data['target_price']}")

    if notes_to_add:
        update_automation_notes(live_product['id'], notes_to_add)

    # --- END COMPARISON LOGIC ---

    # 2a. Image Injection (If 0 images)
    if live_product['image_count'] == 0 and variant_list[0]['images']:
        print(f"    [+] Injecting Images: {title}")
        if not DRY_RUN:
            session.put(
                f"{get_shopify_base_url()}/products/{live_product['id']}.json",
                json={"product": {"id": live_product['id'], "images": variant_list[0]['images']}},
                headers=HEADERS
            )

    # 2b. Sync Variants
    if live_product['image_count'] > 0:
        pass 
    
    for v_data in variant_list:
        sku = v_data['sku']
        
        if sku in live_product['variants']:
            live_v = live_product['variants'][sku]
            
            if live_product['image_count'] > 0: continue 
            
            if live_v['compare_at'] != v_data['target_compare']:
                if not DRY_RUN:
                    session.put(
                        f"{get_shopify_base_url()}/variants/{live_v['id']}.json",
                        json={"variant": {"id": live_v['id'], "price": f"{v_data['target_price']:.2f}", "compare_at_price": f"{v_data['target_compare']:.2f}"}},
                        headers=HEADERS
                    )
            # Always sync cost
            session.put(
                f"{get_shopify_base_url()}/inventory_items/{live_v['inventory_item_id']}.json",
                json={"inventory_item": {"id": live_v['inventory_item_id'], "cost": f"{v_data['target_cost']:.2f}"}},
                headers=HEADERS
            )

        else:
            # Add Missing Variant
            print(f"    [+] Adding Missing Variant {sku} to existing product {title}...")
            if not DRY_RUN:
                var_payload = {
                    "sku": sku,
                    "price": f"{v_data['target_price']:.2f}",
                    "compare_at_price": f"{v_data['target_compare']:.2f}",
                    "barcode": v_data['barcode'],
                    "grams": v_data['weight'],
                    "inventory_management": "shopify",
                    "option1": v_data['option1'], "option2": v_data['option2'], "option3": v_data['option3']
                }
                try:
                    r = session.post(
                        f"{get_shopify_base_url()}/products/{live_product['id']}/variants.json",
                        json={"variant": var_payload},
                        headers=HEADERS
                    )
                    r.raise_for_status()
                    new_v = r.json()['variant']
                    session.put(
                        f"{get_shopify_base_url()}/inventory_items/{new_v['inventory_item_id']}.json",
                        json={"inventory_item": {"id": new_v['inventory_item_id'], "cost": f"{v_data['target_cost']:.2f}"}},
                        headers=HEADERS
                    )
                except Exception as e:
                    print(f"       [!] Failed to add variant {sku}: {e}")

# ==========================================
#              MAIN EXECUTION
# ==========================================

def main():
    print(f"--- STARTING UNIVERSAL SYNC V6: {datetime.now()} ---")
    if not ACCESS_TOKEN: return

    deltona_id = get_location_id_by_name(TARGET_LOCATION_NAME)
    
    release_map = fetch_asmodee_release_calendar()
    source_map_flat = compile_source_data(release_map) 
    grouped_source = group_data_by_title(source_map_flat)
    print(f"    [i] Grouped into {len(grouped_source)} unique Titles.")
    
    live_products = fetch_live_catalog()
    
    print(f"\n--- PHASE 4: EXECUTING UPDATES ---")
    
    total_titles = len(grouped_source)
    processed = 0
    
    for title, variants in grouped_source.items():
        if TEST_MODE and processed >= TEST_LIMIT: break
        
        if processed % 5 == 0: update_status_file(processed, total_titles)
        
        live_prod = live_products.get(title)
        sync_product_group(title, variants, live_prod, deltona_id)
        
        processed += 1
        time.sleep(0.5)

    update_status_file(total_titles, total_titles)
    print("\n--- DONE ---")

if __name__ == "__main__":
    main()
