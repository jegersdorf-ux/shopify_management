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
# --- ENVIRONMENT VARIABLES ---
SHOP_URL = os.environ.get("SHOPIFY_STORE_URL", "the-guillotine-life.myshopify.com")
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2025-10")
TARGET_LOCATION_NAME = "Deltona Florida Store"

# Google Config
SHEET_URL = os.environ.get("SHEET_URL", "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit")
GOOGLE_CREDS_B64 = os.environ.get("GOOGLE_CREDENTIALS_BASE64")

# --- SCRAPE SOURCES ---
EXTERNAL_SOURCES = {
    "Moonstone": "https://shop.moonstonethegame.com",
    "Warsenal": "https://warsen.al",
    "Asmodee": "https://store.asmodee.com"
}

ASMODEE_CALENDAR_URL = "https://store.asmodee.com/pages/release-calendar"

# --- SETTINGS ---
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

# --- LOGIC LISTS ---
# Only used for Sheet Filtering now
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
        if sku_upper.startswith(prefix):
            return "Asmodee"
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
    release_map = {} # { "Product Title": "YYYY-MM-DD" }
    
    try:
        r = session.get(ASMODEE_CALENDAR_URL, timeout=20)
        if r.status_code != 200:
            print(f"    [!] Failed to load calendar: {r.status_code}")
            return {}
        
        html = r.text
        # Logic: Find Date Headers, then list items until next header
        # Regex to find dates like "January 30th" or "December 12th"
        # This is a basic parser. It assumes <h3>Date</h3><ul><li>Product</li></ul> structure often used by Shopify pages.
        
        # Split by possible date headers (e.g. <strong>Month Day</strong> or <h3>Month Day</h3>)
        # We will iterate through lines to be safer
        
        lines = html.split('\n')
        current_date_str = None
        current_year = datetime.now().year
        today = datetime.now()
        
        month_map = {
            "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
            "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
        }

        for line in lines:
            # 1. Detect Date Header
            clean_line = re.sub(r'<[^>]+>', '', line).strip() # Remove tags
            
            # Match "Month Day" (e.g. January 30th, Dec 12)
            date_match = re.search(r'^([A-Z][a-z]+)\s+(\d+)(st|nd|rd|th)?', clean_line)
            
            if date_match and date_match.group(1).lower() in month_map:
                month_name = date_match.group(1).lower()
                day = int(date_match.group(2))
                month_num = month_map[month_name]
                
                # Intelligent Year Guessing
                # If we are in Dec and see Jan, it's next year.
                # If we are in Jan and see Dec, it's this year (end of year) or last year?
                # Usually release calendars are future-focused.
                calc_year = current_year
                if today.month > 10 and month_num < 3:
                    calc_year = current_year + 1
                elif today.month < 3 and month_num > 10:
                    calc_year = current_year - 1 # Unlikely for a "Release" calendar but possible for archives
                
                try:
                    current_date_str = f"{calc_year}-{month_num:02d}-{day:02d}"
                    # print(f"    Found Date Group: {current_date_str}")
                except:
                    current_date_str = None
                continue
            
            # 2. Detect Product (if we have a date)
            if current_date_str and "Add to cart" not in clean_line and len(clean_line) > 5:
                # Calendar often lists: "- Product Name"
                if clean_line.startswith("- ") or clean_line.startswith("• "):
                    prod_title = clean_line[2:].split("$")[0].strip() # Remove Price if present
                    prod_title = prod_title.split(" - ")[0].strip() # Remove trailing dash descriptions
                    if prod_title:
                        release_map[prod_title.lower()] = current_date_str
                        
    except Exception as e:
        print(f"    [!] Error parsing calendar: {e}")
        
    print(f"    [✓] Found {len(release_map)} release dates.")
    return release_map

# ==========================================
#        PHASE 1: SCRAPE EXTERNAL
# ==========================================

def fetch_external_source(source_name, base_url):
    print(f"    --> Scraping {source_name} ({base_url})...")
    products_found = []
    page = 1
    
    while True:
        try:
            url = f"{base_url}/products.json?limit=250&page={page}"
            r = session.get(url, timeout=20)
            if r.status_code != 200: break
                
            data = r.json()
            batch = data.get('products', [])
            if not batch: break 
                
            products_found.extend(batch)
            print(f"        Page {page}: Got {len(batch)} items...")
            page += 1
            time.sleep(1) 
            
        except Exception as e:
            print(f"        [!] Error scraping {source_name}: {e}")
            break
    return products_found

def compile_scraped_data(release_map):
    print("\n--- PHASE 1: COMPILING SCRAPED DATA (All Sources) ---")
    combined = {}
    
    for name, url in EXTERNAL_SOURCES.items():
        if name == "Moonstone" and not ENABLE_MOONSTONE: continue
        if name == "Warsenal" and not ENABLE_WARSENAL: continue
        if name == "Asmodee" and not ENABLE_ASMODEE: continue
        
        raw_products = fetch_external_source(name, url)
        
        for p in raw_products:
            raw_vendor = p.get('vendor', '')
            if name == "Moonstone" and not raw_vendor: raw_vendor = "Goblin King Games"
            
            # --- UPDATED: NO FILTERING FOR SCRAPED ASMODEE ---
            # If it's on the Asmodee site, we take it.
            
            title = p.get('title', 'Unknown')
            tags_raw = p.get('tags', [])
            tags_list = tags_raw.split(',') if isinstance(tags_raw, str) else tags_raw
            
            faction = determine_faction(raw_vendor, title, tags_list)
            game_system = detect_game_system(raw_vendor, name)
            
            # Check for Release Date Match
            release_date = release_map.get(title.lower())
            # Fallback: check if title is contained in map keys (fuzzy match)
            if not release_date:
                for k, v in release_map.items():
                    if k in title.lower():
                        release_date = v
                        break
            
            images = [{"src": img['src']} for img in p.get('images', [])]
            
            for v in p.get('variants', []):
                sku = v.get('sku')
                if not sku: continue
                
                price = safe_float(v.get('price'))
                compare = safe_float(v.get('compare_at_price'))
                msrp = max(price, compare)
                cost = calculate_cost(msrp, raw_vendor, name)
                weight = safe_int(v.get('grams'))
                
                combined[sku.strip()] = {
                    "title": title,
                    "description": p.get('body_html', ''),
                    "product_type": game_system, 
                    "images": images,
                    "weight": weight,
                    "barcode": v.get('barcode', ''),
                    "target_compare": msrp,
                    "target_price": msrp,
                    "target_cost": cost,
                    "target_vendor": raw_vendor,
                    "target_faction": faction,
                    "release_date": release_date,
                    "source_origin": f"Scrape-{name}"
                }
                
    print(f"    [✓] Total Unique Scraped Items: {len(combined)}")
    return combined

# ==========================================
#        PHASE 2: FETCH SHEET (BACKUP)
# ==========================================

def fetch_sheet_data(existing_skus):
    print("\n--- PHASE 2: FETCHING GOOGLE SHEET (Filtered Backup) ---")
    if not GOOGLE_CREDS_B64:
        print("    [!] No Google Creds found. Skipping Sheet.")
        return {}

    creds = get_google_creds()
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet_id = extract_sheet_id(SHEET_URL)
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range="A:Z").execute()
        rows = result.get('values', [])
    except Exception as e:
        print(f"    [!] Error fetching sheet: {e}")
        return {}

    if not rows: return {}

    headers = [h.lower().strip() for h in rows[0]]
    sheet_data = {}
    skipped_count = 0
    filtered_out_count = 0
    
    try:
        idx_sku = headers.index('sku')
        idx_title = headers.index('title')
        idx_price = headers.index('price') 
        idx_vendor = headers.index('vendor') if 'vendor' in headers else -1
        idx_barcode = headers.index('barcode') if 'barcode' in headers else -1
    except ValueError:
        print("    [!] Critical Header Missing in Sheet (sku, title, price)")
        return {}

    for row in rows[1:]:
        if len(row) <= idx_sku: continue
        
        sku = row[idx_sku].strip()
        if not sku: continue

        # 1. SKIP if found in Scrape
        if sku in existing_skus:
            skipped_count += 1
            continue
            
        # 2. FILTER: Only accept if it looks like an Asmodee Product (Prefix Match)
        # We only want "Asmodee Backup" items here.
        is_asmodee_prefix = any(sku.upper().startswith(pre) for pre in ASMODEE_PREFIXES)
        
        # If it's NOT an Asmodee prefix, we check if the Sheet explicitly says "Warsenal" or "Moonstone"
        # but since those are fully scraped, this is mostly for the generic Asmodee stuff.
        # If user wants ONLY smart filter for Asmodee on Sheet:
        if not is_asmodee_prefix:
            filtered_out_count += 1
            continue
        
        title = row[idx_title] if len(row) > idx_title else "Unknown Item"
        msrp_raw = row[idx_price] if len(row) > idx_price else "0"
        vendor_raw = row[idx_vendor] if idx_vendor != -1 and len(row) > idx_vendor else ""
        barcode = row[idx_barcode] if idx_barcode != -1 and len(row) > idx_barcode else ""
        
        msrp = safe_float(msrp_raw)
        final_vendor = auto_detect_vendor(sku, vendor_raw)
        cost = calculate_cost(msrp, final_vendor, "Sheet")
        faction = determine_faction(final_vendor, title)
        
        sheet_data[sku] = {
            "title": title,
            "description": "", 
            "product_type": "Tabletop Game",
            "images": [], 
            "weight": 0,
            "barcode": barcode,
            "target_compare": msrp,
            "target_price": msrp,
            "target_cost": cost,
            "target_vendor": final_vendor,
            "target_faction": faction,
            "release_date": None,
            "source_origin": "GoogleSheet-Filtered"
        }
        
    print(f"    [✓] Sheet Backup Items: {len(sheet_data)}")
    print(f"    [-] Skipped (Found in Scrape): {skipped_count}")
    print(f"    [-] Filtered Out (No Prefix Match): {filtered_out_count}")
    return sheet_data

# ==========================================
#        PHASE 3: FETCH LIVE CATALOG
# ==========================================

def fetch_live_catalog():
    print("\n--- PHASE 3: FETCHING LIVE SHOPIFY CATALOG ---")
    catalog = {}
    statuses = ["active", "draft", "archived"]
    
    for status in statuses:
        print(f"    --> Fetching Status: {status.upper()}...")
        url = f"{get_shopify_base_url()}/products.json"
        params = {"limit": 250, "status": status}

        while url:
            try:
                r = session.get(url, headers=HEADERS, params=params)
                r.raise_for_status()
                data = r.json()
                
                products = data.get("products", [])
                if not products: break
                
                for p in products:
                    for v in p.get('variants', []):
                        sku = v.get('sku')
                        if sku:
                            catalog[sku.strip()] = {
                                "product_id": p['id'],
                                "variant_id": v['id'],
                                "inventory_item_id": v['inventory_item_id'],
                                "status": p['status'],
                                "tags": p['tags'],
                                "vendor": p['vendor'],
                                "current_price": safe_float(v.get('price')),
                                "current_compare": safe_float(v.get('compare_at_price')),
                                "title": p['title']
                            }

                link_header = r.headers.get('Link')
                url = None
                if link_header and 'rel="next"' in link_header:
                    links = link_header.split(',')
                    next_link = [l for l in links if 'rel="next"' in l]
                    if next_link:
                        url = next_link[0].split(';')[0].strip('<> ')
                        params = {} 
            except Exception as e:
                print(f"    [!] Error fetching live catalog ({status}): {e}")
                break
            
    print(f"    [✓] Live Catalog Size: {len(catalog)}")
    return catalog

def get_location_id_by_name(target_name):
    try:
        r = session.get(f"{get_shopify_base_url()}/locations.json", headers=HEADERS)
        r.raise_for_status()
        locations = r.json().get('locations', [])
        for loc in locations:
            if target_name.lower() in loc['name'].lower():
                return loc['id']
    except Exception:
        pass
    return None

# ==========================================
#        PHASE 4: SYNC LOGIC
# ==========================================

def create_product(target_data, sku, location_id):
    if DRY_RUN:
        print(f"    [DRY] CREATING {sku} | {target_data['title']}")
        return

    tags = ["Tabletop Gaming", "Auto Import", f"Source: {target_data['source_origin']}"]
    if target_data['target_faction']: 
        tags.append(target_data['target_faction'])
    
    # Payload Construction
    payload = {
        "product": {
            "title": target_data['title'],
            "vendor": target_data['target_vendor'],
            "product_type": target_data['product_type'],
            "status": "draft",
            "tags": ", ".join(tags),
            "variants": [{
                "sku": sku,
                "price": f"{target_data['target_price']:.2f}",
                "compare_at_price": f"{target_data['target_compare']:.2f}",
                "barcode": target_data['barcode'],
                "grams": target_data.get('weight', 0),
                "inventory_management": "shopify"
            }],
            # Add Release Date Metafield if present
            "metafields": []
        }
    }
    
    if target_data['release_date']:
        payload['product']['metafields'].append({
            "namespace": "custom",
            "key": "release_date",
            "value": target_data['release_date'],
            "type": "date"
        })

    if target_data['description']: payload['product']['body_html'] = target_data['description']
    if target_data['images']: payload['product']['images'] = target_data['images']

    try:
        r = session.post(f"{get_shopify_base_url()}/products.json", json=payload, headers=HEADERS)
        r.raise_for_status()
        new_prod = r.json()['product']
        inv_item_id = new_prod['variants'][0]['inventory_item_id']
        
        session.put(
            f"{get_shopify_base_url()}/inventory_items/{inv_item_id}.json", 
            json={"inventory_item": {"id": inv_item_id, "cost": f"{target_data['target_cost']:.2f}"}}, 
            headers=HEADERS
        )
        
        if location_id:
            session.post(
                f"{get_shopify_base_url()}/inventory_levels/connect.json",
                json={"inventory_item_id": inv_item_id, "location_id": location_id, "relocate_if_necessary": True},
                headers=HEADERS
            )

        print(f"    [+] CREATED {sku} (Release: {target_data.get('release_date', 'N/A')})")
        
    except Exception as e:
        print(f"    [!] Error Creating {sku}: {e}")

def update_product(live_data, target_data, sku):
    # Rule: Skip Active if Price > 0
    if live_data['status'] == 'active' and live_data['current_price'] > 0:
        if live_data['current_compare'] != target_data['target_compare']:
            print(f"    [!] Price Change on ACTIVE item {sku}. Flipping to DRAFT.")
            if not DRY_RUN:
                new_tags = live_data['tags'] + ", price changed"
                session.put(
                    f"{get_shopify_base_url()}/products/{live_data['product_id']}.json",
                    json={"product": {"id": live_data['product_id'], "status": "draft", "tags": new_tags}},
                    headers=HEADERS
                )
        return

    # Rule: Vendor Safety
    live_vendor_lower = live_data['vendor'].lower()
    is_safe_vendor = any(v in live_vendor_lower for v in SAFE_VENDORS_FOR_UPDATE)
    if not is_safe_vendor and live_data['vendor'] != "":
        if "Sheet" in target_data['source_origin']:
            print(f"    [-] Skipping {sku}: Unsafe Vendor '{live_data['vendor']}'")
            return

    updates_needed = False
    tgt_cmp = target_data['target_compare']
    
    if live_data['current_compare'] != tgt_cmp:
        updates_needed = True

    if DRY_RUN:
        if updates_needed: print(f"    [DRY] UPDATE {sku}")
        return

    try:
        if updates_needed:
            session.put(
                f"{get_shopify_base_url()}/variants/{live_data['variant_id']}.json",
                json={"variant": {
                    "id": live_data['variant_id'], 
                    "price": f"{target_data['target_price']:.2f}",
                    "compare_at_price": f"{tgt_cmp:.2f}"
                }}, 
                headers=HEADERS
            )

        session.put(
            f"{get_shopify_base_url()}/inventory_items/{live_data['inventory_item_id']}.json",
            json={"inventory_item": {"id": live_data['inventory_item_id'], "cost": f"{target_data['target_cost']:.2f}"}},
            headers=HEADERS
        )
        
        # Update Metafield (Release Date) if new data found
        if target_data['release_date']:
            # We blindly update the release date if we found one
            session.post(
                f"{get_shopify_base_url()}/products/{live_data['product_id']}/metafields.json",
                json={"metafield": {
                    "namespace": "custom",
                    "key": "release_date",
                    "value": target_data['release_date'],
                    "type": "date"
                }},
                headers=HEADERS
            )

        if updates_needed: print(f"    [✓] Synced {sku}")

    except Exception as e:
        print(f"    [!] Error updating {sku}: {e}")

# ==========================================
#              MAIN EXECUTION
# ==========================================

def main():
    print(f"--- STARTING UNIVERSAL SYNC V2: {datetime.now()} ---")
    
    if not ACCESS_TOKEN:
        print("    [!] ERROR: Missing SHOPIFY_ACCESS_TOKEN")
        return

    deltona_id = get_location_id_by_name(TARGET_LOCATION_NAME)
    
    # 0. Get Release Dates
    release_map = fetch_asmodee_release_calendar()
    
    # 1. Scrape All (Includes Release Date Injection)
    scraped_data = compile_scraped_data(release_map)
    
    # 2. Fetch Sheet (Backup - Filtered by Prefix)
    sheet_data = fetch_sheet_data(scraped_data.keys())
    
    # 3. Merge
    full_source_map = {**scraped_data, **sheet_data}
    
    # 4. Fetch Live
    live_map = fetch_live_catalog()
    
    print(f"\n--- PHASE 4: EXECUTING UPDATES ({len(full_source_map)} Items) ---")
    processed_count = 0
    
    for sku, target in full_source_map.items():
        if TEST_MODE and processed_count >= TEST_LIMIT: 
            print("--- TEST LIMIT REACHED ---")
            break
            
        if sku in live_map:
            update_product(live_map[sku], target, sku)
        else:
            create_product(target, sku, deltona_id)
            
        processed_count += 1
        time.sleep(0.5) 

    print("\n--- DONE ---")

if __name__ == "__main__":
    main()
