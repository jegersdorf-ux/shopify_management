import os
import json
import base64
import smtplib
import requests
import gspread
import cloudinary
import cloudinary.uploader
import pdfplumber
import io
import re
import time
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from github import Github, Auth
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# --- CONFIGURATION & SECRETS ---
DRY_RUN = False        
TEST_MODE = True       
TEST_LIMIT = 20        
RESET_IGNORED_ITEMS = True

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME')
CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = "2025-10" 

# Target Location Name (Must match exactly what is in Shopify)
TARGET_LOCATION_NAME = "Deltona Florida Store"

EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
EMAIL_RECEIVER = os.getenv('EMAIL_RECEIVER')

# Files & Folders
JSON_FILE_PATH = "product_inventory.json"
REPORT_FILE_PATH = "dry_run_report.md" 
PROMPTS_FILE_PATH = "gemini_prompts.txt"
CLOUDINARY_ROOT_FOLDER = "Extra-Turn-Games"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit"

# Asmodee Prefixes
ASMODEE_PREFIXES = [
    "CHX", "ESS", "FF", "G2", "GG49", "GG3", "GG2", "GGS2", "NEM", 
    "SWA", "SWC", "SWO", "SWD", "SWF", "SWL", "SWU", "USWA", 
    "CPE", "CP", "SWP", "SWQ", "CA"
]

# --- CLOUDINARY SETUP ---
RATE_LIMIT_HIT = False 

if not DRY_RUN:
    cloudinary.config(
        cloud_name=CLOUDINARY_CLOUD_NAME, 
        api_key=CLOUDINARY_API_KEY, 
        api_secret=CLOUDINARY_API_SECRET,
        secure=True
    )

dry_run_logs = []

def log_action(action_type, sku, details):
    entry = f"| {action_type} | **{sku}** | {details} |"
    print(f"[DRY RUN] {entry}", flush=True)
    dry_run_logs.append(entry)

# ==========================================
#           SHOPIFY HELPER
# ==========================================
def get_shopify_url():
    if not SHOPIFY_STORE_URL: return None
    clean_url = SHOPIFY_STORE_URL.strip()
    if "admin.shopify.com" in clean_url and "/store/" in clean_url:
        try:
            handle = clean_url.split("/store/")[1].split("/")[0]
            return f"https://{handle}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        except: pass
    
    clean_url = clean_url.replace("https://", "").replace("http://", "")
    if "/" in clean_url: clean_url = clean_url.split("/")[0]
    return f"https://{clean_url}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

def get_location_id():
    """Fetches the ID for the Deltona Florida Store"""
    if DRY_RUN: return "gid://shopify/Location/123456789"
    
    query = "{ locations(first: 10) { edges { node { id, name } } } }"
    url = get_shopify_url()
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": query}, headers=headers, timeout=10)
        data = r.json()
        for edge in data['data']['locations']['edges']:
            loc = edge['node']
            if TARGET_LOCATION_NAME.lower() in loc['name'].lower():
                print(f"[INFO] Found Location '{loc['name']}': {loc['id']}", flush=True)
                return loc['id']
        
        # Fallback to first location if not found
        fallback = data['data']['locations']['edges'][0]['node']['id']
        print(f"[WARN] Target location '{TARGET_LOCATION_NAME}' not found. Using default: {fallback}", flush=True)
        return fallback
    except Exception as e:
        print(f"[!] Error fetching locations: {e}", flush=True)
        return None

def test_shopify_connection():
    print("\n--- DIAGNOSTICS ---", flush=True)
    
    if not CLOUDINARY_CLOUD_NAME:
        print("[FAIL] CLOUDINARY_CLOUD_NAME is missing/null.", flush=True)
    else:
        print(f"[OK] Cloudinary Configured: {CLOUDINARY_CLOUD_NAME}", flush=True)

    print("--- TESTING SHOPIFY CONNECTION ---", flush=True)
    url = get_shopify_url()
    
    domain_part = url.split("//")[1].split("/")[0] if url else "None"
    print(f"Target Domain: {domain_part}", flush=True)
    
    if "myshopify.com" not in domain_part:
        print("\n[!!!] CRITICAL WARNING: Domain is not .myshopify.com", flush=True)
        return False

    query = "{ shop { name, myshopifyDomain } }"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": query}, headers=headers, timeout=10)
        if r.status_code == 200:
            shop = r.json().get('data', {}).get('shop', {})
            print(f"SUCCESS: Connected to '{shop.get('name')}'", flush=True)
            print("----------------------------------\n", flush=True)
            return True
        elif r.status_code == 403:
             print(f"FAIL: Status 403 (Forbidden). Firewall blocking.", flush=True)
             return False
        elif r.status_code == 401:
             print(f"FAIL: Status 401 (Unauthorized). Invalid Token.", flush=True)
             return False
        else:
            print(f"FAIL: Status {r.status_code}: {r.text}", flush=True)
            return False
    except Exception as e:
        print(f"FAIL: Exception connecting: {e}", flush=True)
        return False

# ==========================================
#           DATA MAPPING HELPERS
# ==========================================

def determine_game_name_asmodee(sku):
    s = str(sku).upper()
    if s.startswith("SWP"): return "Star Wars: Shatterpoint"
    if s.startswith("SWL") or s.startswith("SWQ"): return "Star Wars: Legion"
    if s.startswith("CP") or s.startswith("CA"): return "Marvel: Crisis Protocol"
    return "Asmodee Game"

def format_title_prefix(title, game_name):
    """Ensures titles start with the correct game prefix"""
    clean_title = str(title).strip()
    prefix = f"{game_name}: "
    if not clean_title.startswith(game_name):
        return f"{prefix}{clean_title}"
    return clean_title

def determine_vendor(source_vendor, game_name):
    """Maps source vendors to your preferred Publisher names"""
    if "Moonstone" in game_name:
        return "Goblin King Games"
    if "Infinity" in game_name:
        return "Corvus Belli"
    if "Asmodee" in source_vendor:
        return "Asmodee"
    return source_vendor

# ==========================================
#           CATALOG DISCOVERY
# ==========================================

def discover_shopify_catalog(store_url, source_label):
    print(f"  > Discovering {source_label} Catalog (Rich Data)...", flush=True)
    base_url = f"{store_url}/products.json"
    page = 1
    found_items = []
    
    # Pre-calculate Game Name
    game_name = "Moonstone" if "Moonstone" in source_label else "Infinity"
    
    while True:
        try:
            if page % 5 == 0: print(f"    ...scanning page {page}...", flush=True)
            
            r = requests.get(f"{base_url}?limit=250&page={page}", timeout=15)
            if r.status_code != 200: break
            products = r.json().get('products', [])
            if not products: break
            
            for p in products:
                if not p.get('variants'): continue
                variant = p['variants'][0]
                sku = variant.get('sku')
                if not sku: continue
                
                # Capture Data
                images = [img['src'] for img in p.get('images', [])]
                source_vendor = p.get('vendor', source_label)
                
                # Map Fields
                final_vendor = determine_vendor(source_vendor, game_name)
                final_title = format_title_prefix(p.get('title', ''), game_name)
                
                # Price Logic (MSRP priority)
                base_price = variant.get('compare_at_price')
                sell_price = variant.get('price', '0.00')
                final_price = base_price if (base_price and float(base_price) > 0) else sell_price

                found_items.append({
                    "sku": sku, 
                    "title": final_title, 
                    "vendor": final_vendor,
                    "game_name": game_name,
                    "description": p.get('body_html', ''), 
                    "images_source": images, 
                    "pdf": "", 
                    "upc": variant.get('barcode', ''),
                    "weight": variant.get('weight', 0),
                    "weight_unit": variant.get('weight_unit', 'g'),
                    "source_price": final_price, 
                    "active_status": variant.get('available', True), 
                    "release_date": None
                })
            page += 1
            time.sleep(0.5)
        except: break
    return found_items

def discover_corvus_catalog():
    print("  > Discovering Corvus Belli Catalog...", flush=True)
    sitemap_url = "https://store.corvusbelli.com/sitemap.xml"
    found_items = []
    try:
        r = requests.get(sitemap_url, timeout=15)
        if r.status_code == 200:
            urls = re.findall(r'<loc>(.*?)</loc>', r.text)
            p_urls = [u for u in urls if '/products/' in u or '/wargames/' in u or '/boardgames/' in u]
            p_urls = [u for u in p_urls if not u.endswith('/wargames') and not u.endswith('/boardgames')]
            
            for url in p_urls:
                slug = url.split('/')[-1]
                raw_title = slug.replace('-', ' ').title()
                
                found_items.append({
                    "sku": f"CB_LOOKUP_{slug}", 
                    "title": format_title_prefix(raw_title, "Infinity"),
                    "vendor": "Corvus Belli",
                    "game_name": "Infinity",
                    "images_source": [], 
                    "pdf": "", 
                    "url": url,
                    "is_skeleton": True,
                    "description": "",
                    "weight": 0,
                    "weight_unit": "g",
                    "upc": ""
                })
    except: pass
    return found_items

# ==========================================
#           SCRAPING / RESOLVING
# ==========================================

def scrape_asmodee_status(sku):
    search_url = f"https://store.asmodee.com/search?q={sku}&type=product"
    if DRY_RUN: return True
    try:
        r = requests.get(search_url, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        link = soup.find('a', href=re.compile(r'/products/'))
        if not link: return False 
        
        r2 = requests.get("https://store.asmodee.com" + link['href'], timeout=10)
        text = BeautifulSoup(r2.text, 'html.parser').get_text().lower()
        return "add to cart" in text and "sold out" not in text
    except: return False

def resolve_corvus_skeleton(item):
    url = item['url']
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        sku = None
        ref_match = re.search(r'Ref:\s*([A-Za-z0-9\-]+)', soup.get_text())
        if ref_match: sku = ref_match.group(1)
        if sku:
            item['sku'] = sku
            h1 = soup.find('h1')
            if h1: 
                raw_title = h1.get_text().strip()
                item['title'] = format_title_prefix(raw_title, "Infinity")
            
            desc_tag = soup.select_one('.product-description, .description')
            if desc_tag:
                item['description'] = str(desc_tag)
            
            images = []
            og_img = soup.find("meta", property="og:image")
            if og_img: images.append(og_img['content'])
            for img in soup.find_all('img'):
                src = img.get('src') or img.get('data-src')
                if src and 'assets.corvusbelli' in src and not any(x in src for x in ['icon', 'logo', 'banner', 'footer']):
                    images.append(src)
            
            item['images_source'] = list(set(images))
            item['active_status'] = "add to cart" in soup.get_text().lower()
            item['is_skeleton'] = False
            return item
    except: pass
    return None

# ==========================================
#           STANDARD HELPERS
# ==========================================

def send_email(subject, body_html):
    if DRY_RUN: return
    if not EMAIL_SENDER or not EMAIL_PASSWORD: return
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
    except: pass

def determine_cloudinary_folder(vendor):
    v_lower = str(vendor).lower()
    if "moonstone" in v_lower or "goblin" in v_lower:
        return f"{CLOUDINARY_ROOT_FOLDER}/Moonstone"
    elif "warsenal" in v_lower or "infinity" in v_lower or "corvus" in v_lower:
        return f"{CLOUDINARY_ROOT_FOLDER}/Infinity"
    elif "asmodee" in v_lower:
        return f"{CLOUDINARY_ROOT_FOLDER}/Asmodee"
    else:
        return f"{CLOUDINARY_ROOT_FOLDER}/Other"

def process_and_upload_images(sku, source_urls, vendor):
    global RATE_LIMIT_HIT 
    
    print(f"    [DEBUG] Found {len(source_urls)} source images for {sku}", flush=True)
    
    if not source_urls: return []
    uploaded_urls = []
    source_urls = list(set(source_urls)) 

    target_folder = determine_cloudinary_folder(vendor)

    for i, url in enumerate(source_urls):
        if not url or "http" not in url: continue
        if RATE_LIMIT_HIT:
            uploaded_urls.append(url)
            continue

        suffix = f"_{i}" if i > 0 else ""
        clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
        public_id_name = f"{clean_sku}{suffix}"
        
        if DRY_RUN:
            log_action("CLOUDINARY", sku, f"Upload {public_id_name} to {target_folder}")
            uploaded_urls.append("https://res.cloudinary.com/demo/image.jpg")
            continue
            
        try:
            print(f"    [DEBUG] Uploading img for {sku} to {target_folder}...", flush=True)
            res = cloudinary.uploader.upload(
                url, 
                folder=target_folder, 
                public_id=public_id_name, 
                overwrite=True, 
                unique_filename=False
            )
            secure_url = res['secure_url']
            print(f"    [DEBUG] Success! Cloudinary URL: {secure_url}", flush=True)
            uploaded_urls.append(secure_url)
        except Exception as e:
            error_msg = str(e)
            if "420" in error_msg or "Rate Limit" in error_msg:
                print(f"    [🛑] CLOUDINARY RATE LIMIT REACHED.", flush=True)
                RATE_LIMIT_HIT = True
                uploaded_urls.append(url) 
            else:
                print(f"    [!] Cloudinary Error for {sku}: {error_msg}", flush=True)
                pass
    return uploaded_urls

# ==========================================
#           SHOPIFY API 2025 COMPATIBILITY
# ==========================================

def create_shopify_product_shell(product_data, release_date=None):
    """ STEP 1: Create Product Shell """
    if DRY_RUN:
        log_action("SHOPIFY", product_data['sku'], f"Draft Shell: {product_data['title']}")
        return "gid://shopify/Product/1", "gid://shopify/ProductVariant/1"

    tags = ["Review Needed", "New Auto-Import", product_data['vendor']]
    if release_date: tags.append(f"Release: {release_date}")

    gql_input = {
        "title": product_data['title'],
        "status": "DRAFT",
        "vendor": product_data['vendor'],
        "productType": "Dice Sets & Games", # User Requested Category
        "tags": tags,
        "descriptionHtml": product_data.get('description') or (f"<p>Release: {release_date}</p>" if release_date else "")
    }

    mutation = """
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        product { id, variants(first:1) { edges { node { id, inventoryItem { id } } } } }
        userErrors { field, message }
      }
    }
    """
    
    url = get_shopify_url() 
    if not url or not SHOPIFY_ACCESS_TOKEN: return None, None, None
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": {"input": gql_input}}, headers=headers, timeout=20)
        data = r.json()
        
        if 'data' in data and data['data']['productCreate']['product']:
            prod = data['data']['productCreate']['product']
            prod_id = prod['id']
            variant_node = prod['variants']['edges'][0]['node']
            variant_id = variant_node['id']
            inventory_item_id = variant_node['inventoryItem']['id']
            print(f"    [SUCCESS] Created Draft: {prod_id}", flush=True)
            time.sleep(0.5) 
            return prod_id, variant_id, inventory_item_id
        
        if data.get('data', {}).get('productCreate', {}).get('userErrors'):
            print(f"    [!] Shopify Shell Error: {data['data']['productCreate']['userErrors']}", flush=True)
        
        return None, None, None
    except Exception as e:
        print(f"    [!] Connect Error (Shell): {e}", flush=True)
        return None, None, None

def update_default_variant(variant_id, sku, upc=None, weight=0, weight_unit='GRAMS', price="0.00"):
    """ STEP 2: Update Variant Data """
    if DRY_RUN or not variant_id: return

    # Normalize Weight Unit
    shopify_unit = "GRAMS"
    w_unit_clean = str(weight_unit).upper()
    if "KG" in w_unit_clean: shopify_unit = "KILOGRAMS"
    elif "OZ" in w_unit_clean: shopify_unit = "OUNCES"
    elif "LB" in w_unit_clean: shopify_unit = "POUNDS"

    mutation = """
    mutation productVariantUpdate($input: ProductVariantInput!) {
      productVariantUpdate(input: $input) {
        productVariant { id, sku }
        userErrors { field, message }
      }
    }
    """
    
    gql_input = {
        "id": variant_id,
        "sku": sku,
        "inventoryManagement": "SHOPIFY", 
        "inventoryPolicy": "DENY",
        "price": price if float(price) > 0 else "0.00", 
        "barcode": upc or "",
        "weight": float(weight),
        "weightUnit": shopify_unit
    }
    
    url = get_shopify_url() 
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": {"input": gql_input}}, headers=headers, timeout=20)
        data = r.json()
        if data.get('data', {}).get('productVariantUpdate', {}).get('userErrors'):
             print(f"    [!] Variant Update Error: {data['data']['productVariantUpdate']['userErrors']}", flush=True)
        time.sleep(0.5) 
    except: pass

def activate_inventory_at_location(inventory_item_id, location_id):
    """ STEP 2.5: Activate Inventory at Deltona """
    if DRY_RUN or not inventory_item_id or not location_id: return

    mutation = """
    mutation inventoryActivate($inventoryItemId: ID!, $locationId: ID!, $available: Int) {
      inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available) {
        inventoryLevel { id }
        userErrors { field, message }
      }
    }
    """
    
    url = get_shopify_url() 
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        # Set initial stock to 0 but activate the location
        r = requests.post(url, json={"query": mutation, "variables": {"inventoryItemId": inventory_item_id, "locationId": location_id, "available": 0}}, headers=headers, timeout=20)
        data = r.json()
        if data.get('data', {}).get('inventoryActivate', {}).get('userErrors'):
             print(f"    [!] Inventory Activation Error: {data['data']['inventoryActivate']['userErrors']}", flush=True)
        else:
             print(f"    [SUCCESS] Activated at Deltona.", flush=True)
        time.sleep(0.5) 
    except: pass

def update_product_metafields(product_id, game_name):
    """ STEP 2.75: Add Game Name Metafield """
    if DRY_RUN or not product_id: return

    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id, key, value }
        userErrors { field, message }
      }
    }
    """
    
    variables = {
        "metafields": [
            {
                "ownerId": product_id,
                "namespace": "custom",
                "key": "game_name",
                "type": "single_line_text_field",
                "value": game_name
            }
        ]
    }
    
    url = get_shopify_url() 
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": variables}, headers=headers, timeout=20)
        data = r.json()
        if data.get('data', {}).get('metafieldsSet', {}).get('userErrors'):
             print(f"    [!] Metafield Error: {data['data']['metafieldsSet']['userErrors']}", flush=True)
    except: pass

def update_shopify_images(shopify_id, image_urls, alt_text=""):
    """ STEP 3: Add Media """
    if DRY_RUN or not image_urls: return

    gql_media = []
    for url in image_urls:
        media_obj = {"originalSource": url, "mediaContentType": "IMAGE"}
        if alt_text:
            media_obj["alt"] = alt_text
        gql_media.append(media_obj)

    mutation = """
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        media { id, status }
        mediaUserErrors { field, message }
      }
    }
    """
    
    url = get_shopify_url() 
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": {"productId": shopify_id, "media": gql_media}}, headers=headers, timeout=20)
        data = r.json()
        if data.get('data', {}).get('productCreateMedia', {}).get('mediaUserErrors'):
            print(f"    [!] Image Sync Error: {data['data']['productCreateMedia']['mediaUserErrors']}", flush=True)
        else:
            print(f"    [SUCCESS] Synced {len(image_urls)} images.", flush=True)
        time.sleep(0.5) 
    except: pass

def check_shopify_status(sku):
    url = get_shopify_url()
    if not url or not SHOPIFY_ACCESS_TOKEN: return None

    query = """query($query: String!) { products(first: 1, query: $query) { edges { node { id, status, tags, variants(first:1) { edges { node { sku } } } } } } }"""
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": query, "variables": {"query": f"sku:{sku}"}}, headers=headers, timeout=15)
        node = r.json()['data']['products']['edges'][0]['node']
        if node['variants']['edges'][0]['node']['sku'] == sku:
            return {"id": node['id'], "status": node['status'], "tags": node['tags']}
    except: pass
    return None

def add_tag(shopify_id, tag, sku):
    if DRY_RUN: return
    url = get_shopify_url()
    mutation = """mutation tagsAdd($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { node { id } } }"""
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    requests.post(url, json={"query": mutation, "variables": {"id": shopify_id, "tags": [tag]}}, headers=headers, timeout=10)

def extract_pdf_text(pdf_url):
    if not pdf_url or "http" not in pdf_url: return ""
    if DRY_RUN: return "DRY RUN PDF"
    try:
        if "drive.google.com" in pdf_url and "/view" in pdf_url:
             pdf_url = pdf_url.replace("/file/d/", "/uc?id=").replace("/view", "").split("?")[0] + "?export=download"
        r = requests.get(pdf_url, allow_redirects=True, timeout=30)
        if r.status_code != 200: return ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            return "\n".join([page.extract_text() or "" for page in pdf.pages])
    except: return ""

def get_col_index(headers, name):
    for i, h in enumerate(headers):
        if str(h).lower().strip() == name.lower().strip(): return i
    return -1

# ==========================================
#              MAIN LOGIC
# ==========================================

def main():
    print(f"--- STARTING MASTER SYNC (TEST MODE: {TEST_MODE} | LIMIT: {TEST_LIMIT} SUCCESSFUL ITEMS) ---", flush=True)
    
    if not test_shopify_connection():
        print("CRITICAL: Cannot connect to Shopify. Exiting.", flush=True)
        return

    # FETCH LOCATION ID
    DELTONA_LOCATION_ID = get_location_id()
    if not DELTONA_LOCATION_ID:
        print("CRITICAL: Could not find 'Deltona Florida Store' location ID. Exiting.", flush=True)
        return

    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth)
    repo = g.get_repo(REPO_NAME)
    
    try:
        contents = repo.get_contents(JSON_FILE_PATH)
        inventory_data = json.loads(contents.decoded_content.decode())
    except: inventory_data = []
    inventory_map = {item.get('sku'): item for item in inventory_data}
    print(f"Loaded {len(inventory_map)} existing items.", flush=True)

    moonstone_items = discover_shopify_catalog("https://shop.moonstonethegame.com", "Moonstone")
    warsenal_items = discover_shopify_catalog("https://warsen.al", "Warsenal")
    corvus_skeletons = discover_corvus_catalog()
    
    asmodee_items = []
    try:
        print("Connecting to Google Sheet (Filtering Hidden Rows)...", flush=True)
        all_values = get_visible_sheet_values(SHEET_URL)
        if all_values:
            h_idx = -1
            for i, row in enumerate(all_values[:5]):
                if any('sku' in str(c).lower() for c in row): h_idx = i; break
            if h_idx != -1:
                headers = all_values[h_idx]
                idx_sku = get_col_index(headers, "SKU")
                idx_title = get_col_index(headers, "Title")
                idx_img = get_col_index(headers, "POS Images")
                idx_pdf = get_col_index(headers, "Sell Sheet")
                for row in all_values[h_idx+1:]:
                    if len(row) <= idx_sku: continue
                    sku = str(row[idx_sku]).strip()
                    if not sku: continue
                    if any(sku.startswith(p) for p in ASMODEE_PREFIXES):
                        raw_link = str(row[idx_img]) if idx_img != -1 else ""
                        resolved_images = get_asmodee_image_links(raw_link)
                        
                        game_name = determine_game_name_asmodee(sku)
                        
                        asmodee_items.append({
                            "sku": sku, 
                            "title": row[idx_title], 
                            "vendor": "Asmodee",
                            "game_name": game_name,
                            "images_source": resolved_images,
                            "pdf": row[idx_pdf], 
                            "active_status": None,
                            "description": "",
                            "weight": 0,
                            "weight_unit": "g",
                            "source_price": "0.00"
                        })
    except Exception as e: print(f"Sheet Error: {e}", flush=True)

    all_rows = asmodee_items + moonstone_items + warsenal_items + corvus_skeletons
    print(f"Total Combined Queue: {len(all_rows)} items", flush=True)
    
    updates_made = False
    new_items_added = []
    active_conflicts = []
    prompts_to_generate = []
    
    successful_drafts_count = 0 
    skips_count = 0

    for i, item in enumerate(all_rows):
        if TEST_MODE and successful_drafts_count >= TEST_LIMIT:
            print(f"--- TEST LIMIT REACHED ({TEST_LIMIT} successful uploads). Stopping Loop. ---", flush=True)
            break

        if item.get('is_skeleton'):
            if i % 10 == 0: print(f"    ...Resolving Skeleton Item {i} ({item['sku']})...", flush=True)
            resolved = resolve_corvus_skeleton(item)
            if not resolved: continue
            item = resolved
        
        sku = item['sku']
        vendor = item['vendor']
        
        existing_product = inventory_map.get(sku)
        
        if existing_product and existing_product.get('shopify_status') == "PERMANENTLY_IGNORED":
            if not RESET_IGNORED_ITEMS:
                skips_count += 1
                continue
        
        if (skips_count + successful_drafts_count) % 50 == 0:
            print(f"Processing... (Skips: {skips_count} | Success: {successful_drafts_count})", flush=True)

        is_avail = True
        if vendor == "Asmodee":
            is_avail = scrape_asmodee_status(sku)
        else:
            is_avail = item.get('active_status', True)

        source_images = item.get('images_source', [])

        if not existing_product:
            if not is_avail:
                skips_count += 1
                continue
            
            if vendor == "Asmodee" and not source_images:
                if not is_avail:
                    print(f"    [DEAD] Asmodee Item {sku} has no images AND is not found. Marking Ignored.", flush=True)
                    new_entry = {
                        "sku": sku, "title": item['title'], "vendor": vendor,
                        "cloudinary_images": [], "shopify_id": None,
                        "shopify_status": "PERMANENTLY_IGNORED", 
                        "release_date": item.get('release_date'), "upc": item.get('upc')
                    }
                    inventory_map[sku] = new_entry
                    updates_made = True
                    skips_count += 1
                    continue
                else:
                    print(f"    [SKIP] Asmodee Item {sku} has no images but is active. Skipping.", flush=True)
                    skips_count += 1
                    continue

            print(f"   > Uploading {sku}...", flush=True)
            cloud_urls = process_and_upload_images(sku, source_images, vendor)
            
            # --- 3-STEP CREATION WITH LOCATION ---
            prod_id, variant_id, inventory_item_id = create_shopify_product_shell(item, item.get('release_date'))
            
            if prod_id:
                # 1. Update Variant Data (SKU, GTIN, Weight, Price)
                update_default_variant(
                    variant_id, 
                    sku, 
                    item.get('upc'),
                    weight=item.get('weight', 0),
                    weight_unit=item.get('weight_unit', 'g'),
                    price=item.get('source_price', "0.00")
                )
                
                # 2. Activate Inventory at Deltona
                if inventory_item_id and DELTONA_LOCATION_ID:
                    activate_inventory_at_location(inventory_item_id, DELTONA_LOCATION_ID)
                
                # 3. Set Game Name Metafield
                if item.get('game_name'):
                    update_product_metafields(prod_id, item['game_name'])

                # 4. Attach Images
                update_shopify_images(prod_id, cloud_urls, alt_text=item['title'])
                
                new_entry = {
                    "sku": sku, "title": item['title'], "vendor": vendor,
                    "cloudinary_images": cloud_urls, "shopify_id": prod_id,
                    "release_date": item.get('release_date'), "upc": item.get('upc'),
                    "shopify_status": "DRAFT_CREATED"
                }
                inventory_map[sku] = new_entry
                new_items_added.append(new_entry)
                updates_made = True
                successful_drafts_count += 1 
            else:
                 print(f"   [FAIL] Shopify upload failed for {sku}", flush=True)
                 skips_count += 1
            
            if item.get('pdf') or vendor == "Asmodee":
                txt = extract_pdf_text(item.get('pdf')) if item.get('pdf') else ""
                prompt = f"PROMPT FOR {sku}: {item['title']} (Vendor: {vendor})\n{txt[:1000]}"
                prompts_to_generate.append(prompt)

        else:
            # Self Healing Logic
            stored_images = existing_product.get('cloudinary_images')
            shopify_id = existing_product.get('shopify_id')
            
            if not shopify_id:
                status_data = check_shopify_status(sku)
                if status_data: 
                    shopify_id = status_data['id']
                    existing_product['shopify_id'] = shopify_id
                    updates_made = True

            if (not stored_images) and source_images:
                print(f"   > Backfilling images for {sku}...", flush=True)
                cloud_urls = process_and_upload_images(sku, source_images, vendor)
                if cloud_urls:
                    existing_product['cloudinary_images'] = cloud_urls
                    updates_made = True
                    if shopify_id:
                        update_shopify_images(shopify_id, cloud_urls, alt_text=existing_product['title'])
                        successful_drafts_count += 1
            else:
                skips_count += 1
                
            if not is_avail:
                if not shopify_id:
                    s_data = check_shopify_status(sku)
                    if s_data and s_data['status'] == 'ACTIVE' and "Review Needed" not in s_data['tags']:
                        add_tag(s_data['id'], "Review Needed", sku)
                        active_conflicts.append(existing_product)

    if new_items_added: send_email(f"Import: {len(new_items_added)} New", "Check Shopify.")
    if active_conflicts: send_email(f"ALERT: {len(active_conflicts)} Conflicts", "Check Shopify.")

    if DRY_RUN:
        print("Saving Dry Run Report...", flush=True)
        report = "\n".join(dry_run_logs)
        try: repo.update_file(REPORT_FILE_PATH, "Report", report, repo.get_contents(REPORT_FILE_PATH).sha)
        except: repo.create_file(REPORT_FILE_PATH, "Report", report)
    elif updates_made:
        print("Saving JSON...", flush=True)
        try: repo.update_file(JSON_FILE_PATH, "Sync", json.dumps(list(inventory_map.values()), indent=2), repo.get_contents(JSON_FILE_PATH).sha)
        except: repo.create_file(JSON_FILE_PATH, "Sync", json.dumps(list(inventory_map.values()), indent=2))
        if prompts_to_generate:
             full_prompt = "\n".join(prompts_to_generate)
             try: repo.update_file(PROMPTS_FILE_PATH, "Prompts", full_prompt, repo.get_contents(PROMPTS_FILE_PATH).sha)
             except: repo.create_file(PROMPTS_FILE_PATH, "Prompts", full_prompt)

    print("--- MASTER SYNC COMPLETE ---", flush=True)

if __name__ == "__main__":
    main()
