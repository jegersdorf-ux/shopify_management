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

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME')
CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = "2026-01" 

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
    "CPE", "CP", "SWP", "SWQ"
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
    print(f"[DRY RUN] {entry}")
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

def test_shopify_connection():
    print("\n--- TESTING SHOPIFY CONNECTION ---", flush=True)
    url = get_shopify_url()
    
    domain_part = url.split("//")[1].split("/")[0] if url else "None"
    print(f"Target Domain: {domain_part}", flush=True)
    
    if "myshopify.com" not in domain_part:
        print("\n[!!!] CRITICAL WARNING:", flush=True)
        print(f"      You are using: '{domain_part}'", flush=True)
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
            print(f"FAIL: Status {r.status_code}", flush=True)
            print(f"Response: {r.text}", flush=True)
            return False
    except Exception as e:
        print(f"FAIL: Exception connecting: {e}", flush=True)
        return False

# ==========================================
#           GOOGLE API HELPERS
# ==========================================
def get_credentials():
    if not GOOGLE_CREDENTIALS_BASE64: return None
    try:
        return ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode()), 
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
    except: return None

def get_drive_service():
    creds = get_credentials()
    if not creds: return None
    return build('drive', 'v3', credentials=creds)

def get_sheets_service():
    creds = get_credentials()
    if not creds: return None
    return build('sheets', 'v4', credentials=creds)

def extract_drive_id(url):
    if not url or "drive.google.com" not in url: return None
    match = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
    if match: return match.group(1)
    match = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    if match: return match.group(1)
    return None

def walk_drive_folder(service, folder_id):
    found_images = []
    page_token = None
    while True:
        try:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                q=query, pageSize=1000, 
                fields="nextPageToken, files(id, name, mimeType, webContentLink)",
                pageToken=page_token
            ).execute()
            items = results.get('files', [])
            for item in items:
                if 'image/' in item['mimeType']:
                    if item.get('webContentLink'): found_images.append(item.get('webContentLink'))
                elif item['mimeType'] == 'application/vnd.google-apps.folder':
                    found_images.extend(walk_drive_folder(service, item['id']))
            page_token = results.get('nextPageToken')
            if not page_token: break
        except: break
    return found_images

def get_asmodee_image_links(url):
    if not url: return []
    folder_id = extract_drive_id(url)
    if folder_id:
        service = get_drive_service()
        if service: return walk_drive_folder(service, folder_id)
    return [url]

def get_visible_sheet_values(sheet_url):
    match_id = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not match_id: return []
    spreadsheet_id = match_id.group(1)
    target_gid = 0
    match_gid = re.search(r'[#&]gid=([0-9]+)', sheet_url)
    if match_gid: target_gid = int(match_gid.group(1))

    service = get_sheets_service()
    if not service: return []
    
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id, includeGridData=True).execute()
        target_sheet = None
        for s in spreadsheet['sheets']:
            if s['properties']['sheetId'] == target_gid:
                target_sheet = s
                break
        if not target_sheet: return []
        print(f"    > Reading Tab: {target_sheet['properties']['title']} (GID: {target_gid})", flush=True)
        
        rows = target_sheet['data'][0].get('rowData', [])
        visible_rows = []
        for row in rows:
            user_hidden = row.get('rowMetadata', {}).get('hiddenByUser', False)
            filter_hidden = row.get('rowMetadata', {}).get('hiddenByFilter', False)
            if user_hidden or filter_hidden: continue 
            values = []
            if 'values' in row:
                for cell in row['values']:
                    val = cell.get('userEnteredValue', {})
                    text = val.get('stringValue', str(val.get('numberValue', '')))
                    values.append(text)
            visible_rows.append(values)
        return visible_rows
    except: return []

# ==========================================
#           CATALOG DISCOVERY
# ==========================================

def discover_shopify_catalog(store_url, vendor_name):
    print(f"  > Discovering {vendor_name} Catalog...", flush=True)
    base_url = f"{store_url}/products.json"
    page = 1
    found_items = []
    while True:
        try:
            r = requests.get(f"{base_url}?limit=250&page={page}", timeout=15)
            if r.status_code != 200: break
            products = r.json().get('products', [])
            if not products: break
            for p in products:
                if not p.get('variants'): continue
                variant = p['variants'][0]
                sku = variant.get('sku')
                if not sku: continue
                images = [img['src'] for img in p.get('images', [])]
                found_items.append({
                    "sku": sku, "title": p['title'], "vendor": vendor_name,
                    "images_source": images, "pdf": "", "upc": variant.get('barcode'),
                    "active_status": variant.get('available', True), "release_date": None
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
                found_items.append({
                    "sku": f"CB_LOOKUP_{slug}", "title": slug.replace('-', ' ').title(),
                    "vendor": "Corvus Belli", "images_source": [], "pdf": "", "url": url,
                    "is_skeleton": True
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
            if h1: item['title'] = h1.get_text().strip()
            images = []
            og_img = soup.find("meta", property="og:image")
            if og_img: images.append(og_img['content'])
            for img in soup.find_all('img'):
                src = img.get('src') or img.get('data-src')
                if src and 'assets.corvusbelli' in src and not any(x in src for x in ['icon', 'logo']):
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

def process_and_upload_images(sku, source_urls):
    global RATE_LIMIT_HIT 
    
    if not source_urls: return []
    uploaded_urls = []
    source_urls = list(set(source_urls)) 

    for i, url in enumerate(source_urls):
        if not url or "http" not in url: continue
        
        if RATE_LIMIT_HIT:
            print(f"    [!] Skipping Cloudinary upload for {sku} (Rate Limit Active). Using source URL.", flush=True)
            uploaded_urls.append(url)
            continue

        suffix = f"_{i}" if i > 0 else ""
        clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
        public_id = f"{CLOUDINARY_ROOT_FOLDER}/{clean_sku}{suffix}"
        
        if DRY_RUN:
            log_action("CLOUDINARY", sku, f"Upload {public_id}")
            uploaded_urls.append("https://res.cloudinary.com/demo/image.jpg")
            continue
            
        try:
            res = cloudinary.uploader.upload(
                url, 
                public_id=public_id, 
                overwrite=True, 
                unique_filename=False
            )
            uploaded_urls.append(res['secure_url'])
            
        except Exception as e:
            error_msg = str(e)
            if "420" in error_msg or "Rate Limit" in error_msg:
                print(f"    [🛑] CLOUDINARY RATE LIMIT REACHED. Switching to pass-through mode.", flush=True)
                RATE_LIMIT_HIT = True
                uploaded_urls.append(url) 
            else:
                print(f"    [!] Cloudinary Upload Failed for {sku}: {error_msg}", flush=True)
                pass
            
    return uploaded_urls

def create_shopify_draft(product_data, image_urls, release_date=None, upc=None):
    if DRY_RUN:
        log_action("SHOPIFY", product_data['sku'], f"Draft: {product_data['title']} ({len(image_urls)} images)")
        return "gid://shopify/Product/1"
    
    tags = ["Review Needed", "New Auto-Import", product_data['vendor']]
    if release_date: tags.append(f"Release: {release_date}")
    
    gql_media = []
    for url in image_urls:
        gql_media.append({
            "originalSource": url,
            "mediaContentType": "IMAGE"
        })

    gql_variants = [{
        "sku": product_data['sku'],
        "inventoryManagement": "SHOPIFY",
        "price": "0.00",
        "barcode": upc or ""
    }]

    gql_input = {
        "title": product_data['title'],
        "status": "DRAFT",
        "vendor": product_data['vendor'],
        "tags": tags,
        "descriptionHtml": f"<p>Release: {release_date}</p>" if release_date else ""
    }

    mutation = """
    mutation productCreate($input: ProductInput!, $media: [CreateMediaInput!], $variants: [ProductVariantInput!]) {
      productCreate(input: $input, media: $media, variants: $variants) {
        product { id }
        userErrors { field, message }
      }
    }
    """
    
    variables = {"input": gql_input, "media": gql_media, "variants": gql_variants}
    
    url = get_shopify_url() 
    if not url or not SHOPIFY_ACCESS_TOKEN: return None
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": variables}, headers=headers, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if 'errors' in data:
                print(f"    [!] Shopify API Error: {json.dumps(data['errors'])}", flush=True)
                return None
            if data['data']['productCreate']['userErrors']:
                print(f"    [!] Shopify UserError: {data['data']['productCreate']['userErrors']}", flush=True)
                return None
            print(f"    [SUCCESS] Created Draft: {data['data']['productCreate']['product']['id']}", flush=True)
            return data['data']['productCreate']['product']['id']
        else:
            print(f"    [!] Shopify API Status {r.status_code}: {r.text}", flush=True)
            return None
    except Exception as e:
        print(f"    [!] Connect Error: {e}", flush=True)
        return None

def update_shopify_images(shopify_id, image_urls):
    if DRY_RUN or not image_urls: return

    gql_media = []
    for url in image_urls:
        gql_media.append({
            "originalSource": url,
            "mediaContentType": "IMAGE"
        })

    mutation = """
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        media { id, status }
        mediaUserErrors { field, message }
      }
    }
    """
    
    variables = {"productId": shopify_id, "media": gql_media}
    url = get_shopify_url() 
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, json={"query": mutation, "variables": variables}, headers=headers, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if data['data']['productCreateMedia']['mediaUserErrors']:
                print(f"    [!] Image Sync Error: {data['data']['productCreateMedia']['mediaUserErrors']}", flush=True)
            else:
                print(f"    [SUCCESS] Synced {len(image_urls)} images to existing product.", flush=True)
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
                        asmodee_items.append({
                            "sku": sku, "title": row[idx_title], "vendor": "Asmodee",
                            "images_source": resolved_images,
                            "pdf": row[idx_pdf], "active_status": None 
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

    for item in all_rows:
        if TEST_MODE and successful_drafts_count >= TEST_LIMIT:
            print(f"--- TEST LIMIT REACHED ({TEST_LIMIT} successful uploads). Stopping Loop. ---", flush=True)
            break

        if item.get('is_skeleton'):
            resolved = resolve_corvus_skeleton(item)
            if not resolved: continue
            item = resolved
        
        sku = item['sku']
        vendor = item['vendor']
        
        if (skips_count + successful_drafts_count) % 50 == 0:
            print(f"Processing... (Skips: {skips_count} | Success: {successful_drafts_count})", flush=True)

        is_avail = True
        if vendor == "Asmodee":
            is_avail = scrape_asmodee_status(sku)
        else:
            is_avail = item.get('active_status', True)

        existing_product = inventory_map.get(sku)
        source_images = item.get('images_source', [])

        if not existing_product:
            if not is_avail:
                skips_count += 1
                continue
            
            if vendor == "Asmodee" and not source_images:
                print(f"    [SKIP] Asmodee Item {sku} has no images.", flush=True)
                new_entry = {
                    "sku": sku, "title": item['title'], "vendor": vendor,
                    "cloudinary_images": [], "shopify_id": None,
                    "shopify_status": "SKIPPED_NO_IMAGE", 
                    "release_date": item.get('release_date'), "upc": item.get('upc')
                }
                inventory_map[sku] = new_entry
                updates_made = True
                skips_count += 1
                continue

            print(f"   > Uploading {sku}...", flush=True)
            cloud_urls = process_and_upload_images(sku, source_images)
            shopify_id = create_shopify_draft(item, cloud_urls, item.get('release_date'), item.get('upc'))
            
            if shopify_id:
                new_entry = {
                    "sku": sku, "title": item['title'], "vendor": vendor,
                    "cloudinary_images": cloud_urls, "shopify_id": shopify_id,
                    "release_date": item.get('release_date'), "upc": item.get('upc')
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
                cloud_urls = process_and_upload_images(sku, source_images)
                
                if cloud_urls:
                    existing_product['cloudinary_images'] = cloud_urls
                    updates_made = True
                    
                    if shopify_id:
                        print(f"   > Syncing new images to Shopify Product {sku}...", flush=True)
                        update_shopify_images(shopify_id, cloud_urls)
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

if __name__ == "__main__":
    main()
