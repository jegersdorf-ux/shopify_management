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
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from github import Github, Auth
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

# --- CONFIGURATION & SECRETS ---
DRY_RUN = True  # Set to False to GO LIVE

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME')
CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')

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

# Initialize Cloudinary
if not DRY_RUN:
    cloudinary.config(
        cloud_name=CLOUDINARY_CLOUD_NAME, 
        api_key=CLOUDINARY_API_KEY, 
        api_secret=CLOUDINARY_API_SECRET
    )

# --- GLOBAL LOGGING ---
dry_run_logs = []

def log_action(action_type, sku, details):
    entry = f"| {action_type} | **{sku}** | {details} |"
    print(f"[DRY RUN] {entry}")
    dry_run_logs.append(entry)

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
                    if item.get('webContentLink'): found_images.append(item['webContentLink'])
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
        print(f"    > Walking Drive Folder ID: {folder_id}...")
        service = get_drive_service()
        if service: return walk_drive_folder(service, folder_id)
    return [url]

def get_visible_sheet_values(sheet_url):
    """
    Fetches ONLY rows that are NOT hidden in the Google Sheet.
    """
    # 1. Parse Sheet ID from URL
    # URL format: .../d/SPREADSHEET_ID/edit...
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not match: 
        print("    [!] Could not parse Spreadsheet ID.")
        return []
    spreadsheet_id = match.group(1)
    
    # 2. Get Data + Metadata
    service = get_sheets_service()
    if not service: return []
    
    try:
        # Fetch data AND row metadata (to check 'hiddenByUser')
        # We assume the first sheet (gid=0 or index 0)
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            includeGridData=True
        ).execute()
        
        sheet = spreadsheet['sheets'][0] # First tab
        rows = sheet['data'][0].get('rowData', [])
        
        visible_rows = []
        for row in rows:
            # Check for hidden property
            user_hidden = row.get('rowMetadata', {}).get('hiddenByUser', False)
            filter_hidden = row.get('rowMetadata', {}).get('hiddenByFilter', False)
            
            if user_hidden or filter_hidden:
                continue # Skip this row
            
            # Extract cell values
            values = []
            if 'values' in row:
                for cell in row['values']:
                    # Prioritize userEnteredValue (raw) or formattedValue
                    val = cell.get('userEnteredValue', {})
                    text = val.get('stringValue', str(val.get('numberValue', '')))
                    values.append(text)
            visible_rows.append(values)
            
        return visible_rows

    except Exception as e:
        print(f"    [!] Sheet API Error: {e}")
        return []

# ==========================================
#           CATALOG DISCOVERY
# ==========================================

def discover_shopify_catalog(store_url, vendor_name):
    print(f"  > Discovering {vendor_name} Catalog...")
    base_url = f"{store_url}/products.json"
    page = 1
    found_items = []
    
    while True:
        try:
            r = requests.get(f"{base_url}?limit=250&page={page}")
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
    print("  > Discovering Corvus Belli Catalog...")
    sitemap_url = "https://store.corvusbelli.com/sitemap.xml"
    found_items = []
    try:
        r = requests.get(sitemap_url)
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
        r = requests.get(search_url)
        soup = BeautifulSoup(r.text, 'html.parser')
        link = soup.find('a', href=re.compile(r'/products/'))
        if not link: return False
        r2 = requests.get("https://store.asmodee.com" + link['href'])
        text = BeautifulSoup(r2.text, 'html.parser').get_text().lower()
        return "add to cart" in text and "sold out" not in text
    except: return False

def resolve_corvus_skeleton(item):
    url = item['url']
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, headers=headers)
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
    if not source_urls: return []
    uploaded_urls = []
    source_urls = list(set(source_urls)) # Dedup

    for i, url in enumerate(source_urls):
        if not url or "http" not in url: continue
        suffix = f"_{i}" if i > 0 else ""
        clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
        public_id = f"{CLOUDINARY_ROOT_FOLDER}/{clean_sku}{suffix}"
        
        if DRY_RUN:
            log_action("CLOUDINARY", sku, f"Upload {public_id}")
            uploaded_urls.append("https://res.cloudinary.com/demo/image.jpg")
            continue
        try:
            res = cloudinary.uploader.upload(url, public_id=public_id, overwrite=True)
            uploaded_urls.append(res['secure_url'])
        except: pass
    return uploaded_urls

def create_shopify_draft(product_data, image_urls, release_date=None, upc=None):
    if DRY_RUN:
        log_action("SHOPIFY", product_data['sku'], f"Draft: {product_data['title']} ({len(image_urls)} images)")
        return "gid://shopify/Product/1"
    
    tags = ["Review Needed", "New Auto-Import", product_data['vendor']]
    if release_date: tags.append(f"Release: {release_date}")
    gql_images = [{"src": url} for url in image_urls]

    mutation = """mutation productCreate($input: ProductInput!) { productCreate(input: $input) { product { id } } }"""
    variables = {
        "input": {
            "title": product_data['title'], "status": "DRAFT", "vendor": product_data['vendor'], "tags": tags,
            "descriptionHtml": f"<p>Release: {release_date}</p>" if release_date else "",
            "images": gql_images,
            "variants": [{"sku": product_data['sku'], "inventoryManagement": "SHOPIFY", "price": "0.00", "barcode": upc or ""}]
        }
    }
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN: return None
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    r = requests.post(url, json={"query": mutation, "variables": variables}, headers=headers)
    if r.status_code == 200 and 'data' in r.json(): return r.json()['data']['productCreate']['product']['id']
    return None

def check_shopify_status(sku):
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN: return None
    query = """query($query: String!) { products(first: 1, query: $query) { edges { node { id, status, tags, variants(first:1) { edges { node { sku } } } } } } }"""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    r = requests.post(url, json={"query": query, "variables": {"query": f"sku:{sku}"}}, headers=headers)
    try:
        node = r.json()['data']['products']['edges'][0]['node']
        if node['variants']['edges'][0]['node']['sku'] == sku:
            return {"id": node['id'], "status": node['status'], "tags": node['tags']}
    except: pass
    return None

def add_tag(shopify_id, tag, sku):
    if DRY_RUN: return
    mutation = """mutation tagsAdd($id: ID!, $tags: [String!]!) { tagsAdd(id: $id, tags: $tags) { node { id } } }"""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/graphql.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN, "Content-Type": "application/json"}
    requests.post(url, json={"query": mutation, "variables": {"id": shopify_id, "tags": [tag]}}, headers=headers)

def extract_pdf_text(pdf_url):
    if not pdf_url or "http" not in pdf_url: return ""
    if DRY_RUN: return "DRY RUN PDF"
    try:
        if "drive.google.com" in pdf_url and "/view" in pdf_url:
             pdf_url = pdf_url.replace("/file/d/", "/uc?id=").replace("/view", "").split("?")[0] + "?export=download"
        r = requests.get(pdf_url, allow_redirects=True)
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
    print(f"--- STARTING MASTER SYNC (DRY RUN MODE: {DRY_RUN}) ---")
    
    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth)
    repo = g.get_repo(REPO_NAME)
    
    # 1. Load Inventory
    try:
        contents = repo.get_contents(JSON_FILE_PATH)
        inventory_data = json.loads(contents.decoded_content.decode())
    except: inventory_data = []
    inventory_map = {item.get('sku'): item for item in inventory_data}
    print(f"Loaded {len(inventory_map)} existing items.")

    moonstone_items = discover_shopify_catalog("https://shop.moonstonethegame.com", "Moonstone")
    warsenal_items = discover_shopify_catalog("https://warsen.al", "Warsenal")
    corvus_skeletons = discover_corvus_catalog()
    
    # 3. LOAD ASMODEE (VISIBLE ROWS ONLY)
    asmodee_items = []
    try:
        print("Connecting to Google Sheet (Filtering Hidden Rows)...")
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
                        # Recursive Folder Scan for images
                        raw_link = str(row[idx_img]) if idx_img != -1 else ""
                        resolved_images = get_asmodee_image_links(raw_link)
                        
                        asmodee_items.append({
                            "sku": sku, "title": row[idx_title], "vendor": "Asmodee",
                            "images_source": resolved_images,
                            "pdf": row[idx_pdf], "active_status": None 
                        })
    except Exception as e: print(f"Sheet Error: {e}")

    # 4. MERGE
    all_rows = asmodee_items + moonstone_items + warsenal_items + corvus_skeletons
    print(f"Total Combined Queue: {len(all_rows)} items")
    
    updates_made = False
    new_items_added = []
    active_conflicts = []
    prompts_to_generate = []

    for item in all_rows:
        if item.get('is_skeleton'):
            if not DRY_RUN: time.sleep(0.5)
            resolved = resolve_corvus_skeleton(item)
            if not resolved: continue
            item = resolved
        
        sku = item['sku']
        vendor = item['vendor']
        
        is_avail = True
        if vendor == "Asmodee":
            if not DRY_RUN: time.sleep(0.5)
            is_avail = scrape_asmodee_status(sku)
        else:
            is_avail = item.get('active_status', True)

        existing_product = inventory_map.get(sku)
        source_images = item.get('images_source', [])

        if not existing_product:
            if not is_avail: continue
            cloud_urls = process_and_upload_images(sku, source_images)
            shopify_id = create_shopify_draft(item, cloud_urls, item.get('release_date'), item.get('upc'))
            new_entry = {
                "sku": sku, "title": item['title'], "vendor": vendor,
                "cloudinary_images": cloud_urls, "shopify_id": shopify_id,
                "release_date": item.get('release_date'), "upc": item.get('upc')
            }
            inventory_map[sku] = new_entry
            new_items_added.append(new_entry)
            updates_made = True
            
            if item.get('pdf') or vendor == "Asmodee":
                txt = extract_pdf_text(item.get('pdf')) if item.get('pdf') else ""
                prompt = f"PROMPT FOR {sku}: {item['title']} (Vendor: {vendor})\n{txt[:1000]}"
                prompts_to_generate.append(prompt)

        else:
            has_images = existing_product.get('cloudinary_images') or existing_product.get('cloudinary_url')
            if (not has_images) and source_images:
                cloud_urls = process_and_upload_images(sku, source_images)
                if cloud_urls:
                    existing_product['cloudinary_images'] = cloud_urls
                    updates_made = True
            
            if not is_avail:
                s_data = check_shopify_status(sku)
                if s_data and s_data['status'] == 'ACTIVE' and "Review Needed" not in s_data['tags']:
                    add_tag(s_data['id'], "Review Needed", sku)
                    active_conflicts.append(existing_product)

    if new_items_added: send_email(f"Import: {len(new_items_added)} New", "Check Shopify.")
    if active_conflicts: send_email(f"ALERT: {len(active_conflicts)} Conflicts", "Check Shopify.")

    if DRY_RUN:
        print("Saving Dry Run Report...")
        report = "\n".join(dry_run_logs)
        try: repo.update_file(REPORT_FILE_PATH, "Report", report, repo.get_contents(REPORT_FILE_PATH).sha)
        except: repo.create_file(REPORT_FILE_PATH, "Report", report)
    elif updates_made:
        print("Saving JSON...")
        try: repo.update_file(JSON_FILE_PATH, "Sync", json.dumps(list(inventory_map.values()), indent=2), repo.get_contents(JSON_FILE_PATH).sha)
        except: repo.create_file(JSON_FILE_PATH, "Sync", json.dumps(list(inventory_map.values()), indent=2))
        if prompts_to_generate:
             full_prompt = "\n".join(prompts_to_generate)
             try: repo.update_file(PROMPTS_FILE_PATH, "Prompts", full_prompt, repo.get_contents(PROMPTS_FILE_PATH).sha)
             except: repo.create_file(PROMPTS_FILE_PATH, "Prompts", full_prompt)

if __name__ == "__main__":
    main()
