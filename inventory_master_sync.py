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
RESET_IGNORED_ITEMS = False

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME')
CLOUDINARY_CLOUD_NAME = os.getenv('CLOUDINARY_CLOUD_NAME')
CLOUDINARY_API_KEY = os.getenv('CLOUDINARY_API_KEY')
CLOUDINARY_API_SECRET = os.getenv('CLOUDINARY_API_SECRET')
GOOGLE_CREDENTIALS_BASE64 = os.getenv('GOOGLE_CREDENTIALS_BASE64')

SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = "2025-10" 

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
            # Heartbeat log every 5 pages
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
            print(f"    > Found {len(p_urls)} potential Corvus items.", flush=True)
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
        r = requests.get(url,
