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
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from github import Github, Auth
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION & SECRETS ---
DRY_RUN = True  # Set to False to go live

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

# Initialize Cloudinary
if not DRY_RUN:
    cloudinary.config(
        cloud_name=CLOUDINARY_CLOUD_NAME, 
        api_key=CLOUDINARY_API_KEY, 
        api_secret=CLOUDINARY_API_SECRET
    )

# --- GLOBAL LOGGING FOR REPORT ---
dry_run_logs = []

def log_action(action_type, sku, details):
    entry = f"| {action_type} | **{sku}** | {details} |"
    print(f"[DRY RUN] {entry}")
    dry_run_logs.append(entry)

# --- HELPER FUNCTIONS ---

def send_email(subject, body_html):
    if DRY_RUN:
        log_action("EMAIL", "N/A", f"Would send email: '{subject}'")
        return

    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        return

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
    except Exception as e:
        print(f"   !!! Email Failed: {e}")

def get_google_sheet_client():
    if not GOOGLE_CREDENTIALS_BASE64:
        raise ValueError("CRITICAL ERROR: Secret 'GOOGLE_CREDENTIALS_BASE64' is missing.")

    try:
        json_creds = base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8')
        creds_dict = json.loads(json_creds)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"   !!! Google Auth Failed: {e}")
        raise e

def extract_pdf_text(pdf_url):
    if not pdf_url or "http" not in pdf_url: return ""
    
    if DRY_RUN:
        return "DRY RUN: PDF content would be extracted here."

    try:
        if "drive.google.com" in pdf_url and "/view" in pdf_url:
             pdf_url = pdf_url.replace("/file/d/", "/uc?id=").replace("/view", "").split("?")[0] + "?export=download"

        response = requests.get(pdf_url, allow_redirects=True)
        if response.status_code != 200: return ""
        
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            return "\n".join([page.extract_text() or "" for page in pdf.pages])
    except: return ""

def upload_image_to_cloudinary(image_url, sku):
    if not image_url or "http" not in image_url: return None
    
    if DRY_RUN:
        log_action("CLOUDINARY", sku, f"Would upload image from {image_url[:30]}...")
        return "https://res.cloudinary.com/demo/image/upload/sample.jpg"
    
    clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
    public_id = f"{CLOUDINARY_ROOT_FOLDER}/{clean_sku}"
    
    try:
        response = cloudinary.uploader.upload(
            image_url, 
            public_id=public_id, 
            unique_filename=False, 
            overwrite=True, 
            fetch_format="auto", 
            quality="auto"
        )
        return response['secure_url']
    except Exception as e:
        print(f"   !!! Cloudinary Error: {e}")
        return None

def shopify_graphql(query, variables=None):
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        return None
        
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def create_shopify_draft(product_data, image_url):
    if DRY_RUN:
        log_action("SHOPIFY", product_data['sku'], f"Would create DRAFT: {product_data['title']}")
        return "gid://shopify/Product/123456789"

    mutation = """
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        product { id, title, handle }
        userErrors { field, message }
      }
    }
    """
    tags = ["Review Needed", "New Auto-Import"]
    variables = {
        "input": {
            "title": product_data['title'],
            "status": "DRAFT",
            "vendor": "Asmodee",
            "tags": tags,
            "images": [{"src": image_url}] if image_url else [],
            "variants": [{
                "sku": product_data['sku'],
                "inventoryManagement": "SHOPIFY",
                "price": "0.00" 
            }]
        }
    }
    result = shopify_graphql(mutation, variables)
    if result and 'data' in result and result['data']['productCreate']['product']:
        return result['data']['productCreate']['product']['id']
    return None

def check_shopify_status(sku):
    query = """
    query($query: String!) {
      products(first: 1, query: $query) {
        edges { node { id, status, tags, variants(first:1) { edges { node { sku } } } } }
      }
    }
    """
    result = shopify_graphql(query, {"query": f"sku:{sku}"})
    try:
        edges = result['data']['products']['edges']
        if edges:
            node = edges[0]['node']
            found_sku = node['variants']['edges'][0]['node']['sku']
            if found_sku == sku:
                return {"id": node['id'], "status": node['status'], "tags": node['tags']}
    except: pass
    return None

def add_tag(shopify_id, tag, sku):
    if DRY_RUN:
        log_action("SHOPIFY", sku, f"Would add tag: '{tag}'")
        return

    mutation = """
    mutation tagsAdd($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { node { id } }
    }
    """
    shopify_graphql(mutation, {"id": shopify_id, "tags": [tag]})

# --- COLUMN HELPER ---
def get_col_index(headers, name):
    """Finds the index of a column safely."""
    name = name.lower().strip()
    for i, h in enumerate(headers):
        if str(h).lower().strip() == name:
            return i
    return -1

# --- MAIN LOGIC ---

def main():
    print(f"--- STARTING SCRIPT (DRY RUN MODE: {DRY_RUN}) ---")
    
    # 1. GitHub Connection
    auth = Auth.Token(GITHUB_TOKEN)
    g = Github(auth=auth)
    repo = g.get_repo(REPO_NAME)
    
    try:
        contents = repo.get_contents(JSON_FILE_PATH)
        inventory_data = json.loads(contents.decoded_content.decode())
    except:
        inventory_data = []

    inventory_map = {item.get('sku'): item for item in inventory_data}

    # 2. Connect to Google Sheet
    print("Connecting to Google Sheets...")
    client = get_google_sheet_client()
    sheet = client.open_by_url(SHEET_URL).sheet1
    
    all_values = sheet.get_all_values()
    
    if not all_values:
        print("Sheet is empty!")
        return

    # --- SMART HEADER DETECTION ---
    # Scan first 5 rows to find the one with "SKU"
    header_row_index = -1
    headers = []
    
    for i in range(min(5, len(all_values))):
        row = all_values[i]
        # Check if 'SKU' is in this row (case-insensitive)
        if any(str(cell).lower().strip() == 'sku' for cell in row):
            header_row_index = i
            headers = row
            print(f"Found Headers on Row {i+1}: {headers}")
            break
            
    if header_row_index == -1:
        print("CRITICAL: Could not find 'SKU' column in first 5 rows.")
        return

    # Data starts AFTER the header row
    data_rows = all_values[header_row_index + 1:]
    
    print(f"Scanning {len(data_rows)} data rows...")
    
    # Map Columns dynamically
    idx_sku = get_col_index(headers, "SKU")
    idx_title = get_col_index(headers, "Title")
    idx_image = get_col_index(headers, "POS Images")
    idx_pdf = get_col_index(headers, "Sell Sheet")
    idx_status = get_col_index(headers, "Status")

    updates_made = False
    new_items_added = []
    active_conflicts = []
    prompts_to_generate = []

    for row in data_rows:
        # Safely get value by index
        sku = str(row[idx_sku]).strip() if idx_sku < len(row) else ""
        title = str(row[idx_title]) if idx_title != -1 and idx_title < len(row) else "Unknown Title"
        image_source = str(row[idx_image]) if idx_image != -1 and idx_image < len(row) else ""
        pdf_source = str(row[idx_pdf]) if idx_pdf != -1 and idx_pdf < len(row) else ""
        status_text = str(row[idx_status]).lower() if idx_status != -1 and idx_status < len(row) else "available"

        if not sku: continue

        is_available = "out" not in status_text and "unavailable" not in status_text

        # --- SCENARIO A: NEW ITEM ---
        if sku not in inventory_map:
            if not is_available:
                continue

            c_url = upload_image_to_cloudinary(image_source, sku)
            shopify_id = create_shopify_draft({"sku": sku, "title": title}, c_url)
            
            new_entry = {
                "sku": sku,
                "title": title,
                "cloudinary_url": c_url,
                "shopify_id": shopify_id,
                "shopify_status": "DRAFT",
                "specs_extracted": False
            }
            new_items_added.append(new_entry)
            updates_made = True

        # --- SCENARIO B: EXISTING ITEM ---
        else:
            product = inventory_map[sku]
            if not is_available:
                shopify_data = check_shopify_status(sku)
                if shopify_data and shopify_data['status'] == 'ACTIVE':
                    if "Review Needed" not in shopify_data['tags']:
                        add_tag(shopify_data['id'], "Review Needed", sku)
                    active_conflicts.append(product)

        # Process PDF
        needs_specs = (sku in inventory_map and not inventory_map[sku].get('specs_extracted')) or (sku not in inventory_map and is_available)
        
        if needs_specs and pdf_source and "http" in pdf_source:
             raw_text = extract_pdf_text(pdf_source)
             if raw_text:
                if DRY_RUN:
                    log_action("PDF", sku, "Would extract text for Gemini Prompt")
                else:
                    prompt = (
                        f"--- PROMPT FOR SKU: {sku} ({title}) ---\n"
                        f"CONTEXT: Extract specs.\n"
                        f"RAW TEXT:\n{raw_text[:2500]}...\n" 
                        f"--------------------------------------------------\n"
                    )
                    prompts_to_generate.append(prompt)
                    if sku in inventory_map:
                        inventory_map[sku]['specs_extracted'] = "pending"
                    updates_made = True

    # --- EMAIL & REPORTING ---
    if new_items_added:
        send_email(f"Import: {len(new_items_added)} New Items", "Body Omitted in Dry Run")

    if active_conflicts:
        send_email(f"ALERT: {len(active_conflicts)} Inventory Conflicts", "Body Omitted in Dry Run")

    if DRY_RUN:
        print("Generating Dry Run Report...")
        report_content = "# Dry Run Report\n\n| Action | SKU | Details |\n|---|---|---|\n"
        for log in dry_run_logs:
            report_content += f"{log}\n"
        
        if not dry_run_logs:
            report_content += "\n**No actions would be taken.**\n"

        try:
            contents = repo.get_contents(REPORT_FILE_PATH)
            repo.update_file(REPORT_FILE_PATH, "Dry Run Report", report_content, contents.sha)
        except:
            repo.create_file(REPORT_FILE_PATH, "Dry Run Report", report_content)
            
        print("Dry Run Complete. Check dry_run_report.md in your repo.")
        
    elif updates_made:
        print("Saving updates to GitHub...")
        updated_list = list(inventory_map.values())
        json_content = json.dumps(updated_list, indent=2)
        try:
            contents = repo.get_contents(JSON_FILE_PATH)
            repo.update_file(JSON_FILE_PATH, "Inventory Sync", json_content, contents.sha)
        except:
            repo.create_file(JSON_FILE_PATH, "Inventory Sync", json_content)

        if prompts_to_generate:
            full_prompt_text = "\n".join(prompts_to_generate)
            try:
                p_contents = repo.get_contents(PROMPTS_FILE_PATH)
                repo.update_file(PROMPTS_FILE_PATH, "New Prompts", full_prompt_text, p_contents.sha)
            except:
                repo.create_file(PROMPTS_FILE_PATH, "New Prompts", full_prompt_text)

if __name__ == "__main__":
    main()
