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
from github import Github
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION & SECRETS ---
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
PROMPTS_FILE_PATH = "gemini_prompts.txt"
CLOUDINARY_ROOT_FOLDER = "Extra-Turn-Games"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit"

# Initialize Cloudinary
cloudinary.config(
    cloud_name=CLOUDINARY_CLOUD_NAME, 
    api_key=CLOUDINARY_API_KEY, 
    api_secret=CLOUDINARY_API_SECRET
)

# --- HELPER FUNCTIONS ---

def send_email(subject, body_html):
    """Sends HTML email via Gmail."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        print("   !!! Skipping Email: Credentials missing.")
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
        print(f"   --> Email Sent: {subject}")
    except Exception as e:
        print(f"   !!! Email Failed: {e}")

def get_google_sheet_client():
    """Auth for Google Sheets."""
    json_creds = base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8')
    creds_dict = json.loads(json_creds)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def extract_pdf_text(pdf_url):
    """Downloads and extracts text from Sell Sheet PDF."""
    if not pdf_url or "http" not in pdf_url: return ""
    try:
        # Convert Drive View links to Download links
        if "drive.google.com" in pdf_url and "/view" in pdf_url:
             pdf_url = pdf_url.replace("/file/d/", "/uc?id=").replace("/view", "").split("?")[0] + "?export=download"

        response = requests.get(pdf_url, allow_redirects=True)
        if response.status_code != 200: return ""
        
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            return "\n".join([page.extract_text() or "" for page in pdf.pages])
    except: return ""

def upload_image_to_cloudinary(image_url, sku):
    """Uploads to Cloudinary Folder: Extra-Turn-Games/{SKU}"""
    if not image_url or "http" not in image_url: return None
    
    clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
    public_id = f"{CLOUDINARY_ROOT_FOLDER}/{clean_sku}"
    
    print(f"   --> Uploading Image: {public_id}")
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
    """Helper for Shopify Admin API."""
    if not SHOPIFY_STORE_URL or not SHOPIFY_ACCESS_TOKEN:
        print("   !!! Shopify credentials missing.")
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
        print(f"   !!! Shopify API Error: {response.text}")
        return None

def create_shopify_draft(product_data, image_url):
    """Creates a DRAFT product on Shopify."""
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
    """Checks if SKU is ACTIVE on Shopify."""
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

def add_tag(shopify_id, tag):
    """Adds a tag to a Shopify product."""
    mutation = """
    mutation tagsAdd($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { node { id } }
    }
    """
    shopify_graphql(mutation, {"id": shopify_id, "tags": [tag]})

# --- MAIN LOGIC ---

def main():
    # 1. GitHub Connection & Load State
    g = Github(GITHUB_TOKEN)
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
    rows = sheet.get_all_records()
    
    print(f"Scanning {len(rows)} rows from source...")

    updates_made = False
    new_items_added = []
    active_conflicts = []
    prompts_to_generate = []

    for row in rows:
        sku = str(row.get('SKU', '')).strip()
        title = row.get('Title', '')
        image_source = row.get('POS Images', '')
        pdf_source = row.get('Sell Sheet', '')
        status_text = str(row.get('Status', 'Available')).lower()

        # Is it available in the source? (Adjust logic if 'Status' column varies)
        is_available = "out" not in status_text and "unavailable" not in status_text

        if not sku: continue

        # --- SCENARIO A: NEW ITEM ---
        if sku not in inventory_map:
            # Rule: If new AND unavailable -> Skip completely
            if not is_available:
                continue

            print(f"New Product Found: {sku} - {title}")
            
            # 1. Upload Image
            c_url = upload_image_to_cloudinary(image_source, sku)
            
            # 2. Create Shopify Draft
            shopify_id = create_shopify_draft({"sku": sku, "title": title}, c_url)
            
            # 3. Add to Inventory JSON
            new_entry = {
                "sku": sku,
                "title": title,
                "cloudinary_url": c_url,
                "shopify_id": shopify_id,
                "shopify_status": "DRAFT",
                "specs_extracted": False
            }
            inventory_map[sku] = new_entry
            new_items_added.append(new_entry)
            updates_made = True

        # --- SCENARIO B: EXISTING ITEM ---
        else:
            product = inventory_map[sku]
            
            # Rule: If source says unavailable, check if we need to alert
            if not is_available:
                # Check Shopify status
                shopify_data = check_shopify_status(sku)
                
                if shopify_data and shopify_data['status'] == 'ACTIVE':
                    # ALERT: Active on site but Dead in source
                    if "Review Needed" not in shopify_data['tags']:
                        add_tag(shopify_data['id'], "Review Needed")
                        print(f"   --> Tagged {sku} as Review Needed (Active but Unavailable)")
                    
                    active_conflicts.append(product)

        # --- PROCESS PDF (For both New and Existing) ---
        # If we have a PDF and haven't extracted it yet, do so for Gemini Prompts
        if sku in inventory_map and not inventory_map[sku].get('specs_extracted'):
             if pdf_source and "http" in pdf_source:
                raw_text = extract_pdf_text(pdf_source)
                if raw_text:
                    prompt = (
                        f"--- PROMPT FOR SKU: {sku} ({title}) ---\n"
                        f"CONTEXT: Extract specs from sell sheet.\n"
                        f"OUTPUT: JSON with keys: description, player_count, play_time, age_rating.\n"
                        f"RAW TEXT:\n{raw_text[:2500]}...\n" 
                        f"--------------------------------------------------\n"
                    )
                    prompts_to_generate.append(prompt)
                    inventory_map[sku]['specs_extracted'] = "pending"
                    updates_made = True

    # --- EMAIL NOTIFICATIONS ---

    if new_items_added:
        html = "<h2>New Items Added to Shopify (Drafts)</h2><ul>"
        for item in new_items_added:
            html += f"<li><b>{item['sku']}</b>: {item['title']} <br><a href='{item['cloudinary_url']}'>View Image</a></li>"
        html += "</ul><p>Tagged: <b>Review Needed, New Auto-Import</b></p>"
        send_email(f"Import: {len(new_items_added)} New Items", html)

    if active_conflicts:
        html = "<h2>⚠️ ACTION REQUIRED: Active Items Unavailable</h2>"
        html += "<p>The following items are <b>ACTIVE</b> on Shopify but marked <b>UNAVAILABLE</b> in the source sheet.</p><ul>"
        for item in active_conflicts:
            html += f"<li><b>{item['sku']}</b>: {item['title']}</li>"
        html += "</ul><p>These have been tagged <b>Review Needed</b>. Please check inventory.</p>"
        send_email(f"ALERT: {len(active_conflicts)} Inventory Conflicts", html)

    # --- SAVE UPDATES ---
    if updates_made:
        print("Saving updates to GitHub...")
        
        # Save JSON
        updated_list = list(inventory_map.values())
        json_content = json.dumps(updated_list, indent=2)
        try:
            contents = repo.get_contents(JSON_FILE_PATH)
            repo.update_file(JSON_FILE_PATH, "Inventory Sync", json_content, contents.sha)
        except:
            repo.create_file(JSON_FILE_PATH, "Inventory Sync", json_content)

        # Save Prompts
        if prompts_to_generate:
            full_prompt_text = "\n".join(prompts_to_generate)
            try:
                p_contents = repo.get_contents(PROMPTS_FILE_PATH)
                repo.update_file(PROMPTS_FILE_PATH, "New Prompts", full_prompt_text, p_contents.sha)
            except:
                repo.create_file(PROMPTS_FILE_PATH, "New Prompts", full_prompt_text)

if __name__ == "__main__":
    main()
