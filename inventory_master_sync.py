import os
import json
import time
import base64
import smtplib
import requests
import gspread
import cloudinary
import cloudinary.uploader
import pdfplumber
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from github import Github
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION ---
# Secrets
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

# Settings
JSON_FILE_PATH = "product_inventory.json"
CLOUDINARY_ROOT_FOLDER = "Extra-Turn-Games"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1OFpCuFatmI0YAfVGRcqkfLJbfa-2NL9gReQFqkORhtw/edit"

# Column Headers in Source Sheet (Adjust if headers change)
COL_SKU = 'SKU'
COL_TITLE = 'Title'
COL_IMAGE = 'POS Images'
COL_PDF = 'Sell Sheet'
COL_STATUS = 'Status' # Assumption: There is a status column. If not, we assume all are available.

# --- HELPERS ---

def send_email(subject, body_html):
    """Sends an email notification."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        print("   !!! Skipping Email: Credentials not found.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))

    try:
        # Defaults to Gmail SMTP (smtp.gmail.com). Change if using Outlook/Other.
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"   --> Email Sent: {subject}")
    except Exception as e:
        print(f"   !!! Email Failed: {e}")

def shopify_graphql(query, variables=None):
    """Executes a GraphQL query against Shopify."""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/2023-10/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"   !!! Shopify Error: {response.text}")
        return None

def create_shopify_product(product_data, image_url):
    """Creates a new product in Shopify as DRAFT."""
    mutation = """
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        product {
          id
          title
          handle
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    tags = ["Review Needed", "New Auto-Import"]
    
    variables = {
        "input": {
            "title": product_data['title'],
            "status": "DRAFT",
            "vendor": "Asmodee", # Or dynamic from sheet
            "tags": tags,
            "images": [{"src": image_url}] if image_url else [],
            "variants": [
                {
                    "sku": product_data['sku'],
                    "inventoryManagement": "SHOPIFY",
                    "price": "0.00" # Set default or extract from sheet if available
                }
            ]
        }
    }
    
    result = shopify_graphql(mutation, variables)
    if result and 'data' in result and result['data']['productCreate']['product']:
        print(f"   --> Created Shopify Draft: {product_data['title']}")
        return result['data']['productCreate']['product']['id']
    else:
        print(f"   !!! Failed to create Shopify product: {result}")
        return None

def add_tag_to_shopify(shopify_id, tag):
    """Adds a tag to an existing Shopify product."""
    mutation = """
    mutation tagsAdd($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) {
        node {
          id
        }
        userErrors {
          message
        }
      }
    }
    """
    shopify_graphql(mutation, {"id": shopify_id, "tags": [tag]})

def check_shopify_status(sku):
    """Checks if a SKU exists on Shopify and returns its ID and Status."""
    # This uses the REST API for easier SKU lookup, or we search via GraphQL
    query = """
    query($query: String!) {
      products(first: 1, query: $query) {
        edges {
          node {
            id
            status
            tags
            variants(first: 1) {
              edges {
                node {
                  sku
                }
              }
            }
          }
        }
      }
    }
    """
    # Searching specifically by SKU in the query string
    result = shopify_graphql(query, {"query": f"sku:{sku}"})
    
    try:
        edges = result['data']['products']['edges']
        if edges:
            node = edges[0]['node']
            # Double check exact SKU match
            found_sku = node['variants']['edges'][0]['node']['sku']
            if found_sku == sku:
                return {
                    "id": node['id'],
                    "status": node['status'], # ACTIVE, DRAFT, ARCHIVED
                    "tags": node['tags']
                }
    except:
        pass
    return None

# ... (Previous Helper Functions: get_google_sheet_client, extract_pdf_text, upload_image_to_cloudinary remain same) ...
# I will include them briefly to ensure the script is copy-pasteable complete.

cloudinary.config(cloud_name=CLOUDINARY_CLOUD_NAME, api_key=CLOUDINARY_API_KEY, api_secret=CLOUDINARY_API_SECRET)

def get_google_sheet_client():
    json_creds = base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode('utf-8')
    creds_dict = json.loads(json_creds)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def extract_pdf_text(pdf_url):
    if not pdf_url or "http" not in pdf_url: return ""
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
    clean_sku = "".join(x for x in sku if x.isalnum() or x in "-_")
    public_id = f"{CLOUDINARY_ROOT_FOLDER}/{clean_sku}"
    try:
        response = cloudinary.uploader.upload(image_url, public_id=public_id, unique_filename=False, overwrite=True, fetch_format="auto", quality="auto")
        return response['secure_url']
    except Exception as e:
        print(f"Cloudinary Error: {e}")
        return None

# --- MAIN LOGIC ---

def main():
    # 1. Setup & Load Data
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)
    
    try:
        contents = repo.get_contents(JSON_FILE_PATH)
        inventory_data = json.loads(contents.decoded_content.decode())
    except:
        inventory_data = []

    inventory_map = {item.get('sku'): item for item in inventory_data}

    print("Connecting to Google Sheets...")
    client = get_google_sheet_client()
    sheet = client.open_by_url(SHEET_URL).sheet1
    rows = sheet.get_all_records()

    updates_made = False
    
    # Tracking lists for emails
    new_items_added = []
    active_items_unavailable = []

    print(f"Processing {len(rows)} rows...")

    for row in rows:
        sku = str(row.get(COL_SKU, '')).strip()
        title = row.get(COL_TITLE, '')
        image_source = row.get(COL_IMAGE, '')
        status_raw = str(row.get(COL_STATUS, 'Available')).lower() # Default to available if column missing
        
        # Determine Source Availability
        # logic: if status contains "out" or "unavailable", it is NOT available.
        is_source_available = "out" not in status_raw and "unavailable" not in status_raw

        if not sku: continue

        # --- SCENARIO 1: NEW ITEM ---
        if sku not in inventory_map:
            # Skip if source says unavailable
            if not is_source_available:
                continue 
            
            print(f"New Product Discovered: {sku}")
            
            # 1. Upload Image
            c_url = upload_image_to_cloudinary(image_source, sku)
            
            # 2. Create Shopify Draft
            shopify_id = create_shopify_product({
                "sku": sku,
                "title": title
            }, c_url)

            # 3. Add to Inventory JSON
            new_entry = {
                "sku": sku,
                "title": title,
                "cloudinary_url": c_url,
                "shopify_id": shopify_id,
                "shopify_status": "DRAFT",
                "source_available": True
            }
            inventory_map[sku] = new_entry
            new_items_added.append(new_entry)
            updates_made = True

        # --- SCENARIO 2: EXISTING ITEM ---
        else:
            product = inventory_map[sku]
            
            # Update Local Status
            product['source_available'] = is_source_available
            
            # Check Shopify Status
            # We check Shopify periodically or if we suspect a conflict
            # To save API calls, we might only check if source is unavailable
            
            if not is_source_available:
                # Source says unavailable. Check if Shopify is ACTIVE.
                shopify_data = check_shopify_status(sku)
                
                if shopify_data and shopify_data['status'] == 'ACTIVE':
                    # ALERT! Item is active on site but dead in source.
                    print(f"   !!! Alert: {sku} is Active but Source Unavailable.")
                    
                    if "Review Needed" not in shopify_data['tags']:
                        add_tag_to_shopify(shopify_data['id'], "Review Needed")
                        print("   --> Added 'Review Needed' tag.")

                    active_items_unavailable.append(product)

    # --- EMAIL NOTIFICATIONS ---
    
    # Email 1: New Items
    if new_items_added:
        html_body = "<h2>New Items Added to Shopify (Drafts)</h2><ul>"
        for item in new_items_added:
            html_body += f"<li><b>{item['sku']}</b>: {item['title']} <br><img src='{item['cloudinary_url']}' width='50'></li>"
        html_body += "</ul><p>These items are set to <b>Draft</b> and tagged <b>Review Needed</b>.</p>"
        
        send_email(f"New Inventory Added: {len(new_items_added)} items", html_body)

    # Email 2: Action Required (Active but Unavailable)
    if active_items_unavailable:
        html_body = "<h2>ACTION REQUIRED: Unavailable Items Currently Active</h2>"
        html_body += "<p>The following items are marked as <b>ACTIVE</b> on Shopify but are <b>UNAVAILABLE</b> in the source list.</p><ul>"
        for item in active_items_unavailable:
            html_body += f"<li><b>{item['sku']}</b>: {item['title']}</li>"
        html_body += "</ul><p>They have been tagged <b>Review Needed</b> on Shopify.</p>"
        
        send_email(f"ACTION REQUIRED: {len(active_items_unavailable)} Inventory Conflicts", html_body)

    # --- SAVE UPDATES ---
    if updates_made:
        print("Saving JSON updates to GitHub...")
        updated_list = list(inventory_map.values())
        json_content = json.dumps(updated_list, indent=2)
        try:
            contents = repo.get_contents(JSON_FILE_PATH)
            repo.update_file(JSON_FILE_PATH, "Inventory Sync Update", json_content, contents.sha)
        except:
            repo.create_file(JSON_FILE_PATH, "Inventory Sync Update", json_content)

if __name__ == "__main__":
    main()
