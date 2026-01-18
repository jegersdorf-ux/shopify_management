import os
import json
import requests
import time
import sys
import re
from datetime import datetime, timedelta

# --- FORCE UNBUFFERED OUTPUT ---
sys.stdout.reconfigure(line_buffering=True)

# ==========================================
#              CONFIGURATION
# ==========================================

print("--- MAINTENANCE SCRIPT INITIALIZING (PRE-ORDER CLEANUP) ---", flush=True)

# --- 1. SETUP CREDENTIALS (SECURE) ---
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
SHOP_URL = os.environ.get("SHOPIFY_STORE_URL")

# --- LOCAL FALLBACK INSTRUCTIONS ---
if not ACCESS_TOKEN:
    print("\n    [!] CRITICAL ERROR: SHOPIFY_ACCESS_TOKEN is missing.", flush=True)
    print("    ---------------------------------------------------")
    print("    To run this locally, use environment variables:")
    print('    $env:SHOPIFY_ACCESS_TOKEN="shpat_YOUR_NEW_TOKEN_HERE"')
    print('    $env:SHOPIFY_STORE_URL="extraturngames.myshopify.com"')
    print("    python local_maintenance_v4_syntax_fix.py")
    print("    ---------------------------------------------------\n")
    sys.exit(1)

if not SHOP_URL:
    SHOP_URL = "extraturngames.myshopify.com"

# Safety Cleanup
SHOP_URL = SHOP_URL.replace("https://", "").replace("http://", "").strip("/")
API_VERSION = "2025-01" 

# --- SETTINGS ---
DRY_RUN = False          
BULK_FILENAME = "bulk_maintenance.jsonl"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# ==========================================
#              HELPERS
# ==========================================

def get_graphql_url():
    return f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"

session = requests.Session()
session.headers.update(HEADERS)

def run_query(query, variables=None):
    for attempt in range(3):
        try:
            r = session.post(get_graphql_url(), json={"query": query, "variables": variables}, timeout=30)
            if r.status_code == 200: return r.json()
            elif r.status_code == 429: time.sleep(2); continue
        except Exception as e: 
            print(f"    [!] Connection Error: {e}")
            time.sleep(1)
    return None

# ==========================================
#        PHASE 1: SEARCH & FETCH
# ==========================================

def fetch_targeted_products():
    print("--- PHASE 1: SEARCHING CANDIDATES ---", flush=True)
    
    # FIX: Triple escaped quotes for GraphQL string interpolation
    # This ensures the final query is: tag:Pre-Order OR tag:\"New Release\"
    search_query = "tag:Pre-Order OR tag:\\\"New Release\\\""
    
    q_staged = """
    mutation {
      bulkOperationRunQuery(
        query: \"\"\"
        {
          products(query: "%s") {
            edges {
              node {
                id
                title
                descriptionHtml
                tags
                metafield(namespace: "custom", key: "release_date") { value }
              }
            }
          }
        }
        \"\"\"
      ) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }
    """ % search_query
    
    res = run_query(q_staged)
    
    # Check for errors immediately
    if not res:
        print("    [!] Fetch Failed: No response from Shopify.")
        return []
    
    user_errors = res.get('data', {}).get('bulkOperationRunQuery', {}).get('userErrors')
    if user_errors:
        print(f"    [!] Fetch Failed: {user_errors}")
        return []

    op_data = res.get('data', {}).get('bulkOperationRunQuery', {}).get('bulkOperation')
    if not op_data:
        print(f"    [!] Fetch Failed: No bulk operation returned. Full response: {res}")
        return []

    op_id = op_data['id']
    print(f"    [i] Search Job Started: {op_id}", flush=True)

    result_url = None
    while True:
        r = run_query("query($id: ID!) { node(id: $id) { ... on BulkOperation { status, url } } }", {"id": op_id})
        status = r['data']['node']['status']
        if status == "COMPLETED":
            result_url = r['data']['node']['url']
            break
        elif status in ["FAILED", "CANCELED"]:
            print("    [!] Job Failed.")
            return []
        time.sleep(2)

    print("    [i] Downloading Results...", flush=True)
    if not result_url:
        print("    [i] No products found matching criteria.")
        return []
        
    r_file = requests.get(result_url)
    products = []
    
    for line in r_file.text.split('\n'):
        if not line.strip(): continue
        try:
            products.append(json.loads(line))
        except: pass
            
    print(f"    [✓] Found {len(products)} products to analyze.", flush=True)
    return products

# ==========================================
#        PHASE 2: ANALYZE & CLEAN
# ==========================================

def clean_description(html_content):
    if not html_content: return ""
    pattern = r'<p[^>]*>.*?Estimated Release Date:.*?Please Note: This product is brand new.*?</p>'
    new_content = re.sub(pattern, '', html_content, flags=re.DOTALL | re.IGNORECASE)
    return new_content.strip()

def analyze_products(products):
    print("\n--- PHASE 2: ANALYZING DATES ---", flush=True)
    mutations = []
    
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    for p in products:
        p_id = p['id']
        title = p['title']
        tags = set(p['tags'])
        desc = p['descriptionHtml'] or ""
        
        release_date_str = p.get('metafield', {})
        if release_date_str: release_date_str = release_date_str.get('value')
        
        if not release_date_str: continue
            
        try:
            release_date = datetime.strptime(release_date_str, "%Y-%m-%d")
        except:
            print(f"    [!] Invalid Date Format for {title}: {release_date_str}")
            continue

        is_future = release_date > now
        is_new = (release_date >= thirty_days_ago) and (release_date <= now)
        is_old = release_date < thirty_days_ago
        
        changes_needed = False
        new_title = title
        new_desc = desc
        new_tags = tags.copy()
        
        if is_future:
            if not title.upper().startswith("PRE-ORDER:"):
                new_title = f"PRE-ORDER: {title}"
                changes_needed = True
            if "Pre-Order" not in tags:
                new_tags.add("Pre-Order")
                changes_needed = True
                
        elif is_new:
            if title.upper().startswith("PRE-ORDER:"):
                new_title = re.sub(r'^PRE-ORDER:\s*', '', title, flags=re.IGNORECASE)
                changes_needed = True
            if "Pre-Order" in new_tags:
                new_tags.remove("Pre-Order")
                changes_needed = True
            if "New Release" not in new_tags:
                new_tags.add("New Release")
                changes_needed = True
            
            cleaned_desc = clean_description(desc)
            if cleaned_desc != desc:
                new_desc = cleaned_desc
                changes_needed = True
                
        elif is_old:
            if title.upper().startswith("PRE-ORDER:"):
                new_title = re.sub(r'^PRE-ORDER:\s*', '', title, flags=re.IGNORECASE)
                changes_needed = True
            if "Pre-Order" in new_tags:
                new_tags.remove("Pre-Order")
                changes_needed = True
            if "New Release" in new_tags:
                new_tags.remove("New Release")
                changes_needed = True
            
            cleaned_desc = clean_description(desc)
            if cleaned_desc != desc:
                new_desc = cleaned_desc
                changes_needed = True

        if changes_needed:
            payload = {"id": p_id}
            if new_title != title: payload["title"] = new_title
            if new_desc != desc: payload["descriptionHtml"] = new_desc
            if new_tags != tags: payload["tags"] = list(new_tags)
            mutations.append(payload)
            print(f"    [UPDATE] {title} -> {release_date_str} (Future:{is_future}, New:{is_new})")

    print(f"    [i] Generated {len(mutations)} updates.", flush=True)
    return mutations

# ==========================================
#        PHASE 3: EXECUTE
# ==========================================

def write_and_run(mutations):
    if not mutations: return
    
    print(f"    [i] Writing Bulk File...", flush=True)
    with open(BULK_FILENAME, 'w') as f:
        for m in mutations:
            line = {"input": m}
            f.write(json.dumps(line) + "\n")

    if DRY_RUN:
        print("    [DRY RUN] File written but not uploaded.")
        return

    print("    [i] Uploading...", flush=True)
    q_stage = """mutation { stagedUploadsCreate(input: { resource: BULK_MUTATION_VARIABLES, filename: "%s", mimeType: "text/jsonl", httpMethod: POST }) { stagedTargets { url, parameters { name, value } } } }""" % BULK_FILENAME
    res = run_query(q_stage)
    target = res['data']['stagedUploadsCreate']['stagedTargets'][0]
    key = next((p['value'] for p in target['parameters'] if p['name'] == 'key'), None)
    
    with open(BULK_FILENAME, 'rb') as f:
        form = {p['name']: p['value'] for p in target['parameters']}
        requests.post(target['url'], data=form, files={"file": (BULK_FILENAME, f, "text/jsonl")})

    print("    [i] Triggering Update...", flush=True)
    mut = """mutation { bulkOperationRunMutation(mutation: "mutation call($input: ProductInput!) { productUpdate(input: $input) { product { id } userErrors { field message } } }", stagedUploadPath: "%s") { bulkOperation { id } userErrors { field message } } }""" % key
        
    res = run_query(mut)
    if res.get('data', {}).get('bulkOperationRunMutation', {}).get('userErrors'):
        print(f"    [!] Error: {res['data']['bulkOperationRunMutation']['userErrors']}")
        return

    op_id = res['data']['bulkOperationRunMutation']['bulkOperation']['id']
    print(f"    [✓] Job {op_id} Started.")
    
    while True:
        r = run_query("query($id: ID!) { node(id: $id) { ... on BulkOperation { status, objectCount } } }", {"id": op_id})
        status = r['data']['node']['status']
        if status in ["COMPLETED", "FAILED"]:
            print(f"    [✓] Finished: {status}")
            break
        time.sleep(3)

# ==========================================
#              MAIN
# ==========================================

def main():
    products = fetch_targeted_products()
    updates = analyze_products(products)
    write_and_run(updates)
    print("\n--- DONE ---", flush=True)

if __name__ == "__main__":
    main()
