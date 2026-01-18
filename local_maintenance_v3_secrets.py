#!/usr/bin/env python3
import os
import requests
import json
import re
import sys
import time
from datetime import datetime, timedelta

# --- FORCE UNBUFFERED OUTPUT ---
sys.stdout.reconfigure(line_buffering=True)

print("--- MAINTENANCE SCRIPT INITIALIZING (V5 SERIAL CHECK) ---", flush=True)

# ==========================================
#              CONFIGURATION
# ==========================================

# --- 1. SETUP CREDENTIALS (SECURE) ---
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")
SHOP_URL = os.environ.get("SHOPIFY_STORE_URL")

# --- LOCAL FALLBACK INSTRUCTIONS ---
if not ACCESS_TOKEN:
    print("\n    [!] CRITICAL ERROR: SHOPIFY_ACCESS_TOKEN is missing.", flush=True)
    print("    ---------------------------------------------------")
    print("    To run this locally, use environment variables:")
    print('    $env:SHOPIFY_ACCESS_TOKEN="shpat_YOUR_TOKEN_HERE"')
    print('    $env:SHOPIFY_STORE_URL="extraturngames.myshopify.com"')
    print("    python local_maintenance_v5_serial_check.py")
    print("    ---------------------------------------------------\n")
    sys.exit(1)

if not SHOP_URL:
    SHOP_URL = "extraturngames.myshopify.com"

# Clean URL
SHOP_URL = SHOP_URL.replace("https://", "").replace("http://", "").strip("/")
API_VERSION = "2024-10" # Matches the recommendation
GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# ==========================================
#              CORE FUNCTIONS
# ==========================================

def graphql_query(query, variables=None):
    """Execute a GraphQL query with basic error handling"""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    for attempt in range(3):
        try:
            response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload, timeout=30)
            
            if response.status_code == 429:
                print("    [!] Rate Limit. Sleeping 2s...")
                time.sleep(2)
                continue
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"    [!] Connection Error: {e}")
            time.sleep(1)
            
    return None

def get_products_with_preorder_tags(cursor=None):
    """Fetch products with Pre-Order or New Release tags"""
    after_clause = f', after: "{cursor}"' if cursor else ""
    
    # NOTE: Escaped quotes correctly here
    query = f"""
    query {{
      products(first: 25, query: "tag:Pre-Order OR tag:\\"New Release\\"{after_clause}") {{
        edges {{
          node {{
            id
            title
            descriptionHtml
            tags
            metafield(namespace: "custom", key: "release_date") {{
              value
            }}
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    
    result = graphql_query(query)
    if not result or 'data' not in result:
        print(f"    [!] Query Error: {result}")
        return []
        
    return result['data']['products']

def remove_preorder_disclaimer(description_html):
    """Remove the pre-order disclaimer from description (Shopify Logic)"""
    if not description_html:
        return description_html
    
    start_marker = "Estimated Release Date:"
    end_marker = "Please Note: This product is brand new"
    
    if start_marker in description_html and end_marker in description_html:
        # Find start marker
        marker_pos = description_html.find(start_marker)
        # Find the <p tag preceding it
        start_idx = description_html.rfind('<p', 0, marker_pos)
        
        # Find end marker
        end_marker_pos = description_html.find(end_marker)
        # Find the closing </p> tag after it
        end_idx = description_html.find('</p>', end_marker_pos)
        
        if start_idx != -1 and end_idx != -1:
            before = description_html[:start_idx]
            after = description_html[end_idx + 4:] # +4 to skip </p>
            description_html = before + after
    
    # Clean up empty tags left behind
    description_html = re.sub(r'<p>\s*</p>', '', description_html)
    description_html = re.sub(r'\n\s*\n', '\n', description_html)
    
    return description_html.strip()

def remove_preorder_prefix(title):
    """Remove PRE-ORDER: prefix from title"""
    # Case insensitive check
    if title.upper().startswith('PRE-ORDER: '):
        return title[11:] # Cut off first 11 chars
    return title

def update_product(product_id, title, description_html, tags):
    """Update a product via GraphQL mutation"""
    
    # We use json.dumps to handle escaping special characters automatically
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          title
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variables = {
        "input": {
            "id": product_id,
            "title": title,
            "descriptionHtml": description_html,
            "tags": tags
        }
    }
    
    result = graphql_query(mutation, variables)
    
    if not result: return False
    
    user_errors = result['data']['productUpdate']['userErrors']
    if user_errors:
        print(f"  ❌ Error updating product: {user_errors}")
        return False
    
    return True

# ==========================================
#              MAIN LOOP
# ==========================================

def process_products():
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    print(f"🚀 Starting pre-order cleanup at {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 Current date: {now.strftime('%Y-%m-%d')}")
    print(f"📅 30 days ago: {thirty_days_ago.strftime('%Y-%m-%d')}\n")
    
    cursor = None
    total_processed = 0
    total_updated = 0
    
    while True:
        products_data = get_products_with_preorder_tags(cursor)
        
        if not products_data:
            break
            
        products = products_data['edges']
        if not products:
            break
        
        for edge in products:
            product = edge['node']
            total_processed += 1
            
            product_id = product['id']
            title = product['title']
            description = product['descriptionHtml'] or ""
            tags = product['tags']
            release_date_field = product.get('metafield')
            
            print(f"\n📦 Processing: {title}")
            
            if not release_date_field or not release_date_field.get('value'):
                print("   ⚠️  No release date - skipping")
                continue
            
            release_date_str = release_date_field['value']
            try:
                release_date = datetime.strptime(release_date_str, '%Y-%m-%d')
            except ValueError:
                print(f"   ⚠️  Invalid Date: {release_date_str} - skipping")
                continue
            
            print(f"   📅 Release date: {release_date_str}")
            
            needs_update = False
            new_title = title
            new_description = description
            new_tags = tags.copy()
            
            # --- LOGIC GATES ---
            if release_date > now:
                print("   ✅ Future release - keeping as pre-order")
                continue
                
            elif release_date >= thirty_days_ago:
                print("   🆕 Within 30 days of release - converting to New Release")
                
                if title.upper().startswith('PRE-ORDER: '):
                    new_title = remove_preorder_prefix(title)
                    needs_update = True
                    print(f"   ✏️  Removing PRE-ORDER prefix")
                
                if 'Estimated Release Date:' in description:
                    new_description = remove_preorder_disclaimer(description)
                    if new_description != description:
                        needs_update = True
                        print(f"   ✏️  Removing disclaimer")
                
                if 'Pre-Order' in new_tags:
                    new_tags.remove('Pre-Order')
                    needs_update = True
                    print(f"   🏷️  Removing Pre-Order tag")
                
                if 'New Release' not in new_tags:
                    new_tags.append('New Release')
                    needs_update = True
                    print(f"   🏷️  Adding New Release tag")
                    
            else:
                print("   🧹 More than 30 days past release - removing tags")
                
                if title.upper().startswith('PRE-ORDER: '):
                    new_title = remove_preorder_prefix(title)
                    needs_update = True
                    print(f"   ✏️  Removing PRE-ORDER prefix")
                
                if 'Estimated Release Date:' in description:
                    new_description = remove_preorder_disclaimer(description)
                    if new_description != description:
                        needs_update = True
                        print(f"   ✏️  Removing disclaimer")
                
                tags_to_remove = ['Pre-Order', 'Pre-Order Reminder Sent', 'New Release', 'New Release Reminder Sent']
                for tag in tags_to_remove:
                    if tag in new_tags:
                        new_tags.remove(tag)
                        needs_update = True
                        print(f"   🏷️  Removing {tag} tag")
            
            # --- EXECUTE UPDATE ---
            if needs_update:
                print(f"   💾 Updating product...")
                if update_product(product_id, new_title, new_description, new_tags):
                    print(f"   ✅ Successfully updated!")
                    total_updated += 1
                else:
                    print(f"   ❌ Failed to update")
            else:
                print(f"   ℹ️  No changes needed")
        
        # Pagination
        if products_data['pageInfo']['hasNextPage']:
            cursor = products_data['pageInfo']['endCursor']
            print(f"\n📄 Fetching next page...")
        else:
            break
    
    print(f"\n" + "="*60)
    print(f"✨ Cleanup complete!")
    print(f"📊 Total products processed: {total_processed}")
    print(f"✅ Total products updated: {total_updated}")
    print(f"⏰ Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

if __name__ == "__main__":
    process_products()
