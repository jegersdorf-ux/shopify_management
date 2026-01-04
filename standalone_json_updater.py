import json
import os
import requests
import time
import sys

# --- CONFIGURATION ---
# Uses the same environment variables as your main pipeline
SHOPIFY_STORE_URL = os.getenv("SHOPIFY_STORE_URL")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
API_VERSION = "2025-10"

# List the JSON files you want to process
SOURCE_FILES = [
    "raw_export_moonstone.json",
    "raw_export_warsenal.json"
]

# Toggle to print what WOULD happen without actually updating
DRY_RUN = False 

# Headers for API calls
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

def get_shopify_url():
    """Helper to format the GraphQL endpoint"""
    if not SHOPIFY_STORE_URL: return None
    clean_url = SHOPIFY_STORE_URL.strip().replace("https://", "").replace("http://", "")
    if "/" in clean_url: clean_url = clean_url.split("/")[0]
    return f"https://{clean_url}/admin/api/{API_VERSION}/graphql.json"

def load_local_data():
    """
    Reads the raw JSON files and flattens them into a dictionary keyed by SKU.
    Returns: { 'SKU123': { 'target_cost': 10.50, 'target_compare': 20.00 } }
    """
    sku_map = {}
    total_files = 0
    
    for filename in SOURCE_FILES:
        if not os.path.exists(filename):
            print(f"[WARN] File not found: {filename}")
            continue
            
        print(f"Loading {filename}...", flush=True)
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            for product in data:
                # Handle both structure types (list of products or single product)
                if not isinstance(product, dict): continue
                
                variants = product.get('variants', [])
                for variant in variants:
                    sku = variant.get('sku')
                    if not sku: continue
                    
                    sku = sku.strip()
                    
                    # --- MAPPING LOGIC ---
                    # 1. Source 'price' is the Vendor's selling price -> Your COST
                    # 2. Source 'compare_at_price' -> Your COMPARE AT
                    
                    raw_price = variant.get('price')
                    raw_compare = variant.get('compare_at_price')
                    
                    sku_map[sku] = {
                        "target_cost": raw_price,
                        "target_compare": raw_compare,
                        "title": product.get('title')
                    }
            total_files += 1
        except Exception as e:
            print(f"[ERR] Failed to parse {filename}: {e}")

    print(f"Loaded data for {len(sku_map)} unique SKUs from {total_files} files.\n")
    return sku_map

def find_shopify_product_ids(sku):
    """
    Fetches the Variant ID and InventoryItem ID for a given SKU.
    """
    query = """
    query($query: String!) {
      products(first: 1, query: $query) {
        edges {
          node {
            id
            title
            variants(first: 1) {
              edges {
                node {
                  id
                  sku
                  compareAtPrice
                  inventoryItem {
                    id
                    unitCost { amount }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    
    url = get_shopify_url()
    variables = {"query": f"sku:{sku}"}
    
    try:
        response = requests.post(url, json={"query": query, "variables": variables}, headers=HEADERS, timeout=10)
        data = response.json()
        
        edges = data.get('data', {}).get('products', {}).get('edges', [])
        if not edges:
            return None
            
        # Check strict SKU match on the first variant
        variant_node = edges[0]['node']['variants']['edges'][0]['node']
        if variant_node['sku'] != sku:
            return None
            
        return {
            "variant_id": variant_node['id'],
            "current_compare": variant_node.get('compareAtPrice'),
            "inventory_item_id": variant_node['inventoryItem']['id'],
            "current_cost": variant_node['inventoryItem'].get('unitCost', {}).get('amount') if variant_node['inventoryItem'].get('unitCost') else None
        }
    except Exception as e:
        print(f"[API ERR] Looking up {sku}: {e}")
        return None

def update_cost_and_compare(ids, target_data):
    """
    Performs the GraphQL mutations to update Cost and Compare-At.
    """
    sku_name = target_data.get('title', 'Unknown Item')
    
    # 1. Update Compare At (Variant Level)
    if target_data['target_compare']:
        if str(target_data['target_compare']) != str(ids['current_compare']):
            if not DRY_RUN:
                mutation_variant = """
                mutation productVariantUpdate($input: ProductVariantInput!) {
                  productVariantUpdate(input: $input) {
                    userErrors { field, message }
                  }
                }
                """
                payload = {
                    "id": ids['variant_id'],
                    "compareAtPrice": str(target_data['target_compare'])
                }
                requests.post(get_shopify_url(), json={"query": mutation_variant, "variables": {"input": payload}}, headers=HEADERS)
                print(f"  [UPDATED] Compare At: {ids['current_compare']} -> {target_data['target_compare']}")
            else:
                print(f"  [DRY RUN] Would update Compare At: {ids['current_compare']} -> {target_data['target_compare']}")

    # 2. Update Cost (Inventory Item Level)
    if target_data['target_cost']:
        # Note: target_cost might be a string like "12.50" or float. Safe convert.
        if str(target_data['target_cost']) != str(ids['current_cost']):
            if not DRY_RUN:
                mutation_inventory = """
                mutation inventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
                  inventoryItemUpdate(id: $id, input: $input) {
                    userErrors { field, message }
                  }
                }
                """
                payload = {
                    "cost": str(target_data['target_cost'])
                }
                requests.post(get_shopify_url(), json={"query": mutation_inventory, "variables": {"id": ids['inventory_item_id'], "input": payload}}, headers=HEADERS)
                print(f"  [UPDATED] Cost: {ids['current_cost']} -> {target_data['target_cost']}")
            else:
                print(f"  [DRY RUN] Would update Cost: {ids['current_cost']} -> {target_data['target_cost']}")

def main():
    if not ACCESS_TOKEN or not SHOPIFY_STORE_URL:
        print("CRITICAL: Missing SHOPIFY_STORE_URL or SHOPIFY_ACCESS_TOKEN env vars.")
        sys.exit(1)

    print("--- STANDALONE JSON UPDATER ---")
    source_data = load_local_data()
    
    if not source_data:
        print("No data found in JSON files. Exiting.")
        sys.exit(0)

    print("Starting Update Process...")
    
    count = 0
    updated_count = 0
    
    for sku, data in source_data.items():
        count += 1
        if count % 10 == 0:
            print(f"Processed {count}/{len(source_data)} items...", flush=True)
            
        # 1. Find the item in Shopify
        shopify_ids = find_shopify_product_ids(sku)
        
        if shopify_ids:
            # 2. Execute Updates
            print(f"> Checking {sku}")
            update_cost_and_compare(shopify_ids, data)
            updated_count += 1
            time.sleep(0.5) # Rate limit safety
        else:
            # Item in JSON but not in Shopify
            # print(f"> SKIPPING {sku} (Not found in Shopify)")
            pass

    print(f"\n--- JOB COMPLETE ---")
    print(f"Scanned: {count}")
    print(f"Matched & Checked: {updated_count}")

if __name__ == "__main__":
    main()
