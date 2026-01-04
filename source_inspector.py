import requests
import json
import time
import os

# --- TARGETS ---
SOURCES = [
    {
        "name": "Moonstone",
        "url": "https://shop.moonstonethegame.com"
    },
    {
        "name": "Warsenal",
        "url": "https://warsen.al"
    }
]

def fetch_catalog(source_name, base_url):
    print(f"\n--- PULLING DATA FROM: {source_name} ---")
    products_endpoint = f"{base_url}/products.json"
    
    all_products = []
    page = 1
    
    while True:
        try:
            url = f"{products_endpoint}?limit=250&page={page}"
            print(f"  > Fetching Page {page}...", end="", flush=True)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            r = requests.get(url, headers=headers, timeout=15)
            
            if r.status_code != 200:
                print(f" [FAILED] Status Code: {r.status_code}")
                break
                
            data = r.json()
            products = data.get('products', [])
            
            if not products:
                print(" [DONE] No more products found.")
                break
                
            all_products.extend(products)
            print(f" [OK] Found {len(products)} items.")
            page += 1
            time.sleep(1) # Be polite to their server
            
        except Exception as e:
            print(f" [ERROR] {e}")
            break
            
    return all_products

def analyze_structure(products, source_name):
    """Prints a detailed analysis of the first item to the console"""
    if not products:
        print(f"No products to analyze for {source_name}")
        return

    sample = products[0]
    print(f"\n[{source_name}] DATA STRUCTURE SAMPLE (Item 1):")
    print(f"Title: {sample.get('title')}")
    print(f"Vendor: {sample.get('vendor')}")
    print(f"Tags: {sample.get('tags')}")
    
    if sample.get('variants'):
        v = sample['variants'][0]
        print(f"--- VARIANT DATA (Crucial for Price/Weight) ---")
        print(f"SKU: {v.get('sku')}")
        print(f"Price (Selling): {v.get('price')} (Type: {type(v.get('price'))})")
        print(f"Compare At (MSRP): {v.get('compare_at_price')} (Type: {type(v.get('compare_at_price'))})")
        print(f"Weight: {v.get('weight')}")
        print(f"Weight Unit: {v.get('weight_unit')}")
        print(f"Barcode: {v.get('barcode')}")
        print(f"Inventory Policy: {v.get('inventory_policy')}")
    
    # Save Raw Dump
    filename = f"raw_export_{source_name.lower()}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(products, f, indent=2)
    print(f"\n[SAVED] Full dump saved to {filename}")

def main():
    for source in SOURCES:
        data = fetch_catalog(source['name'], source['url'])
        analyze_structure(data, source['name'])

if __name__ == "__main__":
    main()
