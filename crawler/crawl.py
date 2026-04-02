#!/usr/bin/env python3
"""
PropertyFinder price-drop crawler – Al Hamra Village, Al Marjan Island, Mina Al Arab
Runs via GitHub Actions; commits updated JSON back to the repo.

Parses listing data from __NEXT_DATA__ JSON embedded in the page HTML,
which is more reliable than CSS selectors that change with site redesigns.
"""
import json, time, random, os, re, datetime
from pathlib import Path
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LOCATIONS = [
    {"id": "al-hamra",      "name": "Al Hamra Village",  "l": "151", "max_pages": 120},
    {"id": "marjan-island", "name": "Al Marjan Island",   "l": "152", "max_pages": 120},
    {"id": "mina-al-arab",  "name": "Mina Al Arab",       "l": "156", "max_pages": 120},
]

BASE_DIR = Path(__file__).parent.parent / "data"

# Regex to skip listings whose title is just a mortgage/cashback promo with a number
# e.g. "~ 23K Mortgage Cashback", "Get 50K Mortgage Cashback"
MORTGAGE_TITLE_RE = re.compile(r'^[\W\s]*[\d,]+[KkMm]?\s*(aed\s*)?mortgage', re.IGNORECASE)


def fetch_page(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            time.sleep(5)
    return None


def parse_listings(html):
    """Extract listings from __NEXT_DATA__ JSON embedded in the page."""
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
        search_result = data["props"]["pageProps"]["searchResult"]
        raw_listings = search_result.get("listings", [])
    except (KeyError, json.JSONDecodeError) as e:
        print(f"  Failed to parse __NEXT_DATA__: {e}")
        return []

    listings = []
    for item in raw_listings:
        # Only process regular property listings (skip project cards etc.)
        if item.get("listing_type") != "property":
            continue
        prop = item.get("property")
        if not prop:
            continue
        try:
            prop_id = str(prop["id"])
            price = prop.get("price", {}).get("value", 0)
            if not price:
                continue

            title = prop.get("title", "").strip()

            # Skip mortgage-promo-only titles (e.g. "~ 23K Mortgage Cashback")
            if MORTGAGE_TITLE_RE.match(title):
                continue

            url = "https://www.propertyfinder.ae" + prop.get("details_path", "")
            location = prop.get("location", {}).get("full_name", "")
            prop_type = prop.get("property_type", "")
            beds = prop.get("bedrooms", 0) or 0
            baths = prop.get("bathrooms", 0) or 0
            size_info = prop.get("size", {}) or {}
            sqft = size_info.get("value", 0) or 0
            price_sqft = prop.get("price_per_area", {}).get("price", 0) or 0
            if price_sqft == 0 and sqft > 0:
                price_sqft = round(price / sqft)

            listings.append({
                "id": prop_id,
                "url": url,
                "title": title,
                "type": prop_type,
                "location": location,
                "price": price,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "price_sqft": price_sqft,
            })
        except Exception as e:
            print(f"  Parse error on prop {prop.get('id')}: {e}")
            continue
    return listings


def get_page_count(html):
    """Return total page count from __NEXT_DATA__ meta."""
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
        meta = data["props"]["pageProps"]["searchResult"]["meta"]
        return meta.get("page_count")
    except Exception:
        return None


def crawl_location(session, loc_config):
    loc_id   = loc_config["id"]
    loc_name = loc_config["name"]
    l_param  = loc_config["l"]
    max_pages = loc_config["max_pages"]

    print(f"\n=== Crawling {loc_name} (l={l_param}) ===")

    data_dir = BASE_DIR / loc_id
    data_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = data_dir / "snapshot.json"
    drops_path    = data_dir / "drops.json"
    meta_path     = data_dir / "meta.json"

    old_snapshot = {}
    if snapshot_path.exists():
        try:
            old_snapshot = json.loads(snapshot_path.read_text())
        except Exception:
            pass

    new_snapshot = {}
    total_pages  = 0
    actual_max   = max_pages  # may be updated from page 1 meta

    for page in range(1, actual_max + 1):
        url = (f"https://www.propertyfinder.ae/en/search"
               f"?l={l_param}&c=1&fu=0&ob=np&page={page}")
        print(f"  Page {page}: {url}")
        html = fetch_page(session, url)
        if not html:
            print(f"  Failed to fetch page {page}, stopping.")
            break

        # On first page, read the true page count from metadata
        if page == 1:
            pc = get_page_count(html)
            if pc:
                actual_max = min(pc, max_pages)
                print(f"  Total pages available: {pc} (capped at {actual_max})")

        listings = parse_listings(html)
        if not listings:
            print(f"  No listings on page {page}, done.")
            break

        for prop in listings:
            new_snapshot[prop["id"]] = prop
        total_pages = page
        time.sleep(random.uniform(1.2, 2.5))

    print(f"  Crawled {total_pages} pages, {len(new_snapshot)} listings.")

    # Detect drops
    drops = []
    for prop_id, new_prop in new_snapshot.items():
        if prop_id in old_snapshot:
            old_price = old_snapshot[prop_id]["price"]
            new_price = new_prop["price"]
            if new_price < old_price and old_price > 0:
                drop_aed = old_price - new_price
                drop_pct = round((drop_aed / old_price) * 100, 1)
                if drop_pct >= 1.0:
                    drops.append({
                        **new_prop,
                        "old_price": old_price,
                        "new_price": new_price,
                        "drop_aed":  drop_aed,
                        "drop_pct":  drop_pct,
                        "detected_at": datetime.datetime.utcnow().isoformat() + "Z",
                    })
    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    print(f"  {len(drops)} price drops detected.")

    # Write output
    snapshot_path.write_text(json.dumps(new_snapshot, indent=2))
    drops_path.write_text(json.dumps(drops, indent=2))
    meta_path.write_text(json.dumps({
        "last_run": datetime.datetime.utcnow().isoformat() + "Z",
        "listings_tracked": len(new_snapshot),
        "drops_found": len(drops),
        "location": loc_name,
        "location_id": loc_id,
    }, indent=2))

    return len(new_snapshot), len(drops)


def main():
    session = requests.Session()
    session.headers.update(HEADERS)
    totals = {}
    for loc in LOCATIONS:
        tracked, drops = crawl_location(session, loc)
        totals[loc["id"]] = {"tracked": tracked, "drops": drops}

    print("\n=== Summary ===")
    for k, v in totals.items():
        print(f"  {k}: {v['tracked']} tracked, {v['drops']} drops")


if __name__ == "__main__":
    main()
