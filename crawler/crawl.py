#!/usr/bin/env python3
"""
PropertyFinder price-drop crawler – Al Hamra Village, Al Marjan Island, Mina Al Arab
Runs via GitHub Actions; commits updated JSON back to the repo.
"""
import json, time, random, os, re, datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MAX_PAGES = 120

LOCATIONS = [
    {"id": "al-hamra",       "name": "Al Hamra Village",   "l": "151", "max_pages": 120},
    {"id": "marjan-island",  "name": "Al Marjan Island",   "l": "152", "max_pages": 120},
    {"id": "mina-al-arab",   "name": "Mina Al Arab",       "l": "156", "max_pages": 120},
]

BASE_DIR = Path(__file__).parent.parent / "data"


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
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select('[data-testid="property-card"]')
    listings = []
    for card in cards:
        try:
            link_el = card.select_one('a[href*="/plp/"]')
            if not link_el:
                continue
            url = "https://www.propertyfinder.ae" + link_el["href"]
            prop_id = url.split("/")[-1].split(".")[0]

            price_el = card.select_one('[class*="price__"]')
            price_raw = price_el.get_text(strip=True) if price_el else ""
            price = int(re.sub(r"[^0-9]", "", price_raw)) if price_raw else 0
            if price == 0:
                continue

            title_el = card.select_one('[class*="title__"]')
            title = title_el.get_text(strip=True) if title_el else ""

            type_el = card.select_one('[class*="property-type__"]')
            prop_type = type_el.get_text(strip=True) if type_el else ""

            location_el = card.select_one('[class*="location__"]')
            location = location_el.get_text(strip=True) if location_el else ""

            detail_els = card.select('[class*="details-item__"]')
            beds = baths = sqft = 0
            for el in detail_els:
                t = el.get_text(strip=True).lower()
                num = re.sub(r"[^0-9]", "", t)
                if not num:
                    continue
                if "bed" in t or "studio" in t:
                    beds = int(num) if num else 0
                elif "bath" in t:
                    baths = int(num) if num else 0
                elif "sqft" in t or "sq" in t:
                    sqft = int(num) if num else 0

            price_sqft = round(price / sqft) if sqft > 0 else 0

            listings.append({
                "id": prop_id, "url": url, "title": title,
                "type": prop_type, "location": location,
                "price": price, "beds": beds, "baths": baths,
                "sqft": sqft, "price_sqft": price_sqft,
            })
        except Exception as e:
            print(f"  Parse error: {e}")
            continue
    return listings


def crawl_location(session, loc_config):
    loc_id    = loc_config["id"]
    loc_name  = loc_config["name"]
    l_param   = loc_config["l"]
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

    for page in range(1, max_pages + 1):
        url = (f"https://www.propertyfinder.ae/en/search"
               f"?l={l_param}&c=1&fu=0&ob=np&page={page}")
        print(f"  Page {page}: {url}")
        html = fetch_page(session, url)
        if not html:
            print(f"  Failed to fetch page {page}, stopping.")
            break

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
                drop_aed  = old_price - new_price
                drop_pct  = round((drop_aed / old_price) * 100, 1)
                if drop_pct >= 1.0:
                    drops.append({
                        **new_prop,
                        "old_price":  old_price,
                        "new_price":  new_price,
                        "drop_aed":   drop_aed,
                        "drop_pct":   drop_pct,
                        "detected_at": datetime.datetime.utcnow().isoformat() + "Z",
                    })

    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    print(f"  {len(drops)} price drops detected.")

    # Write output
    snapshot_path.write_text(json.dumps(new_snapshot, indent=2))
    drops_path.write_text(json.dumps(drops, indent=2))
    meta_path.write_text(json.dumps({
        "last_run":        datetime.datetime.utcnow().isoformat() + "Z",
        "listings_tracked": len(new_snapshot),
        "drops_found":     len(drops),
        "location":        loc_name,
        "location_id":     loc_id,
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
