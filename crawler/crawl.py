#!/usr/bin/env python3
"""
PropertyFinder price-drop crawler – Al Hamra Village, Al Marjan Island, Mina Al Arab
Runs via GitHub Actions; commits updated JSON back to the repo.

Uses Playwright (headless Chromium) to bypass AWS WAF bot protection.
Extracts listing data from __NEXT_DATA__ JSON embedded in the page HTML.
"""
import json, time, random, os, re, datetime
from pathlib import Path

LOCATIONS = [
    {"id": "al-hamra", "name": "Al Hamra Village", "l": "151", "max_pages": 120},
    {"id": "marjan-island", "name": "Al Marjan Island", "l": "152", "max_pages": 120},
    {"id": "mina-al-arab", "name": "Mina Al Arab", "l": "156", "max_pages": 120},
]

BASE_DIR = Path(__file__).parent.parent / "data"

# Skip listings whose title is just a mortgage/cashback promo with a number
# e.g. "~ 23K Mortgage Cashback"
MORTGAGE_TITLE_RE = re.compile(r'^[\W\s]*[\d,]+[KkMm]?\s*(aed\s*)?mortgage', re.IGNORECASE)


def parse_next_data(html):
    """Extract listing data from __NEXT_DATA__ JSON embedded in the page."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.DOTALL
    )
    if not match:
        return [], None
    try:
        data = json.loads(match.group(1))
        sr = data["props"]["pageProps"]["searchResult"]
        meta = sr.get("meta", {})
        raw_listings = sr.get("listings", [])
    except (KeyError, json.JSONDecodeError) as e:
        print(f"  Failed to parse __NEXT_DATA__: {e}")
        return [], None

    listings = []
    for item in raw_listings:
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
            print(f"  Parse error: {e}")
    return listings, meta


def make_context(browser):
    """Create a fresh browser context with realistic browser settings."""
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        viewport={"width": 1280, "height": 800},
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    """)
    return context


def crawl_location(browser, loc_config):
    """Crawl one location using a fresh browser context."""
    loc_id = loc_config["id"]
    loc_name = loc_config["name"]
    l_param = loc_config["l"]
    max_pages = loc_config["max_pages"]

    print(f"\n=== Crawling {loc_name} (l={l_param}) ===")

    data_dir = BASE_DIR / loc_id
    data_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = data_dir / "snapshot.json"
    drops_path = data_dir / "drops.json"
    meta_path = data_dir / "meta.json"

    old_snapshot = {}
    if snapshot_path.exists():
        try:
            old_snapshot = json.loads(snapshot_path.read_text())
        except Exception:
            pass

    new_snapshot = {}
    total_pages = 0
    actual_max = max_pages

    # Fresh context for each location to avoid WAF rate limiting
    context = make_context(browser)
    page = context.new_page()

    # Warm up: visit the homepage first to get WAF cookies
    print("  Warming up browser (getting WAF token)...")
    page.goto("https://www.propertyfinder.ae/", wait_until="domcontentloaded", timeout=60000)
    time.sleep(3)

    for pg in range(1, actual_max + 1):
        url = (f"https://www.propertyfinder.ae/en/search"
               f"?l={l_param}&c=1&fu=0&ob=np&page={pg}")
        print(f"  Page {pg}: {url}")

        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            html = page.content()
        except Exception as e:
            print(f"  networkidle timeout on page {pg}: {e}")
            # Fallback: domcontentloaded + sleep
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(3)
                html = page.content()
            except Exception as e2:
                print(f"  Fallback also failed: {e2}")
                break

        # Debug: check if we got __NEXT_DATA__
        if '__NEXT_DATA__' not in html:
            print(f"  WARNING: No __NEXT_DATA__ in response for page {pg}. HTML snippet:")
            print(f"  {html[:200]}")
            break

        listings, meta = parse_next_data(html)

        if pg == 1 and meta:
            pc = meta.get("page_count")
            if pc:
                actual_max = min(pc, max_pages)
            total_count = meta.get("total_count", 0)
            print(f"  Total listings: {total_count}, pages: {pc} (capped at {actual_max})")

        if not listings:
            print(f"  No listings on page {pg}, done.")
            break

        for prop in listings:
            new_snapshot[prop["id"]] = prop
        total_pages = pg

        # Small delay between pages
        time.sleep(random.uniform(0.8, 1.5))

    context.close()
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
                        "drop_aed": drop_aed,
                        "drop_pct": drop_pct,
                        "detected_at": datetime.datetime.utcnow().isoformat() + "Z",
                    })
    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    print(f"  {len(drops)} price drops detected.")

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
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
        )

        totals = {}
        for loc in LOCATIONS:
            tracked, drops = crawl_location(browser, loc)
            totals[loc["id"]] = {"tracked": tracked, "drops": drops}

        browser.close()

    print("\n=== Summary ===")
    for k, v in totals.items():
        print(f"  {k}: {v['tracked']} tracked, {v['drops']} drops")


if __name__ == "__main__":
    main()
