#!/usr/bin/env python3
"""Al Hamra Village Price Drop Crawler"""
import json, re, time, random, logging
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE_URL   = "https://www.propertyfinder.ae"
SEARCH_URL = lambda p: f"{BASE_URL}/en/search?l=151&c=1&fu=0&ob=np&page={p}"
MAX_PAGES  = 120
MIN_DELAY  = 1.2
MAX_DELAY  = 2.5
DATA_DIR   = Path(__file__).parent.parent / "data"
HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36","Accept-Language":"en-US,en;q=0.9","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Referer":"https://www.propertyfinder.ae/"}

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)-7s %(message)s",datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

def parse_page(html):
    soup = BeautifulSoup(html, "lxml")
    results = []
    for card in soup.find_all(attrs={"data-testid":"property-card"}):
        try:
            link = card.find("a", href=re.compile(r"/plp/"))
            if not link: continue
            href = link["href"]
            if href.startswith("/"): href = BASE_URL + href
            listing_id = href.rstrip("/").split("/")[-1].replace(".html","").split("-")[-1]
            if not listing_id or len(listing_id) < 5: continue
            price_el = card.find(class_=re.compile(r"price__"))
            if not price_el: continue
            price = int(re.sub(r"[^0-9]","",price_el.get_text(strip=True)))
            if not price: continue
            type_el  = card.find(class_=re.compile(r"property-type__"))
            title_el = card.find(class_=re.compile(r"title__"))
            loc_el   = card.find(class_=re.compile(r"location__"))
            details  = [d.get_text(strip=True) for d in card.find_all(class_=re.compile(r"details-item__"))]
            results.append({"id":listing_id,"href":href,"price":price,
                "type":  type_el.get_text(strip=True)  if type_el  else "",
                "title": title_el.get_text(strip=True) if title_el else "",
                "location":loc_el.get_text(strip=True) if loc_el   else "",
                "beds":  details[0] if len(details)>0 else "",
                "baths": details[1] if len(details)>1 else "",
                "sqft":  int(re.sub(r"[^0-9]","",details[2])) if len(details)>2 and details[2] else 0})
        except Exception as e:
            log.debug("Card parse error: %s", e)
    return results

def crawl():
    sess = requests.Session(); sess.headers.update(HEADERS)
    snap = {}; max_p = MAX_PAGES; errors = 0
    for page in range(1, max_p+1):
        log.info("Page %3d/%d | %d listings", page, max_p, len(snap))
        try:
            r = sess.get(SEARCH_URL(page), timeout=20); r.raise_for_status()
        except requests.RequestException as e:
            errors += 1; log.warning("Page %d error: %s", page, e); time.sleep(3); continue
        if page == 1:
            m = re.search(r"([0-9,]+)\s*propert", r.text, re.I)
            if m:
                total = int(m.group(1).replace(",",""))
                max_p = min(MAX_PAGES, -(-total//24))
                log.info("Total: %d listings -> %d pages", total, max_p)
        listings = parse_page(r.text)
        if not listings and page > 2: log.info("Page %d empty, stopping.", page); break
        for p in listings: snap[p["id"]] = p
        time.sleep(MIN_DELAY + random.random()*(MAX_DELAY-MIN_DELAY))
    log.info("Done: %d listings, %d errors", len(snap), errors)
    return snap

def compute_drops(new_snap, old_snap):
    drops = []
    for lid, curr in new_snap.items():
        prev = old_snap.get(lid)
        if prev and prev["price"] > curr["price"]:
            drop_abs = prev["price"] - curr["price"]
            drops.append({**curr,"prev_price":prev["price"],"drop_abs":drop_abs,
                "drop_pct":round(drop_abs/prev["price"]*100,2),
                "detected_at":datetime.now(timezone.utc).isoformat()})
    drops.sort(key=lambda d: d["drop_pct"], reverse=True)
    return drops

def merge_history(new_drops, drops_path):
    old = json.loads(drops_path.read_text()) if drops_path.exists() else []
    new_ids = {d["id"] for d in new_drops}
    merged = new_drops + [d for d in old if d["id"] not in new_ids]
    merged.sort(key=lambda d: d["drop_pct"], reverse=True)
    return merged

def main():
    DATA_DIR.mkdir(exist_ok=True)
    snap_path  = DATA_DIR/"snapshot.json"
    drops_path = DATA_DIR/"drops.json"
    meta_path  = DATA_DIR/"meta.json"
    old_snap = json.loads(snap_path.read_text()) if snap_path.exists() else {}
    log.info("Previous snapshot: %d listings", len(old_snap))
    new_snap  = crawl()
    new_drops = compute_drops(new_snap, old_snap)
    all_drops = merge_history(new_drops, drops_path)
    snap_path.write_text(json.dumps(new_snap,   ensure_ascii=False, indent=2))
    drops_path.write_text(json.dumps(all_drops, ensure_ascii=False, indent=2))
    meta = {"last_scan":datetime.now(timezone.utc).isoformat(),"total_listings":len(new_snap),"total_drops":len(all_drops),"new_drops_this_run":len(new_drops)}
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    log.info("Wrote %d drops, %d listings", len(all_drops), len(new_snap))
    print(json.dumps(meta, indent=2))

if __name__ == "__main__":
    main()
