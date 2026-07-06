"""
Extract: Open Beauty Facts product metadata + label images.

Pattern: REST API with pagination, raw JSON kept as-is for later MongoDB load
(per-product schema varies, so this is the NoSQL source).

Saves:
  raw/structured/obf_<category>_page<N>.json   (raw per-page JSON, for Mongo)
  raw/images/<product_code>.jpg                (downloaded label/product images)
  raw/images/manifest.csv                      (product_id, image_path, ingredients_text)

Resilient to transient network/DNS drops: retries connection errors with
backoff, and skips pages/products already saved on disk so a re-run after a
mid-run failure picks up where it left off instead of starting over.
"""
import sys
import csv
import json
import time
import requests
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_IMAGES, RAW_STRUCTURED, get_logger

log = get_logger("extract_obf")

CATEGORIES = ["face-care", "sun-care", "serums"]
PAGE_SIZE = 50
MAX_PAGES_PER_CATEGORY = 4   # bump up once the pipeline is verified end-to-end
BASE_URL = "https://world.openbeautyfacts.org/category/{category}.json"
MAX_RETRIES = 6
BASE_BACKOFF = 5   # seconds; doubles each retry, so 5/10/20/40/80/160s


def fetch_page(category: str, page: int) -> dict:
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                BASE_URL.format(category=category),
                params={"page": page, "page_size": PAGE_SIZE},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = BASE_BACKOFF * (2 ** attempt)
                log.warning("Rate limited, backing off %ss", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            # covers DNS failures, connection resets, timeouts -- transient
            # network issues that a plain status-code check won't catch
            last_err = e
            wait = BASE_BACKOFF * (2 ** attempt)
            log.warning("Network error on %s page %d (attempt %d/%d): %s -- retrying in %ss",
                        category, page, attempt + 1, MAX_RETRIES, e, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {category} page {page} after {MAX_RETRIES} retries") from last_err


def download_image(url: str, dest: Path) -> bool:
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200 and resp.content:
                dest.write_bytes(resp.content)
                return True
            return False
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
    return False


def load_existing_manifest_codes(manifest_path: Path) -> set:
    """Product codes already recorded, so a re-run doesn't duplicate rows."""
    if not manifest_path.exists():
        return set()
    import pandas as pd
    try:
        return set(pd.read_csv(manifest_path)["product_code"].astype(str))
    except Exception:
        return set()


def main():
    manifest_path = RAW_IMAGES / "manifest.csv"
    new_file = not manifest_path.exists()
    already_done = load_existing_manifest_codes(manifest_path)
    if already_done:
        log.info("Resuming: %d products already in manifest will be skipped", len(already_done))

    with open(manifest_path, "a", newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        if new_file:
            writer.writerow(["product_code", "category", "image_path", "ingredients_text", "image_ok"])

        for category in CATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                raw_out = RAW_STRUCTURED / f"obf_{category}_page{page}.json"

                # resume support: if this page's raw JSON is already on disk
                # (saved before a previous run died), reuse it instead of
                # re-hitting the network
                if raw_out.exists():
                    log.info("Using cached %s page %d from disk", category, page)
                    data = json.loads(raw_out.read_text(encoding="utf-8"))
                else:
                    log.info("Fetching %s page %d", category, page)
                    data = fetch_page(category, page)
                    raw_out.write_text(json.dumps(data), encoding="utf-8")

                products = data.get("products", [])
                if not products:
                    log.info("  no more products, moving to next category")
                    break

                new_count = 0
                for p in products:
                    code = str(p.get("code", "unknown"))
                    if code in already_done:
                        continue
                    image_url = p.get("image_url") or p.get("image_front_url")
                    ingredients_text = p.get("ingredients_text", "")
                    image_ok = False
                    img_path = ""
                    if image_url:
                        img_path = str(RAW_IMAGES / f"{code}.jpg")
                        image_ok = download_image(image_url, Path(img_path))
                    writer.writerow([code, category, img_path, ingredients_text, image_ok])
                    mf.flush()  # persist row-by-row so a crash mid-page doesn't lose prior rows
                    already_done.add(code)
                    new_count += 1

                log.info("  %d new products added to manifest (%d already had entries)",
                          new_count, len(products) - new_count)

                if not raw_out.exists() or new_count > 0:
                    time.sleep(1)  # be polite to a free community API, only matters on fresh fetches

    log.info("Done. Manifest at %s", manifest_path)


if __name__ == "__main__":
    main()