"""
Extract: openFDA adverse event reports (cosmetics-adjacent drug/device events
used as the "medical / harm record" source).

Pattern: REST API with TWO pagination strategies:
  - Low-volume ingredients  → skip/limit pagination (fast, simple)
  - High-volume ingredients → date-range chunking (avoids 25k skip ceiling
                              and prevents timeout on deep offsets)

Incremental load: watermark file stores last seen receivedate per term so
re-runs only pull new records. Watermark saved after every term so a mid-run
crash doesn't force re-fetching completed terms.

Resilient to transient network/DNS drops: retries with exponential backoff on
connection errors, timeouts, and DNS failures.
"""
import sys
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_STRUCTURED, OPENFDA_API_KEY, INGREDIENT_TERMS, get_logger

log = get_logger("extract_openfda")

BASE_URL     = "https://api.fda.gov/drug/event.json"
LIMIT        = 100
MAX_SKIP     = 25_000
CHUNK_DAYS   = 90
MAX_RETRIES  = 6
BASE_BACKOFF = 5

WATERMARK_FILE = RAW_STRUCTURED / "_openfda_watermark.json"

HIGH_VOLUME_TERMS = {"vitamin c", "tranexamic acid", "retinol", "retinoid", "ascorbic acid", "salicylic acid", "glycolic acid", "lactic acid"}


# ─── Exceptions ───────────────────────────────────────────────────────────────

class OpenFDAPagingLimitReached(Exception):
    pass


class OpenFDAServerError(Exception):
    pass


# ─── Watermark helpers ────────────────────────────────────────────────────────

def load_watermark() -> dict:
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}


def save_watermark(wm: dict) -> None:
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2))


def wm_to_dt(wm_date: str) -> datetime:
    return datetime.strptime(wm_date, "%Y-%m-%d")


def dt_to_fda(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def fda_to_wm(fda_date: str) -> str:
    return f"{fda_date[:4]}-{fda_date[4:6]}-{fda_date[6:8]}"


# ─── Core HTTP fetch ──────────────────────────────────────────────────────────

def _get(search_query: str, skip: int, term: str) -> dict:
    """
    Builds URL manually using urllib.parse.quote so spaces become %20
    (not + which requests would re-encode as %2B, breaking openFDA's
    Lucene parser and causing 500 errors).
    """
    encoded_search = quote(search_query, safe=':[]"')

    url = (
        f"{BASE_URL}"
        f"?search={encoded_search}"
        f"&limit={LIMIT}"
        f"&skip={skip}"
    )
    if OPENFDA_API_KEY:
        url += f"&api_key={OPENFDA_API_KEY}"

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=30)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 404:
                return {"results": []}

            if resp.status_code == 429:
                wait = BASE_BACKOFF * (2 ** attempt)
                log.warning("Rate limited — backing off %ss", wait)
                time.sleep(wait)
                continue

            if resp.status_code == 500:
                # Non-transient — openFDA rejecting the query server-side.
                # Do not retry; skip this window and move on.
                raise OpenFDAServerError(
                    f"openFDA 500 for '{term}' skip={skip} | url={url}"
                )

            if resp.status_code == 400 and skip + LIMIT > MAX_SKIP:
                raise OpenFDAPagingLimitReached(
                    f"'{term}': hit openFDA {MAX_SKIP}-record ceiling at skip={skip}"
                )

            resp.raise_for_status()

        except (OpenFDAPagingLimitReached, OpenFDAServerError):
            raise

        except requests.exceptions.RequestException as e:
            last_err = e
            wait = BASE_BACKOFF * (2 ** attempt)
            log.warning(
                "Network error for '%s' skip=%d (attempt %d/%d): %s — retrying in %ss",
                term, skip, attempt + 1, MAX_RETRIES, e, wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Failed to fetch openFDA for '{term}' (skip={skip}) "
        f"after {MAX_RETRIES} retries"
    ) from last_err


# ─── Strategy 1: skip-based (low-volume terms) ───────────────────────────────

def fetch_skip_based(term: str, since_date: str) -> list:
    """
    Adds receivedate filter to the query so openFDA only returns records
    newer than the watermark — no pointless full-history page scanning.
    """
    since_fda = since_date.replace("-", "")
    today_fda = datetime.now().strftime("%Y%m%d")

    search_query = (
        f'patient.drug.medicinalproduct:"{term}" '
        f'AND receivedate:[{since_fda} TO {today_fda}]'
    )

    skip = 0
    collected = []

    while True:
        try:
            page = _get(search_query, skip, term)
        except OpenFDAPagingLimitReached as e:
            log.warning("  %s — add to HIGH_VOLUME_TERMS to use date-chunking", e)
            break
        except OpenFDAServerError as e:
            log.error("  Server error, skipping term: %s", e)
            break

        results = page.get("results", [])
        if not results:
            break

        collected.extend(results)
        log.info("  [skip=%d] fetched %d records", skip, len(results))

        if len(results) < LIMIT:
            break

        skip += LIMIT
        if skip >= MAX_SKIP:
            log.warning(
                "  '%s': hit %d-record paging limit — add to HIGH_VOLUME_TERMS",
                term, MAX_SKIP,
            )
            break

        time.sleep(0.5)

    return collected


# ─── Strategy 2: date-chunked (high-volume terms) ────────────────────────────

def fetch_date_chunked(term: str, since_date: str) -> list:
    """
    Splits history into CHUNK_DAYS windows, paginates within each.
    Each window has few enough records that skip never approaches
    the 25k ceiling and requests never time out.
    """
    window_start = wm_to_dt(since_date)
    today        = datetime.now()
    collected    = []

    while window_start < today:
        window_end = min(window_start + timedelta(days=CHUNK_DAYS), today)

        ws_str = dt_to_fda(window_start)
        we_str = dt_to_fda(window_end)

        search_query = (
            f'patient.drug.medicinalproduct:"{term}" '
            f'AND receivedate:[{ws_str} TO {we_str}]'
        )

        log.info("  [%s → %s] querying window …", ws_str, we_str)

        skip = 0
        while True:
            try:
                page = _get(search_query, skip, term)
            except OpenFDAPagingLimitReached:
                log.warning(
                    "  Paging limit inside window [%s → %s] skip=%d "
                    "— reduce CHUNK_DAYS if this recurs",
                    ws_str, we_str, skip,
                )
                break
            except OpenFDAServerError as e:
                log.error(
                    "  Server error in window [%s → %s]: %s — skipping window",
                    ws_str, we_str, e,
                )
                break

            results = page.get("results", [])
            if not results:
                log.info("    → no records in this window")
                break

            collected.extend(results)
            log.info("    skip=%d → %d records", skip, len(results))

            if len(results) < LIMIT:
                break

            skip += LIMIT
            time.sleep(0.3)

        window_start = window_end + timedelta(days=1)

    return collected


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    watermark = load_watermark()
    run_date  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for term in INGREDIENT_TERMS:
        out_path = RAW_STRUCTURED / f"openfda_{term.replace(' ', '_')}_{run_date}.json"

        if out_path.exists():
            log.info("Skipping '%s' — already fetched today (%s)", term, out_path.name)
            continue

        is_high_volume = term in HIGH_VOLUME_TERMS
        default_since  = (
            (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if is_high_volume
            else "1900-01-01"
        )
        since_date = watermark.get(term, default_since)

        log.info(
            "Pulling openFDA events for '%s' (since %s) [strategy: %s]",
            term, since_date,
            "date-chunked" if is_high_volume else "skip-based",
        )

        try:
            results = (
                fetch_date_chunked(term, since_date)
                if is_high_volume
                else fetch_skip_based(term, since_date)
            )
        except RuntimeError as e:
            log.error("Giving up on '%s': %s — run again later to resume", term, e)
            save_watermark(watermark)
            raise

        if results:
            out_path.write_text(json.dumps(results, indent=2))
            log.info("  → %d records saved to %s", len(results), out_path.name)
            newest_fda      = max(r.get("receivedate", "19000101") for r in results)
            watermark[term] = fda_to_wm(newest_fda)
        else:
            log.info("  → no new records")

        save_watermark(watermark)

    log.info("Done.")


if __name__ == "__main__":
    main()