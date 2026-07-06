"""
Extract: PubMed abstracts via NCBI E-utilities.

Pattern: REST API with pagination + exponential backoff.

Flow per ingredient term:
  esearch  -> list of PubMed IDs (PMIDs) matching the term
  efetch   -> abstract XML for those PMIDs
Saved as one XML file per term under raw/text/.
"""
import sys
import time
import requests
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_TEXT, NCBI_API_KEY, NCBI_EMAIL, INGREDIENT_TERMS, get_logger

log = get_logger("extract_pubmed")

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
RETMAX = 200          # PMIDs per term; raise if you want a bigger corpus
MAX_RETRIES = 5


def _request_with_backoff(url: str, params: dict) -> requests.Response:
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning("Rate limited (429). Backing off %ss", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")


def fetch_pmids(term: str) -> list[str]:
    params = {
        "db": "pubmed",
        "term": f"{term}[Title/Abstract] AND skin[Title/Abstract]",
        "retmax": RETMAX,
        "retmode": "json",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    if NCBI_EMAIL:
        params["email"] = NCBI_EMAIL
    resp = _request_with_backoff(ESEARCH_URL, params)
    return resp.json().get("esearchresult", {}).get("idlist", [])


def fetch_abstracts_xml(pmids: list[str]) -> str:
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "rettype": "abstract",
        "retmode": "xml",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    if NCBI_EMAIL:
        params["email"] = NCBI_EMAIL
    resp = _request_with_backoff(EFETCH_URL, params)
    return resp.text


def main():
    for term in INGREDIENT_TERMS:
        log.info("Searching PubMed for: %s", term)
        pmids = fetch_pmids(term)
        if not pmids:
            log.warning("No PMIDs found for %s, skipping", term)
            continue
        log.info("  -> %d PMIDs", len(pmids))
        xml = fetch_abstracts_xml(pmids)
        out_path = RAW_TEXT / f"pubmed_{term.replace(' ', '_')}.xml"
        out_path.write_text(xml, encoding="utf-8")
        log.info("  saved -> %s", out_path)
        # NCBI courtesy: cap at ~3 req/sec without a key, 10/sec with one
        time.sleep(0.34 if not NCBI_API_KEY else 0.11)


if __name__ == "__main__":
    main()
