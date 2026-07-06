"""
Shared configuration: paths, env loading, logging.
Every script in scripts/ imports from here so paths stay consistent.
"""
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# --- folder layout (mirrors the eventual flat-file/cloud storage layer) ---
RAW_DIR = ROOT / "raw"
RAW_TEXT = RAW_DIR / "text"
RAW_IMAGES = RAW_DIR / "images"
RAW_AUDIO_VIDEO = RAW_DIR / "audio_video"
RAW_STRUCTURED = RAW_DIR / "structured"
STAGING_DIR = ROOT / "staging"
PROCESSED_DIR = ROOT / "processed"

for d in (RAW_TEXT, RAW_IMAGES, RAW_AUDIO_VIDEO, RAW_STRUCTURED, STAGING_DIR, PROCESSED_DIR):
    d.mkdir(parents=True, exist_ok=True)

# --- credentials / connection strings ---
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY", "")
POSTGRES_URL = os.getenv("POSTGRES_URL", "")
MYSQL_URL = os.getenv("MYSQL_URL", "")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "skincare_raw")
COSING_CSV_PATH = ROOT / os.getenv("COSING_CSV_PATH", "raw/structured/cosing_export.csv")

# --- ingredient seed list used across PubMed / OBF / risk joins ---
INGREDIENT_TERMS = [
    "niacinamide", "hydroquinone", "tranexamic acid", "retinoid", "retinol",
    "azelaic acid", "kojic acid", "arbutin", "vitamin c", "ascorbic acid",
    "salicylic acid", "glycolic acid", "lactic acid", "benzoyl peroxide",
    "hyaluronic acid", "ceramide", "peptide", "centella asiatica", "licorice extract",
    "alpha arbutin",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
