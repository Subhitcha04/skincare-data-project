"""
Preprocess: product + ingredient label images (Open Beauty Facts).

- filter out broken downloads (quantify as a missing-data stat)
- resize to a consistent resolution
- correct orientation (EXIF)
- OCR (Tesseract) -> extracted ingredient text feature
- normalize pixel values 0-1
- optional: CLIP embeddings for a reduced feature representation
"""
import sys
import csv
import numpy as np
import pandas as pd
from pathlib import Path
from PIL import Image, ImageOps
import pytesseract

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_IMAGES, PROCESSED_DIR, get_logger

log = get_logger("preprocess_images")

TARGET_SIZE = (256, 256)
USE_CLIP = False  # flip on if open-clip-torch is installed and you want embeddings


def load_and_fix(path: Path) -> Image.Image | None:
    try:
        img = Image.open(path)
        img = ImageOps.exif_transpose(img)  # fix rotated label photos
        img = img.convert("RGB")
        return img
    except Exception:
        return None


def ocr_text(img: Image.Image) -> str:
    try:
        return pytesseract.image_to_string(img).strip()
    except Exception:
        return ""


def maybe_clip_embedding(img: Image.Image):
    if not USE_CLIP:
        return None
    import torch
    import open_clip
    model, _, preprocess = open_clip.create_model_and_transforms("ViT-B-32", pretrained="openai")
    model.eval()
    with torch.no_grad():
        tensor = preprocess(img).unsqueeze(0)
        embedding = model.encode_image(tensor).squeeze(0).numpy()
    return embedding.tolist()


def main():
    manifest_path = RAW_IMAGES / "manifest.csv"
    if not manifest_path.exists():
        log.error("No manifest.csv found in raw/images -- run extract_openbeautyfacts.py first.")
        return

    manifest = pd.read_csv(manifest_path)
    results = []
    n_broken = 0

    for _, row in manifest.iterrows():
        img_path = Path(row["image_path"]) if isinstance(row["image_path"], str) else None
        if not img_path or not img_path.exists():
            n_broken += 1
            continue

        img = load_and_fix(img_path)
        if img is None:
            n_broken += 1
            continue

        resized = img.resize(TARGET_SIZE)
        pixel_array = np.asarray(resized).astype("float32") / 255.0  # normalize 0-1
        extracted_text = ocr_text(resized)
        embedding = maybe_clip_embedding(resized)

        results.append({
            "product_code": row["product_code"],
            "category": row.get("category"),
            "width": img.size[0],
            "height": img.size[1],
            "ocr_text": extracted_text,
            "pixel_mean": float(pixel_array.mean()),
            "has_clip_embedding": embedding is not None,
        })

    total = len(manifest)
    log.info("Broken/missing images: %d / %d (%.1f%%)", n_broken, total, 100 * n_broken / max(total, 1))

    out_df = pd.DataFrame(results)
    out_path = PROCESSED_DIR / "images_processed.parquet"
    out_df.to_parquet(out_path, index=False)
    log.info("Saved processed image features -> %s", out_path)


if __name__ == "__main__":
    main()
