"""
Preprocess: PubMed abstracts.

- strip XML/HTML tags
- drop empty/title-only abstracts
- lowercase, tokenize, remove stopwords
- EDA: abstract length distribution, keyword frequency
- features: TF-IDF -> PCA/UMAP for 2D visualization
"""
import sys
import re
import json
from pathlib import Path
from collections import Counter

import pandas as pd
import nltk
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_TEXT, PROCESSED_DIR, get_logger

log = get_logger("preprocess_text")

nltk.download("stopwords", quiet=True)
from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words("english"))


def strip_xml(xml_text: str) -> list[dict]:
    """Pull (pmid, abstract_text) pairs out of an efetch XML blob."""
    soup = BeautifulSoup(xml_text, "lxml-xml")
    records = []
    for article in soup.find_all("PubmedArticle"):
        pmid_tag = article.find("PMID")
        abstract_tag = article.find("AbstractText")
        pmid = pmid_tag.text if pmid_tag else None
        abstract = abstract_tag.text if abstract_tag else ""
        records.append({"pmid": pmid, "abstract": abstract})
    return records


def clean_tokens(text: str) -> list[str]:
    text = re.sub(r"[^a-zA-Z\s]", " ", text.lower())
    tokens = text.split()
    return [t for t in tokens if t not in STOPWORDS and len(t) > 2]


def main():
    all_records = []
    for xml_file in RAW_TEXT.glob("pubmed_*.xml"):
        term = xml_file.stem.replace("pubmed_", "").replace("_", " ")
        for rec in strip_xml(xml_file.read_text(encoding="utf-8")):
            rec["ingredient_term"] = term
            all_records.append(rec)

    df = pd.DataFrame(all_records)
    log.info("Loaded %d raw records", len(df))

    # drop title-only / empty abstracts
    df = df[df["abstract"].str.strip().str.len() > 0].reset_index(drop=True)
    log.info("After dropping empty abstracts: %d records", len(df))

    df["tokens"] = df["abstract"].apply(clean_tokens)
    df["clean_text"] = df["tokens"].apply(" ".join)
    df["abstract_length"] = df["tokens"].apply(len)

    # --- EDA: length distribution ---
    plt.figure(figsize=(8, 5))
    df["abstract_length"].hist(bins=40)
    plt.title("Abstract length distribution (tokens)")
    plt.xlabel("token count")
    plt.ylabel("frequency")
    plt.savefig(PROCESSED_DIR / "eda_abstract_length_hist.png")
    plt.close()

    # --- EDA: keyword frequency ---
    all_tokens = Counter(t for toks in df["tokens"] for t in toks)
    top_terms = pd.DataFrame(all_tokens.most_common(30), columns=["term", "count"])
    top_terms.to_csv(PROCESSED_DIR / "eda_top_keywords.csv", index=False)

    # --- features: TF-IDF + PCA ---
    vectorizer = TfidfVectorizer(max_features=2000, min_df=2)
    tfidf = vectorizer.fit_transform(df["clean_text"])

    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(tfidf.toarray())
    df["pca_x"], df["pca_y"] = coords[:, 0], coords[:, 1]

    plt.figure(figsize=(8, 6))
    for term, group in df.groupby("ingredient_term"):
        plt.scatter(group["pca_x"], group["pca_y"], label=term, alpha=0.6, s=15)
    plt.legend(fontsize=6, loc="best", ncol=2)
    plt.title("PubMed abstracts: TF-IDF -> PCA, colored by ingredient term")
    plt.savefig(PROCESSED_DIR / "eda_pca_clusters.png")
    plt.close()

    out_path = PROCESSED_DIR / "pubmed_processed.parquet"
    df.drop(columns=["tokens"]).to_parquet(out_path, index=False)
    log.info("Saved processed text -> %s", out_path)
    log.info("Missingness: %d / %d original records had no usable abstract",
              len(all_records) - len(df), len(all_records))


if __name__ == "__main__":
    main()
