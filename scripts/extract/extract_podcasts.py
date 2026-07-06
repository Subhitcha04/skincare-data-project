"""
Extract: dermatology / skincare podcast audio via RSS feeds.

Pattern: manifest-then-download, mirroring real flat-file/cloud storage extraction.
Add your chosen feeds to PODCAST_FEEDS below (look up 2-3 dermatology / skincare
podcasts and grab their RSS URL -- usually on the podcast's website or Apple
Podcasts "RSS feed" link).
"""
import sys
import csv
import time
import requests
import feedparser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_AUDIO_VIDEO, get_logger

log = get_logger("extract_podcasts")

# Fill these in with real feed URLs before running.
PODCAST_FEEDS = [
    ("AAD_Dialogues_in_Dermatology", "https://resources.aad.org/library/podcasts/rss"),
]
MAX_EPISODES_PER_FEED = 3


def download_audio(url: str, dest: Path) -> bool:
    try:
        resp = requests.get(url, timeout=60, stream=True)
        if resp.status_code != 200:
            return False
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except requests.RequestException:
        return False


def main():
    if not PODCAST_FEEDS:
        log.error("PODCAST_FEEDS is empty -- add 2-3 RSS feed URLs in this script before running.")
        return

    manifest_path = RAW_AUDIO_VIDEO / "podcast_manifest.csv"
    new_file = not manifest_path.exists()

    with open(manifest_path, "a", newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        if new_file:
            writer.writerow(["show_name", "episode_title", "published", "audio_path", "download_ok"])

        for show_name, feed_url in PODCAST_FEEDS:
            log.info("Parsing feed: %s", show_name)
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:MAX_EPISODES_PER_FEED]:
                audio_url = next(
                    (l.href for l in entry.get("links", []) if "audio" in l.get("type", "")),
                    None,
                )
                if not audio_url:
                    continue
                slug = "".join(c if c.isalnum() else "_" for c in entry.title)[:60]
                dest = RAW_AUDIO_VIDEO / f"{show_name.replace(' ', '_')}_{slug}.mp3"
                ok = download_audio(audio_url, dest)
                writer.writerow([show_name, entry.title, entry.get("published", ""), str(dest), ok])
                time.sleep(0.5)

    log.info("Done. Manifest at %s", manifest_path)


if __name__ == "__main__":
    main()
