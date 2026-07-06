"""
Extract: YouTube skincare-claims video metadata (+ captions where available).

Pattern: REST API (search.list + videos.list), pagination via pageToken.
Captions are pulled with youtube-transcript-api when present; videos without
captions are just flagged in the manifest so preprocessing knows to run Whisper.
"""
import sys
import csv
import time
from pathlib import Path
from googleapiclient.discovery import build

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_AUDIO_VIDEO, YOUTUBE_API_KEY, get_logger

log = get_logger("extract_youtube")

QUERIES = ["skincare review", "ingredient breakdown", "dermatologist explains skincare"]
MAX_RESULTS_PER_QUERY = 25  # raise once verified


def search_videos(youtube, query: str, max_results: int):
    videos, next_token = [], None
    while len(videos) < max_results:
        resp = youtube.search().list(
            q=query, part="snippet", type="video",
            maxResults=min(25, max_results - len(videos)),
            pageToken=next_token,
        ).execute()
        videos.extend(resp.get("items", []))
        next_token = resp.get("nextPageToken")
        if not next_token:
            break
        time.sleep(0.2)
    return videos


def try_get_caption(video_id: str) -> str:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        segments = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join(s["text"] for s in segments)
    except Exception:
        return ""


def main():
    if not YOUTUBE_API_KEY:
        log.error("YOUTUBE_API_KEY not set in .env -- skipping extraction.")
        return

    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    manifest_path = RAW_AUDIO_VIDEO / "youtube_manifest.csv"
    new_file = not manifest_path.exists()

    with open(manifest_path, "a", newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        if new_file:
            writer.writerow(["video_id", "title", "published_at", "query", "has_caption", "caption_path"])

        for query in QUERIES:
            log.info("Searching YouTube for: %s", query)
            items = search_videos(youtube, query, MAX_RESULTS_PER_QUERY)
            for item in items:
                vid = item["id"]["videoId"]
                title = item["snippet"]["title"]
                published = item["snippet"]["publishedAt"]
                caption = try_get_caption(vid)
                caption_path = ""
                has_caption = bool(caption)
                if has_caption:
                    caption_path = str(RAW_AUDIO_VIDEO / f"{vid}_caption.txt")
                    Path(caption_path).write_text(caption, encoding="utf-8")
                writer.writerow([vid, title, published, query, has_caption, caption_path])
            time.sleep(0.5)

    log.info("Done. Manifest at %s", manifest_path)
    log.info("NOTE: actual video/audio download (e.g. via yt-dlp) is a separate, "
              "policy-sensitive step -- only download content you have rights to use.")


if __name__ == "__main__":
    main()
