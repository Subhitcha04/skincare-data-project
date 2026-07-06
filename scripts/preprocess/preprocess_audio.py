"""
Preprocess: podcast/video audio.

- trim silence
- normalize loudness
- resample to one common sample rate
- Whisper transcription fallback for anything without captions (flag + report %)
- MFCC / spectrogram features, then PCA for dim reduction (same pattern as text)
"""
import sys
import numpy as np
import pandas as pd
import librosa
import soundfile as sf
from pathlib import Path
from sklearn.decomposition import PCA

sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import RAW_AUDIO_VIDEO, PROCESSED_DIR, get_logger

log = get_logger("preprocess_audio")

TARGET_SR = 16000
N_MFCC = 20


def trim_and_normalize(y: np.ndarray, sr: int) -> np.ndarray:
    y_trimmed, _ = librosa.effects.trim(y, top_db=30)
    rms = np.sqrt(np.mean(y_trimmed ** 2)) + 1e-9
    target_rms = 0.1
    return y_trimmed * (target_rms / rms)


def whisper_transcribe(path: Path) -> str:
    try:
        import whisper
        model = whisper.load_model("base")
        result = model.transcribe(str(path))
        return result.get("text", "")
    except Exception as e:
        log.warning("Whisper transcription failed for %s: %s", path, e)
        return ""


def main():
    manifest_candidates = list(RAW_AUDIO_VIDEO.glob("*manifest.csv"))
    if not manifest_candidates:
        log.error("No manifest CSV found in raw/audio_video -- run an extract_* script first.")
        return

    audio_files = list(RAW_AUDIO_VIDEO.glob("*.mp3"))
    log.info("Found %d audio files", len(audio_files))

    rows = []
    n_whisper_needed = 0

    for path in audio_files:
        try:
            y, sr = librosa.load(path, sr=TARGET_SR, mono=True)
        except Exception as e:
            log.warning("Could not load %s: %s", path, e)
            continue

        y = trim_and_normalize(y, sr)

        # check for a sibling caption/transcript file; else fall back to Whisper
        caption_path = path.with_suffix(".txt")
        if caption_path.exists():
            transcript = caption_path.read_text(encoding="utf-8")
            used_whisper = False
        else:
            transcript = whisper_transcribe(path)
            used_whisper = True
            n_whisper_needed += 1

        mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=N_MFCC)
        mfcc_mean = mfcc.mean(axis=1)  # collapse time axis -> fixed-length feature vector

        rows.append({
            "file": path.name,
            "duration_sec": len(y) / sr,
            "used_whisper": used_whisper,
            "transcript_len": len(transcript),
            **{f"mfcc_{i}": v for i, v in enumerate(mfcc_mean)},
        })

    df = pd.DataFrame(rows)
    if len(df) == 0:
        log.warning("No audio successfully processed — generating synthetic feature row for pipeline demo.")
        df = pd.DataFrame([{
            "file": "synthetic_demo.mp3",
            "duration_sec": 0.0,
            "used_whisper": False,
            "transcript_len": 0,
            **{f"mfcc_{i}": 0.0 for i in range(N_MFCC)},
        }])

    pct_whisper = 100 * n_whisper_needed / len(df)
    log.info("Whisper fallback used for %d / %d files (%.1f%%)", n_whisper_needed, len(df), pct_whisper)

    mfcc_cols = [c for c in df.columns if c.startswith("mfcc_")]
    if len(df) > 2:
        pca = PCA(n_components=2, random_state=42)
        coords = pca.fit_transform(df[mfcc_cols].fillna(0))
        df["pca_x"], df["pca_y"] = coords[:, 0], coords[:, 1]

    out_path = PROCESSED_DIR / "audio_processed.parquet"
    df.to_parquet(out_path, index=False)
    log.info("Saved processed audio features -> %s", out_path)


if __name__ == "__main__":
    main()
