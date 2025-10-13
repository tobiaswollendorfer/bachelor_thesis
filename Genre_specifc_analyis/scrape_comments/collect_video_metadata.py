import os
import glob
import json
from typing import List, Dict, Set
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from time import sleep

# ----------------------------
# CONFIG
# ----------------------------
API_KEY = os.getenv("API_KEY")
CSV_FOLDER = "comments/per_video"
OUTPUT_CSV = "video_metadata.csv"
BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 0.5

# ----------------------------
# HELPERS
# ----------------------------
def _is_rate_limit_error(e: HttpError) -> bool:
    """Detect YouTube rate/quota limit errors."""
    try:
        if getattr(e, "resp", None) and e.resp.status in (403, 429):
            payload = json.loads(e.content.decode("utf-8"))
            err = payload.get("error", {})
            for d in err.get("errors", []):
                reason = (d.get("reason") or "").lower()
                if reason in {
                    "quotaexceeded", "ratelimitexceeded", "userratelimitexceeded",
                    "dailylimitexceeded", "usagelimitsexceeded"
                }:
                    return True
    except Exception:
        pass
    return False


def collect_video_ids(folder: str) -> Set[str]:
    """
    Read all .csv files in 'folder' and collect unique video IDs.
    Prefers a 'videoId' column; falls back to the filename stem.
    """
    video_ids: Set[str] = set()
    paths = sorted(glob.glob(os.path.join(folder, "*.csv")))
    if not paths:
        print(f"No CSV files found in: {folder}")
        return video_ids

    for path in paths:
        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception as e:
            print(f"Failed to read {path}: {e}")
            continue

        if "videoId" in df.columns:
            col = df["videoId"].dropna().astype(str).str.strip()
            video_ids.update(col[col != ""].tolist())
        else:
            # fallback: use filename stem as video id
            stem = os.path.splitext(os.path.basename(path))[0]
            if stem:
                video_ids.add(stem)

    print(f"Collected {len(video_ids)} unique video IDs.")
    return video_ids


def fetch_video_metadata(youtube, video_ids: List[str]) -> pd.DataFrame:
    """
    Fetch metadata for a list of video IDs using videos().list().
    Returns a DataFrame with one row per video found.
    """
    rows: List[Dict] = []

    # chunk into batches of 50
    for i in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[i:i + BATCH_SIZE]
        try:
            resp = youtube.videos().list(
                part="snippet,contentDetails,statistics,status",
                id=",".join(batch),
                maxResults=BATCH_SIZE
            ).execute()
        except HttpError as e:
            if _is_rate_limit_error(e):
                print("Rate limit/quota reached. Stopping.")
                break
            print(f"API error on batch {i//BATCH_SIZE + 1}: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error on batch {i//BATCH_SIZE + 1}: {e}")
            continue

        for item in resp.get("items", []):
            vid = item.get("id")
            snip = item.get("snippet", {}) or {}
            det  = item.get("contentDetails", {}) or {}
            stat = item.get("statistics", {}) or {}
            stat = {k: (int(v) if str(v).isdigit() else v) for k, v in stat.items()}
            st   = item.get("status", {}) or {}

            rows.append({
                # Core identity
                "videoId": vid,
                "title": snip.get("title"),
                "description": snip.get("description"),
                # Release / uploader
                "publishedAt": snip.get("publishedAt"),              # release (upload) date/time
                "channelId": snip.get("channelId"),
                "channelTitle": snip.get("channelTitle"),            # uploader (person/org)
                # Content info
                "categoryId": snip.get("categoryId"),
                "tags": "|".join(snip.get("tags", [])) if snip.get("tags") else None,
                "liveBroadcastContent": snip.get("liveBroadcastContent"),
                # Content details
                "duration": det.get("duration"),                     # ISO-8601, e.g. PT4M20S
                "dimension": det.get("dimension"),
                "definition": det.get("definition"),
                "caption": det.get("caption"),
                "licensedContent": det.get("licensedContent"),
                # Statistics (some fields may be missing)
                "viewCount": stat.get("viewCount"),
                "likeCount": stat.get("likeCount"),
                "commentCount": stat.get("commentCount"),
                "favoriteCount": stat.get("favoriteCount"),
                # Status flags
                "privacyStatus": st.get("privacyStatus"),
                "madeForKids": st.get("madeForKids"),
                "selfDeclaredMadeForKids": st.get("selfDeclaredMadeForKids"),
                "uploadStatus": st.get("uploadStatus"),
                "embeddable": st.get("embeddable"),
                "license": st.get("license"),
            })

        sleep(SLEEP_BETWEEN_CALLS)

    cols = [
        "videoId", "title", "description",
        "publishedAt", "channelId", "channelTitle",
        "categoryId", "tags", "liveBroadcastContent",
        "duration", "dimension", "definition", "caption", "licensedContent",
        "viewCount", "likeCount", "commentCount", "favoriteCount",
        "privacyStatus", "madeForKids", "selfDeclaredMadeForKids",
        "uploadStatus", "embeddable", "license",
    ]
    return pd.DataFrame(rows, columns=cols)


def main():
    # 1) Collect IDs
    ids = list(collect_video_ids(CSV_FOLDER))
    if not ids:
        return

    # 2) Build API client once
    youtube = build("youtube", "v3", developerKey=API_KEY, cache_discovery=False)

    # 3) Fetch metadata
    df_meta = fetch_video_metadata(youtube, ids)
    print(f"Metadata rows fetched: {len(df_meta)}")

    # 4) Save
    if not df_meta.empty:
        df_meta.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        print(f"Saved: {OUTPUT_CSV}")
        # quick peek
        print(df_meta.head(10).to_string(index=False))
    else:
        print("No metadata fetched (videos may be private/removed or rate-limited).")


if __name__ == "__main__":
    main()
