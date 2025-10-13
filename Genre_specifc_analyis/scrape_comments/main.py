import os
import json
import pickle
from time import sleep
from typing import Dict, List, Tuple
from googleapiclient.errors import HttpError

import pandas as pd
from googleapiclient.discovery import build
import traceback
import time



# ----------------------------
# Config (adjust paths as needed)
# ----------------------------
ASSIGNMENTS_PATH = "../data/assignments_top50_youtube.pkl"            # input
OUTPUT_PARQUET   = "comments/final_comments.parquet"  # aggregated output (preferred)
OUTPUT_CSV       = None                               # set to "comments/final_comments.csv" if you also want CSV
STATE_PATH       = "comments/state.json"              # resume state
COMMENTS_DIR     = "comments/per_video"               # optional per-video dumps
GENRES_PER_RUN   = 500                                # "breakpoint" after this many genres per run


# ----------------------------
# Utilities: state management
# ----------------------------
def load_state(state_path: str) -> dict:
    if os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "completed_genres": [],              # list[str]
        "per_genre_last_index": {},          # dict[str, int]  index of last processed video for that genre
        "last_video": None                   # last successfully scraped video_id (global)
    }

def _is_rate_limit_error(e: HttpError) -> bool:
    """
    Return True if the HttpError indicates rate/quota limiting.
    Handles common YouTube reasons like quotaExceeded / rateLimitExceeded / userRateLimitExceeded.
    """
    try:
        if getattr(e, "resp", None) and e.resp.status in (403, 429):
            pass
        # Parse error payload
        payload = json.loads(e.content.decode("utf-8"))
        err = payload.get("error", {})
        # Check classic errors list
        for d in err.get("errors", []):
            reason = (d.get("reason") or "").lower()
            if reason in {"quotaexceeded", "ratelimitexceeded", "userratelimitexceeded", "dailylimitexceeded", "usagelimitsexceeded"}:
                return True
        # Fallback checks
        if err.get("code") in (403, 429):
            status = (err.get("status") or "").lower()
            if status in {"resource_exhausted", "permission_denied"}:
                return True
    except Exception:
        pass
    return False

def save_state(state_path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ----------------------------
# Load assignments (genre -> [(video_id, score), ...])
# ----------------------------
def load_assignments(path: str) -> Dict[str, List[Tuple[str, float]]]:
    with open(path, "rb") as f:
        return pickle.load(f)


# ----------------------------
# Scraping: newest 100 top-level comments for one video
# ----------------------------
def scrape_video_comments(api_key: str,
                          video_id: str,
                          genre: str = None,
                          max_total_comments: int = 100,
                          print_first: bool = False) -> tuple[pd.DataFrame, bool]:
    """
    Scrape newest top-level comments for a single YouTube video (up to max_total_comments)
    and return (DataFrame, rate_limited_flag).

    DataFrame columns (exact order):
    (commentId, authorChannelId, authorDisplayName, authorChannelUrl, publishedAt,
     updatedAt, likeCount, parentId, textDisplay, textOriginal, videoId, genre)
    """
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText",
        maxResults=100,
        order="time"
    )

    columns = [
        "commentId", "authorChannelId", "authorDisplayName", "authorChannelUrl",
        "publishedAt", "updatedAt", "likeCount", "parentId",
        "textDisplay", "textOriginal", "videoId", "genre"
    ]

    rows: List[dict] = []
    total = 0
    printed_first = False

    while request and total < max_total_comments:
        try:
            response = request.execute()
            items = response.get("items", [])

            if print_first and not printed_first and items:
                first = items[0]
                thread_snippet = first.get("snippet", {})
                top_comment = thread_snippet.get("topLevelComment", {})
                snip = top_comment.get("snippet", {})
                # we don't print; rows below will include all fields
                printed_first = True

            for item in items:
                if total >= max_total_comments:
                    break
                thread_snippet = item.get("snippet", {})
                top_comment = thread_snippet.get("topLevelComment", {})
                snip = top_comment.get("snippet", {})

                rows.append({
                    "commentId":         top_comment.get("id"),
                    "authorChannelId":   (snip.get("authorChannelId") or {}).get("value"),
                    "authorDisplayName": snip.get("authorDisplayName"),
                    "authorChannelUrl":  snip.get("authorChannelUrl"),
                    "publishedAt":       snip.get("publishedAt"),
                    "updatedAt":         snip.get("updatedAt"),
                    "likeCount":         snip.get("likeCount"),
                    "parentId":          None,
                    "textDisplay":       snip.get("textDisplay"),
                    "textOriginal":      snip.get("textOriginal"),
                    "videoId":           thread_snippet.get("videoId") or video_id,
                    "genre":             genre
                })
                total += 1

            if total < max_total_comments:
                request = youtube.commentThreads().list_next(request, response)
                sleep(1)

        except HttpError as e:
            # Stop the entire run if we hit rate/quota limits
            if _is_rate_limit_error(e):
                print(f"[{video_id}] Rate limit/quota reached. Halting run.")
                return pd.DataFrame(rows, columns=columns), True
            # Otherwise, just stop this video, continue outer loop
            print(f"[{video_id}] API error: {e}")
            print(traceback.format_exc())
            break

        except Exception as e:
            print(f"[{video_id}] Error: {e}")
            print(traceback.format_exc())
            break

    return pd.DataFrame(rows, columns=columns), False

# ----------------------------
# Append/save final DataFrame
# ----------------------------
def append_to_outputs(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)

    # Parquet (fast & compact)
    if os.path.exists(OUTPUT_PARQUET):
        existing = pd.read_parquet(OUTPUT_PARQUET)
        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_parquet(OUTPUT_PARQUET, index=False)
    else:
        df.to_parquet(OUTPUT_PARQUET, index=False)

    # Optional CSV mirror
    if OUTPUT_CSV:
        if os.path.exists(OUTPUT_CSV):
            df.to_csv(OUTPUT_CSV, mode="a", header=False, index=False, encoding="utf-8")
        else:
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")


# ----------------------------
# Main orchestration (with resume)
# ----------------------------
def run_scrape(api_key: str,
               already_scraped_genres: List[str] = None,
               max_comments_per_video: int = 100) -> None:
    """
    Same as before, but now stops the whole run if rate/quota limit is encountered.
    State is still updated for the current video before exiting.
    """
    already_scraped_genres = set(already_scraped_genres or [])
    assignments = load_assignments(ASSIGNMENTS_PATH)
    state = load_state(STATE_PATH)

    skip_genres = already_scraped_genres.union(set(state.get("completed_genres", [])))
    candidate_genres = [g for g in assignments.keys() if g not in skip_genres]

    if not candidate_genres:
        print("No remaining genres to process.")
        return

    start_time = time.time()
    videos_done = 0

    total_videos_in_run = 0
    for g in candidate_genres[:GENRES_PER_RUN]:
        video_list_full = [vid for (vid, _score) in assignments[g]]
        start_index_g = state.get("per_genre_last_index", {}).get(g, -1) + 1
        remaining_g = max(0, len(video_list_full) - start_index_g)
        total_videos_in_run += remaining_g

    if total_videos_in_run == 0:
        print("Nothing to do (all selected genres are already fully processed).")
        return

    processed_genres_this_run = 0

    for genre in candidate_genres:
        video_list = [vid for (vid, _score) in assignments[genre]]
        start_index = state.get("per_genre_last_index", {}).get(genre, -1) + 1

        print(f"== Genre: {genre} | videos: {len(video_list)} | resuming from index {start_index} ==")

        for idx in range(start_index, len(video_list)):
            video_id = video_list[idx]

            # First video of the run: keep print_first=True (we're not printing, just keeping parity)
            want_debug_first = (videos_done == 0)

            df_video, rate_limited = scrape_video_comments(
                api_key, video_id, genre,
                max_total_comments=max_comments_per_video,
                print_first=want_debug_first
            )

            if df_video is not None and not df_video.empty:
                os.makedirs(COMMENTS_DIR, exist_ok=True)
                per_video_csv = os.path.join(COMMENTS_DIR, f"{video_id}.csv")
                df_video.to_csv(per_video_csv, index=False, encoding="utf-8")
                append_to_outputs(df_video)
                print(f"Saved {len(df_video)} comments for {video_id} (genre={genre})")
            else:
                print(f"No comments fetched for {video_id} (genre={genre})")

            # Always update state so we don't retry this video
            state["last_video"] = video_id
            state.setdefault("per_genre_last_index", {})[genre] = idx
            save_state(STATE_PATH, state)

            # If rate-limited, stop the whole run immediately
            if rate_limited:
                print("Rate limit/quota reached. Stopping run now.")
                return

            # Progress + ETA logging
            videos_done += 1
            elapsed = time.time() - start_time
            avg_time_per_video = elapsed / max(1, videos_done)
            remaining_videos = max(0, total_videos_in_run - videos_done)
            eta_seconds = avg_time_per_video * remaining_videos
            eta_str = time.strftime("%H:%M:%S", time.gmtime(eta_seconds))
            print(
                f"[Progress] Genre: {genre} | Video {idx+1}/{len(video_list)} scraped "
                f"| Total {videos_done}/{total_videos_in_run} | ETA: {eta_str}"
            )

            time.sleep(0.5)

        state.setdefault("completed_genres", []).append(genre)
        state["completed_genres"] = sorted(set(state["completed_genres"]))
        save_state(STATE_PATH, state)
        print(f"-- Completed genre: {genre} --")

        processed_genres_this_run += 1
        if processed_genres_this_run >= GENRES_PER_RUN:
            print(f"Reached breakpoint after {GENRES_PER_RUN} genres. Exiting.")
            return

    print("All pending genres processed.")

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    API_KEY = os.getenv("API_KEY")
    already_scraped = []  # e.g., ["rock", "pop"]
    run_scrape(API_KEY, already_scraped_genres=already_scraped, max_comments_per_video=100)


