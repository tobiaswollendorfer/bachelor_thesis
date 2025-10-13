from googleapiclient.discovery import build
import pandas as pd
from time import sleep
import traceback

def get_comments(api_key, video_id, max_total_comments=100):
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        textFormat="plainText",
        maxResults=100  # fetch as much as possible per call
    )

    df = pd.DataFrame(columns=['comment', 'date', 'user_name'])

    total_collected = 0

    while request and total_collected < max_total_comments:
        comments = []
        dates = []
        user_names = []

        try:
            response = request.execute()
            items = response.get('items', [])

            for item in items:
                if total_collected >= max_total_comments:
                    break

                snippet = item['snippet']['topLevelComment']['snippet']
                comment = snippet.get('textDisplay', '')
                user_name = snippet.get('authorDisplayName', '')
                date = snippet.get('publishedAt', '')

                comments.append(comment)
                user_names.append(user_name)
                dates.append(date)

                total_collected += 1

            df2 = pd.DataFrame({
                "comment": comments,
                "user_name": user_names,
                "date": dates
            })

            df = pd.concat([df, df2], ignore_index=True)
            print(f"Collected {total_collected} / {max_total_comments} comments")

            # Save progress
            df.to_csv(f"comments/{video_id}_top_level_comments.csv", index=False, encoding='utf-8')
            # Get next page
            if total_collected < max_total_comments:
                request = youtube.commentThreads().list_next(request, response)
                sleep(1)
        except Exception as e:
            print("Error:", str(e))
            print(traceback.format_exc())
            print("Sleeping 10 seconds and saving progress...")
            df.to_csv(f"comments/{video_id}_top_level_comments.csv", index=False, encoding='utf-8')
            sleep(10)
            break

def main():
    api_key = "AIzaSyAE9jh3z_JO2jOxFXTAR1NF6WmTxx3WRAA"
    video_id = "fXivMSJm_kA"
    get_comments(api_key, video_id, max_total_comments=100)


if __name__ == "__main__":
    main()
