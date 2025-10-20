Main dataset:
data/final_dataset_english_only.csv – contains all English comments with metadata (genre, video ID, likes, views, etc.) and computed VADER and LIWC scores.

Results:
stats/ – contains the individual analysis results and aggregated statistics per genre (average values, polarity, polarization, and JSD scores).

Scraping code:
scrape_comments/ – includes the code used to scrape YouTube comments and metadata via the YouTube Data API.

Genre assignment code:
assign_songs_to_genre/ – includes the code for assigning songs to genres based on the Onion dataset.
