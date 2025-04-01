from dlt.sources.helpers import requests
from functions.pipeline_itunes_songs import pipeline
import duckdb
import time

def fetch_and_store_lyrics() -> None:
    """
    Fetch lyrics for adding a new column to db using pipeline.run (Just wanted to try it)
    """
    con = duckdb.connect("itunes_song_info.duckdb")
    df = con.execute("""
        SELECT track_id, artist_name, track_name
        FROM itunes_songs_data.itunes_songs
    """).fetchdf()

    for _, row in df.iterrows():
        track_id = row["track_id"]
        artist_name = row["artist_name"]
        track_name = row["track_name"]

        try:
            url = f"https://api.lyrics.ovh/v1/{artist_name}/{track_name}"
            response = requests.get(url)
            response.raise_for_status()
            lyrics = response.json().get("lyrics", None)

            data = [{"track_id": track_id, "lyrics": lyrics}]
            pipeline.run(data, table_name="itunes_songs", write_disposition="merge", primary_key="track_id")

            time.sleep(0.5)

        except Exception:
            continue  # silently skip any errors since this is a complementary part to add a new column. 