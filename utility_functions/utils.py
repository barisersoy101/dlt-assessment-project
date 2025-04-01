import time
import pandas as pd
import logging
from functions.fetch_sitemaps import fetch_sitemaps, get_sitemaps_from_duckdb
from functions.fetch_song_list import fetch_song_ids_from_sitemaps
from functions.fetch_song_info import itunes_source
from functions.pipeline_itunes_songs import pipeline
import duckdb

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SitemapProcessor:
    def __init__(self, db_name="itunes_song_info.duckdb"):
        self.db_name = db_name

    def fetch_and_load_sitemaps(self):
        """Fetch all sitemaps and load them into the database."""

        logging.info("Fetching and loading all sitemaps into the database.")
        pipeline.run(fetch_sitemaps())

    def fetch_sitemap_data(self, limit: int = 5):
        """Fetch sitemap data from DuckDB."""

        logging.info(f"Fetching {limit} sitemap links from DuckDB.")
        sitemap_df = get_sitemaps_from_duckdb().head(limit)
        return sitemap_df

    def fetch_song_data_from_sitemaps(self, sitemap_df):
        """Fetch song IDs based on sitemaps."""

        if sitemap_df.empty:
            logging.warning("No sitemaps found to process.")
            return None

        logging.info(f"Fetching song IDs for {len(sitemap_df)} sitemaps.")
        song_data_df = fetch_song_ids_from_sitemaps(sitemap_df, column_name="sitemap_links")
        return song_data_df

    def process_batches(self, song_data_df, batch_size=200, max_songs=1000):
        """Process the song IDs in batches with a maximum limit of songs to load."""

        song_ids = song_data_df["song_id"].astype(int).tolist()
        song_ids = song_ids[:max_songs]
        
        logging.info(f"Processing {len(song_ids)} song IDs in batches of {batch_size} with a max of {max_songs} songs.")

        for i in range(0, len(song_ids), batch_size):
            batch_ids = song_ids[i:i + batch_size]
            logging.info(f"Processing batch {i // batch_size + 1} with {len(batch_ids)} songs.")
            source = itunes_source(batch_ids)
            pipeline.run(source)
            time.sleep(2)  # Respect API limits