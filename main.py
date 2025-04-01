import argparse
from utility_functions import SitemapProcessor

def main():
    # Argument parser to get max_songs and sitemap limit
    parser = argparse.ArgumentParser(description="Sitemap and song processing")
    parser.add_argument('--max_songs', type=int, default=1000, help="Maximum number of songs to process")
    parser.add_argument('--sitemap_limit', type=int, default=5, help="Limit for the number of sitemaps to process")
    args = parser.parse_args()

    processor = SitemapProcessor()  # Create an instance of the SitemapProcessor class

    # Step 1: Fetch and load sitemaps if not already loaded
    processor.fetch_and_load_sitemaps()

    # Step 2: Fetch sitemap data from DuckDB with the given limit
    sitemap_df = processor.fetch_sitemap_data(limit=args.sitemap_limit)

    if sitemap_df is None or sitemap_df.empty:
        print("No sitemaps found to process. Exiting.")
        return

    # Step 3: Fetch song data from sitemaps
    song_data_df = processor.fetch_song_data_from_sitemaps(sitemap_df)
    if song_data_df is None or song_data_df.empty:
        print("No song data found to process. Exiting.")
        return

    # Step 4: Process the song IDs in batches with the max_songs limit passed from command line
    processor.process_batches(song_data_df, max_songs=args.max_songs)

    # Step 5: Optional — fetch lyrics
    # try:
    #     fetch_and_store_lyrics()
    # except Exception as e:
    #     print(f"⚠️ Skipped lyrics due to error: {e}")

if __name__ == "__main__":
    main()