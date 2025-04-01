import dlt

pipeline = dlt.pipeline(
    pipeline_name="itunes_pipeline",
    destination=dlt.destinations.duckdb("itunes_song_info.duckdb"),
    dataset_name="itunes_songs_data"
    )