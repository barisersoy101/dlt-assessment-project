DLT Music Data Pipeline
This project is a Data Loading and Transformation (DLT) pipeline designed to fetch and process music-related data. The pipeline fetches song metadata, sitemaps, and lyrics, then stores them in a DuckDB-backed database. The pipeline is modular, with separate components for sitemap processing, song metadata fetching, and lyrics fetching.

Project Structure
The project is organized as follows:

graphql
DLT_ITUNES_SONGS/
│
├── functions/
│   ├── __init__.py           # Imports necessary functions for the pipeline
│   ├── fetch_lyrics.py       # Lyrics fetching functionality
│   ├── fetch_sitemaps.py     # Fetches sitemap URLs and parses them
│   ├── fetch_song_info.py    # Fetches song metadata from iTunes API
│   ├── fetch_song_list.py    # Extracts song IDs from sitemaps
│   └── pipeline_itunes_songs.py  # Defines the DLT pipeline
│
├── utility_functions/
│   ├── __init__.py           # Imports the SitemapProcessor class
│   ├── utils.py              # Contains SitemapProcessor class and logic for processing data
│
├── .gitignore                # Specifies files/folders to ignore in version control
├── environment.yaml          # Conda environment file for dependencies
├── itunes_song_info.duckdb   # DuckDB database for storing song and sitemap data
├── main.py                   # Main script that orchestrates the pipeline
└── __init__.py               # Initializes the package and imports necessary modules
Requirements
Before running the project, create the environment using the provided environment.yaml:

bash
conda env create -f environment.yaml
conda activate dlt_project
How It Works
1. Sitemap Processing
The fetch_sitemaps.py file contains a function fetch_sitemaps which fetches the sitemap XML files containing URLs for songs. These URLs are parsed, and the links are stored in a DuckDB database. The SitemapProcessor class in utils.py is responsible for fetching and processing these sitemaps and retrieving song data.

2. Song Metadata Fetching
Song metadata is fetched from the Apple iTunes API. The fetch_song_info.py file provides functions for making requests to the iTunes Lookup API to fetch song metadata for a list of song IDs. The results are then stored in the DuckDB database.

3. Lyrics Fetching
The fetch_lyrics.py file fetches lyrics for songs using a public API (Lyrics.ovh) and stores them in the database. The process is part of the pipeline but can be optionally skipped or retried in case of errors.

4. Main Script
The main.py file orchestrates the entire process by:

Fetching sitemaps

Retrieving song IDs from the sitemaps

Fetching song metadata from iTunes

Storing the data in the DuckDB database

The script uses argument parsing to specify the maximum number of songs to process and the sitemap limit.

5. Pipeline Execution
The pipeline_itunes_songs.py file defines the DLT pipeline that runs all tasks, ensuring data is fetched and stored correctly.

6. Utility Functions
The utility_functions/utils.py file contains helper functions like SitemapProcessor, which abstracts the logic for fetching, processing, and storing sitemaps and song data in batches.

How to Run
Setup the environment: Install dependencies using the provided environment.yaml file:

bash
conda env create -f environment.yaml
conda activate dlt_project
Run the pipeline: Run the main.py script to start the data processing:

bash
python main.py --max_songs 1000 --sitemap_limit 5
--max_songs: Maximum number of songs to process (default: 1000).

--sitemap_limit: Limit the number of sitemaps to process (default: 5).

Optional: You can manually fetch and store lyrics using the fetch_and_store_lyrics() function from fetch_lyrics.py.

Dependencies
Python 3.12

DLT (Data Loading and Transformation)

Pandas

DuckDB

Requests

License
This project is licensed under the MIT License.
