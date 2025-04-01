# DLT Music Data Pipeline

This project is a dlt pipeline (data loading tool) designed to fetch and process music-related data. The pipeline fetches song metadata, sitemaps, and lyrics, then stores them in a DuckDB-backed database. The pipeline is modular, with separate components for sitemap processing for both taking sitemaps of iTunes and from these sitemaps, taking song IDs, song metadata fetching with the Itunes API, and lyrics fetching.

## Project Structure

The project is organized as follows:

```
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
```

## Requirements

Before running the project, create the environment using the provided `environment.yaml`:

```bash
conda env create -f environment.yaml
conda activate dlt_project
```

## How It Works

### 1. **Sitemap Processing**
The `fetch_sitemaps.py` file contains a function `fetch_sitemaps` which fetches the sitemap XML files containing URLs for songs. These URLs are parsed, and the links are stored in a DuckDB database. The `SitemapProcessor` class in `utils.py` is responsible for fetching and processing these sitemaps and retrieving song data. 

***Note for Recruiter: In this part, @dlt.resource is used to insert data into the database in an orderly fashion. I couldn't find a way to directly query DuckDB in dlt, so I used a separate function to fetch information from the database. Additionally, this part uses dlt solely to push data into the database.This part includes two separate steps to actually analyze sitemaps and get song ids. 

### 2. **Song Metadata Fetching**
Song metadata is fetched from the Apple iTunes API. The `fetch_song_info.py` file provides functions for making requests to the iTunes Lookup API to fetch song metadata for a list of song IDs. The results are then stored in the DuckDB database.

***Note for Recruiter: In this section, I utilized dlt's Rest_API_Client to fetch data, format it, and then push it to the database with @dlt.source. This approach was much more straightforward because the metadata contained many columns, making it easier to work with.

### 3. **Lyrics Fetching**
The `fetch_lyrics.py` file fetches lyrics for songs using a public API (Lyrics.ovh) and stores them in the database. The process is part of the pipeline but can be optionally skipped or retried in case of errors.

***Note for Recruiter: I manually send requests for lyrics in this section. The code does not expose this part because it sends requests for each song to fetch lyrics. There's no clear documentation available for this API, but I used it as a free resource for fetching lyrics. It takes long time, you can try it if you want, but I don't want you to wait for 10 minutes. 

### 4. **Main Script**
The `main.py` file orchestrates the entire process by:
- Fetching sitemaps
- Retrieving song IDs from the sitemaps
- Fetching song metadata from iTunes
- Storing the data in the DuckDB database

The script uses argument parsing to specify the maximum number of songs to process and the sitemap limit.

### 5. **Pipeline Execution**
The `pipeline_itunes_songs.py` file defines the DLT pipeline that runs all tasks, ensuring data is fetched and stored correctly.

***** I put pipeline execution in one module so as not to write the whole thing every time I call it. 

### 6. **Utility Functions**
The `utility_functions/utils.py` file contains helper functions like `SitemapProcessor`, which abstracts the logic for fetching, processing, and storing sitemaps and song data in batches.

***General Note to Recruiter: I only used "write_disposition="merge" since there is no need for duplication of data. I want to keep a general database for songs in apple music for further analysis, and I can use this information to create analysis on behavior of musicians, album names and all other stuff. 
In addition, with incremental updates, we can keep this database updated for music information, but I would definitely need a server for that, and also incremental loading in dlt to keep sitemaps and song info up to date if any changes happen on them. Also, I could not find anything to parse sitemaps from webscraping, and maybe, it might be useful for dlt since Apple also uses it for it's AppStore to list apps inside it. 

## How to Run

1. **Setup the environment**:
   Install dependencies using the provided `environment.yaml` file:
   ```bash
   conda env create -f environment.yaml
   conda activate dlt_project
   ```

2. **Run the pipeline**:
   Run the `main.py` script to start the data processing:
   ```bash
   python main.py --max_songs 1000 --sitemap_limit 5
   ```

   - `--max_songs`: Maximum number of songs to process (default: 1000).
   - `--sitemap_limit`: Limit the number of sitemaps to process (default: 5).

3. **Optional**: You can manually fetch and store lyrics using the `fetch_and_store_lyrics()` function from `fetch_lyrics.py` but you need to activate relevant functions for that. 

## Dependencies

- Python 3.12
- DLT (Data Loading and Transformation)
- Pandas
- DuckDB
- Requests

## License

This project is licensed under the MIT License.

