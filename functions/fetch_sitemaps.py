import dlt
import requests
import xml.etree.ElementTree as ET
import duckdb
import pandas as pd

# Fetch all sitemaps and save it to db with dlt.resource, used for both first-time and subsequent runs.
@dlt.resource(
    name="sitemap_links", 
    write_disposition="merge",
    primary_key="sitemap_links"
)
def fetch_sitemaps():
    url = "https://music.apple.com/sitemaps_music_index_song_1.xml"
    try:
        response = requests.get(url)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        # Loop through all the sitemaps and yield each sitemap's information
        for sitemap in root.findall("ns:sitemap", ns):
            loc = sitemap.find("ns:loc", ns).text
            lastmod = sitemap.find("ns:lastmod", ns).text
            yield {
                "sitemap_links": loc,
                "lastmod": lastmod
            }

    except Exception as e:
        print(f"Error in sitemap fetch: {e}")
        yield from []  # Ensure an empty response is returned in case of an error

#Fetches all sitemap data from the DuckDB and creates a DataFrame.
def get_sitemaps_from_duckdb() -> pd.DataFrame:
    con = duckdb.connect("itunes_song_info.duckdb")
    try:
        query = "SELECT * FROM itunes_songs_data.sitemap_links"
        sitemaps_df = con.execute(query).fetchdf()
        
        return sitemaps_df
    
    except Exception as e:
        print(f"Error fetching sitemaps from DuckDB: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error