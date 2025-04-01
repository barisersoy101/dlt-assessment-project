import requests 
import xml.etree.ElementTree as ET
import pandas as pd
import gzip
import io
from functions.pipeline_itunes_songs import pipeline

# Parses each sitemap URL to extract song IDs from <loc> tags and writes them to DuckDB via DLT using song_id as the primary key
def fetch_song_ids_from_sitemaps(df: pd.DataFrame, column_name: str) -> pd.DataFrame:

    all_song_data = []

    for index, row in df.iterrows():
        sitemap_url = row[column_name]
        
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status() 
            
            if not response.content:
                print(f"No content returned from {sitemap_url}")
                continue
            
            content_encoding = response.headers.get('Content-Encoding')
            
            if 'gzip' in (content_encoding or '').lower():
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                    xml_content = gz.read()
            else:
                xml_content = response.content
            
            root_second_xml = ET.fromstring(xml_content)

            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            for url in root_second_xml.findall("ns:url", namespace):
                loc = url.find("ns:loc", namespace).text
                song_id = loc.split("/")[-1] 
                all_song_data.append({"song_id": song_id, "sitemap_url": sitemap_url})
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {sitemap_url}: {e}")
        except ET.ParseError:
            print(f"Error parsing XML from {sitemap_url}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    song_df = pd.DataFrame(all_song_data)
    song_df = song_df.drop_duplicates()
    data = song_df.to_dict(orient="records")

    load_info = pipeline.run(data, table_name="itunes_song_list", write_disposition="merge", primary_key="song_id")

    print(load_info)

    return song_df