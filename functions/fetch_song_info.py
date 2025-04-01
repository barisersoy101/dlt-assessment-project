import dlt
from dlt.sources.rest_api import rest_api_resources

# DLT-compatible REST resource for iTunes metadata
# Uses the public Apple iTunes Lookup API:
def build_itunes_resource(ids: list[int]):
    ids_param = ",".join(str(id_) for id_ in ids)

    song_resource = rest_api_resources(
        {
            "client": {
                "base_url": "https://itunes.apple.com/lookup",
                "paginator": {
                    "type": "single_page",
                },
            },
            "resources": [
                {
                    "name": "itunes_songs",
                    "endpoint": {
                        "path": "?",
                        "params": {
                            "id": ids_param,
                        },
                        "data_selector": "results"
                    }
                }
            ]
        }
    )

    return song_resource


# This wraps build_itunes_resource() inside a @dlt.source
# so it can be used directly with pipeline.run(...)
@dlt.source
def itunes_source(ids: list[int]):
    return build_itunes_resource(ids)