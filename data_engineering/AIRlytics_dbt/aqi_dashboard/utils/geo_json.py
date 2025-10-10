import json
import requests
import geojson
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

# Download from URL
geojson_url = "https://raw.githubusercontent.com/india-in-data/india-states-2019/master/india_states.geojson"
india_geo = requests.get(geojson_url).json()

# The states are duplicated in geojson data which confuses plotly
# This function merge duplicate states 
def merge_duplicate_features(geojson_obj, key='ST_NM'):
    from collections import defaultdict
    merged = {}
    grouped = defaultdict(list)

    for feature in geojson_obj["features"]:
        state_name = feature["properties"][key]
        grouped[state_name].append(feature)

    merged_features = []
    for state, features in grouped.items():
        if len(features) == 1:
            merged_features.append(features[0])
        else:
            geoms = [shape(f["geometry"]) for f in features]
            merged_geom = unary_union(geoms)
            merged_feature = geojson.Feature(
                geometry=mapping(merged_geom),
                properties=features[0]["properties"]
            )
            merged_features.append(merged_feature)

    return {
        "type": "FeatureCollection",
        "features": merged_features
    }

# Merge and overwrite india_geo
india_geo = merge_duplicate_features(india_geo)
# Save to local file
with open("./utils/india_states.geojson", "w") as f:
    json.dump(india_geo, f)