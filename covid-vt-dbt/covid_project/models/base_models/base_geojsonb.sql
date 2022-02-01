select feature_collection
from {{ source('public', 'raw_geojsonb') }}