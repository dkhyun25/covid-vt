select 
	"id" as gid
	, ST_SRID(ST_GeomFromGeoJSON(geometry)) as SRID
	, ST_AsText(ST_GeomFromGeoJSON(geometry)) as geometry_object
	, properties
into dim_county_shapes
from raw_geojsonb, jsonb_to_record(feature_collection) as x(
	"id" text
	, geometry jsonb
	, properties jsonb
)