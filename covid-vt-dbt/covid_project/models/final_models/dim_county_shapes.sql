with county as (
    select * from {{ ref('stg_county') }}
),
arcgis as (
    select * from {{ ref('stg_arcgis') }}
)
select
    county."fips_id"
	, county."lat"
	, county."long"
	, arcgis."gid"
	, arcgis."srid"
	, arcgis."geometry_object"
--	, arcgis."geometry_object_long"
--	, arcgis."geometry_object_lat"
    , arcgis."shape_area"
    , arcgis."shape_length"
    , arcgis."shape__area"
    , arcgis."shape__length"
from county
    join arcgis
        on county.fips_id = arcgis.fips_id