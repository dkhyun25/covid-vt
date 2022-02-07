with county as (
    select * from {{ ref('stg_county') }}
),
arcgis as (
    select * from {{ ref('stg_arcgis') }}
)
select
	county."fips_id"
	, county."country"
	, county."state"
	, county."county"
	, county."level"
	, county."location_id"
	, county."last_updated_date"
    , arcgis."name"
    , arcgis."square_miles"
    , arcgis."asian"
    , arcgis."black"
    , arcgis."males"
    , arcgis."other"
    , arcgis."white"
    , arcgis."vacant"
    , arcgis."age_5_9"
    , arcgis."females"
    , arcgis."hawaiian_pacific_islander"
    , arcgis."median_age"
    , arcgis."native_american"
    , arcgis."families"
    , arcgis."hispanic"
    , arcgis."age_10_14"
    , arcgis."age_15_19"
    , arcgis."age_20_24"
    , arcgis."age_25_34"
    , arcgis."age_35_44"
    , arcgis."age_45_54"
    , arcgis."age_55_64"
    , arcgis."age_65_74"
    , arcgis."age_75_84"
    , arcgis."age_85_up"
    , arcgis."county_fips"
    , arcgis."median_age_female"
    , arcgis."median_age_male"
    , arcgis."owner_occupied"
    , arcgis."age_under_5"
    , arcgis."average_family_size"
    , arcgis."households"
    , arcgis."population"
    , arcgis."renter_occupied"
    , arcgis."state_fips"
from county
    join arcgis
        on county.fips_id = arcgis.fips_id