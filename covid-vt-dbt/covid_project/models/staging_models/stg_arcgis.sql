with base_geojsonb as (
    select feature_collection from {{ ref('base_geojsonb') }}
)
select
	ST_SRID(ST_GeomFromGeoJSON(feature_collection -> 'geometry')) as srid
	, ST_GeomFromGeoJSON(feature_collection -> 'geometry') as geometry_object
	, "FID" as fid
	, "FIPS" as fips_id
	, "NAME" as name
	, "SQMI" as square_miles
	, "ASIAN" as asian
	, "BLACK" as black
	, "MALES" as males
	, "OTHER" as other
	, "WHITE" as white
	, "VACANT" as vacant
	, "AGE_5_9" as age_5_9
	, "FEMALES" as females
	, "HAWN_PI" as hawaiian_pacific_islander
	, "MED_AGE" as median_age
--	, "POP2010"
	, "AMERI_ES" as native_american
	, "FAMILIES" as families
	, "HISPANIC" as hispanic
--	, "POP_SQMI"
	, "AGE_10_14" as age_10_14
	, "AGE_15_19" as age_15_19
	, "AGE_20_24" as age_20_24
	, "AGE_25_34" as age_25_34
	, "AGE_35_44" as age_35_44
	, "AGE_45_54" as age_45_54
	, "AGE_55_64" as age_55_64
	, "AGE_65_74" as age_65_74
	, "AGE_75_84" as age_75_84
	, "AGE_85_UP" as age_85_up
--	, "AVE_HH_SZ"
	, "CNTY_FIPS" as county_fips
--	, "FHH_CHILD"
--	, "HSE_UNITS"
--	, "MARHH_CHD"
	, "MED_AGE_F" as median_age_female
	, "MED_AGE_M" as median_age_male
--	, "MHH_CHILD"
--	, "MULT_RACE"
	, "OWNER_OCC" as owner_occupied
	, "AGE_UNDER5" as age_under_5
	, "AVE_FAM_SZ" as average_family_size
--	, "AVE_SALE12"
--	, "AVE_SALE17"
--	, "AVE_SIZE12"
--	, "AVE_SIZE17"
--	, "CROP_ACR12"
--	, "CROP_ACR17"
	, "HOUSEHOLDS" as households
--	, "HSEHLD_1_F"
--	, "HSEHLD_1_M"
--	, "MARHH_NO_C"
--	, "NO_FARMS12"
--	, "NO_FARMS17"
--	, "POP10_SQMI"
	, "POPULATION" as population
	, "RENTER_OCC" as renter_occupied
	, "STATE_FIPS" as state_fips
from base_geojsonb, jsonb_to_record(feature_collection -> 'properties') as x(
	"FID" integer
    , "FIPS" text
    , "NAME" real
    , "SQMI" integer
    , "ASIAN" integer
    , "BLACK" integer
    , "MALES" integer
    , "OTHER" integer
    , "WHITE" integer
    , "VACANT" integer
    , "AGE_5_9" integer
    , "FEMALES" integer
    , "HAWN_PI" real
    , "MED_AGE" integer
    , "POP2010" integer
    , "AMERI_ES" integer
    , "FAMILIES" integer
    , "HISPANIC" real
    , "POP_SQMI" integer
    , "AGE_10_14" integer
    , "AGE_15_19" integer
    , "AGE_20_24" integer
    , "AGE_25_34" integer
    , "AGE_35_44" integer
    , "AGE_45_54" integer
    , "AGE_55_64" integer
    , "AGE_65_74" integer
    , "AGE_75_84" integer
    , "AGE_85_UP" real
    , "AVE_HH_SZ" text
    , "CNTY_FIPS" integer
    , "FHH_CHILD" integer
    , "HSE_UNITS" integer
    , "MARHH_CHD" real
    , "MED_AGE_F" real
    , "MED_AGE_M" integer
    , "MHH_CHILD" integer
    , "MULT_RACE" integer
    , "OWNER_OCC" integer
    , "AGE_UNDER5" real
    , "AVE_FAM_SZ" integer
    , "AVE_SALE12" integer
    , "AVE_SALE17" integer
    , "AVE_SIZE12" integer
    , "AVE_SIZE17" integer
    , "CROP_ACR12" integer
    , "CROP_ACR17" integer
    , "HOUSEHOLDS" integer
    , "HSEHLD_1_F" integer
    , "HSEHLD_1_M" integer
    , "MARHH_NO_C" integer
    , "NO_FARMS12" integer
    , "NO_FARMS17" real
    , "POP10_SQMI" integer
    , "POPULATION" integer
    , "RENTER_OCC" text
    , "STATE_FIPS" text
    , "STATE_NAME" double precision
    , "Shape_Area" double precision
    , "Shape_Leng" double precision
    , "Shape__Area" double precision
)