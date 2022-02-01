with base_geojsonb as (
    select feature_collection from {{ ref('base_geojsonb') }}
)
select
	feature_collection -> 'id' as gid
	, feature_collection -> 'properties' ->> 'FIPS' as fips_id
	, ST_SRID(ST_GeomFromGeoJSON(feature_collection -> 'geometry')) as srid
	, ST_AsText(ST_GeomFromGeoJSON(feature_collection -> 'geometry')) as geometry_object
	, "SQMI" as sqmi
	, "ASIAN" as asian
	, "BLACK" as black
	, "MALES" as males
	, "OTHER" as other
	, "WHITE"  as white
	, "VACANT" as vacant
	, "AGE_5_9" as age_5_9
	, "FEMALES" as females
	, "HAWN_PI" as hawn_pi
	, "MED_AGE" as med_age
	, "POP2010" as pop2010
	, "AMERI_ES" as ameri_es
	, "FAMILIES" as families
	, "HISPANIC" as hispanic
	, "POP_SQMI" as pop_sqmi
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
	, "AVE_HH_SZ" as ave_hh_sz
	, "FHH_CHILD" as fhh_child
	, "HSE_UNITS" as hse_units
	, "MARHH_CHD" as marhh_chd
	, "MED_AGE_F" as med_age_f
	, "MED_AGE_M" as med_age_m
	, "MHH_CHILD" as mhh_child
	, "MULT_RACE" as mult_race
	, "OWNER_OCC" as owner_occ
	, "AGE_UNDER5" as age_under5
	, "AVE_FAM_SZ" as ave_fam_sz
	, "AVE_SALE12" as ave_sale12
	, "AVE_SALE17" as ave_sale17
	, "AVE_SIZE12" as ave_size12
	, "AVE_SIZE17" as ave_size17
	, "CROP_ACR12" as crop_acr12
	, "CROP_ACR17" as crop_acr17
	, "HOUSEHOLDS" as households
	, "HSEHLD_1_F" as hsehld_1_f
	, "HSEHLD_1_M" as hsehld_1_m
	, "MARHH_NO_C" as marhh_no_c
	, "NO_FARMS12" as no_farms12
	, "NO_FARMS17" as no_farms17
	, "POP10_SQMI" as pop10_sqmi
	, "POPULATION" as population
	, "RENTER_OCC" as renter_occ
	, "Shape_Area" as shape_area
	, "Shape_Leng" as shape_length
	, "Shape__Area" as shape__area
	, "Shape__Length" as shape__length
from base_geojsonb, jsonb_to_record(feature_collection -> 'properties') as x(
	"FID" integer
	, "FIPS" text
	, "NAME" text
	, "SQMI" real
	, "ASIAN" integer
	, "BLACK" integer
	, "MALES" integer
	, "OTHER" integer
	, "WHITE" integer
	, "VACANT" integer
	, "AGE_5_9" integer
	, "FEMALES" integer
	, "HAWN_PI" integer
	, "MED_AGE" real
	, "POP2010" integer
	, "AMERI_ES" integer
	, "FAMILIES" integer
	, "HISPANIC" integer
	, "POP_SQMI" real
	, "AGE_10_14" integer
	, "AGE_15_19" integer
	, "AGE_20_24" integer
	, "AGE_25_34" integer
	, "AGE_35_44" integer
	, "AGE_45_54" integer
	, "AGE_55_64" integer
	, "AGE_65_74" integer
	, "AGE_75_84" integer
	, "AGE_85_UP" integer
	, "AVE_HH_SZ" real
	, "CNTY_FIPS" text
	, "FHH_CHILD" integer
	, "HSE_UNITS" integer
	, "MARHH_CHD" integer
	, "MED_AGE_F" real
	, "MED_AGE_M" real
	, "MHH_CHILD" integer
	, "MULT_RACE" integer
	, "OWNER_OCC" integer
	, "AGE_UNDER5" integer
	, "AVE_FAM_SZ" real
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
	, "NO_FARMS17" integer
	, "POP10_SQMI" real
	, "POPULATION" integer
	, "RENTER_OCC" integer
	, "STATE_FIPS" text
	, "STATE_NAME" text
	, "Shape_Area" double precision
	, "Shape_Leng" double precision
	, "Shape__Area" double precision
	, "Shape__Length" double precision
)