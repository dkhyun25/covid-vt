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
    , arcgis."sqmi"
    , arcgis."asian"
    , arcgis."black"
    , arcgis."males"
    , arcgis."other"
    , arcgis."white"
    , arcgis."vacant"
    , arcgis."age_5_9"
    , arcgis."females"
    , arcgis."hawn_pi"
    , arcgis."med_age"
    , arcgis."pop2010"
    , arcgis."ameri_es"
    , arcgis."families"
    , arcgis."hispanic"
    , arcgis."pop_sqmi"
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
    , arcgis."ave_hh_sz"
    , arcgis."fhh_child"
    , arcgis."hse_units"
    , arcgis."marhh_chd"
    , arcgis."med_age_f"
    , arcgis."med_age_m"
    , arcgis."mhh_child"
    , arcgis."mult_race"
    , arcgis."owner_occ"
    , arcgis."age_under5"
    , arcgis."ave_fam_sz"
    , arcgis."ave_sale12"
    , arcgis."ave_sale17"
    , arcgis."ave_size12"
    , arcgis."ave_size17"
    , arcgis."crop_acr12"
    , arcgis."crop_acr17"
    , arcgis."households"
    , arcgis."hsehld_1_f"
    , arcgis."hsehld_1_m"
    , arcgis."marhh_no_c"
    , arcgis."no_farms12"
    , arcgis."no_farms17"
    , arcgis."pop10_sqmi"
    , arcgis."population"
    , arcgis."renter_occ"
from county
    join arcgis
        on county.fips_id = arcgis.fips_id