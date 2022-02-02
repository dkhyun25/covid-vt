with actuals as (
    select
        fips_id
        , record_date
        , cases
        , deaths
        , positive_tests
        , negative_tests
        , contact_tracers
        , hospital_beds_capacity
        , hospital_beds_current_usage_total
        , hospital_beds_current_usage_covid
        , icu_beds_capacity
        , icu_beds_current_usage_total
        , icu_beds_current_usage_covid
        , new_cases
        , new_deaths
        , vaccines_distributed
        , vaccinations_initiated
        , vaccinations_completed
        , vaccines_administered
    from {{ ref('stg_actuals_series') }}
),
metrics as (
    select
        fips_id
        , record_date
        , test_positivity_ratio
        , case_density
        , contact_tracer_capacity_ratio
        , infection_rate
        , infection_rate_ci90
        , icu_capacity_ratio
        , vaccinations_initiated_ratio
        , vaccinations_completed_ratio
    from {{ ref('stg_metrics_series') }}
),
cdc_transmission as (
    select
        fips_id
        , record_date
        , cdc_transmission_level
    from {{ ref('stg_cdc_transmission_level') }}
),
risk as (
    select
        fips_id
        , record_date
        , overall
        , case_density
    from {{ ref('stg_risk_level') }}
)
select
    act.fips_id
    , act.record_date
    , act.cases
    , act.deaths
    , act.positive_tests
    , act.negative_tests
    , act.contact_tracers
    , act.hospital_beds_capacity
    , act.hospital_beds_current_usage_total
    , act.hospital_beds_current_usage_covid
    , act.icu_beds_capacity
    , act.icu_beds_current_usage_total
    , act.icu_beds_current_usage_covid
    , act.vaccines_distributed
    , act.vaccinations_initiated
    , act.vaccinations_completed
    , act.vaccines_administered
    , met.test_positivity_ratio
    , met.case_density
    , met.contact_tracer_capacity_ratio
    , met.infection_rate
    , met.infection_rate_ci90
    , met.icu_capacity_ratio
    , met.vaccinations_initiated_ratio
    , met.vaccinations_completed_ratio
	, cdc.cdc_transmission_level
    , risk.overall as risk_overall
    , risk.case_density as risk_case_density
from actuals as act
    join metrics as met
        on act.fips_id = met.fips_id
        and act.record_date = met.record_date
    join cdc_transmission as cdc
        on act.fips_id = cdc.fips_id
        and act.record_date = cdc.record_date
    join risk as risk
        on act.fips_id = risk.fips_id
        and act.record_date = risk.record_date
