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
)
select
    act.fips_id
    , act.new_cases
    , act.new_deaths
from actuals as act
