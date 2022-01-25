with base_jsonb as (
    select covid_data from {{ ref('base_jsonb') }}
),
array_elements as (
	select
		fips_id
		, jsonb_array_elements("metricsTimeseries") as jsonb_array_elements
	from (
		select
			covid_data ->> 'fips' as fips_id
			, covid_data -> 'metricsTimeseries' as "metricsTimeseries"
		from base_jsonb
	) as s
)
select
	fips_id
	, "date" as record_date
	, "testPositivityRatio" as test_positivity_ratio
	, "caseDensity" as case_density
	, "contactTracerCapacityRatio" as contact_tracer_capacity_ratio
	, "infectionRate" as infection_rate
	, "infectionRateCI90" as infection_rate_ci90
	, "icuCapacityRatio" as icu_capacity_ratio
	, "vaccinationsInitiatedRatio" as vaccinations_initiated_ratio
	, "vaccinationsCompletedRatio" as vaccinations_completed_ratio
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" date
	, "testPositivityRatio" real
	, "caseDensity" real
	, "contactTracerCapacityRatio" real
	, "infectionRate" real
	, "infectionRateCI90" real
	, "icuCapacityRatio" real
	, "vaccinationsInitiatedRatio" real
	, "vaccinationsCompletedRatio" real
)