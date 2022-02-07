with base_jsonb as (
    select covid_data from {{ ref('base_jsonb') }}
),
array_elements as (
	select
		fips_id
		, jsonb_array_elements("actualsTimeseries") as jsonb_array_elements
	from (
		select
			covid_data ->> 'fips' as fips_id
			, covid_data -> 'actualsTimeseries' as "actualsTimeseries"
		from base_jsonb
	) as s
)
select
	fips_id
	, "cases"
	, "deaths"
	, "positiveTests" as positive_tests
	, "negativeTests" as negative_tests
	, "contactTracers" as contact_tracers
	, (jsonb_array_elements -> 'hospitalBeds' ->> 'capacity')::integer as hospital_beds_capacity
	, (jsonb_array_elements -> 'hospitalBeds' ->> 'currentUsageTotal')::integer as hospital_beds_current_usage_total
	, (jsonb_array_elements -> 'hospitalBeds' ->> 'currentUsageCovid')::integer as hospital_beds_current_usage_covid
	, (jsonb_array_elements -> 'icuBeds' ->> 'capacity')::integer as icu_beds_capacity
	, (jsonb_array_elements -> 'icuBeds' ->> 'currentUsageTotal')::integer as icu_beds_current_usage_total
	, (jsonb_array_elements -> 'icuBeds' ->> 'currentUsageCovid')::integer as icu_beds_current_usage_covid
	, "newCases" as new_cases
	, "newDeaths" as new_deaths
	, "vaccinesDistributed" as vaccines_distributed
	, "vaccinationsInitiated" as vaccinations_initiated
	, "vaccinationsCompleted" as vaccinations_completed
	, "vaccinationsAdditionalDose" as vaccinations_additional_dose
	, "vaccinesAdministered" as vaccines_administered
	, "date" as record_date
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"cases" integer
	, "deaths" integer
	, "positiveTests" integer
	, "negativeTests" integer
	, "contactTracers" integer
	, "newCases" integer
	, "newDeaths" integer
	, "vaccinesDistributed" integer
	, "vaccinationsInitiated" integer
	, "vaccinationsCompleted" integer
	, "vaccinationsAdditionalDose" integer
	, "vaccinesAdministered" integer
	, "date" date
)