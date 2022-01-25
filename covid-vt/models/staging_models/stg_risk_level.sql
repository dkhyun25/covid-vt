with base_jsonb as (
    select covid_data from {{ ref('base_jsonb') }}
),
array_elements as (
	select
		fips_id
		, jsonb_array_elements("riskLevelsTimeseries") as jsonb_array_elements
	from (
		select
			covid_data ->> 'fips' as fips_id
			, covid_data -> 'riskLevelsTimeseries' as "riskLevelsTimeseries"
		from base_jsonb
	) as s
)
select
	fips_id
	, "date" as record_date
	, "overall"
	, "caseDensity" as case_density
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" date
	, "overall" integer
	, "caseDensity" integer
)