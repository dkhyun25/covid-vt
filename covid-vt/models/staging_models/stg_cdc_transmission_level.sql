with base_jsonb as (
    select covid_data from {{ ref('base_jsonb') }}
),
array_elements as (
	select
		fips_id
		, jsonb_array_elements("cdcTransmissionLevelTimeseries") as jsonb_array_elements
	from (
		select
			covid_data ->> 'fips' as fips_id
			, covid_data -> 'cdcTransmissionLevelTimeseries' as "cdcTransmissionLevelTimeseries"
		from base_jsonb
	) as s
)
select
	fips_id
	, "date" as record_date
	, "cdcTransmissionLevel" as cdc_transmission_level
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" date
	, "cdcTransmissionLevel" integer
)