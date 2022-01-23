with base_jsonb as (
    select covid_data from {{ ref('base_jsonb') }}
)
select
	"fips" as fips_id
	, "country"
	, "state"
	, "county"
	, "level"
	, "lat"
	, "locationId" as location_id
	, "long"
	, "population"
	, "lastUpdatedDate" as last_updated_date
	, "url"
from
	base_jsonb
	, jsonb_populate_record(null::jsonb_record, base_jsonb.covid_data) as jsonb_data
