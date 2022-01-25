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
from base_jsonb, jsonb_to_record(covid_data) as x(
	"fips" text
	, "country" text
	, "state" text
	, "county" text
	, "level" text
	, "lat" real
	, "locationId" text
	, "long" real
	, "population" integer
	, "lastUpdatedDate" date
	, "url" text
)