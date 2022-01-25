create type jsonb_record as (
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
);



select 
	"fips" as fips_id
	, "country"
	, "state"
	, "county"
	, "level"
	, "lat"
	, "locationId"
	, "long"
	, "population"
	, "cdcTransmissionLevel"
	, "lastUpdatedDate"
	, "url"
from 
	raw_jsonb
	, jsonb_populate_record(null::jsonb_record, raw_jsonb.covid_data) as jsonb_data
	
	


with array_elements as (
	select
		fips_id
		, jsonb_array_elements(metricstimeseries) as jsonb_array_elements
	from (
		select 
			data -> 'fips' as fips_id
			, data -> 'metricsTimeseries' as metricstimeseries
		from raw_jsonb
		where data ->> 'fips' = '50001'
	) as s
)
select
	fips_id
	, "date"
	, "testPositivityRatio"
	, "caseDensity"
	, "contactTracerCapacityRatio"
	, "infectionRate"
	, "infectionRateCI90"
	, "icuCapacityRatio"
	, "vaccinationsInitiatedRatio"
	, "vaccinationsCompletedRatio"
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" jsonb
	, "testPositivityRatio" jsonb
	, "caseDensity" jsonb
	, "contactTracerCapacityRatio" jsonb
	, "infectionRate" jsonb
	, "infectionRateCI90" jsonb
	, "icuCapacityRatio" jsonb
	, "vaccinationsInitiatedRatio" jsonb
	, "vaccinationsCompletedRatio" jsonb
)



with array_elements as (
	select
		fips_id
		, jsonb_array_elements(actualstimeseries) as jsonb_array_elements
	from (
		select 
			data -> 'fips' as fips_id
			, data -> 'actualsTimeseries' as actualstimeseries
		from raw_jsonb
		where data ->> 'fips' = '50001'
	) as s
)
select
	fips_id
	, "cases"
	, "deaths"
	, "positiveTests"
	, "negativeTests"
	, "contactTracers"
	, jsonb_array_elements -> 'hospitalBeds' -> 'capacity' as "hospitalBedsCapacity"
	, jsonb_array_elements -> 'hospitalBeds' -> 'currentUsageTotal' as "hospitalBedsCurrentUsageTotal"
	, jsonb_array_elements -> 'hospitalBeds' -> 'currentUsageCovid' as "hospitalBedsCurrentUsageCovid"
	, "newCases"
	, "newDeaths"
	, "vaccinesDistributed"
	, "vaccinationsInitiated"
	, "vaccinationsCompleted"
	, "vaccinesAdministered"
	, "date"
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" jsonb
	, "cases" jsonb
	, "deaths" jsonb
	, "positiveTests" jsonb
	, "negativeTests" jsonb
	, "contactTracers" jsonb
	, "hospitalBeds" jsonb
	, "newCases" jsonb
	, "newDeaths" jsonb
	, "vaccinesDistributed" jsonb
	, "vaccinationsInitiated" jsonb
	, "vaccinationsCompleted" jsonb
	, "vaccinesAdministered" jsonb
)




with array_elements as (
	select
		fips_id
		, jsonb_array_elements(risklevelstimeseries) as jsonb_array_elements
	from (
		select 
			data -> 'fips' as fips_id
			, data -> 'riskLevelsTimeseries' as risklevelstimeseries
		from raw_jsonb
		where data ->> 'fips' = '50001'
	) as s
)
select
	fips_id
	, "overall"
	, "caseDensity"
	, "date"
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" jsonb
	, "overall" jsonb
	, "caseDensity" jsonb
)





with array_elements as (
	select
		fips_id
		, jsonb_array_elements(cdctransmissionleveltimeseries) as jsonb_array_elements
	from (
		select 
			data -> 'fips' as fips_id
			, data -> 'cdcTransmissionLevelTimeseries' as  cdctransmissionleveltimeseries
		from raw_jsonb
		where data ->> 'fips' = '50001'
	) as s
)
select
	fips_id
	, "cdcTransmissionLevel"
	, "date"
from array_elements, jsonb_to_record(jsonb_array_elements) as x(
	"date" date
	, "cdcTransmissionLevel" jsonb
)
	