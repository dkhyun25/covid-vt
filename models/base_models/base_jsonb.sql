select covid_data
from {{ source('public', 'raw_jsonb') }}