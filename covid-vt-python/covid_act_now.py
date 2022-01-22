import requests
import os
import json
import psycopg

api_token = os.environ['COVID_ACT_NOW_API_TOKEN']

query = {'apiKey': api_token}
url = 'https://api.covidactnow.org/v2/county/VT.timeseries.json'

response = requests.get(url, params=query)

data = response.json()

conn = psycopg.connect(host='localhost', port=5432, dbname='covid_vt', user='postgres', password='djneosan')

with conn.cursor() as cur:
    cur.execute('truncate table raw_jsonb')
    for i in data:
        cur.execute('insert into raw_jsonb (covid_data) values (%s)', (json.dumps(i),))

conn.commit()
conn.close()