import requests
import geopandas as gpd
import psycopg2
import json

url = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Counties/FeatureServer/0/query?where" \
      "=STATE_NAME%20%3D%20\'VERMONT\'&outFields=*&outSR=4326&f=geojson"

response = requests.get(url)

data = response.json()

# gdf = gpd.GeoDataFrame.from_features(data['features'])

conn = psycopg2.connect(host='', port=, dbname='', user='', password='')

with conn.cursor() as cur:
    cur.execute('truncate table raw_geojsonb')
    for i in data['features']:
        cur.execute('insert into raw_geojsonb (feature_collection) values (%s)', (json.dumps(i),))
    print('loading data complete')

conn.commit()
conn.close()
