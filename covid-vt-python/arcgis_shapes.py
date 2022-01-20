import requests
import os
import json
import psycopg
import urllib

params = {'where': 'STATE_NAME=Vermont',
		   'outFields': '*',
		   'returnGeometry': 'true',
		   'geometryPrecision':'',
		   'outSR': '',
		   'returnIdsOnly': 'false',
		   'returnCountOnly': 'false',
		   'orderByFields': '',
		   'groupByFieldsForStatistics': '',
		   'returnZ': 'false',
		   'returnM': 'false',
		   'returnDistinctValues': 'false',
		   'f': 'pjson'
		   }

encode_params = urllib.parse.urlencode(params).encode("utf-8")


url = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Counties/FeatureServer/0/query?where=STATE_NAME%20%3D%20'VERMONT'&outFields=*&outSR=4326&f=json"

response = requests.get(url)

data = response.json()