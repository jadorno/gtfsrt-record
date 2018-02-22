import requests
import json
import time
import sys
import os

from google.protobuf.message import DecodeError
from google.protobuf.json_format import MessageToJson
sys.path.insert(0, 'protobuf')
import gtfs_realtime_pb2 as gtfsrt

PB_OUTPUT_DIR = "data"

config = {}

if os.environ.get('API_URL') is not None:
	req = requests.get(os.environ.get('API_URL')+"/status/gtfsrt-config").json()
	config['DB_URL'] = os.environ.get('DB_URL', req['DB_URL'])
	config['DB_UPLOAD'] = os.environ.get('DB_UPLOAD', req['DB_UPLOAD'])
	config['PB_DOWNLOAD'] = os.environ.get('PB_DOWNLOAD', req['PB_DOWNLOAD'])
	config['PB_PATH'] = os.environ.get('PB_PATH', PB_OUTPUT_DIR)
	config['SLEEP_TIME'] = os.environ.get('SLEEP_TIME', req['SLEEP_TIME'])
	config['SLEEP_ADAPTIVE'] = os.environ.get('SLEEP_ADAPTIVE', req['SLEEP_ADAPTIVE'])
	config['URL_TRIP_UPDATES_ENABLED'] = os.environ.get('URL_TRIP_UPDATES_ENABLED', req['URL_TRIP_UPDATES_ENABLED'])
	config['URL_TRIP_UPDATES'] = os.environ.get('URL_TRIP_UPDATES', req['URL_TRIP_UPDATES'])
	config['URL_VEHICLE_POSITIONS_ENABLED'] = os.environ.get('URL_VEHICLE_POSITIONS_ENABLED', req['URL_VEHICLE_POSITIONS_ENABLED'])
	config['URL_VEHICLE_POSITIONS'] = os.environ.get('URL_VEHICLE_POSITIONS', req['URL_VEHICLE_POSITIONS'])
	config['URL_ALERTS_ENABLED'] = os.environ.get('URL_ALERTS_ENABLED', req['URL_ALERTS_ENABLED'])
	config['URL_ALERTS'] = os.environ.get('URL_ALERTS', req['URL_ALERTS'])
else:
	config['DB_URL'] = os.environ.get('DB_URL')
	config['DB_UPLOAD'] = os.environ.get('DB_UPLOAD')
	config['PB_DOWNLOAD'] = os.environ.get('PB_DOWNLOAD')
	config['PB_PATH'] = os.environ.get('PB_PATH', PB_OUTPUT_DIR)
	config['SLEEP_TIME'] = os.environ.get('SLEEP_TIME')
	config['SLEEP_ADAPTIVE'] = os.environ.get('SLEEP_ADAPTIVE')
	config['URL_TRIP_UPDATES_ENABLED'] = os.environ.get('URL_TRIP_UPDATES_ENABLED')
	config['URL_TRIP_UPDATES'] = os.environ.get('URL_TRIP_UPDATES')
	config['URL_VEHICLE_POSITIONS_ENABLED'] = os.environ.get('URL_VEHICLE_POSITIONS_ENABLED')
	config['URL_VEHICLE_POSITIONS'] = os.environ.get('URL_VEHICLE_POSITIONS')
	config['URL_ALERTS_ENABLED'] = os.environ.get('URL_ALERTS_ENABLED')
	config['URL_ALERTS'] = os.environ.get('URL_ALERTS')

if not config['DB_UPLOAD'] == 'true' and not config['PB_DOWNLOAD'] == 'true':
	print("You have Protobuffer Download and Database Upload Disabled.")
	print("The program doesn't do anything else.")
	exit()

sleep_time = int(config['SLEEP_TIME'])
gtfsrt_enabled = []
gtfsrt_url = {}
if config['URL_TRIP_UPDATES_ENABLED'] == 'true':
	gtfsrt_enabled.append('trip_updates')
	gtfsrt_url['trip_updates'] = config['URL_TRIP_UPDATES']
if config['URL_VEHICLE_POSITIONS_ENABLED'] == 'true':
	gtfsrt_enabled.append('vehicle_positions')
	gtfsrt_url['vehicle_positions'] = config['URL_VEHICLE_POSITIONS']
if config['URL_ALERTS_ENABLED'] == 'true':
	gtfsrt_enabled.append('alerts')
	gtfsrt_url['alerts'] = config['URL_ALERTS']

if config['DB_UPLOAD'] == 'true':
	import pymongo

	client = pymongo.MongoClient(config['DB_URL'])
	db = client.get_database()

	for table_name in gtfsrt_enabled:
		timestamp_index_exist = False
		if table_name in db.collection_names():
			index_info = db[table_name].index_information()
			for index in index_info:
				for key in index_info[index]['key']:
					for field in key:
						if field == "header.timestamp":
							timestamp_index_exist = True
		if not timestamp_index_exist:
			print("No Timestamp Index Located for "+table_name+". Creating...")
			db[table_name].create_index(
				[("header.timestamp", pymongo.DESCENDING)],
				unique=True,background=True
			)

while True:

	increase_sleep = False

	for table_name in gtfsrt_enabled:

		try:

			r = requests.get(gtfsrt_url[table_name], timeout=10)
			fm = gtfsrt.FeedMessage()
			fm.ParseFromString(r.content)
			data = json.loads(MessageToJson(fm))

			if config['PB_DOWNLOAD'] == 'true':
				outputFile = config['PB_PATH']+'/'+data['header']['timestamp']+"_"+table_name+".pb"
				if os.path.isfile(outputFile):
					print(str(data['header']['timestamp']),"- Protobuf File Duplicate on "+outputFile)
					increase_sleep = True				
				f = open(outputFile, 'wb')
				f.write(r.content)
				f.close()
				print(data['header']['timestamp'],"- Protobuf File Written on "+outputFile)

			if config['DB_UPLOAD'] == 'true':
				data['header']['timestamp'] = int(data['header']['timestamp'])
				try:
					db[table_name].insert_one(data)
					print(str(data['header']['timestamp']),"- DB Inserted to "+table_name+".")
				except pymongo.errors.DuplicateKeyError:
					print(str(data['header']['timestamp']),"- DB Rejected to "+table_name+". Duplicate Keys.")
					increase_sleep = True

		except requests.exceptions.ReadTimeout as e:
			print("Connection Error to: "+gtfsrt_url[table_name])
			print(e)
		except requests.exceptions.ConnectionError as e:
			print("Connection Error to: "+gtfsrt_url[table_name])
			print(e)
		except requests.exceptions.ChunkedEncodingError as e:
			print("Connection Error to: "+gtfsrt_url[table_name])
			print(e)
		except DecodeError:
			print("Unable to decode: "+gtfsrt_url[table_name])
		except KeyError as e:
			print("Missing Value in Protobuffer from: "+gtfsrt_url[table_name])
			print(e)

	if config['SLEEP_ADAPTIVE'] == 'true' and increase_sleep:
		sleep_time = sleep_time + 5
		print("Increased Sleep Time to "+str(sleep_time))
	else: 
		print("Sleeping for "+str(sleep_time))
	time.sleep(sleep_time)
