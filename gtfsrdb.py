import requests
import json
import time
import sys
import os

from google.protobuf.json_format import MessageToJson
sys.path.insert(0, 'protobuf')
import gtfs_realtime_pb2 as gtfsrt

with open(sys.argv[1]) as config_file:
        config = json.load(config_file)

if not config['database_upload'] and not config['proto_download']:
	print("You have Protobuffer Download and Database Upload Disabled.")
	print("The program doesn't do anything else.")
	exit()

if config['database_upload']:
	import pymongo

	if os.environ.get('URL') != None:
		config['conn_url'] = os.environ.get('URL')
	client = pymongo.MongoClient(config['conn_url'])
	db = client[config['conn_db']]

	for table_name in ["trip_updates", "vehicle_positions"]:
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

	for table_name in ["trip_updates", "vehicle_positions"]:

		r = requests.get(config[table_name+'_url'])
		fm = gtfsrt.FeedMessage()
		fm.ParseFromString(r.content)
		data = json.loads(MessageToJson(fm))

		if config['proto_download']:
			outputFile = config['proto_path']+'/'+data['header']['timestamp']+"_"+table_name+".pb"
			f = open(outputFile, 'wb')
			f.write(r.content)
			f.close()
			print(data['header']['timestamp'],"- Protobuf File Written on "+outputFile)

		if config['database_upload']:
			data['header']['timestamp'] = int(data['header']['timestamp'])
			try:
				db[table_name].insert_one(data)
				print(str(data['header']['timestamp']),"- DB Inserted to "+table_name+".")
			except DuplicateKeyError:
				print(str(data['header']['timestamp']),"- DB Rejected to "+table_name+". Duplicate Keys.")

	time.sleep(config['sleep_time'])
