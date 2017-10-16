from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse
from pymongo.errors import DuplicateKeyError
from pymongo.errors import DocumentTooLarge
from pymongo import MongoClient
from objdict import ObjDict
import datetime
import time
import json
import sys
sys.path.insert(0, 'protobuf')
import gtfs_realtime_pb2 as pb2

with open(sys.argv[1]) as config_file:
        config = ObjDict( json.load(config_file) )
client = MongoClient(config.connURL)
db = client[config.connDB]
f = open(config.filePath, 'r')
if config.fileSkipFirst:
	f.readline()

printCount = 0
curr_data = []

for line in f:
	file_data = line.split(",")

	entity = ObjDict()
	entity.id = "NULL"
	entity.vehicle = ObjDict()
	entity.vehicle.trip = ObjDict()
	entity.vehicle.vehicle = ObjDict()
	entity.vehicle.position = ObjDict()
 
	for field in config.fileFields:
		if len(file_data[field['pos']]) != 0:
			if field['type'] == 'auto':
				exec( field['name']+" = file_data[field['pos']]" )
			elif field['type'] == 'special':
				if field['name'] == 'header.timestamp':
					timestamp = int(time.mktime(datetime.datetime.strptime(file_data[field['pos']].rstrip('\n'), '%Y-%m-%d %H:%M:%S').timetuple()))
					try:
						curr_timestamp
					except NameError:
						curr_timestamp = timestamp
				else:
					print("[ERROR] Unable to Parse Key: "+field['name'])
					exit()
			else:
				print("[ERROR] Unable to Parse Type: "+field['type'])
				exit()

	if timestamp != curr_timestamp:
		id_list = []
		rm_list = []
		for idx, val in enumerate(curr_data):
			val['id'] = 'vehicle_position_'+val['vehicle']['vehicle']['id'];
			if val['id'] in id_list:
				print("DUPLICATE VEHICLE UPDATE ON ENTITY: "+val['id']+" "+str(idx))
				rm_list.append(idx)
			else:
				id_list.append(val['id'])
		if len(rm_list) != 0:
			for idx in reversed(rm_list):
				del curr_data[idx]


		vehicle_position = ObjDict()
		vehicle_position.entity = curr_data
		vehicle_position.header = ObjDict()
		vehicle_position.header.timestamp = curr_timestamp
		vehicle_position.header.incrementality = "FULL_DATASET"
		vehicle_position.header.gtfsRealtimeVersion = "1.0"

		print(str(vehicle_position.header.timestamp)+" "+str(len(curr_data)))
		printCount = printCount + 1
		if printCount % 100 == 0:
			printCount = 0
			sys.stdout.flush()

		fm = Parse( vehicle_position.dumps(), pb2.FeedMessage())
		data = json.loads(MessageToJson(fm))

		data['header']['timestamp'] = int(data['header']['timestamp'])

		try:
			db['vehicle_positions'].insert_one( data )
		except DuplicateKeyError:
			if config.connMerge:
				print('DB Rejected Vehicle Position Data. Duplicate Keys. Merging...')
				onlineData = db['vehicle_positions'].find_one({"header.timestamp": vehicle_position.header.timestamp})
				mergeEntity = onlineData['entity'] + curr_data


				id_list = []
				rm_list = []
				for idx, val in enumerate(mergeEntity):
					if val['id'] in id_list:
						print("DUPLICATE VEHICLE UPDATE: "+val['id']+" "+str(idx))
						rm_list.append(idx)
					else:
						id_list.append(val['id'])
				if len(rm_list) != 0:
					for idx in reversed(rm_list):
						del mergeEntity[idx]


				onlineData['entity'] = mergeEntity
				db['vehicle_positions'].replace_one({"header.timestamp": fm.header.timestamp}, onlineData)
			else: 
				print('DB Rejected Vehicle Position Data. Duplicate Keys. Merging Disabled')
		except DocumentTooLarge:
			print('DB Rejected Vehicle Position Data. Document Too Large')
		curr_timestamp = timestamp
		curr_data = []
	curr_data.append( json.loads( entity.dumps()) )

print('Completed Executing')