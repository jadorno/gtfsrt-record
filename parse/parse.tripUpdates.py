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
f_1 = open(config.tripUpdatePath, 'r')
if config.tripUpdateSkipFirst:
	f_1.readline()
f_2 = open(config.stopTimeUpdatePath, 'r')
if config.stopTimeUpdateSkipFirst:
	f_2.readline()

printCount = 0
curr_data = []

for line in f_1:
	trip_data = line.rstrip('\n').split(",")

	entity = ObjDict()
	entity.id = "NULL"
	entity.tripUpdate = ObjDict()
	entity.tripUpdate.trip = ObjDict()
	entity.tripUpdate.vehicle = ObjDict()
	entity.tripUpdate.stopTimeUpdate = []

	for field in config.tripUpdateFields:
		if len(trip_data[field['pos']]) != 0:

			if field['type'] == 'auto':
				exec( field['name']+" = trip_data[field['pos']]" )
			elif field['type'] == 'timestamp':
				val = int(time.mktime(datetime.datetime.strptime(trip_data[field['pos']].rstrip('\n'), '%Y-%m-%d %H:%M:%S').timetuple()))
				exec( field['name']+" = val" )
			elif field['type'] == 'special':

				if field['name'] == 'header.timestamp':
					timestamp = int(time.mktime(datetime.datetime.strptime(trip_data[field['pos']].rstrip('\n'), '%Y-%m-%d %H:%M:%S').timetuple()))
					try:
						curr_timestamp
					except NameError:
						curr_timestamp = timestamp
				elif field['name'] == 'header.mergeKey':
					mergeKey = int(trip_data[field['pos']])
				elif field['name'] == 'entity.tripUpdate.trip.scheduleRelationship':
					try:
						enumPos = int(trip_data[field['pos']])
						value = pb2.TripDescriptor.ScheduleRelationship.DESCRIPTOR.values_by_number[enumPos].name
						entity.tripUpdate.trip.scheduleRelationship = value
					except ValueError:
						entity.tripUpdate.trip.scheduleRelationship = trip_data[field['pos']]

				else:
					print("[ERROR] Unable to Parse Key: "+field['name'])
					exit()
			else:
				print("[ERROR] Unable to Parse Type: "+field['type'])
				exit()

	done = False;
	while not done:

		skipData = False
		stop_time_prev_pos = f_2.tell()
		stop_time_data = f_2.readline().rstrip('\n').split(",")

		stopTimeUpdate = ObjDict()
		stopTimeUpdate.arrival = ObjDict()
		stopTimeUpdate.departure = ObjDict()

		for field in config.stopTimeUpdateFields:
			if len(stop_time_data[field['pos']]) != 0:

				if field['type'] == 'auto':
					exec( field['name']+" = stop_time_data[field['pos']]" )
				elif field['type'] == 'timestamp':
					try:
						val = int(time.mktime(datetime.datetime.strptime(stop_time_data[field['pos']].rstrip('\n'), '%Y-%m-%d %H:%M:%S').timetuple()))
						exec( field['name']+" = val" )
					except ValueError:
						if stop_time_data[field['pos']] != '0':
							print("[ERROR] Unable to Parse Timestamp")
							print(stop_time_data)
							exit()
				elif field['type'] == 'special':

					if field['name'] == 'header.mergeKey':
						stop_merge_key = int(stop_time_data[field['pos']])
						if stop_merge_key == mergeKey:
							done = True
						else:
							if stop_merge_key > mergeKey:
								f_2.seek(stop_time_prev_pos)
								done = True
							skipData = True

					elif field['name'] == 'stopTimeUpdate.scheduleRelationship':
						try:
							enumPos = int(stop_time_data[field['pos']])
							value = pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.DESCRIPTOR.values_by_number[enumPos].name
							stopTimeUpdate.scheduleRelationship = value
						except ValueError:
							stopTimeUpdate.scheduleRelationship = stop_time_data[field['pos']]

					else:
						print("[ERROR] Unable to Parse Key: "+field['name'])
						exit()
				else:
					print("[ERROR] Unable to Parse Type: "+field['type'])
					exit()

		if skipData:
			print("SKIP: ",str(mergeKey)," ",str(stop_merge_key))
		else:
			entity.tripUpdate.stopTimeUpdate.append(stopTimeUpdate)
		if done:
			break;

	if curr_timestamp != timestamp:
		id_list = []
		rm_list = []
		for idx, val in enumerate(curr_data):
			val['id'] = 'trip_update_'+val['tripUpdate']['trip']['tripId'];
			if val['id'] in id_list:
				print("DUPLICATE TRIP UPDATE ON ENTITY: "+val['id']+" "+str(idx))
				rm_list.append(idx)
			else:
				id_list.append(val['id'])
		if len(rm_list) != 0:
			for idx in reversed(rm_list):
				del curr_data[idx]

		trip_update = ObjDict()
		trip_update.entity = curr_data
		trip_update.header = ObjDict()
		trip_update.header.timestamp = str(curr_timestamp)
		trip_update.header.incrementality = "FULL_DATASET"
		trip_update.header.gtfsRealtimeVersion = "1.0"

		print(str(trip_update.header.timestamp)+" "+str(len(curr_data)))
		printCount = printCount + 1
		if printCount % 100 == 0:
			printCount = 0
			sys.stdout.flush()

		fm = Parse( trip_update.dumps(), pb2.FeedMessage())
		data = json.loads(MessageToJson(fm))

		data['header']['timestamp'] = int(data['header']['timestamp'])

		try:
			db['trip_updates'].insert_one(data)
		except DuplicateKeyError:
			if config.connMerge:
				print('DB Rejected Trip Update Data. Duplicate Keys. Merging...')
				onlineData = db['trip_updates'].find_one({"header.timestamp": trip_update.header.timestamp})
				mergeEntity = onlineData['entity'] + curr_data


				id_list = []
				rm_list = []
				for idx, val in enumerate(mergeEntity):
					if val['id'] in id_list:
						print("DUPLICATE TRIP UPDATE: "+val['id']+" "+str(idx))
						rm_list.append(idx)
					else:
						id_list.append(val['id'])
				if len(rm_list) != 0:
					for idx in reversed(rm_list):
						del mergeEntity[idx]


				onlineData['entity'] = mergeEntity
				db['trip_updates'].replace_one({"header.timestamp": trip_update.header.timestamp}, onlineData)
			else:
				print('DB Rejected Trip Update Data. Duplicate Keys. Merging Disabled')
		except DocumentTooLarge:
			print('DB Rejected Trip Update Data. Document Too Large')

		curr_timestamp = timestamp
		curr_data = []

	curr_data.append( json.loads(entity.dumps()) )

print('Completed Executing')