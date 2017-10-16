from pymongo.errors import DuplicateKeyError
from pymongo.errors import DocumentTooLarge
from pymongo import MongoClient
from objdict import ObjDict
import zipfile
import time
from datetime import datetime
import sys
import json
from bson import json_util

def find_shape(shape_arr, shape_id):
	for idx, shape in enumerate(shape_arr):
		if shape.properties.shape_id == shape_id:
			return idx;
	return -1;

def find_trip(stop_times_arr, trip_id):
	for idx, trip in enumerate(stop_times_arr):
		if trip.trip_id == trip_id:
			return idx;
	return -1;

def find_route(trips_arr, route_id):
	for idx, route in enumerate(trips_arr):
		if route.route_id == route_id:
			return idx;
	return -1;

db_tables = {}
db_tables['shapes.txt'] = 'shapes'
db_tables['stop_times.txt'] = 'stop_times'
db_tables['stops.txt'] = 'stops'
db_tables['trips.txt'] = 'trips'
db_tables['agency.txt'] = 'agency'
db_tables['calendar.txt'] = 'calendar'
db_tables['calendar_dates.txt'] = 'calendar_dates'
db_tables['fare_attributes.txt'] = 'fare_attributes'
db_tables['fare_rules.txt'] = 'fare_rules'
db_tables['routes.txt'] = 'routes'

with open(sys.argv[2]) as config_file:
        config = ObjDict( json.load(config_file) )
client = MongoClient(config.connURL)
db = client[config.connDB]

with zipfile.ZipFile(sys.argv[1]) as zf:
	
	for filename in zf.namelist():

		print("File: "+filename)
		file_created = datetime(*zf.getinfo(filename).date_time)
		print("Date Associated: "+file_created.strftime("%Y-%m-%d %H:%M:%S"))
		timestamp = int(file_created.timestamp())

		if len(sys.argv) == 4:
			print("WARNING: Timestamp Override ")
			try: 
				timestamp = int(datetime.strptime(sys.argv[3], "%Y%m%d").timestamp())
			except ValueError:
				print("ERORR: Unable to parse date. Format: YYYYMMDD")
				exit()

		obj = ObjDict()
		obj.header = {'timestamp': timestamp}
		obj.data = []

		with zf.open(filename, 'r') as file:
			headers = None

			for line in file:

				data = line.decode('utf8').rstrip('\n').rstrip('\r').split(",")
				if len(data) != 1:
					if headers is None:
						headers = data
					else:
						entry = {}
						for idx, val in enumerate(headers):
							entry[val] = data[idx]

						if filename == 'shapes.txt':

							pos = find_shape(obj.data, entry['shape_id'])
							if pos == -1:
								shape = ObjDict()
								shape.type = "LineString"
								shape.properties = ObjDict()
								shape.properties.shape_id = entry['shape_id']
								shape.coordinates = [];
								shape.coordinates.append([float(entry['shape_pt_lon']), float(entry['shape_pt_lat'])])
								obj.data.append(shape)
							else:
								obj.data[pos].coordinates.append([entry['shape_pt_lon'], entry['shape_pt_lat']])

						elif filename == 'stop_times.txt':

							pos = find_trip(obj.data, entry['trip_id'])
							if pos == -1:
								trip = ObjDict()
								trip.trip_id = entry['trip_id'];
								trip.stops = [];
								del entry['trip_id']
								trip.stops.append(entry)
								obj.data.append(trip)
							else:
								del entry['trip_id']
								obj.data[pos].stops.append(entry)

						elif filename == 'stops.txt':

							stop = ObjDict()
							stop.type = "Point"
							stop.coordinates = [];
							stop.coordinates.append([float(entry['stop_lon']), float(entry['stop_lat'])])
							del entry['stop_lon']
							del entry['stop_lat']
							stop.properties = ObjDict()
							stop.properties = entry
							obj.data.append(stop)

						elif filename == 'trips.txt':

							pos = find_route(obj.data, entry['route_id'])
							if pos == -1:
								route = ObjDict()
								route.route_id = entry['route_id'];
								route.trips = [];
								del entry['route_id']
								route.trips.append(entry)
								obj.data.append(route)
							else:
								del entry['route_id']
								obj.data[pos].trips.append(entry)
						else: 

							obj.data.append(entry)

		if filename == 'stop_times.txt':

			for trip in obj.data:
				trip_obj = ObjDict()
				trip_obj.header = {'timestamp': timestamp}
				trip_obj.data = trip
				try:
					lastEntry = db[ db_tables[filename] ].find({"header.timestamp": obj.header['timestamp'],"data.trip_id": trip['trip_id']}).sort("header.timestamp",-1).limit(1)[0]
				except IndexError: 
					db[ db_tables[filename] ].insert_one( json.loads(trip_obj.dumps()) )
					print("Trip "+trip['trip_id']+" from "+str(timestamp)+" inserted to DB")				

		elif filename in 'trips.txt':

			for route in obj.data:
				route_obj = ObjDict()
				route_obj.header = {'timestamp': timestamp}
				route_obj.data = route
				try:
					lastEntry = db[ db_tables[filename] ].find({"header.timestamp": obj.header['timestamp'],"data.route_id": route['route_id']}).sort("header.timestamp",-1).limit(1)[0]
				except IndexError: 
					db[ db_tables[filename] ].insert_one( json.loads(route_obj.dumps()) )
					print("Route "+route['route_id']+" from "+str(timestamp)+" inserted to DB")

		elif filename in 'stops.txt':

			for stop in obj.data:
				stop_obj = ObjDict()
				stop_obj.header = {'timestamp': timestamp}
				stop_obj.data = stop
				try:
					lastEntry = db[ db_tables[filename] ].find({"header.timestamp": obj.header['timestamp'],"data.properties.stop_id": stop['properties']['stop_id']}).sort("header.timestamp",-1).limit(1)[0]
				except IndexError: 
					db[ db_tables[filename] ].insert_one( json.loads(stop_obj.dumps()) )
					print("Stop "+stop['properties']['stop_id']+" from "+str(timestamp)+" inserted to DB")

		elif filename in 'shapes.txt':

			for shape in obj.data:
				shape_obj = ObjDict()
				shape_obj.header = {'timestamp': timestamp}
				shape_obj.data = shape
				try:
					lastEntry = db[ db_tables[filename] ].find({"header.timestamp": obj.header['timestamp'],"data.properties.shape_id": shape['properties']['shape_id']}).sort("header.timestamp",-1).limit(1)[0]
				except IndexError: 
					db[ db_tables[filename] ].insert_one( json.loads(shape_obj.dumps()) )
					print("Shape "+shape['properties']['shape_id']+" from "+str(timestamp)+" inserted to DB")

		elif filename in db_tables:

			try:
		#		Altered to disable space saving features 
		#		lastEntry = db[ db_tables[filename] ].find({"header.timestamp": {"$lte": obj.header['timestamp']}}).sort("header.timestamp",-1).limit(1)[0]
				lastEntry = db[ db_tables[filename] ].find({"header.timestamp": obj.header['timestamp']}).sort("header.timestamp",-1).limit(1)[0]
			except IndexError: 
				lastEntry = None

		#	if lastEntry is None or json.dumps(obj.data, sort_keys=True) != json_util.dumps(lastEntry['data'], sort_keys=True):
			if lastEntry is None:
				try: 
					db[ db_tables[filename] ].insert_one( json.loads(obj.dumps()) )
					print("Inserted New Set to DB")
				except DuplicateKeyError:
		#			This shouldn't happen anymore
					print("ERROR: Database Rejected Entry. Same Header")
			else:
				print("SKIPPED: Found older timestamp with same data")

		else: 
			print("ERROR: Parse not implemented for: "+filename)
			exit()

print('Completed Executing')