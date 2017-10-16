import datetime as dt
import calendar
import pymongo
import time
import json
import sys

def strTimestamp(obj, strFormat):
	return dt.datetime.utcfromtimestamp(obj['header']['timestamp']).strftime(strFormat)

def datTimestamp(obj, strFormat):
	return dt.datetime.strptime(obj, strFormat)

def intTimestamp(obj):
	return calendar.timegm(obj.timetuple())



with open(sys.argv[1]) as config_file:
        config = json.load(config_file)
client = pymongo.MongoClient(config['connURL'])
db = client[config['connDB']]

str_date = strTimestamp( db.vehicle_positions.find_one({}, sort=[("header.timestamp", pymongo.ASCENDING)]), "%Y-%m-%d" );
end_date = strTimestamp( db.vehicle_positions.find_one({}, sort=[("header.timestamp", pymongo.DESCENDING)]) , "%Y-%m-%d")

d1 = datTimestamp(str_date, "%Y-%m-%d")
d2 = datTimestamp(end_date, "%Y-%m-%d")
delta = d2 - d1

myData = []

for i in range(delta.days):
	start = intTimestamp( d1 + dt.timedelta(days=i) )
	end = intTimestamp( d1 + dt.timedelta(days=(i+1)) )

	query = db.vehicle_positions.find({
		"header.timestamp": { 
			"$lt" : end
		},
		"$and" : [
			{
				"header.timestamp" : {
					"$gte" : start
				}
			}
		]
	}).sort([("header.timestamp", pymongo.ASCENDING)]);
	for result in query:
		myData.append(strTimestamp(result, "%Y-%m-%d"))

for i in range(delta.days + 1):
	current_date = str(d1 + timedelta(days=i))
	print(current_date+" "+str(myData.count(current_date)))