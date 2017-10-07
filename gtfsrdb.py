from google.protobuf.json_format import MessageToJson
from pymongo.errors import DuplicateKeyError
from urllib.request import urlopen
from optparse import OptionParser
from pymongo import MongoClient
import json
import time
import sys
sys.path.insert(0, 'protobuf')
import gtfs_realtime_pb2

p = OptionParser()

p.add_option('-t', '--trip-updates', dest='tripUpdatesURL', default=None, help='Trip Updates URL', metavar='URL')
p.add_option('-a', '--alerts', dest='alertsURL', default=None, help='Alerts URL', metavar='URL')
p.add_option('-p', '--vehicle-positions', dest='vehiclePositionsURL', default=None, help='Vehicle Positions URL', metavar='URL')
p.add_option('-d', '--database', dest='dbName', default=None, help='Database Name', metavar='DB')
p.add_option('-c', '--connection', dest='connURL', default=None, help='Database Connection', metavar='mongo://URL')
p.add_option('-w', '--wait', dest='sleepTime', default=30, type='int', metavar='SECS', help='Time to wait between requests (in seconds)')

opts, args = p.parse_args()

if opts.connURL == None:
	print('No database connection specified!')
	exit(1)

if opts.dbName == None:
	print('No database name specified!')
	exit(1)

client = MongoClient(opts.connURL)
db = client[opts.dbName]

while True:

	if opts.tripUpdatesURL != None:
		fm = gtfs_realtime_pb2.FeedMessage()
		fm.ParseFromString(urlopen(opts.tripUpdatesURL).read())
		data = json.loads(MessageToJson(fm))
		try:
			db['tripUpdates'].insert_one(data)
			print(data['header']['timestamp'],"- Inserted Trip Update Data.")
		except DuplicateKeyError:
			print(data['header']['timestamp'],"- DB Rejected Trip Update Data. Duplicate Keys.")

	if opts.vehiclePositionsURL != None:
		fm = gtfs_realtime_pb2.FeedMessage()
		fm.ParseFromString(urlopen(opts.vehiclePositionsURL).read())
		data = json.loads(MessageToJson(fm))
		try:
			db['vehiclePositions'].insert_one(data)
			print(data['header']['timestamp'],"- Inserted Vehicle Position Data.")
		except DuplicateKeyError:
			print(data['header']['timestamp'],"- DB Rejected Vehicle Position Data. Duplicate Keys.")

	time.sleep(opts.sleepTime)