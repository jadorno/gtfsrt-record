from google.protobuf.json_format import MessageToJson
from google.protobuf.message import DecodeError
import gtfs_realtime_pb2 as gtfsrt
from pathlib import Path
import datetime as dt
import requests
import logging
import json
import pytz
import time
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
data_path = Path('/usr/src/data')

config = None
with open(data_path.joinpath('dataset.json')) as json_file:
    config = json.load(json_file)
tz = pytz.timezone(config['timezone'])

logging.info('Job Started')

logging.info('GTFS-RT - %s', config['data_name'])
for setting in ['archive_db','archive_pb','archive_json']:
	logging.info('%s --> %s', setting, config[setting])

feeds = os.environ.get('MONITOR', config['gtfsrt'].keys())
if isinstance(feeds, str):
	feeds = [feeds]
else:
	feeds = list(feeds)
logging.info('Feeds %s', str(feeds))
sleep_time = config['gtfsrt'][feeds[0]]['sleep']
sleep_adaptive_duplicate = False
db_url = os.environ.get('DB_URL', None)
db_init = False

while True:

	increase_sleep = False

	for feed in feeds:

		try:

			r = requests.get(config['gtfsrt'][feed]['url'], timeout=10)
			fm = gtfsrt.FeedMessage()
			fm.ParseFromString(r.content)
			data = json.loads(MessageToJson(fm))

			timestamp_str = data['header']['timestamp']
			timestamp = dt.datetime.fromtimestamp(float(timestamp_str), tz)
			logging.debug('PB Timestamp: %s', timestamp_str)

			if config['archive_pb'] is True:

				week_folder = config['data_name']+"-GTFSRT-"+timestamp.strftime("%G-%V")+".pb"
				item_path = data_path.joinpath('gtfsrt', week_folder, feed, timestamp_str).with_suffix('.pb')
				logging.debug('PB Path: %s', item_path)

				if not item_path.parent.exists():
					item_path.parent.mkdir(parents=True, exist_ok=True)
				if not item_path.exists():
					with open(item_path, 'wb') as output_file:
						output_file.write(r.content)
				else:
					logging.error('Path Already Exists: %s', item_path.relative_to(data_path))
					increase_sleep = True

			if config['archive_json'] is True:

				week_folder = config['data_name']+"-GTFSRT-"+timestamp.strftime("%G-%V")+".json"
				item_path = data_path.joinpath('gtfsrt-json', week_folder, feed, timestamp_str).with_suffix('.json')
				logging.debug('JSON Path: %s', item_path)

				if not item_path.parent.exists():
					item_path.parent.mkdir(parents=True, exist_ok=True)
				if not item_path.exists():
					with open(item_path, 'w') as output_file:
						json.dump(data, output_file)
				else:
					logging.error('Path Already Exists: %s', item_path.relative_to(data_path))
					increase_sleep = True

			if config['archive_db'] is True:

				if not db_init is True:

					import pymongo
					client = pymongo.MongoClient(db_url)
					db = client.get_database()

					for table_name in feeds:
						timestamp_index_exist = False
						if table_name in db.collection_names():
							index_info = db[table_name].index_information()
							for index in index_info:
								for key in index_info[index]['key']:
									for field in key:
										if field == "header.timestamp":
											timestamp_index_exist = True
						if not timestamp_index_exist:
							logging.warning('No Timestamp Index Located for %s. Creating...', table_name)
							db[table_name].create_index(
								[("header.timestamp", pymongo.DESCENDING)],
								unique=True,background=True
							)
					db_init = True

				data['header']['timestamp'] = int(data['header']['timestamp'])
				try:
					db[feed].insert_one(data)
				except pymongo.errors.DuplicateKeyError:
					logging.error('Entry Already Exists. %s %s', feed, str(data['header']['timestamp']))
					increase_sleep = True

		except requests.exceptions.ReadTimeout as e:
			logging.exception('Connection Error to: %s', config['gtfsrt'][feed]['url'])
			print(e)
		except requests.exceptions.ConnectionError as e:
			logging.exception('Connection Error to: %s', config['gtfsrt'][feed]['url'])
			print(e)
		except requests.exceptions.ChunkedEncodingError as e:
			logging.exception('Connection Error to: %s', config['gtfsrt'][feed]['url'])
			print(e)
		except DecodeError as e:
			logging.exception('Unable to decode: %s', config['gtfsrt'][feed]['url'])
			print(e)
		except KeyError as e:
			logging.exception('Missing Value in Protobuffer from: %s', config['gtfsrt'][feed]['url'])
			print(e)

	if config['sleep_adaptive'] is True:
		if increase_sleep:
			if not sleep_adaptive_duplicate:
				sleep_time = sleep_time + 5
			sleep_adaptive_duplicate = True
			time.sleep(2)
		else:
			sleep_time = sleep_time - 1
			sleep_adaptive_duplicate = False
			logging.info('Sleeping for %s seconds', str(sleep_time))
			time.sleep(sleep_time)
	else:
		time.sleep(sleep_time)
