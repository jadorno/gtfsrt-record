import sys
from urllib.request import urlopen
from google.protobuf.json_format import MessageToJson
sys.path.insert(0, 'protobuf')
import gtfs_realtime_pb2

fm = gtfs_realtime_pb2.FeedMessage()
fm.ParseFromString(urlopen("http://api.tampa.onebusaway.org:8088/vehicle-positions").read())
print( MessageToJson(fm) )