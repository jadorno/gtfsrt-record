# Record GTFS-RT Service

### Program Type:

	Service

### Execution Frequency:

	N/A

### Program Description

This service monitors GTFS-RT feeds of an agency at a specified interval. Data can be stored as .pb files on-disk and/or stored on a MongoDB instance. 

The program can be run independently from other services or dependent on the Data and State API Services to retrieve dataset configuration and logging. It is recommended that services are managed through an orchestrator. Refer to [getting started project](https://gitlab.com/cutr-at-usf/transi/core/getting-started) for deployment information.

### Program Execution (API Dependent)

```
docker run --rm --network=my_network \
-e "DATA_URL=http://basic_api" \
-e "STATE_URL=http://state_api" \
-e "DATASET=hart" \
-v /transi/data/pb/hart:/usr/src/app/data \
registry.gitlab.com/cutr-at-usf/transi/core/record-gtfsrt
```

### Execution Parameters

**DATA_URL**: URL used to access the Data API Service on docker network

**STATE_URL**: URL used to access the Logging API Service on docker network

**DATASET**: Dataset configuration name as specified in config.json for the Data API service

### Program Execution (Independent Runtime)

```
docker run --rm --network=my_network \
-e "DB_URL=mongodb://db:27017/hart" \
-e "DB_UPLOAD=true" \
-e "PB_DOWNLOAD=true" \
-e "SLEEP_TIME=30" \
-e "SLEEP_ADAPTIVE=false" \
-e "URL_TRIP_UPDATES_ENABLED=true" \
-e "URL_TRIP_UPDATES=http://api.tampa.onebusaway.org:8088/trip-updates" \
-e "URL_VEHICLE_POSITIONS_ENABLED=true" \
-e "URL_VEHICLE_POSITIONS=http://api.tampa.onebusaway.org:8088/vehicle-positions" \
-e "URL_ALERTS_ENABLED=false" \
-e "URL_ALERTS=null" \
-v /transi/data/pb/hart:/usr/src/app/data \
registry.gitlab.com/cutr-at-usf/transi/core/record-gtfsrt
```

### Execution Parameters

**DB_URL**: URL scheme used to log into a MongoDB database.

**DB_UPLOAD**: Boolean value to enable or disable PB upload to MongoDB

**PB_DOWNLOAD**: Boolean value to enable or disable PB upload to MongoDB

**PB_PATH** (optional): Directory used to store PB responses within container environment

**SLEEP_TIME**: Polling interval for GTFS-RT feed

**SLEEP_ADAPTIVE**: Boolean value to enable or disable adaptive polling. SLEEP_TIME is baseline.

**URL_TRIP_UPDATES_ENABLED**: Boolean value to monitor trip_updates feed (true/false)

**URL_TRIP_UPDATES**: URL for trip_updates feed

**URL_VEHICLE_POSITIONS_ENABLED**: Boolean value to monitor vehicle_positions feed (true/false)

**URL_VEHICLE_POSITIONS**: URL for vehicle_positions feed

**URL_ALERTS_ENABLED**: Boolean value to monitor alerts feed (true/false)

**URL_ALERTS**: URL for alerts feed

Please refer to [getting started project](https://gitlab.com/cutr-at-usf/transi/core/getting-started) for more information about the system
