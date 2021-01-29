FROM alpine/git as protobuf

WORKDIR /usr/src/
RUN git clone https://github.com/MobilityData/gtfs-realtime-bindings.git

WORKDIR /usr/src/gtfs-realtime-bindings
RUN git checkout tags/final-google-version

FROM python:3.8-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends p7zip-full\
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY --from=protobuf /usr/src/gtfs-realtime-bindings/python/google/transit/gtfs_realtime_pb2.py ./gtfs_realtime_pb2.py

CMD [ "python", "-u", "./main.py"]