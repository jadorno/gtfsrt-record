FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY gtfsrdb.py ./
COPY protobuf ./

CMD [ "python", "./gtfsrdb.py", "config.json"]
