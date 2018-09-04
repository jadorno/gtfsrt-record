FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY record.py ./
COPY protobuf ./

CMD [ "python", "-u", "./record.py"]
