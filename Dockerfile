FROM python:3.8-alpine

RUN apk add --no-cache --update \
    gcc \
    gfortran musl-dev g++ \
    libffi-dev openssl-dev \
    libxml2 libxml2-dev \
    libxslt libxslt-dev \
    libjpeg-turbo-dev zlib-dev

WORKDIR /app

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./common ./common
COPY ./db ./db
COPY ./common ./common
COPY ./tunnel ./tunnel

COPY ./config.json config.json
COPY ./secret.json secret.json
COPY ./yearly_partition.py yearly_partition.py

CMD [ "python", "yearly_partition.py"]
