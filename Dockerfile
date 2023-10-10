FROM python:3.8-alpine

RUN apk update && \
    apk add --no-cache curl ca-certificates && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/

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

COPY ./config.json .
COPY ./secret.json .
COPY ./live_partitioning.py .
COPY ./cleanup_live.py .
COPY ./rollback_live.py .
COPY ./maintenance_partition.py .

CMD [ "python", "live_partitioning.py"]
