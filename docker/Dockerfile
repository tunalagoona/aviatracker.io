FROM ubuntu:18.04

WORKDIR /aviatracker.io

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    libpq-dev \
    iputils-ping \
    vim \
    curl \
    gnupg \
    apt-transport-https
RUN apt-get install -y --fix-missing rabbitmq-server

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

RUN mkdir logs beat

COPY aviatracker aviatracker
COPY logging.yaml .
COPY docker/resource/launcher.sh /aviatracker.io/
COPY docker/resource/init_launcher.sh /aviatracker.io/
