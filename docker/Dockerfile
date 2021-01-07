FROM ubuntu:18.04

WORKDIR /flight-tracker

RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get update && apt-get install -y libpq-dev
RUN apt-get update && apt-get install -y iputils-ping
RUN apt-get update && apt-get install -y vim
RUN apt-get update && apt-get install -y curl
RUN mkdir logs
RUN touch logs/log.log
RUN touch logs/celery_log.log

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY flighttracker flighttracker
COPY beat beat
COPY logging.yaml .


