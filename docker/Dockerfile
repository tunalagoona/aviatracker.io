FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get update && apt-get install -y libpq-dev
RUN apt-get update && apt-get install -y iputils-ping
RUN apt-get update && apt-get install -y vim
RUN apt-get update && apt-get install -y curl

WORKDIR /flight-tracker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY docker docker
COPY flighttracker flighttracker

#don't copy config, logs
