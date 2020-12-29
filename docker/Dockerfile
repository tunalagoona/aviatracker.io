FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get update && apt-get install -y libpq-dev
RUN apt-get update && apt-get install -y iputils-ping
RUN apt-get update && apt-get install -y vim

WORKDIR /flight-tracker

COPY . .

RUN pip3 install -r requirements.txt
