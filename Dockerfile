FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get update && apt-get install -y libpq-dev

WORKDIR /flight-tracker

COPY . .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "/flight-tracker/flask_app/web_app.py"]
