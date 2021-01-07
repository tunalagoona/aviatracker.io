USER := tunalagoona
NAME := flight-tracker
IMG := ${USER}/${NAME}:latest

build:
	@docker build -t ${IMG} -f docker/Dockerfile .

shell:

	@docker run -it -p 5000:5000 -v /Users/mas5mk/Work/flight-tracker/config:/flight-tracker/config ${IMG} bash -c "/bin/bash"

app:
	@docker run -it -p 5000:5000 -v /Users/mas5mk/Work/flight-tracker/config:/flight-tracker/config ${IMG} bash -c "python3 -m flighttracker.web.app"

push:
	@docker push ${IMG}

