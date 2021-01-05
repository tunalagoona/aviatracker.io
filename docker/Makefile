USER := tunalagoona
NAME := flight-tracker
IMG := ${USER}/${NAME}:latest

build:
	@docker build -t ${IMG} -f docker/Dockerfile .

shell:
	@docker run -it -p 8080:80 ${IMG} bash -c "/bin/bash"

app:
	@docker run -it -p 8080:80 ${IMG} bash -c "python3 -m flighttracker.web.app"

push:
	@docker push ${IMG}

