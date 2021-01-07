USER := tunalagoona
NAME := flight-tracker
IMG := ${USER}/${NAME}:latest

build:
	@docker build -t ${IMG} -f docker/Dockerfile .

shell:

	@docker run -it -p 5000:5000 --env LC_ALL=C.UTF-8 --env LANG=LANG=C.UTF-8 -v /Users/mas5mk/Work/flight-tracker/config:/flight-tracker/config ${IMG} bash -c "/bin/bash"

app:
	@docker run -it -p 5000:5000 --env LC_ALL=C.UTF-8 --env LANG=LANG=C.UTF-8 -v /Users/mas5mk/Work/flight-tracker/config:/flight-tracker/config ${IMG} bash -c "/flight-tracker/launcher.sh"

push:
	@docker push ${IMG}
