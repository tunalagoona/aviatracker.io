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


# rabbitmq:
# 	rabbitmq-server -detached
#
# run-celery:
#     elery -A task_scheduling beat
#     celery -A task_scheduling worker -l INFO
