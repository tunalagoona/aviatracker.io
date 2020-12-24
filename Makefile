USER := tunalagoona
NAME := flight-tracker
IMG := ${USER}/${NAME}:v0.0.1
LATEST := ${USER}/${NAME}:latest

build:
	@docker build -t ${IMG} .
	@docker tag ${IMG} ${LATEST}

run:
	@docker run -it -p 31337:80 ${LATEST}

push:
	@docker push ${LATEST}


# rabbitmq:
# 	rabbitmq-server -detached
#
# run-celery:
#     celery -A celery_proj beat
#     celery -A celery_proj worker -l INFO
