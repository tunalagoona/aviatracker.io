USER := tunalagoona
NAME := aviatracker.io
IMG := ${USER}/${NAME}:latest

build:
	@docker build -t ${IMG} -f docker/Dockerfile .

shell:
	@sudo docker run -it -p 5000:5000 --env LC_ALL=C.UTF-8 --env LANG=LANG=C.UTF-8 -v /home/ubuntu/config:/aviatracker.io/config -v /home/ubuntu/logs:/aviatracker.io/logs ${IMG} bash -c "/bin/bash"

app:
	@sudo docker run -it -p 5000:5000 --env LC_ALL=C.UTF-8 --env LANG=LANG=C.UTF-8 -v /home/ubuntu/config:/aviatracker.io/config -v /home/ubuntu/logs:/aviatracker.io/logs ${IMG} bash -c "/aviatracker.io/launcher.sh"

init:
	@sudo docker run -it -p 5000:5000 --env LC_ALL=C.UTF-8 --env LANG=LANG=C.UTF-8 -v /home/ubuntu/config:/aviatracker.io/config -v /home/ubuntu/logs:/aviatracker.io/logs ${IMG} bash -c "/aviatracker.io/init_launcher.sh"

push:
	@sudo docker push ${IMG}

pull:
	@sudo docker pull ${IMG}
