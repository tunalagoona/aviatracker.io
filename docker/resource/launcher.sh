rabbitmq-server -detached

celery -A aviatracker.tasks beat --detach -l INFO -f logs/celery.log -s beat/celerybeat-schedule

celery -A aviatracker.tasks worker --detach -l DEBUG -f logs/celery.log --without-gossip --without-mingle -n worker1@%h -Q states --concurrency=1
celery -A aviatracker.tasks worker --detach -l INFO -f logs/celery.log --without-gossip --without-mingle -n worker2@%h -Q paths --concurrency=1
celery -A aviatracker.tasks worker --detach -l INFO -f logs/celery.log --without-gossip --without-mingle -n worker3@%h -Q celery --concurrency=1

python3 -m aviatracker.web.app
