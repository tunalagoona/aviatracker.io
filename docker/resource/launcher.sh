rabbitmq-server -detached

celery -A aviatracker.tasks beat --detach -l INFO -f /logs/celery.log -s /aviatracker/beat/celerybeat-schedule
celery -A aviatracker.tasks worker --detach -l INFO -f /logs/celery.log --without-gossip --without-mingle

python3 -m aviatracker.web.app
