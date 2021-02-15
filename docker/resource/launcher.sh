rabbitmq-server -detached

celery -A aviatracker.task_scheduling beat --detach -l INFO -f /aviatracker/logs/celery.log -s /aviatracker/beat/celerybeat-schedule
celery -A aviatracker.task_scheduling worker --detach -l INFO -f /aviatracker/logs/celery.log --without-gossip --without-mingle

python3 -m aviatracker.web.app
