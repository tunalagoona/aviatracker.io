rabbitmq-server -detached

celery -A flighttracker.task_scheduling beat --detach -l INFO -f /flight-tracker/logs/celery_beat_log.log -s /flight-tracker/beat/celerybeat-schedule
celery -A flighttracker.task_scheduling worker --detach -l INFO -f /flight-tracker/logs/celery_log.log --without-gossip --without-mingle

python3 -m flighttracker.web.app
