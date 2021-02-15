from celery import Celery


app = Celery(
    'scheduled_tasks',
    broker='pyamqp://guest@localhost//',
    backend='rpc://',
    include=['flighttracker.scheduled_tasks.tasks'],
)

app.conf.beat_schedule = {
    'every-five-sec-insert-states': {
        'task': 'flighttracker.scheduled_tasks.tasks.insert_states',
        'schedule': 5.0,
        'args': (),
    },
    'every-twenty-sec-update-paths': {
        'task': 'flighttracker.scheduled_tasks.tasks.update_paths',
        'schedule': 20.0,
        'args': (),
    },
    'every-hour-update-stats': {
        'task': 'flighttracker.scheduled_tasks.tasks.update_stats',
        'schedule': 3600.0,
        'args': (),
    }
}
