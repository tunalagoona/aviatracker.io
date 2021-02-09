from celery import Celery


app = Celery(
    "task_scheduling",
    broker="pyamqp://guest@localhost//",
    backend="rpc://",
    include=["flighttracker.task_scheduling.vector_insertion"],
)

app.conf.beat_schedule = {
    'every-five-sec-insert-states': {
        'task': 'flighttracker.task_scheduling.vector_insertion.insert_states',
        'schedule': 5.0,
        'args': (),
    },
    'every-twenty-sec-update-paths': {
        'task': 'flighttracker.task_scheduling.vector_insertion.update_paths',
        'schedule': 20.0,
        'args': (),
    }
}
