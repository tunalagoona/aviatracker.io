from celery import Celery


app = Celery(
    "task_scheduling",
    broker="pyamqp://guest@localhost//",
    backend="rpc://",
    include=["flighttracker.task_scheduling.vector_insertion"],
)

app.conf.beat_schedule = {
    "run-me-every-ten-seconds": {
        "task": "flighttracker.task_scheduling.vector_insertion.task",
        "schedule": 10.0,
        "args": (),
    }
}
