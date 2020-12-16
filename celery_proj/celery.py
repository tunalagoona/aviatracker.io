from celery import Celery


app = Celery('celery_proj',
             broker='pyamqp://guest@localhost//',
             backend='rpc://',
             include=['celery_proj.get_states'])

app.conf.beat_schedule = {
    "run-me-every-ten-seconds": {
         "task": "celery_proj.get_states.task",
         "schedule": 10.0,
         "args": ()
    }
}


if __name__ == '__main__':
    app.start()
