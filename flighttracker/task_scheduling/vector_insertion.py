from typing import List, Dict
import time
from contextlib import closing

from psycopg2 import DatabaseError

from flighttracker import config
from flighttracker.task_scheduling.celery import app
from flighttracker.opensky_api import OpenskyStates
from flighttracker.database import DB
from flighttracker.log import setup_logging as log

State_vector = Dict
State_vectors = List[State_vector]

logger = log.setup()


def insert_state_vectors_to_db(cur_time: int) -> None:
    api = OpenskyStates()
    try:
        logger.info("Connecting to the PostgreSQL database...")
        print(f'config.pg_username = {config.pg_username}')
        with closing(
            DB(dbname="opensky", user=config.pg_username, password=config.pg_password, host=config.pg_hostname,
               port=config.pg_port_number)
        ) as db:
            logger.info("Successful connection to the PostgreSQL database")

            logger.info(f"A request has been sent to API with req_time: {cur_time}")
            states: State_vectors = api.get_states(time_sec=cur_time)
            logger.info(
                f"A response has been received from API for req_time: {cur_time}"
            )

            state_vectors_quantity = 0
            for _ in states:
                state_vectors_quantity += 1

            resp_time: int = states[0]["request_time"]
            logger.info(
                f"Quantity of state vectors received from API for the time {resp_time}:"
                f" {state_vectors_quantity}"
            )
            logger.info("_________ before upsert _________")
            db.upsert_state_vectors(states)
            logger.info("_________ after upsert __________")
            logger.debug(f"State vectors upserted: {states}")

    except (Exception, DatabaseError) as e:
        logger.exception(f"Exception in main(): {e}")
        insert_state_vectors_to_db(cur_time=cur_time)


@app.task
def task():
    task.time_limit = 15
    logger.info("Celery task has begun")
    cur_time = int(time.time())
    insert_state_vectors_to_db(cur_time)
    logger.info("Celery task has finished")
