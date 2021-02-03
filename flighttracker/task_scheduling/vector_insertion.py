from typing import List, Dict
import time
from contextlib import closing
import logging
import os

from psycopg2 import DatabaseError
from celery.signals import after_setup_logger

from flighttracker.task_scheduling.celery import app
from flighttracker.opensky_api import OpenskyStates
from flighttracker.database import DB
from config.parser import ConfigParser

State_vector = Dict
State_vectors = List[State_vector]

logger = logging.getLogger()


@after_setup_logger.connect
def on_celery_setup_logging(logger):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    script_dir = os.path.abspath(__file__ + "/../../../")
    rel_path = 'logs/main.log'
    path = os.path.join(script_dir, rel_path)
    fh = logging.FileHandler(path, 'w+')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def insert_state_vectors_to_db(cur_time: int) -> None:
    api = OpenskyStates()
    try:
        conf = ConfigParser
        with closing(
            DB(dbname=conf.dbname, user=conf.user_name, password=conf.password, host=conf.hostname, port=conf.port_number)
        ) as db:
            logger.info(f"A request has been sent to API for the timestamp: {cur_time}")
            states: State_vectors = api.get_states(time_sec=cur_time)
            logger.info(f"A response has been received from API for the timestamp: {cur_time}")

            state_vectors_quantity = 0
            for _ in states:
                state_vectors_quantity += 1

            resp_time: int = states[0]["request_time"]
            logger.info(
                f"Received {state_vectors_quantity} state vectors for the timestamp {resp_time}:"
            )
            logger.info("Insertion to DB has started")
            db.upsert_state_vectors(states)
            logger.info("Insertion to DB has finished")
            logger.debug(f"Inserted state vectors: {states}")

    except (Exception, DatabaseError) as e:
        logger.exception(f"Exception in main(): {e}")
        insert_state_vectors_to_db(cur_time=cur_time)


@app.task
def task():
    task.time_limit = 15
    cur_time = int(time.time())
    insert_state_vectors_to_db(cur_time)
