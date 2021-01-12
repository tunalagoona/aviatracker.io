from typing import List, Dict
import time
from contextlib import closing
import logging
import yaml
import os

from psycopg2 import DatabaseError
from celery.signals import after_setup_logger

from flighttracker.task_scheduling.celery import app
from flighttracker.opensky_api import OpenskyStates
from flighttracker.database import DB

State_vector = Dict
State_vectors = List[State_vector]

logger = logging.getLogger()


@after_setup_logger.connect
def on_celery_setup_logging(logger, *args, **kwargs):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    script_dir = os.path.abspath(__file__ + "/../../../")
    print(f'vector insertion script_dir = {script_dir}')
    rel_path = 'logs/celery_log.log'
    path = os.path.join(script_dir, rel_path)
    fh = logging.FileHandler(path, 'w+')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def insert_state_vectors_to_db(cur_time: int) -> None:
    api = OpenskyStates()
    try:
        logger.info("Connecting to the PostgreSQL database...")

        script_dir = os.path.abspath(__file__ + "/../../../")
        rel_path = 'config/config.yaml'
        path = os.path.join(script_dir, rel_path)

        with open(path, 'r') as cnf:
            parsed_yaml_file = yaml.load(cnf, Loader=yaml.FullLoader)
            dbname = parsed_yaml_file['postgres']['pg_dbname']
            user_name = parsed_yaml_file['postgres']['pg_username']
            password = parsed_yaml_file['postgres']['pg_password']
            hostname = parsed_yaml_file['postgres']['pg_hostname']
            port_number = parsed_yaml_file['postgres']['pg_port_number']

        with closing(
            DB(dbname=dbname, user=user_name, password=password, host=hostname, port=port_number)
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
