import config
from typing import List, Dict
import logging
import time

from contextlib import closing
from psycopg2 import DatabaseError

from celery_proj.celery import app
from opensky_api import OpenskyStates
from database_update import DbConnection

State_vector = Dict
State_vectors = List[State_vector]

module_logger = logging.getLogger()


def insert_state_vectors_to_db(cur_time):
    api = OpenskyStates()
    try:
        module_logger.info('Connecting to the PostgreSQL database...')
        with closing(DbConnection(dbname="opensky", user=config.pg_username, password=config.pg_password)) as db:
            module_logger.info('Successful connection to the PostgreSQL database')

            module_logger.info(f'A request has been sent to API with req_time: {cur_time}')
            states = api.get_states(time_sec=cur_time)
            module_logger.info(f'A response has been received from API for req_time: {cur_time}')

            state_vectors_quantity = 0
            for _ in states:
                state_vectors_quantity += 1

            resp_time = states[0]['request_time']
            module_logger.info(f'Quantity of state vectors received from API for the time {resp_time}:'
                               f' {state_vectors_quantity}')
            module_logger.info('_________ before upsert _________')
            db.upsert_state_vectors(states)
            module_logger.info('_________ after upsert __________')
            module_logger.debug(f'State vectors upserted: {states}')

    except (Exception, DatabaseError) as e:
        module_logger.exception(f'Exception in main(): {e}')
        insert_state_vectors_to_db(cur_time=cur_time)


@app.task
def task():
    task.time_limit = 10
    module_logger.info('Celery task has begun')
    cur_time = int(time.time())
    insert_state_vectors_to_db(cur_time)





