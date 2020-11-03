import eventlet
eventlet.monkey_patch()

import time
import config
import threading
from typing import List, Dict

from psycopg2 import DatabaseError
from contextlib import closing

from log import setup_logging as log
from opensky_api import OpenskyStates
from database_update import DbConnection
import flask_app.web_app as webapp


State_vector = Dict
State_vectors = List[State_vector]

logger = log.setup()


def insert_state_vectors_to_db(cur_time):
    api = OpenskyStates()
    try:
        logger.info('Connecting to the PostgreSQL database...')
        with closing(DbConnection(dbname="opensky", user=config.pg_username, password=config.pg_password)) as db:
            logger.info('Successful connection to the PostgreSQL database')
            while True:
                logger.info(f'A request has been sent to API with req_time: {cur_time}')
                states = api.get_states(time_sec=cur_time)
                logger.info(f'A response has been received from API for req_time: {cur_time}')

                state_vectors_quantity = 0
                for _ in states:
                    state_vectors_quantity += 1

                resp_time = states[0]['request_time']
                logger.info(f'Quantity of state vectors received from API for the time {resp_time}:'
                            f' {state_vectors_quantity}')
                logger.info('___ before upsert')
                db.upsert_state_vectors(states)
                logger.info('___ after upsert')
                logger.debug(f'State vectors upserted: {states}')
                cur_time += 5
                time.sleep(5)

    except (Exception, DatabaseError) as e:
        logger.exception(f'Exception in main(): {e}')
        insert_state_vectors_to_db(cur_time=cur_time)


if __name__ == "__main__":
    x = threading.Thread(target=webapp.start_webapp, daemon=True)
    x.start()
    y = threading.Thread(target=insert_state_vectors_to_db(cur_time=int(time.time())))
    y.start()
