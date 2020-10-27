import socket
import time
import requests
import config
import setup_logging as log
from opensky_api import OpenskyStates
from database_update import DbConnection
from psycopg2 import DatabaseError
from contextlib import closing
from typing import List, Dict


State_vector = Dict
State_vectors = List[State_vector]

logger = log.setup()


class OpenskyDataExtraction:

    def get_state_vectors(self, cur_time: int) -> State_vectors:
        api = OpenskyStates()
        try:
            logger.info(f'A request has been sent to API with req_time: {cur_time}')
            api_response = api.get_states(time_sec=cur_time)
            logger.info(f'A response has been received from API for req_time: {cur_time}')
            states = api_response[0]
            status_code = api_response[1]
            if status_code == 200:
                logger.info('Status_code is 200. Successful connection to Opensky API.')
                return states
            else:
                logger.error(f'Could not connect to Opensky API. Status code is {status_code}')
                return self.get_state_vectors(cur_time)

        except (OSError, requests.exceptions.ReadTimeout, socket.timeout) as e:
            logger.error(f'Could not get state vectors from API due to {e}')
            return self.get_state_vectors(cur_time)

    def insert_state_vectors_to_db(self, cur_time):
        try:
            logger.info('Connecting to the PostgreSQL database...')
            with closing(DbConnection(dbname="opensky", user=config.pg_username, password=config.pg_password)) as db:
                logger.info('Successful connection to the PostgreSQL database')
                while True:
                    states = self.get_state_vectors(cur_time)
                    state_vectors_quantity = 0
                    for _ in states:
                        state_vectors_quantity += 1
                    resp_time = states[0]['request_time']
                    logger.info(f'Quantity of state vectors received from API for the time {resp_time}:'
                                f' {state_vectors_quantity}')
                    logger.info('___ before upsert')
                    db.upsert_state_vectors(states)
                    logger.info('___ after upsert')
                    # new cursor is created for a batch of states, transaction is committed after a batch upserted
                    logger.debug(f'State vectors upserted: {states}')
                    cur_time += 5
                    time.sleep(5)
        except (Exception, DatabaseError):
            logger.exception("Exception in main(): ")
            self.insert_state_vectors_to_db(cur_time=cur_time)


if __name__ == "__main__":
    data_extraction = OpenskyDataExtraction()
    data_extraction.insert_state_vectors_to_db(cur_time=int(time.time()))
