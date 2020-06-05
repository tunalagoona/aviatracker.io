import time
import config
import yaml
import setup_logging as log
from opensky_api import StateVector, OpenskyStates
from database import DbConnection
from psycopg2 import DatabaseError
from contextlib import closing
from typing import List, Dict


State_vector = Dict
State_vectors = List[State_vector]


class OpenskyDataExtraction:

    @staticmethod
    def get_state_vectors(cur_time: int) -> State_vectors:
        api = OpenskyStates()
        api_response = api.get_states(epoch_time=cur_time)
        states = api_response[0]
        status_code = api_response[1]
        if status_code == 200:
            logger.debug('Status_code is 200. Successful connection to Opensky API.')
        else:
            logger.error(f'Could not connect to server. Status code is {st}')
            return self.get_state_vectors(cur_time)
        return states

    def insert_state_vectors_to_db(self, cur_time):
        try:
            logger.info('Connecting to the PostgreSQL database...')
            with closing(DbConnection(dbname="openskydb", user=config.pg_username, password=config.pg_password)) as db:
                logger.info('Successful connection to the PostgreSQL database')
                while True:
                    states = self.get_state_vectors(cur_time)
                    db.upsert_state_vectors(states)
                    # new cursor is created for a batch of states, transaction is committed after a batch upserted
                    logger.debug(f'State vectors upserted: {states}')
                    cur_time += 5
            logger.info('Database connection closed.')
        except (Exception, DatabaseError):
            logger.exception("Exception in main(): ")
            self.insert_state_vectors_to_db(cur_time=cur_time)
        # try:
        #     logger.info('Connecting to the PostgreSQL database...')
        #     db = DbConnection(dbname="openskydb", user=config.pg_username, password=config.pg_password)
        #     logger.info('Successful connection to the PostgreSQL database')
        #
        #     api = OpenskyStates()
        #
        #     while True:
        #         os_states = api.get_states(epoch_time=cur_time)
        #         request_time = os_states["time"]
        #         current_states = os_states["states"]
        #         states = []
        #
        #         for state in current_states:
        #             state_vector = StateVector(state, request_time)
        #             states.append(state_vector)
        #
        #         db.upsert_state_vectors(states)
        #         # new cursor is created for a batch of states, transaction is committed after a batch upserted
        #
        #         logger.debug(f'State vectors upserted: {states}')
        #
        #         cur_time += 5
        #
        # except (Exception, DatabaseError):
        #     logger.exception("Exception in main(): ")
        #     connect_to_api_and_insert_data_to_db(cur_time=cur_time)
        #
        # finally:
        #     if db.conn is not None:
        #         db.close()
        #         logger.info('Database connection closed.')


logger = log.setup()
data_extraction = OpenskyDataExtraction()
data_extraction.insert_state_vectors_to_db(cur_time=int(time.time()))

