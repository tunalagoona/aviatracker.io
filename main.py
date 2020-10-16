import time
import config
import yaml
import setup_logging as log
from opensky_api import StateVector, OpenskyStates
from database_update import DbConnection
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
            logger.error(f'Could not connect to server. Status code is {status_code}')
            return self.get_state_vectors(cur_time)
        return states

    def insert_state_vectors_to_db(self, cur_time):
        try:
            logger.info('Connecting to the PostgreSQL database...')
            with closing(DbConnection(dbname="opensky", user=config.pg_username, password=config.pg_password)) as db:
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


logger = log.setup()

if __name__ == "__main__":
    data_extraction = OpenskyDataExtraction()
    data_extraction.insert_state_vectors_to_db(cur_time=int(time.time()))