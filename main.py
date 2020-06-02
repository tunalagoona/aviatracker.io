import time
from psycopg2 import connect, DatabaseError
import yaml
import setup_logging as log
from opensky_api import StateVector, OpenskyStates
from pgsql_transaction import PostgreSQLTransaction as PgTrans


def upsert_states(cur_time):
    try:
        logger.info('Connecting to the PostgreSQL database...')
        pg_session = PgTrans()
        connection = connect(dbname=pg_session.database_name, user=pg_session.username, password=pg_session.password)
        curs = connection.cursor()
        logger.info('Successful connection to the PostgreSQL database')

        while True:
            api = OpenskyStates()
            states = api.get_states(epoch_time=cur_time)
            req_time = states["time"]
            current_states = states["states"]

            for state in current_states:
                state_vector = StateVector(state, req_time)
                upsert_query = pg_session.upsert(state_vector.dict)
                curs.execute(upsert_query[0], upsert_query[1])
                logger.debug(f'State vector upserted: {state_vector}')

            connection.commit()

            cur_time += 5

        curs.close(curs)

    except (Exception, DatabaseError):
        logger.exception("Exception in main(): ")
        upsert_states(cur_time=cur_time)

    finally:
        if connection is not None:
            connection.close()
            logger.info('Database connection closed.')


logger = log.setup()
upsert_states(cur_time=int(time.time()))
