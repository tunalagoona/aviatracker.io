import requests
import time
import json
import psycopg2
import logging
import yaml
import logging.config
import config


def setup_logging(path='logging.yaml', level=logging.INFO):
    with open(path, 'rt') as f:
        try:
            configurations = yaml.safe_load(f.read())
            logging.config.dictConfig(configurations)
            print(f'Logging configurations implemented successfully.')
        except Exception as e:
            print(e)
            print('Error in Logging Configuration. Using default configs')
            logging.basicConfig(level=level)
    log = logging.getLogger(__name__)
    return log


class StateVector(object):
    def __init__(self, st_vector, request_time):
        self.keys = ["request_time", "icao24", "callsign", "origin_country", "time_position",
                     "last_contact", "longitude", "latitude", "baro_altitude", "on_ground",
                     "velocity", "true_track", "vertical_rate", "sensors",
                     "geo_altitude", "squawk", "spi", "position_source"]
        self.values = [request_time] + st_vector
        self.dict = dict(zip(self.keys, self.values))


class OpenskyAPI(object):
    def __init__(self, username=config.os_username, password=config.os_password):
        self.auth = (username, password)
        self.api_url = "https://opensky-network.org/api"
        self.url_operation = "/states/all"

    def get_states(self, epoch_time=0, icao24=None):
        parameters = {"time": int(epoch_time), "icao24": icao24}
        r = requests.get('{}{}'.format(self.api_url, self.url_operation), auth=self.auth, params=parameters, timeout=15)
        st = r.status_code
        if st == 200:
            logger.info('Status_code is 200. Successful connection to Opensky API.')
        else:
            logger.error(f'Could not connect to server. Status code is {st}')
            return get_states(epoch_time=epoch_time)
        return json.loads(r.text)


# def get_states(username=config.os_username, password=config.os_password, epoch_time=0, icao24=None):
#     auth = (username, password)
#     api_url = "https://opensky-network.org/api"
#     url_operation = "/states/all"
#     parameters = {"time": int(epoch_time), "icao24": icao24}
#
#     r = requests.get('{}{}'.format(api_url, url_operation), auth=auth, params=parameters, timeout=15)
#     st = r.status_code
#     if st == 200:
#         logger.info('Status_code is 200. Successful connection to Opensky API.')
#     else:
#         logger.error(f'Could not connect to server. Status code is {st}')
#         return get_states(epoch_time=epoch_time)
#     return json.loads(r.text)


class SQLRequest:
    def __init__(self):
        self.insert = """
                            INSERT INTO opensky_state_vectors (request_time, icao24, callsign, origin_country, 
                            time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity,
                            true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source)
    
                            VALUES (%(request_time)s, %(icao24)s, %(callsign)s, %(origin_country)s, %(time_position)s, 
                            %(last_contact)s, %(longitude)s, %(latitude)s, %(baro_altitude)s, %(on_ground)s, %(velocity)s,
                            %(true_track)s, %(vertical_rate)s, %(sensors)s, %(geo_altitude)s, %(squawk)s, %(spi)s, 
                            %(position_source)s);
                        """
        self.count =    """
                            SELECT COUNT(request_time) FROM opensky_state_vectors 
                            WHERE (request_time, icao24) = (%(request_time)s, %(icao24)s);
                        """

        self.update =   """
                            UPDATE opensky_state_vectors 
                            SET (time_position, last_contact, longitude, latitude, baro_altitude, on_ground, 
                            velocity, true_track, vertical_rate, geo_altitude) = (%(time_position)s, %(last_contact)s, 
                            %(longitude)s, %(latitude)s, %(baro_altitude)s, %(on_ground)s, %(velocity)s, 
                            %(true_track)s, %(vertical_rate)s, %(geo_altitude)s)
                            WHERE (request_time, icao24) = (%(request_time)s, %(icao24)s);
                        """


class PostgreSQL(object):
    def __init__(self):
        self.username = config.pg_username
        self.password = config.pg_password
        self.database_name = "openskydb"

    def pg_transaction(self, cur_time=None):
        request = SQLRequest()
        try:
            logger.info('Connecting to the PostgreSQL database...')
            connection = psycopg2.connect(dbname=self.database_name, user=self.username, password=self.password)
            cur = connection.cursor()
            logger.info('Successful connection to the PostgreSQL database')

            while True:
                api = OpenskyAPI()
                states = api.get_states(epoch_time=cur_time)
                req_time = states["time"]
                current_states = states["states"]

                for state in current_states:
                    state_vector = StateVector(state, req_time)
                    cur.execute(request.count, {'request_time': state_vector.dict["request_time"],
                                                'icao24': state_vector.dict["icao24"]})
                    count = int(cur.fetchone()[0])
                    logger.debug(f'selection parameters:'
                                 f'{state_vector.dict["request_time"], state_vector.dict["icao24"]},'
                                 f'count: {count}')

                    if count == 0:
                        cur.execute(request.insert, {
                            'request_time': state_vector.dict["request_time"],
                            'icao24': state_vector.dict["icao24"],
                            'callsign': state_vector.dict["callsign"],
                            'origin_country': state_vector.dict["origin_country"],
                            'time_position': state_vector.dict["time_position"],
                            'last_contact': state_vector.dict["last_contact"],
                            'longitude': state_vector.dict["longitude"],
                            'latitude': state_vector.dict["latitude"],
                            'baro_altitude': state_vector.dict["baro_altitude"],
                            'on_ground': state_vector.dict["on_ground"],
                            'velocity': state_vector.dict["velocity"],
                            'true_track': state_vector.dict["true_track"],
                            'vertical_rate': state_vector.dict["vertical_rate"],
                            'sensors': state_vector.dict["sensors"],
                            'geo_altitude': state_vector.dict["geo_altitude"],
                            'squawk': state_vector.dict["squawk"],
                            'spi': state_vector.dict["spi"],
                            'position_source': state_vector.dict["position_source"]
                        })
                        logger.debug('No duplicates, new record completed')
                    else:
                        cur.execute(request.update, {
                            'time_position': state_vector.dict["time_position"],
                            'last_contact': state_vector.dict["last_contact"],
                            'longitude': state_vector.dict["longitude"],
                            'latitude': state_vector.dict["latitude"],
                            'baro_altitude': state_vector.dict["baro_altitude"],
                            'on_ground': state_vector.dict["on_ground"],
                            'velocity': state_vector.dict["velocity"],
                            'true_track': state_vector.dict["true_track"],
                            'vertical_rate': state_vector.dict["vertical_rate"],
                            'geo_altitude': state_vector.dict["geo_altitude"]
                        })
                        logger.debug('Duplicate found, record updated')
                connection.commit()
                cur_time += 5
            cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            logger.exception("Exception in main(): ")
            self.pg_transaction(cur_time=cur_time)

        finally:
            if connection is not None:
                connection.close()
                logger.info('Database connection closed.')


# def start_records_to_pg(tran_insert, tran_count, tran_update, cur_time=int(time.time())):
#     try:
#         print('Connecting to the PostgreSQL database...')
#         connection = psycopg2.connect(dbname="openskydb", user=config.pg_username, password=config.pg_password)
#         cur = connection.cursor()
#         logger.info('Successful connection to the PostgreSQL database')
#
#         while True:
#             states = get_states(epoch_time=cur_time)
#             req_time = states["time"]
#             current_states = states["states"]
#
#             for s in current_states:
#                 state_vector = StateVector(s, req_time)
#                 cur.execute(tran_count, {'request_time': state_vector.dict["request_time"],
#                                          'icao24': state_vector.dict["icao24"]})
#                 count = int(cur.fetchone()[0])
#                 logger.debug(f'selection parameters: {state_vector.dict["request_time"], state_vector.dict["icao24"]}')
#                 logger.debug(f'count:{count}')
#
#                 if count == 0:
#                     cur.execute(tran_insert, {
#                         'request_time': state_vector.dict["request_time"],
#                         'icao24': state_vector.dict["icao24"],
#                         'callsign': state_vector.dict["callsign"],
#                         'origin_country': state_vector.dict["origin_country"],
#                         'time_position': state_vector.dict["time_position"],
#                         'last_contact': state_vector.dict["last_contact"],
#                         'longitude': state_vector.dict["longitude"],
#                         'latitude': state_vector.dict["latitude"],
#                         'baro_altitude': state_vector.dict["baro_altitude"],
#                         'on_ground': state_vector.dict["on_ground"],
#                         'velocity': state_vector.dict["velocity"],
#                         'true_track': state_vector.dict["true_track"],
#                         'vertical_rate': state_vector.dict["vertical_rate"],
#                         'sensors': state_vector.dict["sensors"],
#                         'geo_altitude': state_vector.dict["geo_altitude"],
#                         'squawk': state_vector.dict["squawk"],
#                         'spi': state_vector.dict["spi"],
#                         'position_source': state_vector.dict["position_source"]
#                     })
#                     logger.debug('No duplicates, new record completed')
#                 else:
#                     cur.execute(tran_update, {
#                         'time_position': state_vector.dict["time_position"],
#                         'last_contact': state_vector.dict["last_contact"],
#                         'longitude': state_vector.dict["longitude"],
#                         'latitude': state_vector.dict["latitude"],
#                         'baro_altitude': state_vector.dict["baro_altitude"],
#                         'on_ground': state_vector.dict["on_ground"],
#                         'velocity': state_vector.dict["velocity"],
#                         'true_track': state_vector.dict["true_track"],
#                         'vertical_rate': state_vector.dict["vertical_rate"],
#                         'geo_altitude': state_vector.dict["geo_altitude"]
#                     })
#                     logger.debug('Duplicate found, record updated')
#
#             connection.commit()
#             cur_time += 5
#         cur.close()
#
#     except (Exception, psycopg2.DatabaseError) as error:
#         logger.exception("Exception in main(): ")
#         start_records_to_pg(tran_insert=tran_insert, tran_count=tran_count, tran_update=tran_update,
#                             cur_time=cur_time)
#
#     finally:
#         if connection is not None:
#             connection.close()
#             print('Database connection closed.')


logger = setup_logging()

postgres_query = PostgreSQL()
postgres_query.pg_transaction(cur_time=int(time.time()))
