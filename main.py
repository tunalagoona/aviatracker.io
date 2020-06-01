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


class PostgreSQL(object):
    def __init__(self):
        self.username = config.pg_username
        self.password = config.pg_password
        self.database_name = "openskydb"

    def upsert(self, vector: dict):
        trans_upsert = """
                        INSERT INTO opensky_state_vectors (request_time, icao24, callsign, origin_country, 
                        time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity, 
                        true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source)
                        VALUES (%(request_time)s, %(icao24)s, %(callsign)s, %(origin_country)s, %(time_position)s,
                        %(last_contact)s, %(longitude)s, %(latitude)s, %(baro_altitude)s, %(on_ground)s, %(velocity)s,
                        %(true_track)s, %(vertical_rate)s, %(sensors)s, %(geo_altitude)s, %(squawk)s, %(spi)s,
                        %(position_source)s)
                        ON CONFLICT (request_time, icao24) 
                        DO UPDATE SET (time_position, last_contact, longitude, latitude, baro_altitude, on_ground, 
                        velocity, true_track, vertical_rate, geo_altitude) = (%(time_position)s, %(last_contact)s, 
                        %(longitude)s, %(latitude)s, %(baro_altitude)s, %(on_ground)s, %(velocity)s, 
                        %(true_track)s, %(vertical_rate)s, %(geo_altitude)s);
                        """
        upsert_values = {
                            'request_time': vector["request_time"],
                            'icao24': vector["icao24"],
                            'callsign': vector["callsign"],
                            'origin_country': vector["origin_country"],
                            'time_position': vector["time_position"],
                            'last_contact': vector["last_contact"],
                            'longitude': vector["longitude"],
                            'latitude': vector["latitude"],
                            'baro_altitude': vector["baro_altitude"],
                            'on_ground': vector["on_ground"],
                            'velocity': vector["velocity"],
                            'true_track': vector["true_track"],
                            'vertical_rate': vector["vertical_rate"],
                            'sensors': vector["sensors"],
                            'geo_altitude': vector["geo_altitude"],
                            'squawk': vector["squawk"],
                            'spi': vector["spi"],
                            'position_source': vector["position_source"]
                        }
        return [trans_upsert, upsert_values]

    def pg_transaction(self, cur_time=None):
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
                    upsert_query = self.upsert(state_vector.dict)
                    cur.execute(upsert_query[0], upsert_query[1])
                    logger.debug(f'State vector upserted: {state_vector}')
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


logger = setup_logging()

postgres_query = PostgreSQL()
postgres_query.pg_transaction(cur_time=int(time.time()))
