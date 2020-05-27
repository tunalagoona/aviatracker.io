import requests
import time
import json
import psycopg2
import logging
import yaml
import logging.config


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


# class StateVector:
#     def __init__(self, arr):
#         self.keys = ["icao24", "callsign", "origin_country", "time_position",
#                      "last_contact", "longitude", "latitude", "baro_altitude", "on_ground",
#                      "velocity", "true_track", "vertical_rate", "sensors",
#                      "geo_altitude", "squawk", "spi", "position_source"]
#         self.dict = dict(zip(self.keys, arr))


logger = setup_logging()


def get_states(username='bigtuna', password='BMid8mSm7wq@9fC', epoch_time=0, icao24=None):
    auth = (username, password)
    api_url = "https://opensky-network.org/api"
    url_operation = "/states/all"
    parameters = {"time": int(epoch_time), "icao24": icao24}

    r = requests.get('{}{}'.format(api_url, url_operation), auth=auth, params=parameters, timeout=15)
    st = r.status_code
    if st == 200:
        logger.info('Status_code is 200. Successful connection to Opensky API.')
    else:
        logger.error(f'Could not connect to server. Status code is {st}')
        return get_states(epoch_time=epoch_time)
    return json.loads(r.text)


def start_records_to_pg(tran_insert, tran_select, tran_update, cur_time=int(time.time())):
    try:
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect("dbname=openskydb user=mas5mk password=$GV9^MJGk8gn")
        cur = connection.cursor()
        logger.info('Successful connection to the PostgreSQL database')

        while True:
            states = get_states(epoch_time=cur_time)
            req_time = states["time"]
            current_states = states["states"]

            for s in current_states:
                cur.execute(tran_select, (req_time, s[0]))
                data = cur.fetchall()
                logger.debug(f'selection parameters: {req_time, s[0], s[1], s[2], s[5], s[6], s[7]}')
                logger.debug(f'data:{data}')

                if len(data) == 0:
                    cur.execute(tran_insert, [req_time] + s)
                    logger.debug('No duplicates, new record completed')
                else:
                    cur.execute(tran_update, (s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10], s[11], s[13],
                                              req_time, s[0]))
                    logger.debug('Duplicate found, record updated')

            connection.commit()
            cur_time += 5
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception("Exception in main(): ")
        start_records_to_pg(tran_insert=tran_insert, tran_select=tran_select, tran_update=tran_update,
                            cur_time=cur_time)

    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')


insertion = """
                    INSERT INTO opensky_state_vectors (request_time, icao24, callsign, origin_country, time_position, 
                    last_contact, longitude, latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate, 
                    sensors, geo_altitude, squawk, spi, position_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
selection = """
                    SELECT * FROM opensky_state_vectors 
                    WHERE (request_time, icao24) = (%s, %s);
                    """

update = """
                    UPDATE opensky_state_vectors 
                    SET (time_position, last_contact, longitude, latitude, baro_altitude, on_ground, velocity,
                    true_track, vertical_rate, geo_altitude) = (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    WHERE (request_time, icao24) = (%s, %s);
                    """

start_records_to_pg(tran_insert=insertion, tran_select=selection, tran_update=update)
