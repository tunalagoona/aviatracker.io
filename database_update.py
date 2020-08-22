from psycopg2 import connect
from typing import List, Dict


State_vector = Dict
State_vectors = List[State_vector]


class DbConnection:
    def __init__(self, dbname, user, password):
        self.conn = connect(dbname=dbname, user=user, password=password)

    def upsert_state_vectors(self, vectors: State_vectors):
        with self.conn:
            """ When a connection exits the with block, if no exception has been raised by the block,
            the transaction is committed"""
            with self.conn.cursor() as curs:
                for vector in vectors:
                    state_vector = {
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
                    # print(f'state vector: {state_vector}')
                    curs.execute("""
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
                            """, state_vector)

    def close(self):
        self.conn.close()