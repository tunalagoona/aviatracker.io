import config


class PostgreSQLTransaction(object):
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
