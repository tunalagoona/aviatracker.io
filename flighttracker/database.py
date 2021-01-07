from typing import List, Dict, Tuple

from psycopg2 import connect

State_vector = Dict
State_vectors = List[State_vector]


class DB:
    def __init__(self, dbname, user, password, host, port):
        self.conn = connect(dbname=dbname, user=user, password=password, host=host, port=port)
        # self.conn = connect(dbname=dbname, user=user, password=password)

    def create_table(self):
        new_table = (
            """
                CREATE TABLE opensky_state_vectors (
                        request_time INTEGER,
                        icao24 VARCHAR,
                        callsign VARCHAR,
                        origin_country VARCHAR,
                        time_position INTEGER,
                        last_contact INTEGER,
                        longitude DOUBLE PRECISION,
                        latitude DOUBLE PRECISION,
                        baro_altitude DOUBLE PRECISION,
                        on_ground BOOLEAN,
                        velocity DOUBLE PRECISION,
                        true_track DOUBLE PRECISION,
                        vertical_rate DOUBLE PRECISION,
                        sensors INTEGER,
                        geo_altitude DOUBLE PRECISION,
                        squawk TEXT,
                        spi BOOLEAN,
                        position_source INTEGER,
                        PRIMARY KEY (request_time, icao24)
                );
            """
        )
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(new_table)

    def upsert_state_vectors(self, vectors: State_vectors) -> None:
        with self.conn:
            """When a connection exits the with block, if no exception has been raised by the block,
            the transaction is committed"""
            with self.conn.cursor() as curs:
                curs.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)",
                             ('opensky_state_vectors',))
                table_exists = curs.fetchone()[0]
                if table_exists is not True:
                    self.create_table()

                for vector in vectors:
                    state_vector = {
                        "request_time": vector["request_time"],
                        "icao24": vector["icao24"],
                        "callsign": vector["callsign"],
                        "origin_country": vector["origin_country"],
                        "time_position": vector["time_position"],
                        "last_contact": vector["last_contact"],
                        "longitude": vector["longitude"],
                        "latitude": vector["latitude"],
                        "baro_altitude": vector["baro_altitude"],
                        "on_ground": vector["on_ground"],
                        "velocity": vector["velocity"],
                        "true_track": vector["true_track"],
                        "vertical_rate": vector["vertical_rate"],
                        "sensors": vector["sensors"],
                        "geo_altitude": vector["geo_altitude"],
                        "squawk": vector["squawk"],
                        "spi": vector["spi"],
                        "position_source": vector["position_source"],
                    }
                    curs.execute(
                        """
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
                            """,
                        state_vector,
                    )

    def get_last_inserted_state(self) -> Tuple[State_vectors, int]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM opensky_state_vectors "
                "WHERE request_time = (SELECT MAX(request_time) FROM opensky_state_vectors);"
            )
            vectors = curs.fetchall()

            state_vectors_quantity = 0
            for _ in vectors:
                state_vectors_quantity += 1

            return vectors, state_vectors_quantity

    def close(self):
        self.conn.close()
