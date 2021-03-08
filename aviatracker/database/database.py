import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from psycopg2 import connect, extras

from aviatracker.database import (
    Airport,
    AirportStats,
    CallsignMemo,
    column_value_to_str,
    FlightAirportInfo,
    FlightPath,
    StateVector,
    OpenskyFlight)

from aviatracker.opensky import Opensky

logger = logging.getLogger()


class DB:
    def __init__(self, name: str, user: str, password: str, host: str, port: int) -> None:
        logger.info(f"Connecting to the PostgreSQL database - {user}@{host}:{port}/{name}")
        self.conn = connect(dbname=name, user=user, password=password, host=host, port=port)

        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute("SELECT 1;")
        logger.info(f"Connected to the Postgres database - {user}@{host}:{port}/{name}")

    def set_timezone(self):
        with self.conn.cursor() as curs:
            curs.execute(
                "SET timezone = 0;"
            )

    def insert_airports(self, airports: List[Airport]) -> None:
        with self.conn:
            with self.conn.cursor() as curs:
                columns_str, values_str = column_value_to_str(Airport._fields)
                insert_query = f"INSERT INTO airports ({columns_str}) VALUES %s"
                template = f"({columns_str})"
                extras.execute_values(curs, insert_query, airports, template)
                logger.debug(f"Inserted {len(airports)} airports")

    def insert_current_states(self, aircraft_states: List[StateVector]) -> None:
        with self.conn.cursor() as curs:
            """Row deletion can be implemented either by DELETE or by TRUNCATE TABLE.
            Though the DELETE option requires VACUUM to remove dead row versions,
            it is still preferred in our context as TRUNCATE would violate MVCC semantics
            and hinder parallel requests to access current states"""
            curs.execute(
                f"DELETE FROM current_states;"
            )

            resp_time: int = getattr(aircraft_states[0], "request_time")

            # aircraft_states = sorted(aircraft_states, key=lambda k: k.icao24)

            for state in aircraft_states:
                columns_str, values_str = column_value_to_str(state._fields)
                row: Dict = state._asdict()

                curs.execute(
                    f"INSERT INTO current_states ({columns_str}) VALUES ({values_str}) "
                    f"ON CONFLICT DO NOTHING",
                    row,
                )
            logger.debug(f"Inserted {len(aircraft_states)} aircraft states for the timestamp {resp_time}")

    def get_current_states(self) -> Optional[List[Dict]]:
        with self.conn.cursor() as curs:
            curs.execute("SELECT * FROM current_states")
            aircraft_states = curs.fetchall()
            states = []
            if len(aircraft_states) != 0:
                for state in aircraft_states:
                    states.append(StateVector(*state[1:])._asdict())
            return states

    def insert_path(self, path: FlightPath) -> None:
        columns_str, values_str = column_value_to_str(path._fields)
        path = path._asdict()
        with self.conn.cursor() as curs:
            curs.execute(
                f"INSERT INTO flight_paths ({columns_str}) VALUES ({values_str})",
                path
            )
        # logger.info("Inserted new path")

    def delete_outdated_paths(self) -> None:
        """A timestamp of a record was finished is compared to the current timestamp with time zone."""
        with self.conn.cursor() as curs:
            curs.execute(
                "DELETE FROM flight_paths "
                "WHERE now() - to_timestamp(finished_at) > interval '5 days' AND finished_at != 0;"
            )

    def update_path_when_finished(self):
        """Updates finished and finished_at columns of flight_paths for records with no update for more then 30 min."""
        with self.conn.cursor() as curs:
            time_now = int(time.time())
            curs.execute(
                f"UPDATE flight_paths SET finished = True, finished_at = %s WHERE finished = False "
                f"AND %s - last_update > 1800", (time_now, time_now)
            )

    def upsert_callsigns(self, flights: List[OpenskyFlight]) -> None:
        for flight in flights:
            if flight.callsign:
                self.upsert_one_callsign(
                    flight.callsign.strip().upper(),
                    flight.estArrivalAirport,
                    flight.estDepartureAirport
                )

    def upsert_one_callsign(self, callsign, arrival_airport, departure_airport):
        with self.conn.cursor() as curs:
            curs.execute(
                f"SELECT * FROM callsign_memo WHERE callsign = %s", (callsign,)
            )
            record = curs.fetchone()
            if record:
                record = CallsignMemo(*record[1:])
                if record.arrival_airport != arrival_airport or record.departure_airport != departure_airport:
                    curs.execute(
                        f"UPDATE callsign_memo SET est_arrival_airport = %s, est_departure_airport = %s "
                        f"WHERE callsign = %s ",
                        (arrival_airport, departure_airport, callsign)
                    )
            else:
                curs.execute(
                    f"INSERT INTO callsign_memo (callsign, est_arrival_airport, est_departure_airport)"
                    f"VALUES (%s, %s, %s)", (callsign, arrival_airport, departure_airport)
                )

    def get_arrival_airport(self, callsign):
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT est_arrival_airport FROM callsign_memo WHERE callsign = %s", (callsign,)
            )
            arr_airport = curs.fetchone()
            return arr_airport

    def find_unfinished_path_for_aircraft(self, icao) -> Optional[FlightPath]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM flight_paths WHERE icao24 = %s AND finished = False", (icao,)
            )
            record = curs.fetchone()
            if record:
                record = record[1:]
            # logger.info(f"record unfinished: {record}")
            if record:
                path = FlightPath(*record)
                return path

    def get_airport_long_lat(self, airport_icao: str) -> Tuple[float, float]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT longitude, latitude FROM airports WHERE icao = %s", (airport_icao,)
            )
            longitude, latitude = curs.fetchone()
        return longitude, latitude

    def get_airports_for_callsign(self, callsign: str) -> Optional[Tuple[str, str]]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT est_arrival_airport, est_departure_airport FROM callsign_memo "
                "WHERE callsign = %s", (callsign,)
            )
            coords = curs.fetchone()
            if coords:
                arrival_airport, departure_airport = coords
                return arrival_airport, departure_airport

    def update_unfinished_path(self, icao, old_last_update, path_travelled, new_last_update, arr_airp, dep_airp):
        with self.conn.cursor() as curs:
            if arr_airp is None and dep_airp is None:
                curs.execute(
                    "UPDATE flight_paths "
                    "SET path = path::jsonb || %s::jsonb, last_update = %s "
                    "WHERE icao24 = %s AND last_update = %s",
                    (path_travelled, new_last_update, icao, old_last_update)
                )
            else:
                curs.execute(
                    "UPDATE flight_paths "
                    "SET path = path::jsonb || %s::jsonb, last_update = %s, "
                    "departure_airport_icao = %s, arrival_airport_icao = %s "
                    "WHERE icao24 = %s AND last_update = %s",
                    (path_travelled, new_last_update, dep_airp, arr_airp, icao, old_last_update)
                )

    def delete_outdated_stats(self) -> None:
        with self.conn.cursor() as curs:
            curs.execute(
                "DELETE FROM airport_stats "
                "WHERE now() - the_date > interval '1 month' "
            )

    def update_stats_for_arrival_airport(self, airport: str, day: str) -> None:
        with self.conn.cursor() as curs:
            curs.execute(
                "UPDATE airport_stats "
                "SET airplane_quantity_arrivals = airplane_quantity_arrivals + 1"
                "WHERE airport_icao = %s AND the_date = %s",
                (airport, day),
            )

    def update_stats_for_departure_airport(self, airport: str, day: str) -> None:
        with self.conn.cursor() as curs:
            curs.execute(
                "UPDATE airport_stats "
                "SET airplane_quantity_departures = airplane_quantity_departures + 1"
                "WHERE airport_icao = %s AND the_date = %s",
                (airport, day),
            )

    def get_stats_for_airport(self, airport: str, update_day: str) -> Optional[AirportStats]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM airport_stats WHERE airport_icao = %s AND the_date = %s",
                (airport, update_day)
            )
            stats = curs.fetchone()
            if stats:
                stats = AirportStats(*stats[1:])
                return stats
            return None

    def insert_arrival_airport_stats(self, airport_icao, day):
        with self.conn.cursor() as curs:
            new_stats = AirportStats(
                airport_icao=airport_icao,
                the_date=day,
                airplane_quantity_arrivals=1,
                airplane_quantity_departures=0,
            )
            stats = new_stats._asdict()

            columns_str, values_str = column_value_to_str(new_stats._fields)
            curs.execute(
                f"INSERT INTO airport_stats ({columns_str})" f"VALUES ({values_str})",
                stats,
            )

    def insert_departure_airport_stats(self, airport_icao, day):
        with self.conn.cursor() as curs:
            new_stats = AirportStats(
                airport_icao=airport_icao,
                the_date=day,
                airplane_quantity_arrivals=0,
                airplane_quantity_departures=1,
            )
            stats = new_stats._asdict()

            columns_str, values_str = column_value_to_str(new_stats._fields)
            curs.execute(
                f"INSERT INTO airport_stats ({columns_str})" f"VALUES ({values_str})",
                stats,
            )

    def update_airport_stats_last_update(self, last_update):
        with self.conn.cursor() as curs:
            curs.execute(
                f"INSERT INTO airport_stats_last_update (last_stats_update_time) VALUES (%s)", (last_update,)
            )

    def get_stats_last_update(self) -> int:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT MAX(last_stats_update_time) FROM airport_stats_last_update;"
            )
            last_update = curs.fetchone()[0]
            if last_update:
                return last_update
            else:
                return 0

    def get_not_considered_paths(self, last_update) -> List[Tuple]:
        with self.conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM flight_paths WHERE last_update > %s;", (last_update,)
            )
            paths = curs.fetchall()
            return paths

    def execute_script(self, script: str) -> None:
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(script)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self.conn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.__exit__(exc_type, exc_val, exc_tb)
