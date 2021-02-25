import json
from contextlib import closing
from typing import List, Optional, Dict

from aviatracker.database import DB
from aviatracker.config import common_conf
from aviatracker.database import FlightPath


def update_flight_paths():
    params = common_conf.db_params
    with closing(DB(**params)) as db:
        with db:
            db.delete_outdated_paths()
            db.update_path_when_finished()

            states: Optional[List[Dict]] = db.get_current_states()

            if len(states) > 0:
                for state in states:
                    icao = state["icao24"]
                    callsign = state["callsign"]
                    update_time = state["request_time"]

                    unfinished_path: FlightPath = db.find_unfinished_path_for_aircraft(icao)

                    current_location = {
                        "longitude": state["longitude"],
                        "latitude": state["latitude"],
                    }
                    path = [json.dumps(current_location)]

                    if len(unfinished_path) == 0:
                        arrival_airport_icao, departure_airport_icao = db.get_airports_for_callsign(callsign)

                        new_path = FlightPath(
                            last_update=update_time,
                            icao24=icao,
                            callsign=callsign,
                            departure_airport_icao=departure_airport_icao,
                            arrival_airport_icao=arrival_airport_icao,
                            path=path,
                            finished=False,
                            finished_at=0
                        )

                        db.insert_path(new_path)
                    else:
                        last_update = unfinished_path["last_update"]
                        db.update_unfinished_path(icao, last_update, path, update_time)
