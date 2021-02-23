import json
from contextlib import closing
from typing import List, Optional, Dict

from aviatracker.database import DB
from aviatracker.config import common_conf
from aviatracker.opensky import OpenskyStates
from aviatracker.database import (
    FlightAirportInfo,
    FlightPath
)


def update_flight_paths():
    params = common_conf.db_params
    with closing(DB(**params)) as db:
        with db:
            db.delete_outdated_paths()
            db.update_finished_flag()

            states: Optional[List[Dict]] = db.get_current_states()

            if len(states) > 0:
                cur_states_time = states[0]["request_time"]

                api = OpenskyStates()
                airports_response: List[FlightAirportInfo] = api.get_airports(
                    begin=cur_states_time - 5, end=cur_states_time + 5
                )

                for state in states:
                    unfinished_path: FlightPath = db.find_unfinished_path_for_aircraft(state["icao24"])

                    for flight in airports_response:
                        if flight["icao24"] == state["icao24"]:
                            airport_long, airport_lat = db.get_airport_long_lat('"' + arrival_airport_icao + '"')

                            departure_airport_icao = flight["estDepartureAirport"]
                            arrival_airport_icao = flight["estArrivalAirport"]
                            estimated_arr_time = flight["estArrivalTime"]
                            arrival_airport_lat = airport_lat
                            arrival_airport_long = airport_long
                            break

                    current_location = {
                        "longitude": state["longitude"],
                        "latitude": state["latitude"],
                    }
                    path = [json.dumps(current_location)]
                    icao = state["icao24"]

                    if len(unfinished_path) == 0:
                        new_path = FlightPath(
                            last_update=state["request_time"],
                            icao24=icao,
                            departure_airport_icao=departure_airport_icao,
                            arrival_airport_icao=arrival_airport_icao,
                            arrival_airport_long=arrival_airport_long,
                            arrival_airport_lat=arrival_airport_lat,
                            estimated_arrival_time=estimated_arr_time,
                            path=path,
                            finished=False,
                            finished_at=0
                        )
                        db.insert_path(new_path)
                    else:
                        last_update = unfinished_path["last_update"]
                        db.update_unfinished_path(icao, last_update, path)
