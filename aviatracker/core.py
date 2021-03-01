import json
import logging
from contextlib import closing
from datetime import datetime
from typing import List, Optional, Dict

from aviatracker.database import DB
from aviatracker.config import common_conf
from aviatracker.database import FlightPath


logger = logging.getLogger()
params = common_conf.db_params


def update_flight_paths():
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
                    path: str = json.dumps(current_location)

                    airports = db.get_airports_for_callsign(callsign)
                    if airports:
                        arrival_airport_icao, departure_airport_icao = airports
                    else:
                        arrival_airport_icao, departure_airport_icao = None, None

                    if unfinished_path:
                        last_update = unfinished_path.last_update
                        db.update_unfinished_path(icao, last_update, path, update_time,
                                                  arrival_airport_icao, departure_airport_icao)
                    else:
                        path = json.dumps([current_location])
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


def update_airport_stats():
    with closing(DB(**params)) as db:
        with db:
            db.delete_outdated_stats()

            last_update: int = db.get_stats_last_update()
            logger.info(f"_______________________STATS last_update: {last_update}")

            not_considered_paths = db.get_not_considered_paths(last_update)
            logger.info(f"Quantity of not considered in stats calculation paths: {len(not_considered_paths)}")

            max_update = 0

            for path in not_considered_paths:
                path = FlightPath(*path[1:])
                update = path.last_update
                update_day = datetime.utcfromtimestamp(update).strftime('%Y-%m-%d')

                arrival_airport = path.arrival_airport_icao
                if arrival_airport:
                    arr_stats = db.get_stats_for_airport(arrival_airport, update_day)
                    if arr_stats:
                        db.update_stats_for_arrival_airport(arrival_airport, update_day)
                    else:
                        db.insert_arrival_airport_stats(arrival_airport, update_day)

                departure_airport = path.departure_airport_icao
                if departure_airport:
                    dep_stats = db.get_stats_for_airport(departure_airport, update_day)
                    if dep_stats:
                        db.update_stats_for_departure_airport(departure_airport, update_day)
                    else:
                        db.insert_departure_airport_stats(departure_airport, update_day)

                if update > max_update:
                    max_update = update

            if max_update != 0:
                db.update_airport_stats_last_update(max_update)
