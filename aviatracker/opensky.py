import json
import logging
import socket
from typing import Dict, List, Optional
import time

from requests import exceptions, get, codes

from aviatracker.config import common_conf
from aviatracker.database import FlightAirportInfo, StateVector, OpenskyFlight

logger = logging.getLogger()


class Opensky(object):
    def __init__(self, username: Optional = None, password: Optional = None) -> None:
        if username is None or password is None:
            username, password = common_conf.opensky_user, common_conf.opensky_pass
        self.auth = (username, password)
        self.api_url = "https://opensky-network.org/api"

    def get_from_opensky(self, params: Dict, operation: str, timeout: int) -> Dict:
        try:
            r = get(
                "{}{}".format(self.api_url, operation),
                auth=self.auth,
                params=params,  # type: ignore
                timeout=timeout,
            )
            if r.status_code == codes.ok:
                logger.info("Successful connection to Opensky API.")
                response = json.loads(r.text)
                return response
            else:
                logger.error(f"Could not connect to Opensky API. Status code is {r.status_code}.")

        except (OSError, exceptions.ReadTimeout, socket.timeout) as e:
            logger.error(f"Could not get data from API {operation} endpoint: {e}. ")

    def get_current_states(self, time_sec: int = 0, icao24: str = None) -> Optional[List[StateVector]]:
        parameters = {"time": int(time_sec), "icao24": icao24}
        operation = "/states/all"

        resp: Optional[Dict] = self.get_from_opensky(parameters, operation, 15)

        if resp is not None:
            request_time = resp["time"]
            dirty_states = resp["states"]
            if dirty_states:
                states = [StateVector(*([request_time] + state)) for state in dirty_states]
                return states

    def get_flights_for_period(self, begin, period=3600) -> Optional[List[OpenskyFlight]]:
        """Gets flights history for an hour interval"""
        end = begin + period
        parameters = {"begin": begin, "end": end}
        operation = "/flights/all"

        resp: Optional[Dict] = self.get_from_opensky(parameters, operation, 35)

        if resp is not None:
            flights = [OpenskyFlight(**x) for x in resp]
            return flights
