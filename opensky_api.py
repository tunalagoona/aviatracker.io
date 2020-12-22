import socket
import config
import json
from typing import List, Dict, Union
import logging

from requests import get, exceptions


State_vector = Dict
State_vectors = List[State_vector]

module_logger = logging.getLogger()


class StateVector(object):
    def __init__(self, st_vector: List, request_time: int):
        self.keys = [
            "request_time",
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source",
        ]
        self.values = [request_time] + st_vector
        self.dict = dict(zip(self.keys, self.values))


class OpenskyStates(object):
    def __init__(self, username=config.os_username, password=config.os_password):
        self.auth = (username, password)
        self.api_url = "https://opensky-network.org/api"
        self.url_operation = "/states/all"

    def get_states(self, time_sec: int = 0, icao24: str = None) -> State_vectors:
        module_logger.info("get_states has started")
        parameters: Dict[str, Union[int, str, None]] = {"time": int(time_sec), "icao24": icao24}  # type: ignore
        try:
            r = get(
                "{}{}".format(self.api_url, self.url_operation),
                auth=self.auth,
                params=parameters,
                timeout=15,
            )
            st_code = r.status_code
            if st_code == 200:
                module_logger.info(
                    "Status_code is 200. Successful connection to Opensky API."
                )
                response = json.loads(r.text)
                request_time = response["time"]
                dirty_states = response["states"]
                states = [
                    StateVector(st_vector=state, request_time=request_time).dict
                    for state in dirty_states
                ]
                return states
            else:
                module_logger.error(
                    f"Could not connect to Opensky API. Status code is {st_code}"
                )
                return self.get_states(time_sec)
        except (OSError, exceptions.ReadTimeout, socket.timeout) as e:
            module_logger.error(f"Could not get state vectors from API: {e}")
            return self.get_states(time_sec)
