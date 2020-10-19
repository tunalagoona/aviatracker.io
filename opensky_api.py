from requests import get
import config
import json
from typing import List, Dict


State_vector = Dict
State_vectors = List[State_vector]


class StateVector(object):
    def __init__(self, st_vector, request_time):
        self.keys = ["request_time", "icao24", "callsign", "origin_country", "time_position",
                     "last_contact", "longitude", "latitude", "baro_altitude", "on_ground",
                     "velocity", "true_track", "vertical_rate", "sensors",
                     "geo_altitude", "squawk", "spi", "position_source"]
        self.values = [request_time] + st_vector
        self.dict = dict(zip(self.keys, self.values))


class OpenskyStates(object):
    def __init__(self, username=config.os_username, password=config.os_password):
        self.auth = (username, password)
        self.api_url = "https://opensky-network.org/api"
        self.url_operation = "/states/all"

    def get_states(self, time_sec=0, icao24=None) -> [State_vectors, int]:
        parameters = {"time": int(time_sec), "icao24": icao24}
        r = get('{}{}'.format(self.api_url, self.url_operation), auth=self.auth, params=parameters, timeout=15)
        st = r.status_code
        if st == 200:
            raw_states = json.loads(r.text)
            request_time = raw_states["time"]
            print('response time: ', request_time)
            current_states = raw_states["states"]
            states = [StateVector(st_vector=state, request_time=request_time).dict for state in current_states]
        else:
            return ['-', st]
        return [states, st]
