from requests import get
import config
import json


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

    def get_states(self, epoch_time=0, icao24=None):
        parameters = {"time": int(epoch_time), "icao24": icao24}
        r = get('{}{}'.format(self.api_url, self.url_operation), auth=self.auth, params=parameters, timeout=15)
        st = r.status_code
        # if st == 200:
        #     logger.info('Status_code is 200. Successful connection to Opensky API.')
        # else:
        #     logger.error(f'Could not connect to server. Status code is {st}')
        #     return get_states(epoch_time=epoch_time)
        if st != 200:
            return get_states(epoch_time=epoch_time)
        return json.loads(r.text)