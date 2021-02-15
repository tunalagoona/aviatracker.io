import os

import yaml


class Config:
    def __init__(self):
        path = os.path.join(os.path.abspath(__file__ + '/../../'), 'config/config.yaml')

        with open(path, 'r') as config:
            parsed_yaml_file = yaml.load(config, Loader=yaml.FullLoader)
            self.db_name = parsed_yaml_file['db']['name']
            self.db_user = parsed_yaml_file['db']['user']
            self.db_pass = parsed_yaml_file['db']['pass']
            self.db_host = parsed_yaml_file['db']['host']
            self.db_port = parsed_yaml_file['db']['port']
            self.websocket_allowed_origin = parsed_yaml_file['websocket']['allowed_origin']
