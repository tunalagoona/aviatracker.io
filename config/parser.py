import os

import yaml


class ConfigParser:
    script_dir = os.path.abspath(__file__ + "/../../")
    rel_path = 'config/config.yaml'
    path = os.path.join(script_dir, rel_path)
    with open(path, 'r') as cnf:
        parsed_yaml_file = yaml.load(cnf, Loader=yaml.FullLoader)
        dbname = parsed_yaml_file['postgres']['pg_dbname']
        user_name = parsed_yaml_file['postgres']['pg_username']
        password = parsed_yaml_file['postgres']['pg_password']
        hostname = parsed_yaml_file['postgres']['pg_hostname']
        port_number = parsed_yaml_file['postgres']['pg_port_number']

    def __init__(self):
        self.dbname = dbname
        self.user_name = user_name
        self.password = password
        self.hostname = hostname
        self.port_number = port_number
