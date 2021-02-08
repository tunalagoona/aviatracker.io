import os

import yaml


class ConfigParser:
    def __init__(self, host):
        script_dir = os.path.abspath(__file__ + "/../../")
        rel_path = 'config/config.yaml'
        path = os.path.join(script_dir, rel_path)
        with open(path, 'r') as cnf:
            parsed_yaml_file = yaml.load(cnf, Loader=yaml.FullLoader)
            pg_key = 'postgres_' + host
            dbname = parsed_yaml_file[pg_key]['pg_dbname']
            user_name = parsed_yaml_file[pg_key]['pg_username']
            password = parsed_yaml_file[pg_key]['pg_password']
            hostname = parsed_yaml_file[pg_key]['pg_hostname']
            port_number = parsed_yaml_file[pg_key]['pg_port_number']

            cors_key = 'cors_' + host
            origin = parsed_yaml_file[cors_key]['origin']

        self.dbname = dbname
        self.user_name = user_name
        self.password = password
        self.hostname = hostname
        self.port_number = port_number
        self.origin = origin
