import logging
import logging.config
import coloredlogs
import os

import yaml


def setup_logging(level=logging.INFO):
    script_dir = os.path.abspath(__file__ + "/../../")
    rel_path = "logging.yaml"
    path = os.path.join(script_dir, rel_path)
    with open(path, "rt") as f:
        try:
            configurations = yaml.safe_load(f.read())
            logging.config.dictConfig(configurations)
            coloredlogs.install()
        except Exception as e:
            print(e)
            print("Error in Logging Configuration. Using default configs")
            logging.basicConfig(level=level)
    log = logging.getLogger()
    log.info("logger has been initiated")
    return log
