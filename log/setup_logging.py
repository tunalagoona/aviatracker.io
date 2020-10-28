import logging
import logging.config

import yaml


def setup(path='/Users/mas5mk/Work/Flight-tracker/log/logging_config.yaml', level=logging.INFO):
    with open(path, 'rt') as f:
        try:
            configurations = yaml.safe_load(f.read())
            logging.config.dictConfig(configurations)
            # print(f'Logging configurations have been implemented successfully.')
        except Exception as e:
            print(e)
            print('Error in Logging Configuration. Using default configs')
            logging.basicConfig(level=level)
    log = logging.getLogger(__name__)
    log.info('logger was initiated')
    return log
