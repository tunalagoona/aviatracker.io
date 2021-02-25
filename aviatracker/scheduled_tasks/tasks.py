import sys
import time
from contextlib import closing
from typing import Dict, List

from celery.app.log import TaskFormatter
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from aviatracker.config import common_conf
from aviatracker.database import DB, StateVector, OpenskyFlight
from aviatracker.opensky import OpenskyStates
from aviatracker.scheduled_tasks.celery import app
from aviatracker.core import update_flight_paths

logger = get_task_logger(__name__)


@after_setup_task_logger.connect
def setup_task_logger(logger, **kwargs) -> None:
    for handler in logger.handlers:
        handler.setFormatter(TaskFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))


@app.task(bind=True)
def update_callsigns(self) -> None:
    logger.debug("Starting task update_callsigns")
    self.time_limit = 15
    api = OpenskyStates()
    flights: List[OpenskyFlight] = api.get_callsigns_history()

    if flights:
        logger.debug(f"_____Received {len(flights)} Flights!!!!_______")
        with closing(DB(**common_conf.db_params)) as db:
            with db:
                for flight in flights:
                    if flight.callsign:
                        db.update_callsigns(
                            flight.callsign.strip().upper(),
                            flight.estArrivalAirport,
                            flight.estDepartureAirport
                        )


@app.task(bind=True)
def insert_states(self) -> None:
    self.time_limit = 15
    logger.debug("Starting task insert_states")
    cur_time = int(time.time())

    api = OpenskyStates()
    try:
        with closing(DB(**common_conf.db_params)) as db:
            with db:
                logger.debug(f"A request has been sent to API for the timestamp: {cur_time}")
                states: List[StateVector] or None = api.get_current_states(time_sec=cur_time)
                logger.debug(f"A response has been received from API for the timestamp: {cur_time}")
                if states:
                    logger.debug(f"received {len(states)} states")
                    db.insert_current_states(states)

    except Exception as e:
        logger.exception(f"Exception: {e}")

    old_outs = sys.stdout, sys.stderr
    rlevel = app.conf.worker_redirect_stdouts_level

    try:
        app.log.redirect_stdouts_to_logger(logger, rlevel)
    finally:
        sys.stdout, sys.stderr = old_outs


@app.task(bind=True)
def update_paths(self) -> None:
    self.time_limit = 10
    try:
        logger.debug("Starting flight paths update")
        update_flight_paths()
    except Exception as e:
        logger.exception(f"Exception: {e}")


# def update_airport_stats() -> None:
#     try:
#         with closing(DB(**common_conf.db_params)) as db:
#             with db:
#                 logger.info("Starting airports statistics update")
#                 db.update_airport_stats()
#     except Exception as e:
#         logger.exception(f"Exception: {e}")
#

# @app.task(bind=True)
# def update_stats(self) -> None:
#     self.time_limit = 10
#     update_airport_stats()
