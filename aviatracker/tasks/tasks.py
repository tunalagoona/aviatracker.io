import sys
import time
from contextlib import closing
from typing import List, Optional

from celery.app.log import TaskFormatter
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from aviatracker.config import common_conf
from aviatracker.database import DB, StateVector, OpenskyFlight
from aviatracker.opensky import Opensky
from aviatracker.tasks.celery import app
from aviatracker.core import update_flight_paths, update_airport_stats

logger = get_task_logger(__name__)


@after_setup_task_logger.connect
def setup_task_logger(logger, **kwargs) -> None:
    for handler in logger.handlers:
        handler.setFormatter(TaskFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))


@app.task(bind=True)
def update_callsigns(self) -> None:
    logger.debug("Starting task update_callsigns")
    self.time_limit = 15
    api = Opensky()
    flights: Optional[List[OpenskyFlight]] = api.get_flights_for_period(int(time.time()) - 172800)

    if flights:
        logger.debug(f"Received {len(flights)} flights")
        with closing(DB(**common_conf.db_params)) as db:
            with db:
                db.upsert_callsigns(flights)


@app.task(bind=True)
def insert_states(self) -> None:
    self.time_limit = 10
    logger.debug("Starting task insert_states")
    cur_time = int(time.time())

    api = Opensky()
    try:
        logger.debug(f"A request has been sent to API for the timestamp: {cur_time}")
        states: List[StateVector] or None = api.get_current_states(time_sec=cur_time)
        logger.debug(f"A response has been received from API for the timestamp: {cur_time}")
        if states:
            logger.debug(f"received {len(states)} states")
            with closing(DB(**common_conf.db_params)) as db:
                with db:
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


@app.task(bind=True)
def update_stats(self) -> None:
    self.time_limit = 10
    try:
        with closing(DB(**common_conf.db_params)) as db:
            with db:
                logger.debug("Starting airports statistics update")
                update_airport_stats()
    except Exception as e:
        logger.exception(f"Exception: {e}")
