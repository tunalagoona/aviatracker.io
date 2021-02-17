import logging
import sys
import time
from contextlib import closing
from typing import Dict, List

from celery.app.log import TaskFormatter
from celery.signals import after_setup_task_logger
from celery.utils.log import get_task_logger

from flighttracker.config import common_conf
from flighttracker.database import DB, StateVector
from flighttracker.opensky import OpenskyStates
from flighttracker.scheduled_tasks.celery import app

logger = get_task_logger(__name__)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


@after_setup_task_logger.connect
def setup_task_logger(logger, **kwargs) -> None:
    for handler in logger.handlers:
        handler.setFormatter(TaskFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))


def update_aircraft_states(cur_time: int) -> None:
    api = OpenskyStates()
    try:
        with closing(DB(**common_conf.db_params)) as db:
            logger.info(f"A request has been sent to API for the timestamp: {cur_time}")
            states: List[Dict] or None = api.get_current_states(time_sec=cur_time)
            logger.info(f"A response has been received from API for the timestamp: {cur_time}")
            quantity = len(states)
            if quantity != 0:
                resp_time: int = states[0]["request_time"]
                state_vectors = []
                for state in states:
                    state_vectors.append(StateVector(**state))

                db.insert_current_states(state_vectors)
                logger.info(f"{quantity} state vectors inserted to DB for the timestamp {resp_time}")

    except Exception as e:
        logger.exception(f"Exception: {e}")


def update_flight_paths() -> None:
    try:
        with closing(DB(**common_conf.db_params)) as db:
            logger.info("Starting flight paths update")
            db.update_paths()
    except Exception as e:
        logger.exception(f"Exception: {e}")


def update_airport_stats() -> None:
    try:
        with closing(DB(**common_conf.db_params)) as db:
            logger.info("Starting airports statistics update")
            db.update_airport_stats()
    except Exception as e:
        logger.exception(f"Exception: {e}")


@app.task(bind=True)
def insert_states(self) -> None:
    self.time_limit = 15
    cur_time = int(time.time())
    update_aircraft_states(cur_time)
    old_outs = sys.stdout, sys.stderr
    rlevel = app.conf.worker_redirect_stdouts_level
    try:
        app.log.redirect_stdouts_to_logger(logger, rlevel)
    finally:
        sys.stdout, sys.stderr = old_outs


@app.task(bind=True)
def update_paths(self) -> None:
    self.time_limit = 10
    update_flight_paths()


@app.task(bind=True)
def update_stats(self) -> None:
    self.time_limit = 10
    update_airport_stats()
