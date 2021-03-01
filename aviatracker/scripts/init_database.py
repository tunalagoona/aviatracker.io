import logging
import os
from contextlib import closing
import time
from typing import List

import click
import yaml

from aviatracker.config import common_conf
from aviatracker.database import DB, Airport, OpenskyFlight
from aviatracker.opensky import Opensky


logger = logging.getLogger()


@click.group()
def cli():
    pass


@click.command(name="make-tables")
def make_tables() -> None:
    path = os.path.join(os.path.dirname(__file__), "make_tables.yaml")

    with closing(DB(**common_conf.db_params)) as db:
        with db:

            db.set_timezone()

            with open(path, "r") as f:
                scripts = yaml.load(f, Loader=yaml.FullLoader)
                for key in scripts.keys:
                    db.execute_script(scripts[key])


@click.command(name="fill-airports")
@click.option("--file", required=True, help="airport data file", type=str)
def fill_airports(file: str) -> None:
    with open(file) as f:
        values = [line.split(",") for line in f.readlines()]
        airports = []
        for value in values:
            airports.append(Airport(*value))

    with closing(DB(**common_conf.db_params)) as db:
        with db:
            db.insert_airports(airports)


@click.command(name="fill-callsigns")
def fill_callsigns() -> None:
    """Fill callsigns for the period [yesterday - 2 weeks, yesterday].
    There is a delay of when finished flights appear in /flights/all"""
    api = Opensky()
    with closing(DB(**common_conf.db_params)) as db:
        yesterday = int(time.time()) - 86400  # 1 day ago
        begin = yesterday - 1209600  # 2 weeks + 1 day ago

        while begin < yesterday:
            with db:
                flights: List[OpenskyFlight] = api.get_flights_for_period(begin)
                if flights:
                    db.upsert_callsigns(flights)
                    begin += 3600
                    logger.info(f"{len(flights)} flights upserted")
                    time.sleep(30)


if __name__ == "__main__":
    cli.add_command(fill_airports)
    cli.add_command(make_tables)
    cli.add_command(fill_callsigns)

    cli()
