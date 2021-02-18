import os
from contextlib import closing

import click
import yaml

from aviatracker.config import common_conf
from aviatracker.database import Airport
from aviatracker.database.database import DB


@click.command(name="make-tables")
def make_tables():
    path = os.path.join(os.path.dirname(__file__), "make_tables.yaml")

    with closing(DB(**common_conf.db_params)) as db:
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
        db.insert_airports(airports)


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(fill_airports)

    cli()
