from contextlib import closing

import click

from flighttracker.parser import Config
from flighttracker.database import Airport
from flighttracker.database.database import DB


@click.command(name='fill-airports')
@click.option('--file', required=True, help='airport data file', type=str)
def fill_airports(file: str) -> None:
    with open(file) as f:
        values = [line.split(',') for line in f.readlines()]

    conf = Config()
    with closing(DB(dbname=conf.db_name, user=conf.db_user, password=conf.db_pass, host=conf.db_host, port=conf.db_port)) as db:
        for value in values:
            db.insert(Airport(*value))


@click.group()
def cli():
    pass


if __name__ == '__main__':
    cli.add_command(fill_airports)

    cli()
