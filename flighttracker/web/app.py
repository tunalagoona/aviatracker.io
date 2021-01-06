import eventlet

eventlet.monkey_patch()

import threading
import time
from contextlib import closing
import yaml
import os

from flask import Flask
from flask_socketio import SocketIO

from flighttracker import utils
from flighttracker.database import DB

app = Flask(__name__)
logger = utils.setup_logging()
socketio = SocketIO(app, async_mode="eventlet", logger=True, engineio_logger=True,
                    cors_allowed_origins="http://127.0.0.1:5000")


@app.route("/")
def index():
    return app.send_static_file("index.html")


@socketio.on("connect")
def connect() -> None:
    print("go to http://localhost:5000")
    print("connected")
    logger.info("A client has been connected to the server")


@socketio.on("disconnect")
def disconnect() -> None:
    print("disconnected")
    logger.info("A client has been disconnected from the server")


states_memo = None


def fetch_vectors(user, password, host, port) -> None:
    with closing(
            DB(dbname="opensky", user=user, password=password, host=host, port=port)
    ) as db:
        while True:
            logger.info("Fetching from DB has started")
            vectors, quantity = db.get_last_inserted_state()
            global states_memo
            states_memo = vectors
            logger.info(
                f"Quantity of state vectors fetched from the DB for the time {vectors[0][0]}: "
                f"{quantity}"
            )
            time.sleep(10)


def broadcast_vectors() -> None:
    while True:
        socketio.send(states_memo)
        eventlet.sleep(9)
        time.sleep(1)


def start_app() -> None:
    socketio.run(app, log_output=True)
    eventlet.sleep(0.1)


def start_webapp() -> None:
    script_dir = os.path.abspath(__file__ + "/../../../")
    rel_path = 'config/config.yaml'
    path = os.path.join(script_dir, rel_path)

    with open(path, 'r') as cnf:
        parsed_yaml_file = yaml.load(cnf, Loader=yaml.FullLoader)
        user_name = parsed_yaml_file['postgres']['pg_username']
        password = parsed_yaml_file['postgres']['pg_password']
        hostname = parsed_yaml_file['postgres']['pg_hostname']
        port_number = parsed_yaml_file['postgres']['pg_port_number']

    fetching_thread = threading.Thread(target=fetch_vectors, daemon=True,
                                       args=(user_name, password, hostname, port_number))
    fetching_thread.start()
    broadcasting_greenthread = eventlet.spawn(broadcast_vectors)
    app_launch_greenthread = eventlet.spawn(start_app)
    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    fetching_thread.join()


if __name__ == "__main__":
    start_webapp()
