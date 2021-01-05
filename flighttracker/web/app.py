import eventlet
eventlet.monkey_patch()

import threading
import time
from contextlib import closing

from flask import Flask
from flask_socketio import SocketIO

from flighttracker import config
from flighttracker.database import DB
from flighttracker.log_settings import setup_logging as log


app = Flask(__name__)
logger = log.setup()
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


def fetch_vectors() -> None:
    with closing(
        DB(dbname="opensky", user=config.pg_username, password=config.pg_password, host=config.pg_hostname,
           port=config.pg_port_number)
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
    fetching_thread = threading.Thread(target=fetch_vectors, daemon=True)
    fetching_thread.start()
    broadcasting_greenthread = eventlet.spawn(broadcast_vectors)
    app_launch_greenthread = eventlet.spawn(start_app)
    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    fetching_thread.join()


if __name__ == "__main__":
    start_webapp()
