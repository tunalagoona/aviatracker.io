import eventlet

eventlet.monkey_patch()

from contextlib import closing
import threading
import time
from typing import List, Dict, Any

from flask import Flask, render_template
from flask_socketio import SocketIO

from flighttracker import utils
from flighttracker.database import DB
from flighttracker.config import common_conf


app = Flask(__name__)
logger = utils.setup_logging()

socketio = SocketIO(
    app,
    async_mode="eventlet",
    logger=True,
    engineio_logger=True,
    cors_allowed_origins=common_conf.websocket_allowed_origin,
)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/about", methods=["GET"])
def about():
    return render_template("about.html")


@socketio.on("connect")
def connect() -> None:
    logger.debug("A client has been connected to the server")


@socketio.on("disconnect")
def disconnect() -> None:
    logger.debug("A client has been disconnected from the server")


states_memo = None


def fetch_aircraft_states(params: Dict[str, Any]) -> None:
    with closing(DB(**params)) as db:
        while True:
            vectors: List[Dict] or None = db.get_current_states()
            quantity = len(vectors)
            global states_memo
            states_memo = vectors
            logger.info(f"Quantity of state vectors fetched from the DB for the time {vectors[0][0]}: " f"{quantity}")
            time.sleep(4)


def broadcast_vectors() -> None:
    while True:
        socketio.send(states_memo)
        eventlet.sleep(3)
        time.sleep(1)


def start_app() -> None:
    socketio.run(app, host="0.0.0.0", log_output=True)
    eventlet.sleep(0.1)


def start_webapp() -> None:
    fetching_thread = threading.Thread(
        target=fetch_aircraft_states,
        daemon=True,
        args=common_conf.db_params,
    )
    fetching_thread.start()
    broadcasting_greenthread = eventlet.spawn(broadcast_vectors)
    app_launch_greenthread = eventlet.spawn(start_app)
    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    fetching_thread.join()


if __name__ == "__main__":
    start_webapp()
