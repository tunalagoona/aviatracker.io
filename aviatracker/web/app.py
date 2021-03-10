import eventlet

eventlet.monkey_patch()

from contextlib import closing
import threading
import time
from typing import List, Dict, Any, Optional

from flask import Flask, render_template
from flask_socketio import SocketIO

from aviatracker import utils
from aviatracker.database import DB
from aviatracker.config import common_conf


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


@socketio.on("message")
def send_airports(message) -> None:
    logger.info("server received message and going to reply")
    airports: Optional[List[Dict]] = fetch_airports(common_conf.db_params)
    if len(airports) != 0:
        airports_data = ['airports', airports]
        socketio.send(airports_data)


@socketio.on("disconnect")
def disconnect() -> None:
    logger.debug("A client has been disconnected from the server")


states_memo = None


def fetch_airports(params: Dict[str, Any]) -> Optional[List[Dict]]:
    with closing(DB(**params)) as db:
        with db:
            airports: Optional[List[Dict]] = db.get_all_airports()
    return airports


def fetch_aircraft_states(params: Dict[str, Any]) -> None:
    with closing(DB(**params)) as db:
        with db:
            while True:
                vectors: Optional[List[Dict]] = db.get_current_states()
                if len(vectors) != 0:
                    quantity = len(vectors)
                    global states_memo
                    states_memo = vectors
                    logger.info(f"{quantity} states fetched from the DB for the time {vectors[0]['request_time']}")
                    time.sleep(4)


def broadcast_states() -> None:
    while True:
        socketio.send(states_memo)
        eventlet.sleep(3)
        time.sleep(1)


def fetch_aircraft_current_paths(params: Dict[str, Any]) -> None:
    while True:
        current_flights: Optional[List[Dict]] = states_memo
        paths = []
        with closing(DB(**params)) as db:
            with db:
                for flight in current_flights:
                    icao = flight["icao24"]
                    icao_paths: Optional[Dict] = db.find_unfinished_path_for_aircraft(icao)
                    if icao_paths:
                        paths.append(icao_paths)
        paths = ["paths", paths]
        socketio.send(paths)
        eventlet.sleep(19)
        time.sleep(1)


def start_app() -> None:
    socketio.run(app, host="0.0.0.0", log_output=True)
    eventlet.sleep(0.1)


def start_webapp() -> None:
    fetching_thread = threading.Thread(
        target=fetch_aircraft_states,
        daemon=True,
        args=(common_conf.db_params,),
    )
    fetching_thread.start()

    broadcasting_greenthread = eventlet.spawn(broadcast_states)
    path_retrieving_greenthread = eventlet.spawn(fetch_aircraft_current_paths)
    app_launch_greenthread = eventlet.spawn(start_app)

    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    path_retrieving_greenthread.wait()

    fetching_thread.join()


if __name__ == "__main__":
    start_webapp()
