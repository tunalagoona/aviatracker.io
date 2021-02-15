import eventlet
eventlet.monkey_patch()

from contextlib import closing
import threading
import time
from typing import List, Tuple, Dict

from flask import Flask, render_template
from flask_socketio import SocketIO

from flighttracker import utils
from flighttracker.database import DB
from flighttracker.parser import Config


app = Flask(__name__)
logger = utils.setup_logging()
conf = Config()

socketio = SocketIO(app, async_mode='eventlet', logger=True, engineio_logger=True,
                    cors_allowed_origins=conf.websocket_allowed_origin)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/about', methods=['GET'])
def about():
    return render_template('about.html')


@socketio.on('connect')
def connect() -> None:
    logger.info('A client has been connected to the server')


@socketio.on('disconnect')
def disconnect() -> None:
    logger.info('A client has been disconnected from the server')


states_memo = None


def fetch_vectors(db_name, db_user, db_pass, db_host, db_port) -> None:
    with closing(DB(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)) as db:
        while True:
            logger.info('Fetching from DB has started')
            vectors, quantity = db.get_last_inserted_state()
            global states_memo
            states_memo = vectors
            logger.info(
                f'Quantity of state vectors fetched from the DB for the time {vectors[0][0]}: '
                f'{quantity}'
            )
            time.sleep(4)


def make_object(vectors: List[Tuple] or None) -> List[Dict] or None:
    if vectors is None:
        return None
    objects = []
    for i in range(0, len(vectors)):
        vector_object = {'requestTime': vectors[i][0], 'icao24': vectors[i][1], 'callsign': vectors[i][2],
                         'originCountry': vectors[i][3], 'timePosition': vectors[i][4], 'lastContact': vectors[i][5],
                         'longitude': vectors[i][6], 'latitude': vectors[i][7], 'baroAltitude': vectors[i][8],
                         'onGround': vectors[i][9], 'velocity': vectors[i][10], 'trueTrack': vectors[i][11],
                         'verticalRate': vectors[i][12], 'sensors': vectors[i][13], 'geoAltitude': vectors[i][14],
                         'squawk': vectors[i][15], 'spi': vectors[i][16], 'positionSource': vectors[i][17]}
        objects.append(vector_object)
    return objects


def broadcast_vectors() -> None:
    while True:
        vector_object = make_object(states_memo)
        socketio.send(vector_object)
        eventlet.sleep(3)
        time.sleep(1)


def start_app() -> None:
    socketio.run(app, host='0.0.0.0', log_output=True)
    eventlet.sleep(0.1)


def start_webapp() -> None:
    # conf = ConfigParser()
    fetching_thread = threading.Thread(target=fetch_vectors, daemon=True,
                                       args=(conf.db_name, conf.db_user, conf.db_pass,
                                             conf.db_host, conf.db_port))
    fetching_thread.start()
    broadcasting_greenthread = eventlet.spawn(broadcast_vectors)
    app_launch_greenthread = eventlet.spawn(start_app)
    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    fetching_thread.join()


if __name__ == '__main__':
    start_webapp()
