import threading
import time
from contextlib import closing
import setup_logging as log

from flask import Flask
from flask_socketio import SocketIO
import eventlet
import psycopg2


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='eventlet')
logger = log.setup(path='../logging_config.yaml')

eventlet.monkey_patch()


@app.route('/')
def index():
    return app.send_static_file('index.html')


@socketio.on('connect')
def connect():
    print('go to http://localhost:5000')
    print('connected')
    logger.info('A client has been connected to the server')


@socketio.on('disconnect')
def disconnect():
    print('disconnected')
    logger.info('A client has been disconnected from the server')


inter_states = [0]


def fetch_vectors():
    # print('2nd thread has taken control')
    with closing(psycopg2.connect(dbname="opensky", user='mas5mk', password='$GV9^MJGk8gn')) as conn:
        with conn.cursor() as curs:
            while True:
                logger.info('Fetching from DB has started')
                curs.execute("SELECT * FROM opensky_state_vectors "
                             "WHERE request_time = (SELECT MAX(request_time) FROM opensky_state_vectors);")
                vectors = curs.fetchall()
                state_vectors_quantity = 0
                for _ in vectors:
                    state_vectors_quantity += 1
                inter_states[0] = vectors
                logger.info(f'Quantity of state vectors fetched from the DB for the time {vectors[0][0]}: '
                            f'{state_vectors_quantity}')
                time.sleep(5)


def broadcast_vectors():
    while True:
        # print('broadcast_vectors greenthread has taken control')
        socketio.send(inter_states[0])
        eventlet.sleep(4)
        time.sleep(1)


def start_app():
    # print('start_app greenthread has taken control')
    socketio.run(app)
    eventlet.sleep(0.1)


if __name__ == '__main__':
    fetching_thread = threading.Thread(target=fetch_vectors, daemon=True)
    fetching_thread.start()
    broadcasting_greenthread = eventlet.spawn(broadcast_vectors)
    app_launch_greenthread = eventlet.spawn(start_app)
    # print('returned to the main green thread of the thread 1')
    app_launch_greenthread.wait()
    broadcasting_greenthread.wait()
    fetching_thread.join()




