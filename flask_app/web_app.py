import threading
import time
from contextlib import closing

from flask import Flask
from flask_socketio import SocketIO
import eventlet
import psycopg2


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='eventlet')

eventlet.monkey_patch()


@app.route('/')
def index():
    return app.send_static_file('index.html')


@socketio.on('connect')
def connect():
    print('go to http://localhost:5000')
    print('connected')


@socketio.on('disconnect')
def disconnect():
    print('disconnected')


inter_states = [0]


def fetch_vectors():
    print('2nd thread has taken control')
    with closing(psycopg2.connect(dbname="opensky", user='mas5mk', password='$GV9^MJGk8gn')) as conn:
        with conn.cursor() as curs:
            while True:
                curs.execute("SELECT * FROM opensky_state_vectors "
                             "WHERE request_time = (SELECT MAX(request_time) FROM opensky_state_vectors);")
                vectors = curs.fetchall()
                inter_states[0] = vectors
                # print(inter_states)
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




