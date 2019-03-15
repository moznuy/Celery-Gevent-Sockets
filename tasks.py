from celery import Celery

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0') ## pyamqp://guest@localhost//


from typing import Dict, Callable, List
import socket
import time
from eventlet.semaphore import BoundedSemaphore


from enum import Enum
class ConnectionStatus(Enum):
    NOT_CONNECTED = 1
    CONNECTED = 2
     # = 3


class ParallelReadWrite(Exception):
    pass


class PersistentConnection:
    def __init__(self, on_recv: Callable[[str, bytes], None], _id, ip, port=8585):
        self._on_recv = on_recv
        # self._out_buff = out_buff
        self._c_status = None  # type: ConnectionStatus
        self._socket = None  # type: socket.socket
        self._address = ip, port
        self._id = _id
        self._sock_init()
        self._data = None
        self._lock_recv = BoundedSemaphore()
        self._lock_send = BoundedSemaphore()

    def _sock_init(self):
        self._c_status = ConnectionStatus.NOT_CONNECTED
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)

    def _connect(self):
        self._socket.settimeout(2)
        try:
            self._socket.connect(self._address)
        except ConnectionError:
            print("Connection error")
            self._c_status = ConnectionStatus.NOT_CONNECTED
            time.sleep(0.2)
        except socket.timeout:
            print("Connection timeout")
            self._c_status = ConnectionStatus.NOT_CONNECTED
        else:
            print("Connected")
            self._c_status = ConnectionStatus.CONNECTED
            self._socket.settimeout(5)

    def _disconnect(self):
        print("Disconnect")
        if self._c_status == ConnectionStatus.CONNECTED:
            self._socket.close()
            self._sock_init()

    def _connected_main(self):
        try:
            if self._lock_recv.locked():
                raise ParallelReadWrite

            with self._lock_recv:
                buff = self._socket.recv(2048)

            if len(buff) == 0:
                self._disconnect()
                return

            self._on_recv(self._id, buff)
        except socket.timeout:
            print("Nothing")

        except ParallelReadWrite:
            print(f'[main] {self._id} locked read')

        except ConnectionResetError:
            print("Reset By Peer")
            self._sock_init()
            return


        try:
            if self._data is not None:
                if self._lock_send.locked():
                    raise ParallelReadWrite
                with self._lock_send:
                    print('SENDIND ')
                    self._socket.send(self._data)
                self._data = None
        except socket.timeout:
            print("SEND TIMEOUT !!!!!!!!!!!!!")

        except ConnectionResetError:
            print("Reset By Peer")
            self._sock_init()
            return

        except ParallelReadWrite:
            print(f'[main] {self._id} locked write')


    def _disconnected_main(self):
        self._connect()

    def main(self):
        method = {
            ConnectionStatus.CONNECTED:         self._connected_main,
            ConnectionStatus.NOT_CONNECTED:     self._disconnected_main
        }

        method[self._c_status]()

    def send(self, data):
        if self._data is not None:
            raise RuntimeError("wait for can_send")
        self._data = data
        print(f'Ready to send {self._data}')

    # @app.task
    # def send(self, data):
    #     self.send(data)

    def can_send(self):
        return self._c_status == ConnectionStatus.CONNECTED and self._data is None


    def _ping(self):
        pass

    @property
    def status(self):
        return self._c_status


class ConnectionContainer:
    def __init__(self):
        self._container = {}  # type: Dict[str, PersistentConnection]

    @staticmethod
    def on_recv(_id: str, data: bytes):
        print(f'recv {_id} {data}')

    def add(self, _id, ip):
        if _id in self._container:
            raise KeyError("Already in container")
        self._container[_id] = PersistentConnection(ConnectionContainer.on_recv, _id, ip)

    instance = None  # type: ConnectionContainer

@app.task
def send(_id, data: str):
    c = ConnectionContainer.instance
    connection = c._container.get(_id)
    if connection is None:
        raise KeyError("Not Found")

    if connection.can_send():
        print(f"{_id} send")
        connection.send(data.encode())
    else:
        print("not sent yet")


@app.task
def main(_id=None):
    c = ConnectionContainer.instance
    connection = c._container.get(_id)
    if connection is not None:
        print(f"{_id} MAIN")
        connection.main()
        print(f"{_id} MAIN END")
        return

    print("ALL MAIN")
    for _id, connection in c._container.items():
        main.delay(_id)

@app.task
def beat():
    c = ConnectionContainer.instance
    if c is None:
        print("New CONTAINER")
        c = ConnectionContainer()
        c.add(1, '127.0.0.1')
        ConnectionContainer.instance = c

    print('BEAT')
    # TODO: check cameras
    main.delay()
