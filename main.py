from tasks import ConnectionContainer, beat, send
from celery.result import AsyncResult
from time import sleep

from tasks import PersistentConnection, ConnectionStatus
# import io
import time

# add.delay(5, 6)
#
# try:
#     while True:
#         t = status.delay()  # type: AsyncResult
#         print(t.ready())
#         sleep(1)
#         print(t.ready())
#         sleep(10000)
# except KeyboardInterrupt:
#     pass



try:
    # socket_create.delay('127.0.0.1')
    # socket_main.delay()
    r = None  # type: AsyncResult
    while True:
        if r is None:
            r = beat.delay()
            print('Beat sent')
        else:
            if r.ready():
                print('Beat end')
                r.forget()
                r = None
                time.sleep(3)

                send.delay(1, 'afsg')





except KeyboardInterrupt:
    pass


