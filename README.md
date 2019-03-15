- launch redis `docker run -d --rm -p 6379:6379 redis:alpine`

- launch dev_server.py

- launch `celery -A tasks worker -P eventlet -c 1000 -l info`

- launch main.py
