from celery import Celery
import os

RABBIT_USER = os.environ.get('RABBIT_USER', 'guest')
RABBIT_PASS = os.environ.get('RABBIT_PASS', 'guest')
RABBIT_SERVER = os.environ.get('RABBIT_SERVER', 'rabbitmq')
RABBIT_VHOST = os.environ.get('RABBIT_VHOST', '')
BROKER_URL = "amqp://{}:{}@{}:5672/{}".format(RABBIT_USER, RABBIT_PASS, RABBIT_SERVER, RABBIT_VHOST)

app = Celery('worker', broker=BROKER_URL)
