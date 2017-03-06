from celery import Celery
import os
from tasks.process_rows import ProcessRowsTask

RABBIT_USER = os.environ.get('RABBIT_USER', 'guest')
RABBIT_PASS = os.environ.get('RABBIT_PASS', 'guest')
RABBIT_SERVER = os.environ.get('RABBIT_SERVER', 'rabbitmq')
RABBIT_VHOST = os.environ.get('RABBIT_VHOST', '')
BROKER_URL = "amqp://{}:{}@{}:5672/{}".format(RABBIT_USER, RABBIT_PASS, RABBIT_SERVER, RABBIT_VHOST)

celery_app = Celery('worker', broker=BROKER_URL, backend=BROKER_URL)


@celery_app.task(bind=True)
def process_rows_task(self, rethink_job_id):
    process = ProcessRowsTask(rethink_job_id)
    process.run()
