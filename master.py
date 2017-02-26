from connection import mysql
import rethinkdb as r
from connection import rethink
from worker import process_rows_task
from celery.result import AsyncResult
import numpy as np
import time

# This class will only search for incomplete process, will check whether the process has not started yet, or there
# has been a problem in the way. given the case there was problem during a last process, automatically will search which
# chunks of data got lost in the way, by either an error during the execution or the job was never queued
# and will try to reprocess them again.
# In case is just a process has not started yet will get the data to process, chunked and  queued for further processing
class Master:
    def __init__(self, chunk_size=20, id_key='id'):
        self.chunk_size = chunk_size
        self.id_key = id_key

    def start_process(self, process_id):
        with mysql.Mysql() as db_connection:
            id_min = db_connection.getIdMin()
            id_max = db_connection.getIdMax()
            cursor = db_connection.get_data().get_cursor()
            number_of_rows = cursor.rowcount
            if number_of_rows > 0:
                number_of_chunks = np.ceil(float(number_of_rows)/float(self.chunk_size))
                timestamp = time.time()
                filename = str(timestamp) + "_"+process_id+".csv"
                with rethink.Rethink() as rethink_conn:
                    rethink_conn.set_database()
                    conn = rethink_conn.get_connection()
                    resp = r.table('process').get(process_id).update({
                        'num_records': number_of_rows,
                        'chunks': number_of_chunks,
                        'processed_chunks': 0,
                        'chunk_size': int(self.chunk_size),
                        'id_min': id_min,
                        'id_max': id_max,
                        'file_name': filename,
                        'timestamp': timestamp,
                        'uploaded': False,
                    }).run(conn)
                    if resp['errors'] == 0:
                        self.chunk_data(cursor, process_id, int(self.chunk_size))

    def chunk_data(self, cursor, process_id, chunk_size, job_no=0):
        update_min = True
        id_min = 0
        num_rows = 0
        for element in cursor:
            num_rows += 1
            if update_min:
                update_min = False
                id_min = element['id']
                job_no += 1
            if cursor.rownumber % chunk_size == 0:
                try:
                    self.create_job_register(job_no, process_id, id_min, element['id'], num_rows)
                except Exception as e:
                    # There was en error while creating job, no problem will be retryed later
                    # Perhaps adding a logger using the process key as a handler will give further information
                    print(e)
                finally:
                    update_min = True
                    num_rows= 0

    def create_job_register(self, job_no, process_id, id_min, id_max, num_rows):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            res = r.table('job').insert({
                'process_id': process_id,
                'job_no': job_no, # this is basically the chunk number
                'id_min': id_min,
                'id_max': id_max,
                'finished': False,
                'num_rows': num_rows,
                'processed_rows': 0,
                'file_name': str(process_id)+"_"+str(job_no)+".csv",
                'file_processed': False}).run(conn)
            if res['errors'] == 0:
                # will queue the job only in the case there was not an error, otherwise do not worry
                # we will handle it in the function search_failed_jobs
                self.queue_chunk(res['generated_keys'][0])

    def queue_chunk(self, rethink_job_id):
        task_queued = process_rows_task.delay(rethink_job_id)
        task_id = task_queued.id
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            r.table('job').get(rethink_job_id).update({'queue_id': task_id}).run(conn)

    def search_incomplete_process(self):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            processes = r.table('process').filter({'finished': False}).run(conn)
            for process in processes:
                # the process started and can either still processing or has failed jobs
                if 'num_records' in process:
                    self.search_failed_jobs(process)
                #the process has not started yes
                else:
                    self.start_process(process['id'])

    def search_failed_jobs(self, process):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            jobs = r.table('job').filter(
                {'process_id': process['id']})\
                .order_by(r.asc('job_no')).run(conn)
            min_id = int(process['id_min'])
            max_id = process['id_max']
            job_no = 0
            for job in jobs:
                job_diff = int(job['job_no']) - job_no
                # In case for some reason we miss processed a job, It's ok we will handle it
                if job_diff > 1:
                    job_no = int(job['job_no'])
                    self.retry_job(job_no=job_no, min_id=min_id, max_id=int(job['id_min']) - 1,
                                   chunk_size=process['chunk_size'], process_id=process['id'])

                job_no = int(job['job_no'])

                if job['finished']:
                    continue
                # In case the job just failed, it is ok, as well :)
                if self.is_falilure(job):
                    self.retry_job(rethink_job_id=job['id'])

                min_id = int(job['id_max']) + 1

            if int(process['chunks']) > job_no:
                self.retry_job(job_no=job_no, min_id=min_id, max_id=int(max_id),
                               chunk_size=process['chunk_size'], process_id=process['id'])

    def is_falilure(self, job):
        if 'queue_id' in job:
            res = AsyncResult(job['queue_id'])
            return res.failed() or not res.ready()
        # the job was not even queued so lets queued
        return True

    def retry_job(self, rethink_job_id=None, **kwars):
        try:
            if rethink_job_id:
                self.queue_chunk(rethink_job_id)
            else:
                with mysql.Mysql() as db_connection:
                    cursor = db_connection.findIdByRange(kwars['min_id'], kwars['max_id']).get_cursor()
                    self.chunk_data(cursor, kwars['process_id'], kwars['chunk_size'], kwars['job_no'])
        except Exception as e:
            print(e)

    def start(self):
        self.search_incomplete_process()


