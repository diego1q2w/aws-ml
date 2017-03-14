import rethinkdb as r
from connection import rethink
from mappers.join_files import JoinFiles
import os

class GenerateFile:

    def __init__(self, rethink_job_id):
        self.rethink_job_id = rethink_job_id
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            self.record = r.table('job').get(self.rethink_job_id).run(conn)

    def update(self):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            number = r.table('job').filter({
                'process_id': self.record['process_id'],
                'finished': True}).count().run(conn)
            r.table('process').get(self.record['process_id']).\
                update({'processed_chunks': number}).\
                run(conn)
        return number

    def run(self):
        number = self.update()
        if number >= self.record['chunks']:
            with rethink.Rethink() as rethink_conn:
                rethink_conn.set_database()
                conn = rethink_conn.get_connection()
                r.table('process').get(self.record['process_id']).\
                update({'finished': True}).\
                run(conn)
            path = os.path.join('/','home','admin','data', 'worker', self.record['id'])
            JoinFiles().run(path, self.record['file_name'])
