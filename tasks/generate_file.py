import rethinkdb as r
from connection import rethink
from mappers.join_files import JoinFiles
import os


class GenerateFile:

    def __init__(self, rethink_process_id):
        self.rethink_process_id = rethink_process_id
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            self.record = r.table('process').get(self.rethink_process_id).run(conn)

            self.number = r.table('job').filter({
                'process_id': self.record['id'],
                'finished': True}).count().run(conn)

            r.table('process').get(self.record['id']).\
                update({'processed_chunks': self.number}).\
                run(conn)

    def run(self):
        if self.number >= self.record['chunks']:
            with rethink.Rethink() as rethink_conn:
                rethink_conn.set_database()
                conn = rethink_conn.get_connection()
                r.table('process').get(self.record['id'])\
                    .update({'finished': True})\
                    .run(conn)

            path = os.path.join('/', 'home', 'admin', 'data', 'worker', self.record['id'])
            JoinFiles().run(path, self.record['file_name'])
