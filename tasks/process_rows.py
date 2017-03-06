import rethinkdb as r
from connection import rethink, mysql
from mappers import columnas

class ProcessRowsTask:

    def __init__(self, rethink_job_id):
        self.rethink_job_id = rethink_job_id
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            self.record = r.table('job').get(self.rethink_job_id).run(conn)

    def finish(self):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            r.table('job').get(self.rethink_job_id).update(
                {'finished': True,
                 'processed_rows': self.record['num_rows']}).run(conn)
            number = r.table('job').filter({
                'process_id': self.record['process_id'],
                'finished': True}).count().run(conn)
            r.table('process').get(self.record['process_id']).\
                update({'processed_chunks': number}).\
                run(conn)

    def run(self):
        with mysql.Mysql() as db_connection:
            ids = db_connection.findIdByRange(self.record['id_min'], self.record['id_max'])
            events = db_connection.findEventsWhereIds(",".join(map(str, ids)))

        if len(events) > 0:
            process_col = columnas.Columnas(events, self.record['process_id'], self.record['file_name'])
            process_col.process()
        self.finish()
        return True
