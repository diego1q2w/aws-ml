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

    def run(self):
        if 'finished' in self.record and self.record['finished']:
            return True
        with mysql.Mysql() as db_connection:
            try:
                ids = db_connection.find_id_by_range(self.record['id_min'], self.record['id_max'])
                ids = ",".join(map(str, ids))
                events = db_connection.find_events_where_ids(ids)
                users_data = db_connection.find_users_data_where_ids(ids)
            except Exception as e:
                db_connection.cursor.close()
                raise e

        if len(events) > 0:
            process_col = columnas.Columns(events, users_data, self.record['process_id'], self.record['file_name'])
            process_col.process()
        self.finish()
        return 'job_no' in self.record and self.record['job_no'] or 'Finished'
