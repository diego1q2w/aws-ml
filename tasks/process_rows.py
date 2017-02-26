import rethinkdb as r
from connection import rethink

class ProcessRowsTask():
    def __init__(self):
        self.name = "tasks.ProcessRowsTask"

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print(str(task_id)+ " Failed")

    def on_success(self, retval, task_id, args, kwargs):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()

    def run(self, rethink_job_id):
        self.rethink_job_id = rethink_job_id
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            self.record = r.table('job').get(rethink_job_id).run(conn)
            print(self.record)

            record = r.table('job').get(self.rethink_job_id).update(
                {'finished': True,
                 'processed_rows': self.record['num_rows']}).run(conn)
        return True