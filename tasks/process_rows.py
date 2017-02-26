from worker import app
import rethinkdb as r
from connection.rethink import Rethink

class ProcessRowsTask(app.Task):
    rethink_job_id = ""
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        pass

    def on_success(self, retval, task_id, args, kwargs):
        pass

    def run(self, rethink_job_id):
        self.rethink_job_id = rethink_job_id
        task_id = self.request.id
        with Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            r.table('job').get(rethink_job_id).update({'queue_id': task_id}).run(conn)
