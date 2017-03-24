import datetime
from connection import rethink
import rethinkdb as r
import time


class Scheduler:

    def start(self):
        with rethink.Rethink() as rethink_conn:
            rethink_conn.set_database()
            conn = rethink_conn.get_connection()
            count = r.table('process').filter({'finished': False}).count().run(conn)
            #will not start unless there is not another process runing
            if count == 0:
                timestamp = time.time() - 24*60*60
                count = r.table('process').filter(r.row('timestamps').gt(timestamp)).count().run(conn)
                 ## run only if there was not completed jobs in the last day
                if count == 0:
                    r.table('process').insert({'finished': False}).run(conn)

    def run(self):
        if datetime.datetime.today().weekday() == 5:
            self.start()
