#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import os
from master import Master
from connection import rethink
from scheduler import Scheduler

if __name__ == '__main__':
    with rethink.Rethink() as rethink_conn:
        rethink_conn.create_database()
        rethink_conn.set_database()
        rethink_conn.create_tables()

    while True:
        try:
            Master().start()
            Scheduler().run()
        except Exception as e:
            print(e)
        time.sleep(int(os.environ.get('TIME_SLEEPING', 10)))
