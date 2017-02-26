#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import os
from .master import Master
from .connection.rethink import Rethink

if __name__ == '__main__':
    with Rethink() as rethink:
        rethink.create_database()
        rethink.set_database()
        rethink.create_tables()

    while True:
        Master().start()
        time.sleep(os.environ.get('TIME_SLEEPING', 10))
