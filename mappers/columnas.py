# -*- coding: utf-8 -*-
import pandas as pd
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 100)


class Columnas:

    def __init__(self, events, process_id, file_name):
        self.process_id = process_id
        self.file_name = file_name
        self.events = events

    def process(self):
            event = pd.DataFrame.from_records(self.events)
            listaTmp = []
            for item in event['eventname']:
                item = item.replace("\event", "", 1)
                item = item.replace("\\", "", 1)
                item = item.replace("\\", "-", 2)
                item = item.replace("mod_", "", 1)
                item = item.replace("course", "crs")
                item = item.replace("user", "usr")
                item = item.replace("viewed", "view")
                item = item.replace("submitted", "log")
                item = item.replace("logged", "submit")
                item = item.replace("module", "mod")
                item = item.replace("message", "msg")
                listaTmp.append(item)

            event['eventname'] = listaTmp

            event['crsevntname'] = ""
            listaTmp = []
            for i in range(len(event['crsevntname'])):
                item = str(event['courseid'][i]) + "-" + event['eventname'][i]
                listaTmp.append(item)
            event['crsevntname'] = listaTmp

            event2 = event[['userid', 'crsevntname', 'component']].\
            groupby(['userid', 'crsevntname']).count()
            # converts panadas dataframe multiindex to columns
            #event2.reset_index(inplace=True)
            tmp = event2.unstack()
            # to convert usrid index to a column:
            tmp.reset_index(inplace = True)
            # to drop column multiindex
            tmp.columns.droplevel()

            path = os.path.join('/data', self.process_id, self.file_name)
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            tmp.to_csv(path, index=True)