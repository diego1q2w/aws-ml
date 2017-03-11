# -*- coding: utf-8 -*-
import pandas as pd
import os


class Columns:

    def __init__(self, events, users_data, process_id, file_name):
        self.users_data = users_data
        self.process_id = process_id
        self.file_name = file_name
        self.events = events

    def process(self):
            events_frame = pd.DataFrame.from_records(self.events)
            users_data_frame = pd.DataFrame(self.users_data)
            list_tmp = []
            for item in events_frame['eventname']:
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
                list_tmp.append(item)

            events_frame['eventname'] = list_tmp

            events_frame['crsevntname'] = ""
            list_tmp = []
            for i in range(len(events_frame['crsevntname'])):
                item = str(events_frame['courseid'][i]) + "-" + events_frame['eventname'][i]
                list_tmp.append(item)
            events_frame['crsevntname'] = list_tmp

            event2 = events_frame[['userid', 'crsevntname', 'component']].groupby(['userid', 'crsevntname']).count()

            # converts pandas dataframe multiindex to columns
            event2.reset_index(inplace=True)
            tmp = event2.pivot_table('component', 'userid', 'crsevntname')

            tmp.reset_index(inplace=True)

            list_tmp = []
            for i in range(len(users_data_frame)):
                diff = (users_data_frame.iloc[i]['dat_firstac'] - users_data_frame.iloc[i]['dat_matricula']).days
                list_tmp.append(diff)

            users_data_frame['tmp_entrar'] = list_tmp

            tmp = tmp.merge(users_data_frame, left_on='usrid', right_on='usrid', how='left')

            path = os.path.join('/','home','admin','data', 'worker', self.process_id, self.file_name)
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                os.makedirs(directory)

            tmp.to_csv(path, index=True)

