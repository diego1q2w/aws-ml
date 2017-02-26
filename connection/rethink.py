import rethinkdb as r
import os


class Rethink:

    DATABASE_NAME = os.environ.get('RETHINK_DB' or 'aws')
    TABLE_NAMES = ['process', 'job']

    def __init__(self, rethink_host='localhost', rethink_port=29015):
        self.conn = r.connect(host=os.environ.get('RETHINK_HOST' or rethink_host),
                              port=os.environ.get('RETHINK_PORT' or rethink_port))

    def create_database(self):
        databases = r.db_list().run(self.conn)
        if self.DATABASE_NAME not in databases:
            r.db_create(self.DATABASE_NAME).run(self.conn)

    def create_tables(self):
        tables = r.table_list().run(self.conn)
        create_tables = filter(lambda table: table not in tables, self.TABLE_NAMES)
        for table in create_tables:
            r.table_create(table).run(self.conn)

    def set_database(self):
        database_name = self.DATABASE_NAME
        self.conn.use(database_name)

    def get_connection(self):
        return self.conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()



process_table = {
    'id': 'uuid',
    'finished': 'boolean',
    'num_records': 'number',
    'chunks': 'number',
    'processed_chunks': 'number',
    'id_min': 'number',
    'id_max': 'number',
    'file_name': 'string',
    'uploaded': 'boolean',
    'timestamp': 'number',
    'chunk_size' : 'number'
}


jobs_table = {
    'id': 'uuid',
    'process_id': 'uuid',
    'queue_id': 'uuid',
    'job_no': 'number',
    'id_min': 'number',
    'id_max': 'number',
    'finished': 'boolean',
    'num_rows': 'number',
    'processed_rows': 'number',
    'file_name': 'string',
    'file_processed': 'boolean',
}