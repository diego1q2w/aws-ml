import pymysql
import pymysql.cursors
import os

class Mysql:
    sql = os.environ.get('MYSQL_QUERY', "SELECT {select} FROM auth_permission")

    def __init__(self):
        conn = pymysql.connect(host=os.environ.get('MYSQL_HOST', 'db.forgecoders.com'),
                               user=os.environ.get('MYSQL_USER', 'backadmin'),
                               password=os.environ.get('MYSQL_PASSWORD', '123Bac1*'),
                               db=os.environ.get('MYSQL_DB', 'callgenie'),
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.cursor = conn.cursor()

    def get_data(self):
        sql = self.sql.format({'select': 'id'})
        self.cursor.execute(sql)
        return self

    def findById(self, id):
        sql = self.get_cursor() + " WHERE id={id}".format(
            {'id': id,
             'select': '*'})
        self.cursor.execute(sql)
        return self

    def findByRange(self, min_id, max_id):
        sql = self.get_cursor() + " WHERE id BETWEEN {min_id} AND {max_id}".format(
            {'min_id': min_id,
             'max_id': max_id,
             'select': '*'})
        self.cursor.execute(sql)
        return self

    def findIdByRange(self, min_id, max_id):
        sql = self.get_cursor() + " WHERE id BETWEEN {min_id} AND {max_id}".format(
            {'min_id': min_id,
             'max_id': max_id,
             'select': 'id'})
        self.cursor.execute(sql)
        return self

    def getIdMax(self):
        sql = self.sql + " ORDER BY id DESC LIMIT 1;".format({'select': 'id'})
        self.cursor.execute(sql)
        return self

    def getIdMin(self):
        sql = self.sql + " ORDER BY id ASC LIMIT 1;".format({'select': 'id'})
        self.cursor.execute(sql)
        return self

    def get_cursor(self):
        return self.cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()




