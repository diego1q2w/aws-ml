import pymysql
import pymysql.cursors
import os


class Mysql:
    sql = os.environ.get('MYSQL_QUERY', "SELECT *  FROM ("
        "SELECT {select}"
        " FROM mdl_groups_members"
        " INNER JOIN mdl_user ON mdl_user.id = mdl_groups_members.userid"
        " INNER JOIN mdl_groups ON mdl_groups_members.groupid  = mdl_groups.id"
        " INNER JOIN mdl_course ON mdl_course.id = mdl_groups.courseid"
        " INNER JOIN mdl_enrol ON mdl_enrol.courseid = mdl_groups.courseid"
        " INNER JOIN mdl_user_enrolments ON mdl_user_enrolments.enrolid = mdl_enrol.id "
        " AND mdl_user_enrolments.userid = mdl_groups_members.userid "
        " AND mdl_user.id = mdl_user_enrolments.userid"
        " INNER JOIN mdl_context ON mdl_context.instanceid = mdl_groups.courseid"
        " INNER JOIN mdl_role_assignments ON mdl_role_assignments.contextid = mdl_context.id "
        " AND mdl_role_assignments.userid = mdl_groups_members.userid"
        " INNER JOIN mdl_role ON mdl_role.id = mdl_role_assignments.roleid "
        " WHERE"
        " mdl_user.username not like '%demo%'"
        " AND mdl_user.id >= 16687"
        " AND mdl_user_enrolments.`status` = 0"
        " AND mdl_enrol.courseid in (8911, 8915, 8919, 8923)"
        " AND mdl_context.contextlevel = 50"
        " AND mdl_role.id in (5, 16)"
        " group by "
        " mdl_groups_members.userid) AS T1")

    def __init__(self):
        conn = pymysql.connect(host=os.environ.get('MYSQL_HOST', 'db'),
                               user=os.environ.get('MYSQL_USER', 'root'),
                               password=os.environ.get('MYSQL_PASSWORD', 'root'),
                               db=os.environ.get('MYSQL_DB', 'default'),
                               port=int(os.environ.get('MYSQL_PORT', 3306)),
                               charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.cursor = conn.cursor()

    def get_data(self):
        sql = (self.sql + " ORDER BY id ASC;").format(**{'select': 'mdl_groups_members.userid as id'})
        self.cursor.execute(sql)
        return self

    def find_by_id(self, id):
        sql = (self.sql + " WHERE id={id}").format(
            **{'id': id,
               'select': '*'})
        self.cursor.execute(sql)
        return self

    def find_by_range(self, min_id, max_id):
        sql = (self.sql + " WHERE id BETWEEN {min_id} AND {max_id} ORDER BY id ASC;").format(
            **{'min_id': min_id,
               'max_id': max_id,
               'select': 'mdl_groups_members.userid as id'})
        self.cursor.execute(sql)
        return [cursor for cursor in self.cursor]

    def find_id_by_range(self, min_id, max_id):
        sql = (self.sql + " WHERE id BETWEEN {min_id} AND {max_id} ORDER BY id ASC;").format(
            **{'min_id': min_id,
               'max_id': max_id,
               'select': 'mdl_groups_members.userid as id'})
        self.cursor.execute(sql)
        return [cursor['id'] for cursor in self.cursor]

    def find_events_where_ids(self, ids):
        # 1483941600 is UNIX_DATESTAMP for "2017-01-09"
        sql = "SELECT mdl_logstore_standard_log.courseid," \
              " mdl_logstore_standard_log.userid," \
              " mdl_logstore_standard_log.eventname," \
              " mdl_logstore_standard_log.component" \
              " FROM mdl_logstore_standard_log" \
              " where mdl_logstore_standard_log.userid in ({ids})" \
              " and mdl_logstore_standard_log.timecreated < 1483941600".format(**{'ids': ids})
        self.cursor.execute(sql)
        return [cursor for cursor in self.cursor]

    def find_users_data_where_ids(self, ids):
        sql = "SELECT \
                mdl_user.id as userid, \
                from_unixtime(mdl_user.timecreated) as dat_matricula, \
                from_unixtime(mdl_user.firstaccess) as dat_firstac, \
                from_unixtime(mdl_user.lastlogin) as dat_lastlog, \
                from_unixtime(mdl_user.lastaccess) as dat_lastac \
                FROM \
                mdl_user \
                WHERE \
                mdl_user.username not like '%demo%' \
                AND mdl_user.id >= 16687 \
                AND mdl_user.id in ({ids})".format(**{'ids': ids})
        self.cursor.execute(sql)
        return [cursor for cursor in self.cursor]

    def get_id_max(self):
        sql = (self.sql + " ORDER BY id DESC LIMIT 1;").format(**{'select': 'mdl_groups_members.userid as id'})
        self.cursor.execute(sql)
        return [cursor for cursor in self.cursor].pop()['id']

    def get_id_min(self):
        sql = (self.sql + " ORDER BY id ASC LIMIT 1;").format(**{'select': 'mdl_groups_members.userid as id'})
        self.cursor.execute(sql)
        return [cursor for cursor in self.cursor].pop()['id']

    def get_cursor(self):
        return self.cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()




