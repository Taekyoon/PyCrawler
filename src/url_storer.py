from db_accessor import MultiThreadDBAccessor

class UrlStorer:

    conn = None

    def __init__(self, file_path):
        UrlStorer.conn = MultiThreadDBAccessor(file_path)

    @staticmethod
    def create_table(table_name):
        UrlStorer.conn.execute("create table if not exists %s\
                                (URL text primary key not null,\
                                LEVEL int not null)" % (table_name))
    @staticmethod
    def get(table_name, limit=10):
        return UrlStorer.conn.select("select * from %s limit %s" % (table_name, str(limit)))

    @staticmethod
    def put(table_name, url, level):
        UrlStorer.conn.execute("insert into %s (URL, LEVEL) VALUES\
                                (%s, %s)" % (table_name, "'" + url + "'", level))

    @staticmethod
    def exist(table_name, key):
        cursor = UrlStorer.conn.execute("select exist(select URL from %s\
                                        where KEY = %s)" % (table_name, "'" + key + "'"))
        return True if [x for x in cursor][0][0] == 1 else False

    @staticmethod
    def delete(table_name, key):
        UrlStorer.conn.execute("delete from %s where URL = %s" % (table_name, "'" + key + "'"))
        return True

    @staticmethod
    def drop_table(table_name):
        UrlStorer.conn.execute("drop table if exists %s" % (table_name))

    @staticmethod
    def close():
        UrlStorer.conn.close()
