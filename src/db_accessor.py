from threading import Thread
from queue import Queue

import apsw

class SingleThreadDBAccessor(object):
    def __init__(self, db):
        self.cnx = apsw.Connection(db)
        self.cursor = self.cnx.cursor()
    def execute(self, req, arg=None):
        self.cursor.execute(req, arg or tuple())
    def select(self, req, arg=None):
        self.execute(req, arg)
        for raw in self.cursor:
            yield raw
    def close(self):
        self.cnx.close()

class MultiThreadDBAccessor(Thread):
    def __init__(self, db):
        super(MultiThreadDBAccessor, self).__init__()
        self.db = db
        self.reqs = Queue()
        self.start()
    def run(self):
        print('running db accessor')
        cnx = apsw.Connection(self.db)
        cursor = cnx.cursor()
        while True:
            req, arg, res = self.reqs.get()
            print(req)
            if req == '--close--': break
            try:
                cursor.execute(req, arg)
                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put('--no more--')
            except Exception as e:
                print(e)
        cnx.close()
    def execute(self, req, arg=None, res=None):
        self.reqs.put((req, arg or tuple(), res))
    def select(self, req, arg=None):
        res = Queue()
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            if rec == '--no more--': break
            yield rec
    def close(self):
        self.execute('--close--')

if __name__ == '__main__':
    db = 'people.db'
    multithread = True

    if multithread:
        sql = MultiThreadDBAccessor(db)
    else:
        sql = SingleThreadDBAccessor(db)

    sql.execute("create table if not exists people(name,first)")
    sql.execute("insert into people values('VAN ROSSUM', 'Guido')")
    sql.execute("insert into people values(?,?)", ('TORVALDS', 'Linux'))

    for f, n in sql.select("select first, name from people"):
        print(f, n)

    sql.close()
