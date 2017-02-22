from url_storer import UrlStorer

table_name = 'TEST'
def create_database():
    UrlStorer('test.db')
    UrlStorer.create_table(table_name)

def close_database():
    UrlStorer.close()

def get_test():
    return UrlStorer.get(table_name)

def insert_test(key):
    UrlStorer.put(table_name,key,1)

def exist_test():
    UrlStorer.exist(table_name, 'naver.com')

def delete_test():
    UrlStorer.delete(table_name, 'naver.com')

def test():
    create_database()
    insert_test('abc')
    delete_test()
    close_database()

import os
import threading
from queue import Queue

NUMBER_OF_THREADS = 8

def create_workers():
    for _ in range(NUMBER_OF_THREADS):
        t = threading.Thread(target=work)
        t.daemon = True
        t.start()

def work():
    while True:
        key = queue.get()
        for i in range(1000):
            insert_test(key + str(i))
            print("%s: %s of 1000 is done." % (threading.current_thread().name, i+1))
        queue.task_done()


create_database()
queue = Queue()
input_list = ['a'*n for n in range(10)]

for l in input_list:
    queue.put(l)

create_workers()
queue.join()
for i,j in get_test():
    print(i,j)
close_database()
os.delete('test.db')


#test()
