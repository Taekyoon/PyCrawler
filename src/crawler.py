import threading
from queue import Queue

from settings import *
from util import *
from url_storer import UrlStorer
from spider import Spider


class Crawler:
    '''
    Crawler class is a class to collect target data using Spider class which
    requests a websource from a web site and extracts target data.
    Crawler class mainly runs with thread which run spiders asynchronously
    using queing method.
    '''

    def __init__(self, name, base_url, domain_url=None, dbpath=None):
        '''
        When crawler starts, the init method should do 3 things.
        1. Setup the base url and extract domain url.
        2. Build crawled list and queue list. The crawled list is a list to
          store data which users want to extract. The queue list is a url list
          to crawl a page from the url. Default storer is SQLite by creating
          tables named 'crawled' and 'queue'.
        3. Create a spider.
        '''
        self.name = name
        self.base_url = base_url
        if domain_url is None:
            self.domain_url = get_domain_url(self.base_url)

        self.create_url_storer(dbpath)
        self.queue = Queue()
        self.spider = Spider(self.name, self.base_url,
                            self.domain_url, self.url_storer, 0)
        print('Finished initialize.')

    def create_url_storer(self, dbpath):
        if dbpath is not None:
            self.url_storer = {
                                'db': UrlStorer(dbpath),
                                'table': {
                                    'queue': self.name + 'queue',
                                    'crawled': self.name + 'crawled',
                                },
                              }
            self.url_storer['db'].create_table(self.url_storer['table']['queue'])
            self.url_storer['db'].create_table(self.url_storer['table']['crawled'])

    def run(self):
        '''
        When run method starts, the method does following directions.
        1. Create worker threads which should be shut down when Processor or
         Crawler instance is dead.
        2. Get the base url and store it to the queue list. (This is initial
          run on the crawling, usually storing url operates after link urls are
          fetched from a target webpage.)
        3. Fetch urls from queue list. If queue list is empty, make this module
          consider whether crawler should be shut down.
        4. Input fetched urls to Queue, so Spider can be ready to run again.
        5. When items are input to Queue then threads will run until the work
          is finished. To assure that implementation, Queue.join is necessary.
        '''
        self.create_workers(self.work)
        self.store_base_url()
        print('All default settings are done.')
        while self.fetch_urls():
            print('waiting for join')
            self.queue.join()

        print('working done')
        self.url_storer['db'].close()

    def create_workers(self, work=None):
        for _ in range(NUMBER_OF_THREADS):
            t = threading.Thread(target=self.work)
            t.daemon = True
            t.start()

    def store_base_url(self):
        self.url_storer['db'].put(self.url_storer['table']['queue'], self.base_url,0)

    def fetch_urls(self):
        print('fetch_urls called')
        url_list = list(self.url_storer['db'].get(self.url_storer['table']['queue']))
        if len(url_list) > 0:
            for url in url_list:
                if url[1] < MAXIMUM_LEVELS:
                    print(url, 'willl put into queue')
                    self.queue.put(url)
                self.url_storer['db'].delete(self.url_storer['table']['queue'], url[0])
            return True
        return False

    def work(self):
        while True:
            url = self.queue.get()
            self.spider = Spider(self.name, url[0],
                                self.domain_url, self.url_storer, url[1])
            print('start spider work', url)
            self.spider.run(threading.current_thread().name, url[0])
            self.queue.task_done()
            print('task is done')
