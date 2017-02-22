from crawler import Crawler

START_URLS = ['http://books.toscrape.com']

test = Crawler('test',START_URLS[0],None,'test.db')

test.run()
