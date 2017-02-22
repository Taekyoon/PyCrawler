import requests

from link_extract import LinkExtractor


class Spider(object):
    '''
    Spider class is a class to get a websource from url-site, extract url links
    , and store to database.
    '''

    def __init__(self, name, base_url, domain_url, url_storer, level):
        self.name = name
        self.base_url = base_url
        self.domain_url = domain_url
        self.url_storer = url_storer
        self.level = level

    def run(self, thread_name, url):
        print(thread_name,'get url source',url)
        source = self.get_web_source(url)
        links = [link for link in self.get_url_links(source)
                if self.domain_url in link]
        self.store_url(links)

    def get_web_source(self, url):
        response = requests.get(url)
        return response.text

    def get_url_links(self, source):
        link_extractor = LinkExtractor(self.base_url)
        link_extractor.feed(source)
        return link_extractor.get_links()

    def store_url(self, links):
        for url in links:
            self.url_storer['db'].put(self.url_storer['table']['crawled'], url, self.level + 1)
            self.url_storer['db'].put(self.url_storer['table']['queue'], url, self.level + 1)
