from lxml import etree
from urllib import parse

class LinkExtractor:
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.links = set()

    def feed(self, source):
        html_source = etree.HTML(source)
        links = set(html_source.xpath('//a/@href'))
        self.links = list(map(lambda x:parse.urljoin(self.base_url,x), links))

    def get_links(self):
        return self.links
