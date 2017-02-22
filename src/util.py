from urllib.parse import urlparse

def get_domain_url(url):
    '''
    Get domain url.
    For example if you input url data 'http://happy.gentleman.com/1023942/abc/'
    then you will get 'http://gentleman.com/'
    '''
    try:
        results = get_sub_domain_name(url).split('.')
        return results[-2] + '.' + results[-1]
    except:
        return ''

def get_sub_domain_name(url):
    try:
        return urlparse(url).netloc
    except:
        return ''
