import scrapy, json
from bs4 import BeautifulSoup

class EbaycatsscraperSpider(scrapy.Spider):
    name = 'ebayCatsScraper'
    allowed_domains = ['ebay.com']
    start_urls = ['https://www.ebay.com/n/all-categories']

    def parse(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        urls = soup.find_all('a', {'class':'top-cat'})
        all_urls = {}
        for u in urls:
            all_urls[u.text] = u.get('href')
        other_urls = soup.find('div', {'class':'pure-links-wrapper'}).find_all('a')
        for u in other_urls:
            all_urls[u.text] = u.get('href')
        
        for u in list(all_urls.keys()):
            yield {"category_tree":u,'url' : 'base'}
            yield scrapy.Request(url=all_urls[u], callback=self.parse1, meta= {'base': u})

    def parse1(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        scripts = soup.find_all('script')
        for sc in scripts:
            try:
                sc_data = json.loads(sc.text)
                if sc_data.get('@type') == 'BreadcrumbList':
                    li = sc_data.get('itemListElement')
                    yield {'category_tree': ">".join((([x.get('name') for x in li]))),'url' : response.request.url}        

            except:
                pass


        sidebar_div = soup.find('div',{'class':'dialog__cell'})
        if sidebar_div:
            first_list = sidebar_div.find('ul')
            if first_list:
                urls = first_list.find_all('a')
                for url in urls:
                    yield scrapy.Request(url = url.get('href'), callback = self.parse1, meta={'crawl_once': True})



        