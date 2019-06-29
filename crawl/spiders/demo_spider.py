from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class MySpider(CrawlSpider):
    name = "downg"
    allowed_domains = ["downg.com"]
    start_urls = [
        'http://www.downg.com/new/0_1.html'
    ]

    rules = [
        Rule(LinkExtractor(allow=(r'new/0_\d\.html',)),follow=True),
        Rule(LinkExtractor(restrict_xpaths=('//span[@class="app-name"]')),callback='getDetail',follow=False)
    ]

    def getDetail(self,response):
        print('result '+response.url)