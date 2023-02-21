import scrapy


class TopzwembadshopSpider(scrapy.Spider):
    name = 'topzwembadshop'
    #allowed_domains = ['a.com']
    start_urls = ['https://www.top-zwembadshop.nl/media/www.top-zwembadshop.nl/products.xml']

    def parse(self, response):
        url = response.xpath("//span[contains(text(),'https://www.top-zwembadshop')]")
        print(url)