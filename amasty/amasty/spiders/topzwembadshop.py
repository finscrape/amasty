import scrapy


class TopzwembadshopSpider(scrapy.Spider):
    name = 'topzwembadshop'
    #allowed_domains = ['a.com']
    start_urls = ['https://www.top-zwembadshop.nl/media/www.top-zwembadshop.nl/products.xml']

    def parse(self, response):
        print(response.body)
        print(response.body)
        print(response.body)
        print(response.body)
        print(response.body)
        yield {"body":response.body}
          
