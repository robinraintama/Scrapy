import scrapy
import datetime

class VnindexSpider(scrapy.Spider):
    name = "vnindex"
    start_urls = [
        'http://stock.vietnammarkets.com/vietnam-stock-market.php'
    ]

    def parse(self, response):
        for td in response.xpath('//table/tr[not(@bgcolor)]'):
            url = td.xpath('//td/a/@href').extract_first()
            ticker = td.xpath('//td/a/text()').extract_first()
            company = td.xpath('//td/text()')[0].extract()
            business = td.xpath('//td/text()')[1].extract()
            bourse = td.xpath('//td/text()')[2].extract()
            crawlAt = datetime.date.today()
            yield {
                'ticker symbol':ticker, 
                'company name':company, 
                'url':url, 
                'business':business,
                'crawled_at':crawlAt, 
                'Listing bourse':bourse }