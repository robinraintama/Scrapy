import scrapy
import datetime

# Python/spiders/VnindexSpider.py
# Crawl URL to generate output according to Task 01
class VnindexSpider(scrapy.Spider):
    # Crawl name
    name = "vnindex"

    #URL List
    start_urls = [
        'http://stock.vietnammarkets.com/vietnam-stock-market.php'
    ]

    def parse(self, response):
        # Iterate each row of index table
        for td in response.xpath('//table/tr[not(@bgcolor)]'):
            
            # Skip if row is not index's row
            if td.xpath('td[@align]'):
                continue

            # Extract master data
            url = td.xpath('td/a/@href').extract_first()
            ticker = td.xpath('td/a/text()').extract_first()
            company = td.xpath('td/text()')[0].extract()
            business = td.xpath('td/text()')[1].extract()
            bourse = td.xpath('td/text()')[2].extract()
            crawlAt = datetime.date.today()

            # Yield master data
            yield {
                'ticker symbol':ticker, 
                'company name':company, 
                'url':url, 
                'business':business,
                'crawled_at':crawlAt, 
                'Listing bourse':bourse
                }