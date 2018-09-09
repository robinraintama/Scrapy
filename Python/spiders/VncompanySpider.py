import scrapy
import datetime
import re

class VncompanySpider(scrapy.Spider):
    name = "vncompany"
    start_urls = [
        'http://stock.vietnammarkets.com/vietnam-stock-market.php'
    ]

    def parse(self, response):
        for idx, td in enumerate(response.xpath('//table/tr[not(@bgcolor)]')):
            if td.xpath('td[@align]') or idx % 1 is not 0:
                continue

            url = td.xpath('td/a/@href').extract_first()
            ticker = td.xpath('td/a/text()').extract_first()
            company = td.xpath('td/text()')[0].extract()
            business = td.xpath('td/text()')[1].extract()
            bourse = td.xpath('td/text()')[2].extract()
            crawlAt = datetime.date.today()

            indexFile = {
                'ticker symbol':ticker, 
                'company name':company, 
                'url':url, 
                'business':business,
                'crawled_at':crawlAt, 
                'Listing bourse':bourse
                }

            yield scrapy.Request(url, callback=self.parse_company, meta={'index': indexFile})

            # if idx is 200:
            #     break

    def parse_company(self, response):
        indexFile = response.meta['index']
        company_profiles = response.xpath('//table/tr[@valign]/td[@width]/text()').extract()

        # trans_table = {ord(c): None for c in u'\r\n\t'}
        # address = ''.join(s.strip().translate(trans_table) for s in company_profiles[1])
        address = company_profiles[1].strip('\n\t')
        indexFile['company_address'] = address
        indexFile['country'] = "Vietnam"
        state = ""
        city = ""
        street = ""
        try:
            # addresses = re.findall(r"[\w']+", address)
            addresses = address.split(', ')
            if len(addresses) > 2:
                state = addresses[-1]
                city = addresses[-2]
                street = " ".join(addresses[:2])
        except Exception as e:
            raise e
            pass

        indexFile['company_state'] = state
        indexFile['company_city'] = city
        indexFile['company_street'] = street

        phones = company_profiles[2:4]
        phone_array = []
        for phone in phones:
            phone = re.split('-|\/|~', phone)
            for phon in phone:
                international_format = ""
                international_format = "".join(re.findall(r'\d+', phon))
                # international_format = "".join(international_format)
                if international_format.startswith("0"):
                    international_format = international_format[1:]
                if not international_format.startswith("84"):
                    international_format = "84"+international_format
                international_format = "+"+international_format
                phone_array.append(international_format)

        indexFile['company phone number'] = phone_array
        indexFile['company phone number raw'] = " ".join(phones).strip('\n\t')

        email = company_profiles[4].strip('\n\t')
        indexFile['company email'] = email

        website = company_profiles[5].strip('\n\t')
        indexFile['company website'] = website

        financial = {}
        for tr in response.xpath('//table/tr[@valign]/td[@width]/table/tr'):
            key = tr.xpath('td/strong/text()').extract_first()
            value = tr.xpath('td/text()').extract_first()
            financial[key] = value
        indexFile['financial summary'] = financial

        descriptions = response.xpath('//table/tr[not(@valign)]/td[@colspan]/text()').extract()
        lists = []
        col = -1
        temp = []

        for line in descriptions:
            line = line.strip('\n\t')

            if line is "":
                if col > -1:
                    lists.append(temp)
                    temp = []
                col = col + 1
            else:
                temp.append(line.strip('\n\t'))
        ##
        indexFile['company description'] = " ".join(lists[0])
        ##

        auditing = {}

        ##
        if len(lists[1]) > 0:
        ##
            auditing['company_name'] = lists[1][0]
            if len(lists[1]) > 1:
                auditing['address'] = lists[1][1]
                for audit_description in lists[1][2:]:
                    if ' - ' not in audit_description:
                        if ": " in audit_description:
                            key = audit_description.split(': ')[0]
                            value = audit_description.split(': ')[1]
                            auditing[key] = value
                        else:
                            auditing['address'] = "".join(auditing['address']) + " " + audit_description
                    else :
                        for desc in audit_description.split(' - '):
                            if ": " in desc:
                                key = desc.split(": ")[0]
                                value = desc.split(": ")[1]
                                auditing[key] = value
        indexFile['auditing company'] = auditing

        registration = {}
        if len(lists[2]) > 0:
            for registration_info in lists[2]:
                print(registration_info)
                if ':' in registration_info:
                    try:
                        key = registration_info.split(": ")[0]
                        value = registration_info.split(": ")[1]
                        registration[key] = value
                    except Exception as e:
                        continue
        indexFile['business registration'] = registration

        return indexFile