from dateutil.parser import parse
import scrapy
import datetime
import re

# Python/spiders/VncompanySpider.py
# Crawl URL to generate output according to Task 02
class VncompanySpider(scrapy.Spider):
    # Crawl name
    name = "vncompany"

    #URL List
    start_urls = [
        'http://stock.vietnammarkets.com/vietnam-stock-market.php'
    ]

    def is_date(self, string):
        try: 
            parse(string)
            return True
        except ValueError:
            return False
        except TypeError:
            return False

    def is_number(self, string):
        try:
            if re.match(r'[^a-zA-Z]', string):
                return True
            else:
                return False
        except TypeError:
            print("TypeError with this value " + str(string))
            return True

    def convert_to_number(self, string):
        try: 
            strings = re.findall(r'\d', string)
            number = int(''.join(strings))

            return number
        except TypeError:
            return string

    def parse(self, response):
        # Iterate each row of index table
        for idx, td in enumerate(response.xpath('//table/tr[not(@bgcolor)]')):
            
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

            # Use index_file to generate output file
            # Add master data to index_file
            index_file = {
                'ticker symbol':ticker, 
                'company name':company, 
                'uid':url, 
                'business':business,
                'crawled_at':crawlAt, 
                'Listing bourse':bourse
                }

            # Parse index_file to parse_company function
            # Capture return of updated index_file with detail data
            # Yield index_file
            yield scrapy.Request(url, callback=self.parse_company, meta={'index': index_file})

    # Function to parse detail data
    def parse_company(self, response):
        # Get master data in index_file
        index_file = response.meta['index']

        # Extract company address
        company_profiles = response.xpath('//table/tr[@valign]/td[@width]/text()').extract()
        address = company_profiles[1].strip('\n\t')

        index_file['company_address'] = address
        index_file['country'] = "Vietnam"

        # Go for bonus point
        state = ""
        city = ""
        street = ""

        # Split address into street, city and state
        try:
            addresses = address.split(', ')
            if len(addresses) > 2:
                state = addresses[-1]
                city = addresses[-2]
                street = " ".join(addresses[:2])
        except Exception as e:
            raise e
            pass

        index_file['company_state'] = state
        index_file['company_city'] = city
        index_file['company_street'] = street

        # Extract company phones
        phones = company_profiles[2:4]
        phone_array = []
        for phone in phones:
            phone = re.split('-|\/|~', phone)
            for phon in phone:
                # Go for bonus point
                international_format = ""
                international_format = "".join(re.findall(r'\d+', phon))

                # International format for Vietnam phone number
                # starts with +84
                # without 0 prefix
                if international_format.startswith("0"):
                    international_format = international_format[1:]
                if not international_format.startswith("84"):
                    international_format = "84"+international_format
                
                international_format = "+"+international_format
                
                if international_format is not "+84":
                    phone_array.append(international_format)

        index_file['company phone number'] = phone_array
        # Save raw phone number for next improvement
        index_file['company phone number raw'] = " ".join(phones).strip('\n\t')

        # Extract company email
        email = company_profiles[4].strip('\n\t')
        index_file['company email'] = email

        # Extract company website
        website = company_profiles[5].strip('\n\t')
        index_file['company website'] = website

        # Extract company financial summary
        financial = {}
        for tr in response.xpath('//table/tr[@valign]/td[@width]/table/tr'):
            key = tr.xpath('td/strong/text()').extract_first().strip(':')
            value = tr.xpath('td/text()').extract_first()

            # Validate value and convert it to integer
            if self.is_number(value):
                if self.is_date(value) is False:
                    value = self.convert_to_number(value)

            financial[key] = value
        index_file['financial summary'] = financial

        # Extract company other information
        descriptions = response.xpath('//table/tr[not(@valign)]/td[@colspan]/text()').extract()
        lists = []
        col = -1
        temp = []

        # Split table into these data:
        # Company description
        # Company auditing company
        # Company business registration information
        #
        # TODO
        # Review company with <p> element
        for line in descriptions:
            line = line.strip('\n\t')

            if line is "":
                if col > -1:
                    lists.append(temp)
                    temp = []
                col = col + 1
            else:
                temp.append(line.strip('\n\t'))
        
        # Extract company description/summary
        if len(lists) > 0:
            index_file['company description'] = " ".join(lists[0])

            # Extract company auditing company
            auditing = {}

            if len(lists) > 1:

                if len(lists[1]) > 0:

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

            index_file['auditing company'] = auditing


            # Extract company registration business information
            registration = {}
            if len(lists) > 2:

                if len(lists[2]) > 0:

                    for registration_info in lists[2]:
                        
                        if ':' in registration_info:
                            try:
                                key = registration_info.split(": ")[0]
                                value = registration_info.split(": ")[1]
                                registration[key] = value
                            except Exception as e:
                                continue

            index_file['business registration'] = registration
        else:
            # Return empty object when data not available
            index_file['company description'] = {}
            index_file['auditing company'] = {}
            index_file['business registration'] = {}

        # Return updated index_file with master dat
        return index_file