from datetime import datetime
from pathlib import Path
import mysql.connector
import scrapy
import re
from time import sleep
from scrapy.spiders import SitemapSpider
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from realtyzoneaiscraper.models.property_dto import PropertyDTO

#class MainSpider(scrapy.Spider):SitemapSpider
class MainSpider(SitemapSpider):
    name = "mainspider"
    sitemap_urls = ["https://www.test.com/sitemap.xml"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.already_scraped_urls = self.fetch_already_scraped_urls()
        #chrome_options = Options()
        #chrome_options.add_argument("--headless")
        #self.driver = webdriver.Chrome(options=chrome_options)
        self.driver = webdriver.Chrome()

    def fetch_already_scraped_urls(self):
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='yourdatabase.mysql.database.azure.com',
            user='youruser',
            password='yourpassword',
            database='yourdatabasename'
        )

        # Create a cursor
        cursor = connection.cursor()

        # Fetch all URLs already scraped
        cursor.execute("SELECT distinct(url) FROM yourdatabasename.indexed_properties")
        already_scraped_urls = {row[0] for row in cursor.fetchall()}

        # Close the database connection
        cursor.close()
        connection.close()

        return already_scraped_urls

    def _parse_sitemap(self, response, **kwargs):
        urls_to_scrape = set()
        count = 0
        
        for entry in super()._parse_sitemap(response, **kwargs):
            loc = entry.url
            pattern = r'https://www.test.com/property/.*'
            url_pattern = re.compile(pattern)
            
            if url_pattern.match(loc):
                urls_to_scrape.add(loc)

                if len(urls_to_scrape) >= 1500:
                    break

        for url in urls_to_scrape:
            #print("urls_to_scrape "+ url)
            if url not in self.already_scraped_urls:
                try:
                    yield scrapy.Request(url=url, callback=self.parse, meta={'my_param_url': url})
                    sleep(10)

                    count += 1
                    print("COUNT: " + count)
                    if count >= 1500:
                        break
                except Exception as e:
                    print(f"An error occurred while processing URL {url}: {e}")

    def parse(self, response):
        # PRICE
        price_comments = response.css('.price').xpath('.//text() | .//comment()').getall()
        extracted_price = ''.join(price_comments).strip()

        numeric_part_price = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', extracted_price)
        if numeric_part_price:
            formatted_numeric_part_price = int(numeric_part_price.group().replace(',', ''))
        else:
            return

        # IMAGES
        self.driver.get(response.url)
        #try:
        gallery_content = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.test_gallery'))
            )

            #print(gallery_content)

        img_tags = gallery_content.find_elements(By.TAG_NAME, 'img')
        image_urls = [img.get_attribute('src') for img in img_tags]

            # Process the scraped data as needed
            # for url in image_urls:
            #     print(url)

        filtered_urls = [url for url in image_urls if '_test/image?url=' in url]

        #BEDROOMS, BATHROOMS, SIZE
        about_tour_div = response.css('.test_container .test')

        bed_img = about_tour_div.css('img[src="/icons/bed.svg"]')
        bathrooms_img = about_tour_div.css('img[src="/icons/bathroom.svg"]')
        size_img = about_tour_div.css('img[src="/icons/size_icon.svg"]')

        bedrooms = bed_img.xpath('following-sibling::span[1]/text()').get()
        bathrooms = bathrooms_img.xpath('following-sibling::span[1]/text()').get()
        size_sqft = size_img.xpath('following-sibling::span[1]/text()').get()
        size_without_comma = size_sqft.replace(',', '')
        size_sqft_as_int = int(size_without_comma)

        #ADDRESS
        tower_detail_div = response.css('.test .test .test .test h1')
        header_text = tower_detail_div.css('::text').get()
        if header_text:
            parts = header_text.split('in', 1)
            if len(parts) > 1:
                result_address = parts[1].strip()

        url_to_insert = response.meta.get('my_param_url')

        if 'sales' in response.url:
            operation_type = 'Sale'
        elif 'lettings' in response.url:
            operation_type = 'Rent'
        else:
            operation_type = 'Unknown'

        words = header_text.split()

        # Check if there are at least three words in the text
        property_type = None
        if len(words) >= 3:
            third_word = words[2]  # Python uses 0-based indexing, so the third word is at index 2
            property_type = third_word

        agency_name = "Test agency name"
        year = None
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        success = True

        property_dto = PropertyDTO(
            price=formatted_numeric_part_price,
            image_urls=[image_url_final for image_url_final in filtered_urls],
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            size_sqft=size_sqft_as_int,
            address=result_address,
            url=url_to_insert,
            date=date,
            success=success,
            operation_type=operation_type,
            property_type=property_type,
            agency_name = agency_name,
            year=year
        )

        self.insert_into_database(property_dto) 
    
    def insert_into_database(self, property_dto):
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host='test.mysql.database.azure.com',
            user='test',
            password='test',
            database='test'
        )

        # Create a cursor
        cursor = connection.cursor()

        # Insert data into the database
        insert_query = """
            INSERT INTO indexed_properties (price, image_urls, bedrooms, bathrooms, size_sqft, address, url, date, success, operation_type, property_type, agency_name, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = (
            property_dto.price,
            ', '.join(property_dto.image_urls),
            property_dto.bedrooms,
            property_dto.bathrooms,
            property_dto.size_sqft,
            property_dto.address,
            property_dto.url,
            property_dto.date,
            property_dto.success,
            property_dto.operation_type,
            property_dto.property_type,
            property_dto.agency_name,
            property_dto.year
        )
        cursor.execute(insert_query, data)

        # Commit and close connections
        connection.commit()
        cursor.close()
        connection.close()
        


