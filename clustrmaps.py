import csv
import time
import pandas as pd
from undetected_chromedriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException
from parsel import Selector
import scrapy
from urllib.parse import urljoin
import coloredlogs, logging
import requests
import time

logger = logging.getLogger("Clustrmaps Scraper")
coloredlogs.install(level='DEBUG', logger=logger)

class Clustr():
    def __init__(self):
        self.common_names = [
            'joseph',
            'mark',
            'david',
            'brandon',
            'michael',
            'john',
            'garcia',
            'lopez',
            'williams',
            'smith',
            'jackson',
            'henderson',
            'garrett',
            'anderson',
            'cooper',
            'taylor'
        ]
        self.BASE_URL = 'https://clustrmaps.com/'
        self.search_api = "https://clustrmaps.com/s/"
        self.CSV_PATH = 'input.csv'
        self.all_person_data = pd.read_csv(self.CSV_PATH)
        self.all_data = []
        self.all_cols = self.all_person_data.columns.to_list()
        if 'PROBABLE EMAIL' not in self.all_cols:
            self.all_cols.append('PROBABLE EMAIL')
        self.headers = {
          'Accept': 'application/json, text/javascript, */*; q=0.01',
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
          'X-Requested-With': 'XMLHttpRequest',
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
          'host': 'clustrmaps.com',
        }



    def init_driver(self):
        driver = Chrome(headless=False)
        return driver

    @staticmethod
    def is_match(found_name, first, last):
        first = first.strip().lower()
        last = last.strip().lower()
        found_name = found_name.strip().lower()
        if first in found_name and last in found_name:
            return True
        else:
            return False


    def is_exist(self, driver, element):
        try:
            driver.find_element(By.XPATH, element)
        except (NoSuchElementException, StaleElementReferenceException):
            return False
        else:
            return True


    def match_county(self, url, county, driver, single_result, recursive=None):
        if not recursive:
            driver.get(url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//div[@class='container mt-4']")))
        time.sleep(5)
        sel = scrapy.Selector(text=driver.page_source)
        if single_result:
            county_path = "//li[@class='breadcrumb-item active'][last()]"
            if self.is_exist(driver, county_path):
                found_county = driver.find_element(By.XPATH, county_path).text
                if county.lower() in found_county.lower():
                    return True
        else:
            counties_path = "//div[@itemprop='Person']"
            if self.is_exist(driver, counties_path):
                counties = sel.xpath(counties_path)
                for c in counties:
                    c_name = c.xpath(".//div[@class='person_city person_details i_home']/text()").get()
                    if c_name:
                        if county.lower() in c_name.lower():
                            url = c.xpath(".//span[@itemprop='name']/parent::a/parent::div/a/@href").get()
                            return url
            next_page = "//a[text()='??']"
            if self.is_exist(driver, next_page):
                driver.find_element(By.XPATH, next_page).click()
                return self.match_county(None, county, driver, None, recursive=True)
        return False


    # search by name
    def get_person_url(self, first, last, county, driver):
        person_name = first+" "+last
        logger.debug(f'search_via name : {person_name}')
        payload = {"action":'tools.suggest_all', "start":person_name}
        response = requests.post(self.search_api, headers=self.headers, data=payload).json()
        if response.get("values").get("person"):
            for person in response.get("values").get("person"):
                name = person.get("name").lower()
                if first.lower() in name and last.lower() in name:
                    url = urljoin(self.BASE_URL, person.get("url"))
                    if "persons" in name:
                        result = self.match_county(url, county, driver, single_result=False)
                        if result:
                            return result
                    else:
                        if self.match_county(url, county, driver, single_result=True):
                            return url


    # search by address
    def get_address_url(self, address):
        logger.debug(f'search_via address : {address}')
        payload = {"action":'tools.suggest_all', "start":address}
        response = requests.post(self.search_api, headers=self.headers, data=payload).json()
        if response.get("values").get("address"):
            for addr in response.get("values").get("address"):
                url = urljoin(self.BASE_URL, addr.get("url"))
                return url
    

    def search_address(self, driver, address, url, first, last, county):
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//input[@name='q']")))
        try:
            driver.find_element(By.XPATH, "//p[@class='lead']")
        except NoSuchElementException:
            residents = driver.find_elements(By.XPATH, "//div[@itemprop='relatedTo']")
            for resident in residents:
                found_name = resident.find_element(By.XPATH, ".//span[@itemprop='name']").text
                url = resident.find_element(By.XPATH, ".//span[@itemprop='name']/ancestor::a").get_attribute("href")
                if self.is_match(found_name, first, last):
                    logger.info(" Match Found ")
                    # if self.match_county(url, county, driver, single_result=True):
                    return url


 
    def extract_person_data(self, row, driver):
        logger.debug(f'extract_person_data')
        sel = Selector(driver.page_source)
        # row['URL'] = driver.current_url
        row['PROBABLE EMAIL'] = sel.xpath('//span[@itemprop="email"]/text()').get()

        phones = []
        for ph_li in sel.xpath('//div//h2[contains(text(), "Phone Number")]/..//li')[:2]:
            phone = ph_li.xpath('./a/span[@itemprop="telephone"]/text()').get()
            if phone:
                phones.append(phone.strip())
        row[f'Phone'] = ",".join(phones)
        return row


    def scrape_person(self, driver, person_url, row):
        person_url = urljoin(self.BASE_URL, person_url)
        driver.get(person_url)
        WebDriverWait(driver, 30).until(EC.visibility_of_element_located( (By.XPATH, '//h1/span[@itemprop="name"]') ))
        person_info = self.extract_person_data(row, driver)
        self.all_data.append(person_info)
        logger.info(f"Scraped: {person_info}")
        df = pd.DataFrame(self.all_data, columns=self.all_cols)
        df.to_csv('out.csv')


    def start(self):
        driver = self.init_driver()
        for n, row in self.all_person_data.iterrows():
            first_name = row['Executive First Name']
            last_name = row['Executive Last Name']
            if type(first_name) != float and type(last_name) != float:
                if not(first_name or last_name or row['County']) :
                    logger.warning(f"Skipping row as there is missing of one of following field: First Name, Last Name, and County")
                    self.all_data.append(row)
                    continue
                elif not(row['Phone']) and row['Phone'] != 'Not Available':
                    logger.warning(f"Skipping row as Phone number already exist for the following row")
                    self.all_data.append(row)
                    continue
                elif any([name for name in self.common_names if name.lower() in first_name.lower() or name.lower() in last_name.lower()]):
                    logger.warning(f"Skipping row (common)")
                    self.all_data.append(row)
                    continue
                
                person_url = self.get_person_url(first_name, last_name, row['County'], driver)
                if person_url:
                    self.scrape_person(driver, person_url, row)
                else:
                    if type(row['Address']) != float:
                        address_url = self.get_address_url(row['Address'])
                        if address_url:
                            person_url = self.search_address(driver, row['Address'], address_url, first_name, last_name, row['County'])
                            if person_url:
                                self.scrape_person(driver, person_url, row)

c = Clustr()
c.start()