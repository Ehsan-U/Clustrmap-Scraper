import csv
import time
import pandas as pd
from undetected_chromedriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException
from parsel import Selector
from urllib.parse import urljoin
import coloredlogs, logging
import requests

logger = logging.getLogger("Clustrmaps Scraper")
coloredlogs.install(level='DEBUG', logger=logger)

class Clustr():
    def __init__(self):
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


    # search by name
    def get_person_url(self, first, last):
        logger.debug(f'search_via name : {first+" "+last}')
        payload = {"action":'tools.suggest_all', "start":first}
        response = requests.post(self.search_api, headers=self.headers, data=payload).json()
        if response.get("values").get("person"):
            for person in response.get("values").get("person"):
                name = person.get("name").lower()
                if first.lower() in name and last.lower() in name:
                    url = urljoin(self.BASE_URL, person.get("url"))
                    return url

    # search by address
    def get_address_url(self, address):
        payload = {"action":'tools.suggest_all', "start":address}
        response = requests.post(self.search_api, headers=self.headers, data=payload).json()
        if response.get("values").get("address"):
            for addr in response.get("values").get("address"):
                url = urljoin(self.BASE_URL, addr.get("url"))
                return url
    

    def search_address(self, driver, address, url, first, last):
        logger.debug(f'search_via address : {address}')
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//input[@name='q']")))
        try:
            driver.find_element(By.XPATH, "//p[@class='lead']")
        except NoSuchElementException:
            residents = driver.find_elements(By.XPATH, "//div[@itemprop='relatedTo']")
            for resident in residents:
                found_name = resident.find_element(By.XPATH, ".//span[@itemprop='name']").text
                url = resident.find_element(By.XPATH, ".//span[@itemprop='name']/ancestor::a").get_attribute("href")
                if self.is_match(found_name, first, last):
                    logger.info(" Match Found ")
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
        driver.get(urljoin(self.BASE_URL,person_url))
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located( (By.XPATH, '//h1/span[@itemprop="name"]') ))
        person_info = self.extract_person_data(row, driver)
        self.all_data.append(person_info)
        logger.info(f"Scraped: {person_info}")
        df = pd.DataFrame(self.all_data, columns=self.all_cols)
        df.to_csv('out.csv')


    def start(self):
        driver = self.init_driver()
        for n, row in self.all_person_data.iterrows():
            if not(row['Executive First Name'] or row['Executive Last Name'] or row['County']):
                logger.warn(f"Skipping row as there is missing of one of following field: First Name, Last Name, and County")
                self.all_data.append(row)
                continue
            elif not(row['Phone']) and row['Phone'] != 'Not Available':
                logger.warn(f"Skipping row as Phone number already exist for the following row")
                self.all_data.append(row)
                continue
            if type(row['Executive First Name']) != float and type(row['Executive Last Name']) != float:
                person_url = self.get_person_url(row['Executive First Name'], row['Executive Last Name'])
                if person_url:
                    self.scrape_person(driver, person_url, row)
                else:
                    address_url = self.get_address_url(row['Address'])
                    if address_url:
                        person_url = self.search_address(driver, row['Address'], address_url, row['Executive First Name'], row['Executive Last Name'])
                        if person_url:
                            self.scrape_person(driver, person_url, row)


c = Clustr()
c.start()