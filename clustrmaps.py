import csv
import time
import pandas as pd
from undetected_chromedriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException
from parsel import Selector
from urllib.parse import urljoin
import coloredlogs, logging

logger = logging.getLogger("Clustrmaps Scraper")
coloredlogs.install(level='DEBUG', logger=logger)



class Clustr():

    def __init__(self):
        self.BASE_URL = 'https://clustrmaps.com/'
        self.CSV_PATH = 'input.csv'
        self.all_person_data = pd.read_csv(self.CSV_PATH)
        self.all_data = []
        self.all_cols = self.all_person_data.columns.to_list()
        if 'PROBABLE EMAIL' not in self.all_cols:
            self.all_cols.append('PROBABLE EMAIL')


    def init_driver(self):
        s = Service(ChromeDriverManager().install())
        driver = Chrome(service=s)
        return driver


    def search_person(self, person_name, driver):
        logger.debug(f'search_person : {person_name}')
        driver.get(self.BASE_URL)
        time.sleep(1.5)
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, '//a[text()="People"]')))
        driver.find_element(By.XPATH, '//a[text()="People"]').click()
        time.sleep(1.5)
        driver.find_element(By.XPATH, '//input[@name="q"]').send_keys(person_name)
        driver.find_element(By.XPATH, '//input[@name="q"]').submit()
        logger.debug(f'search_person completed')

    def get_person_with_specific_city(self, city, driver):
        logger.debug(f'get_person_with_specific_city')
        sel = Selector(driver.page_source)
        return sel.xpath(f'//div[@itemprop="Person" and .//div[@class="person_city person_details i_home" and contains(text(),"{city}")]]/div/div/a/@href').get()

    def extract_person_data(self, row, driver):
        logger.debug(f'extract_person_data')
        sel = Selector(driver.page_source)
        # row['URL'] = driver.current_url
        row['PROBABLE EMAIL'] = sel.xpath('//span[@itemprop="email"]/text()').get()

        for ph_li in sel.xpath('//div//h2[contains(text(), "Phone Number")]/..//li')[:1]:
            row[f'Phone'] = ph_li.xpath('./a/span[@itemprop="telephone"]/text()').get()
            # p1 = ph_li.xpath('./text()').get('').strip()
            # p2 = ph_li.xpath('./a[@class="relphone"]/text()').get('').strip()
            # row[f'Phone {n} Status'] = (p1 + ' ' + p2).strip()

        return row

    def check_if_xpath_exists(self, xpath, driver):
        logger.debug(f'check_if_xpath_exists:{xpath}')
        try:
            driver.find_element(By.XPATH, xpath)
        except NoSuchElementException:
            return False
        return True

    def is_result_found(self, driver):
        logger.debug(f'is_result_found')
        for i in range(20):
            if self.check_if_xpath_exists('//div[@itemprop="Person"]', driver):
                return True
            elif self.check_if_xpath_exists("//h1[contains(text(), 'No results found')]"):
                return False
            time.sleep(1)
        return False

    def start(self):
        driver = self.init_driver()
        try:
            for n, row in self.all_person_data.iterrows():

                if not(row['Executive First Name'] or row['Executive Last Name'] or row['County']):
                    logger.warn(f"Skipping row as there is missing of one of following field: First Name, Last Name, and County")
                    self.all_data.append(row)
                    continue
                elif not(row['Phone']) and row['Phone'] != 'Not Available':
                    logger.warn(f"Skipping row as Phone number already exist for the following row")
                    self.all_data.append(row)
                    continue

                self.search_person(f'{row["Executive First Name"]} {row["Executive Last Name"]}', driver)
                while True:
                    if not self.is_result_found(driver):
                        break
                    person_url = self.get_person_with_specific_city(row['County'], driver)
                    if person_url:
                        driver.get(urljoin(self.BASE_URL,person_url))
                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located( (By.XPATH, '//h1/span[@itemprop="name"]') ))
                        person_info = self.extract_person_data(row, driver)
                        
                        self.all_data.append(person_info)
                        logger.info(f"Scraped: {person_info}")
                        df = pd.DataFrame(self.all_data, columns=self.all_cols)
                        df.to_csv('out.csv')
                        break
                    else:
                        if self.check_if_xpath_exists('//li[@class="page-item active"]/following-sibling::li/a', driver):
                            driver.find_element(By.XPATH, '//li[@class="page-item active"]/following-sibling::li/a').click()
                        else:
                            break
        except Exception:
            pass
        finally:
            driver.close()

if __name__ == '__main__':
    c = Clustr()
    c.start()