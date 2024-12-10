import numpy as np
import pandas as pd
import requests
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service

import config
import mysql
from sqlalchemy import create_engine

from pymongo import MongoClient

ENDPOINT = config.rds_endpoint
USERNAME = config.rds_username
PASSWORD = config.rds_password
DATABASE = config.rds_database
MONGOCNX = config.mongo_cnx_string
MONGODB_NAME = 'tennis_prod'
MONGODB_COLLECTION = 'tennisprod'


class AtlasClient ():

   def __init__ (self, mongocnx, dbname):
       self.mongodb_client = MongoClient(mongocnx)
       self.database = self.mongodb_client[dbname]

   def ping (self):
       self.mongodb_client.admin.command('ping')

   def get_collection (self, collection_name):
       collection = self.database[collection_name]
       return collection

   def find (self, collection_name, filter = {}, limit=0):
       collection = self.database[collection_name]
       items = list(collection.find(filter=filter, limit=limit))
       return items

   def insert (self, collection_name, data = {}):
       collection = self.database[collection_name]
       x = collection.insert_many(data)


def runCrawler():

    #con = mysql.connector.connect(user=username, password=password, host=endpoint, database=database)
    #mycursor = con.cursor()
    engine = create_engine(f'mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{ENDPOINT}/{DATABASE}')

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--remote-debugging-pipe')
    chrome_options.add_argument("enable-automation")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--dns-prefetch-disable")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size = 1920,1080")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("enable-features=NetworkServiceInProcess")
    chrome_options.add_argument("disable-features=NetworkService")

    #service = Service(executable_path='//wsl.localhost/Ubuntu/home/lauhiuyan/airflow/dags/chromedriver-win64/chromedriver-win64')
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.directtennis.co.uk/tennis-rackets")

    df = pd.DataFrame(columns=['Product Name', 'Product Price', 'Old Price', 'Product Cat', 'Time Added'])
    menuitem = driver.find_elements(By.XPATH, '//span[@class="megamenu-title"]//a')

    for b in range(len(menuitem)):
        menuitem = driver.find_elements(By.XPATH, '//span[@class="megamenu-title"]//a')
        topbar = driver.find_elements(By.XPATH, '//a[@class="megamenu-header"]')
        prodcat = menuitem[b].text
        scrapetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for a in range(len(topbar)):
            try:
                hover = ActionChains(driver).move_to_element(topbar[a])
                hover.perform()
                time.sleep(1)

                menuitem[b].click()
                time.sleep(1)
                break
            except:
                continue

        product_names = []
        product_prices = []
        old_prices = []
        product_category = []
        time_added = []

        while True:
            product_names.extend(x.text for x in driver.find_elements(By.XPATH,
                                                                      '//div[@class="col-12 product-row"]//div[@class="block-with-text"]'))
            product_prices.extend(y.text for y in driver.find_elements(By.XPATH,
                                                                       '//div[@class="col-12 product-row"]//span[@class="new-price"]'))
            old_prices.extend(z.text for z in driver.find_elements(By.XPATH,
                                                                   '//div[@class="col-12 product-row"]//span[@class="old-price"]'))
            product_category.extend(prodcat for x in driver.find_elements(By.XPATH,
                                                                          '//div[@class="col-12 product-row"]//div[@class="block-with-text"]'))
            time_added.extend(scrapetime for x in driver.find_elements(By.XPATH,
                                                                       '//div[@class="col-12 product-row"]//div[@class="block-with-text"]'))

            try:
                next_button = driver.find_element(By.ID, 'btnNextTop')
                next_button.click()
                time.sleep(3)
            except:
                break

        dict = {'Product Name': product_names, 'Product Price': product_prices, 'Old Price': old_prices,
                'Product Cat': product_category, 'Time Added': time_added}
        df1 = pd.DataFrame(dict)
        data_dict = json.loads(df1.to_json(orient="records"))

        df1.to_sql(name='tennisprod', con=engine, if_exists='append', index=False)
        atlas_client = AtlasClient(MONGOCNX, MONGODB_NAME)
        atlas_clinet.insert(MONGODB_COLLECTION, data_dict)

        df = pd.concat([df, df1])
        print(f"{b + 1} of {len(menuitem)} items scraped....")

        driver.execute_script("window.scrollTo(0, 0)")

    driver.quit()
    df.to_csv('data/direct_tennis_products.csv', index=False)


if __name__ == '__main__':
    runCrawler()