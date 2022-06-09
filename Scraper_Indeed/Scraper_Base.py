
CHROME_PATH = "./chromedriver"

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup

import time, os
import subprocess as sp
import requests

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


import warnings
warnings.filterwarnings("ignore")

from glob import glob

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from datetime import datetime

class Scraper_Base:
    URL="google.com"
    CHROME_PATH=CHROME_PATH
    LIMIT = 100
        
    def __init__(self) -> None:
        try:
            print("  Creating Request Session..", end="")
            self.request_session()
            print(" --OK.")
        except Exception as e:
            print(e)
        pass
    
    def create_options(self) -> webdriver.ChromeOptions:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("window-size=1024,768")
        options.add_argument("--no-sandbox")
        return options

    def start_driver(self, url=URL, silent=False) -> None:
        if silent is True:
            options = self.create_options()
            self.driver = webdriver.Chrome(self.CHROME_PATH, options=options)
        else:
            self.driver = webdriver.Chrome(self.CHROME_PATH)
        self.driver.implicitly_wait(0.5)
        self.driver.get(url)

    def soup(self, html_content) -> BeautifulSoup:
        soup = BeautifulSoup(html_content, 'html.parser')
        return soup

    def request_session(self) -> None:
        self.session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def extract_data(self, url:str, delay=5) -> str:
        time.sleep(delay)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)'}
        if url == "": return ""
        response = self.session.get(url, headers=headers)
        return response.text
    
    def search_bar(self, key, input=None) -> None:
        if input is None: input = self.driver.find_element_by_xpath("//input[@id='inputword']")
        input.send_keys(key)
        input.send_keys(Keys.RETURN)

    # Toggle Next Page
    def next_page(self, next="//i[@class='wt-icon wt-icon--smaller next']") -> bool:
        try: 
            x = self.driver.find_element_by_xpath(next)
            x.click()
            return True
        except:
            x = None
        
        if x is None: return False

    def close(self):
        try:
            self.driver.close()
            print("  Chrome Driver closed.")
        except Exception as e:
            print(e)

