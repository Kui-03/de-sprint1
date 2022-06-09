
# ====================================== #
# * Begin Config
# ====================================== #

URL = "https://ph.indeed.com"
CHROME_PATH = "./chrome_driver"
OUT = "./Indeed/"
LIMIT = -1
SILENT = True

# -------------------------------------- #
# * Import modules
# -------------------------------------- #

from datetime import datetime
from selenium.webdriver.common.keys import Keys
from Scraper_Base import Scraper_Base

import pandas as pd
import re
from time import sleep
from __file import mkdir

# ====================================== #
# * Class Scraper_Indeed
# -------------------------------------- #
class Scraper_Indeed(Scraper_Base):
    URL = URL
    OUT = OUT
    LIMIT = LIMIT

    # -------------------------------------- #
    # * Initialize
    # -------------------------------------- #
    def __init__(self) -> None:
        print("  Starting Chrome Driver..", end="")
        super().start_driver(url=self.URL, silent=SILENT)
        print(" --OK.")

        # Navigate to PH
        print("  Navigate to PH..", end='')
        input = self.driver.find_element_by_id('text-input-where')
        input.send_keys('Philippines')
        input.send_keys(Keys.RETURN)
        print(" --OK.")

        # Navigate to 24 HR
        print("  Navigate to 24 HR..", end="")
        p = self.soup(self.driver.page_source)
        get = URL+[i for i in p.find_all('a') if i.text.lower()=='last 24 hours'][0].attrs['href']
        sleep(3)
        self.driver.get(get)
        print(" --OK.")

        # Navigate to Remote
        print("  Navigate to Remote..", end="")
        p = self.soup(self.driver.page_source)
        get = URL+[i for i in p.find_all('a') if 'remote' in i.text.lower()][0].attrs['href']
        sleep(3)
        self.driver.get(get)
        print(" --OK.")

        # Close Popup
        self.close_popup()        
        super().__init__()

    # -------------------------------------- #
    # * Close Popups
    # -------------------------------------- #
    def close_popup(self):
        sleep(1)
        try:
            popup = self.driver.find_element_by_xpath('//button[@aria-label=="Close"]')
        except:
            popup = None
        if popup is not None:
            popup.click()

    # -------------------------------------- #
    # * Start Scraping
    # -------------------------------------- #
    def start(self):
        next = True
        _dir = mkdir(self.OUT)
        format='%Y-%m-%d'#_%H-%M-%S'
        print("\n---\nStarted Scraping at %s." % str(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        while next is True:
            page = self.soup(self.driver.page_source)
            on_page = page.find('div', {'class':'pagination'}).find(attrs={'aria-current':'true'}).text

            df = self.get_page_data(page)
            self.check_valid_data(df)

            print("  Creating Dataframe..")
            try:
                now = datetime.now().strftime(format)
                fn = _dir+'Indeed_Page-%s_%s.prq' % (on_page, now)
                df.to_parquet(fn, index=False)
            except Exception as e:
                print(e)
            print("  File Saved to: '%s'" % fn)
            next = self.next_page()

        self.close()
        now = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        print("\n---\nScraping Finished!")
        print("Time: %s" % now)

    # -------------------------------------- #
    # * Get Page Data
    # -------------------------------------- #
    def get_page_data(self, page):
        # Get HTML Job data containers
        jobs = page.find_all('div', {'class':'job_seen_beacon'})
        # Set Limits
        if self.LIMIT == -1: limit=len(jobs)+2
        else: limit = self.LIMIT

        dum={}
        for i, job in enumerate(jobs):
            details = job.find('h2', attrs={'class': re.compile('jobTitle*')}).a
            id=details.attrs['id'].split('_')[1]
            job_title = details.find('span', {'title':re.compile('.')}).text
            url = self.URL+details.attrs['href']
            print("[%d/%d] Scraping %s.." % (i+1, len(jobs), job_title))

            # Job description cannot be empty, skips if None:
            url_page=self.soup(self.extract_data(url))
            job_description = url_page.find('div', {'id':'jobDescriptionText'})
            if job_description is None: 
                print("  No job describoption found, skipping..")
                continue
            else: job_description = job_description.text

            # Qualification & Salary can be Empty
            qualification = url_page.find('div', {'id':'qualificationsSection'})
            if qualification is not None: qualification=qualification.text.lower().replace('qualifications', '')
            salary = details.find(attrs={'aria-label':'Salary'})
            if salary is not None: salary=salary.text

            # Get company data
            company = job.find(attrs={'class':'companyName'}).text
            company_location = job.find(attrs={'class':'companyLocation'}).text

            # Post status: 1 day ago, etc.
            post_status=url_page.find(attrs={'class': 'jobsearch-HiringInsights-entry--text'}).text #job.find('span', {'class':'date'}).text
            post_status=post_status.lower().replace('posted ','')
            # Check for overdue, only to get max of 1 day.
            day = re.findall('(\d) day', post_status)
            if len(day) > 0: 
                day=int(day[0])
                if day > 1: 
                    print("  Warning!: Overdue post detected! %s" % post_status)
                    continue

            # Create dictionary
            data = {
                'id':id,
                'job_title':job_title,
                'job_description':job_description,
                'qualification':qualification,
                'salary':salary,
                'company':company,
                'company_location':company_location,
                'post_status':post_status,
                'url':url,
                }

            # Assign to dict
            dum[i]=data

            # Break if Limit
            if i >= limit:
                print("  Exceeding set Limit: %d, breaking loop." % limit)
                break

        # Create & Return a DataFrame
        df = pd.DataFrame.from_dict(dum, orient='index')
        return df

    # -------------------------------------- #
    # * Validate data
    # -------------------------------------- #
    def check_valid_data(self, df:pd.DataFrame) -> bool:
        print('  Validating Data..', end="")
        # An Empty DataFrame
        if df.empty:
            self.close()
            raise Exception('  Error: DataFrame is empty!')
        
        # Null values on important columns
        cols = ['id', 'job_title', 'job_description', 'company', 'url' ,'post_status']
        if df[cols].isnull().values.any():
            print(df[cols].isna().sum())
            self.close()
            raise Exception('  Error: Null values detected!')
        
        # Primary Key unique
        if not df['id'].is_unique:
            print(df[['id']])
            self.close()
            raise Exception('  Error: Primary key values [id] are not unique!')

        sleep(1)
        print('  --OK.')
        return True

    # -------------------------------------- #
    # * Toggle Next Page
    # -------------------------------------- #
    def next_page(self):
        sleep(3)
        x = super().next_page(next='//a[@aria-label="Next"]')
        self.close_popup()
        return x



# -------------------------------------- #
#  * Main
# -------------------------------------- #
if __name__ == "__main__":
    sc = Scraper_Indeed()
    sc.start()
    