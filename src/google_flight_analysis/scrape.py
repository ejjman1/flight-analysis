# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import date, datetime, timedelta
import re
import os
import csv
import numpy as np
import pandas as pd
from tqdm import tqdm

from src.google_flight_analysis.flight import Flight

# logging
logger_name = os.path.basename(__file__)
logger = logging.getLogger(logger_name)


class Scrape:

    def __init__(self, orig, dest, date_leave, country='US', currency='USD', date_return=None, export=False):
        self._origin = orig
        self._dest = dest
        self._date_leave = date_leave
        self._date_return = date_return
        self._round_trip = (True if date_return is not None else False)
        self._export = export
        self._data = None
        self._url = None
        self._country = country
        self._currency = currency

    def run_scrape(self):
        self._data = self._scrape_data()

        if self._export:
            Flight.export_to_csv(self._data, self._origin,
                                 self._dest, self._date_leave, self._date_return)

    def __str__(self):
        if self._date_return is None:
            return "{dl}: {org} --> {dest}".format(
                dl=self._date_leave,
                org=self._origin,
                dest=self._dest
            )
        else:
            return "{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
                dl=self._date_leave,
                dr=self._date_return,
                org=self._origin,
                dest=self._dest
            )

    def __repr__(self):
        if self._date_return is None:
            return "{n} RESULTS FOR:\n{dl}: {org} --> {dest}".format(
                n=self._data.shape[0],
                dl=self._date_leave,
                org=self._origin,
                dest=self._dest
            )
        else:
            return "{n} RESULTS FOR:\n{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
                n=self._data.shape[0],
                dl=self._date_leave,
                dr=self._date_return,
                org=self._origin,
                dest=self._dest
            )

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, x: str) -> None:
        self._origin = x

    @property
    def dest(self):
        return self._dest

    @dest.setter
    def dest(self, x: str) -> None:
        self._dest = x

    @property
    def date_leave(self):
        return self._date_leave

    @date_leave.setter
    def date_leave(self, x: str) -> None:
        self._date_leave = x

    @property
    def date_return(self):
        return self._date_return

    @date_return.setter
    def date_return(self, x: str) -> None:
        self._date_return = x

    @property
    def round_trip(self):
        return self._round_trip

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        self._data = x

    @property
    def url(self):
        return self._url

    def create_driver(self):
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        # otherwise data such as layover location and emissions is not displayed
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--incognito")
        # options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(service=Service(
            ChromeDriverManager().install()), options=options)

        return driver

    def _scrape_data(self):
        """
        Scrapes the Google Flights page and returns a DataFrame of the results.
        """
        driver = self.create_driver()
        self._url = self._make_url()
        flight_results = self._get_results(driver)
        driver.quit()

        return flight_results

    def _make_url(self):
        """
        From the class parameters, generates a dynamic Google Flight URL to scrape, taking into account if the
        trip is one way or roundtrip.
        """
        if self._round_trip:
            return 'https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20from%20{date_leave}%20to%20{date_return}&curr={curr}&gl={country}'.format(
                dest=self._dest,
                org=self._origin,
                date_leave=self._date_leave,
                date_return=self._date_return,
                curr=self._currency,
                country=self._country
            )
        else:
            return 'https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20on%20{date_leave}%20oneway&curr={curr}&gl={country}'.format(
                dest=self._dest,
                org=self._origin,
                date_leave=self._date_leave,
                curr=self._currency,
                country=self._country
            )

    def _get_results(self, driver):
        """
        Returns the scraped flight results as a DataFrame.
        """
        results = None
        try:
            results = Scrape._make_url_request(self._url, driver, self._date_return)
        except TimeoutException:
            logger.error(f"Scrape timeout reached. It could mean that no flights exist for the combination of airports and dates." )
            return -1
        if self._date_return is None:
            flights = self._clean_results_oneway(results)
        else:
            flights = self._clean_results_roundtrip(results)
        return Flight.dataframe(flights)

    def _clean_results_oneway(self, result):
        """
        Cleans and organizes the raw text strings scraped from the Google Flights results page.
        """
        res2 = [x.encode("ascii", "ignore").decode().strip() for x in result]

        price_trend_dirty = [
            x for x in res2 if x.startswith("Prices are currently")]
        price_trend = Scrape.extract_price_trend(price_trend_dirty)

        # grab destination info
        skip_mid_end = False
        start = res2.index("Sort by:")+1
        try:
            mid_start = res2.index("Price insights")
        except ValueError:
            try:
                mid_start = res2.index("Other flights")
            except:
                try:
                    mid_start = [i for i, x in enumerate(res2) if x.endswith('more flights')][0]
                    skip_mid_end = True
                except:
                    try:
                        mid_start = ([i for i, x in enumerate(res2[start_return:]) if x.startswith('Language')][0]) + start_return
                        skip_mid_end = True
                    except:
                        logger.error(f"mid_start failure with list: {res2}")
        res3 = res2[start:mid_start]
        
        mid_end = -1
        if not skip_mid_end:
            try:
                mid_end = res2.index("Other departing flights")+1
            except:
                try:
                    mid_end = res2.index("Other flights")+1
                except:
                    logger.error(f"mid_end failure with list: {res2}")
            
            try:
                end = [i for i, x in enumerate(res2) if x.endswith('more flights')][0]
            except:
                try:
                    end = [i for i, x in enumerate(res2) if 'Hide' in x][0]
                except:
                    logger.error(f"end failure with list: {res2}")
            res3 += res2[mid_end:end]

        #   grab return info
        if self._date_return != None:
            skip_mid_end_return = False
            start_return = res2.index("Sort by:", start + 1)+1
            try:
                mid_start_return = res2.index("Price insights", start_return + 1)
            except ValueError:
                try:
                    mid_start_return = res2.index("Other flights", start_return + 1)
                except:
                    try:
                        mid_start_return = ([i for i, x in enumerate(res2[start_return:]) if x.endswith('more flights')][0]) + start_return
                        skip_mid_end_return = True
                    except:
                        try:
                            mid_start_return = ([i for i, x in enumerate(res2[start_return:]) if x.startswith('Language')][0]) + start_return
                            skip_mid_end_return = True
                        except:
                            logger.error(f"mid_start_return failure with list: {res2}")
            res3 += res2[start_return:mid_start_return]

            mid_end_return = -1
            if not skip_mid_end_return:
                try:
                    mid_end_return = res2.index("Other departing flights", mid_end + 1)
                except:
                    try:
                        mid_end_return = res2.index("Other flights", mid_end + 1)
                    except:
                        logger.error(f"mid_end_return failure with list: {res2}")

                try:
                    end_return = [i for i, x in enumerate(res2[end:]) if x.endswith('more flights')][0]
                except:
                    try:
                        end_return = [i for i, x in enumerate(res2[end:]) if 'Hide' in x][0]
                    except:
                        logger.error(f"end_return failure with list: {res2}")
                res3 = res2[mid_end_return:end_return]

        matches = []
        # Enumerate over the list 'res3'
        for index, element in enumerate(res3):

            # Check if the length of the element is more than 2
            if len(element) <= 2:
                continue

            # Check if the element ends with 'AM' or 'PM' (or AM+, PM+)
            is_time_format = bool(
                re.search("\d{1,2}\:\d{2}(?:AM|PM)\+{0,1}\d{0,1}", element))

            # If the element doesn't end with '+' and is in time format, then add it to the matches list
            if (element[-2] == '+' or is_time_format):
                matches.append(index)
            # if (element[-2] != '+' and is_time_format):
                # matches.append(index)

        # Keep only every second item in the matches list
        matches = matches[::2]

        flights = [
            Flight(
                self._date_leave,  # date_leave
                self._round_trip,  # round_trip
                self._origin,
                self._dest,
                price_trend,
                res3[matches[i]:matches[i+1]]) for i in range(len(matches)-1)
        ]

        return flights

    #TODO: Finish cleaning results.
    # Thought is round trip is different enough from oneway to separate def.
    #def _clean_results_roundtrip(self, result):

    @staticmethod
    def extract_price_trend(s):
        """
        From a dirty string, return a tuple in format (price_trend, trend value) for a given flight.
        For example:
        (typical, None): Prices for that dates/airports are currently average
        (low, 100): Prices are lower than usual by 100€
        (high, None): Prices are higher than usual
        """
        if not s:
            return (None, None)

        s = s[0]
        if s == "Prices are currently typical":
            return ("typical", None)

        elif s == "Prices are currently high":
            return ("high", None)

        elif "cheaper" in s:
            how_cheap = 0
            numeric_value = ''.join([char for char in s if char.isdigit()])
            if numeric_value:
                how_cheap = str(numeric_value)

            return ("low", how_cheap)

        else:
            return (None, None)

    @staticmethod
    def _identify_google_terms_page(page_source: str):
        """
        Returns True if the page html represent Google's Terms and Coditions page.
        """
        if "Before you continue to Google" in page_source:
            return True
        return False

    @staticmethod
    def _make_url_request(url, driver, dateReturn):
        """
        Get raw results from Google Flights page.
        Also handles auto acceptance of Google's Terms & Conditions page.
        """
        timeout = 15
        driver.get(url)
        moreFlights = False

        # detect Google's Terms & Conditions page (not always there, only in EU)
        if Scrape._identify_google_terms_page(driver.page_source):
            WebDriverWait(driver, timeout).until(
                lambda s: Scrape._identify_google_terms_page(s.page_source))

            # click on accept terms button
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(., 'Accept all')]"))).click()

        #   Click the more flights button at bottom of screen to load more flights
        if moreFlights:
            button_class = "VfPpkd-LgbsSe.VfPpkd-LgbsSe-OWXEXe-k8QpJ.VfPpkd-LgbsSe-OWXEXe-Bz112c-M1Soyc.VfPpkd-LgbsSe-OWXEXe-dgl2Hf.nCP5yc.AjY5Oe.LQeN7.nJawce.OTelKf.iIo4pd"
            try:
                WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, f"button.{button_class}"))).click()
            except:
                logger.info(f"No 'more flights' on: {url}")

        # wait for flight data to load and initial XPATH cleaning
        # originally this was 100, but once expanding More Flights, you get upward of 1000+ results.
        # TODO: Identify 'Help Center' for now, but I think it pops up before page is fully loaded..?
        if moreFlights:
            WebDriverWait(driver, timeout).until(
                lambda d: len(Scrape._get_flight_elements(d)) > 250)
        else:
            WebDriverWait(driver, timeout).until(
                lambda d: len(Scrape._get_flight_elements(d)) > 40)

        results = Scrape._get_flight_elements(driver)

        # TODO: This needs further testing scenarios
        if dateReturn != None:
            depart_headers = driver.find_elements(By.XPATH, "//ul[@class='Rk10dc']")
            for i in range(len(depart_headers)):
                depart_header_element = driver.find_elements(By.XPATH, "//ul[@class='Rk10dc']")[i]
                #   now we have to check how many flights are in this header element.
                group_element = depart_header_element.find_elements(By.XPATH, ".//li[@class='pIav2d']")
                for j in range(len(group_element)):
                    depart_header_element = driver.find_elements(By.XPATH, "//ul[@class='Rk10dc']")[i]
                    group_element = depart_header_element.find_elements(By.XPATH, ".//li[@class='pIav2d']")[j]
                    #   once inside the list item, find the element with button and click
                    flight_element = group_element.find_element(By.XPATH, ".//div[@class='JMc5Xc']")
                    driver.execute_script("arguments[0].click();", flight_element)
                    
                    WebDriverWait(driver, timeout).until(
                        lambda d: any('returning' in element.lower() for element in Scrape._get_flight_elements(d)))

                    results += Scrape._get_flight_elements(driver)

                    return_element = driver.find_element(By.XPATH, "//div[@class='AMbwDd zlyfOd']")
                    driver.execute_script("arguments[0].click();", return_element)

                    WebDriverWait(driver, timeout).until(
                        # lambda d: len(Scrape._get_flight_elements(d)) > 40)
                        lambda d: 'Best departing flights' in Scrape._get_flight_elements(d))
        
        # TODO: This is just here because roundtrips take a long time to parse for debug.
        # with open('bigList.csv', 'w', newline='', encoding='utf-8') as csvfile:
        #     writer = csv.writer(csvfile)
        #     for index in range(len(results)):
        #         try:
        #             writer.writerow([results[index]])
        #         except:
        #             writer.writerow("NONE")
        
        return results

    @staticmethod
    def _get_flight_elements(driver):
        """
        Returns all html elements that contain/have to do with flight data.
        """
        return driver.find_element(by=By.XPATH, value='//body[@id = "yDmH0d"]').text.split('\n')
