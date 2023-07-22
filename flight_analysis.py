# author: Emanuele Salonico, 2023

import utils
import os

# logging
logger_name = os.path.basename(__file__)
logger = utils.setup_logger(logger_name)

import csv
import numpy as np
import pandas as pd
from datetime import timedelta, datetime
from utils import checkRoutes
import configparser

from src.google_flight_analysis.scrape import Scrape
from src.google_flight_analysis.database import Database
import private.private as private

# config
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))


if __name__ == "__main__":

    # TODO: feed this information in somehow else.
    ourCountry = 'US'
    ourCurrency = 'USD'

    # 1. scrape routes
    routes = utils.get_routes_from_config(config)

    # verify config.ini formats
    newMethod, newNewMethod = checkRoutes(routes)

    # TODO: find usage for airportData?
    # airportData = utils.updateAirportCodes(True)

    # compute number of total scrapes
    if newMethod:
        n_total_scrapes = sum(2*[x[3] for x in routes], len(routes))
    elif newNewMethod:
        n_total_scrapes = sum(((x[4] + x[4]) + 1) ** 2 for x in routes)
    else:
        n_total_scrapes = sum([x[2] for x in routes])
    
    all_results = []
    all_iter_times = []
    n_iter = 1

    # iterate over the routes
    for route in routes:
        origin = route[0]
        destination = route[1]
        date_range = []
        
        # [origin, destination, target_date (YYYY - MM - DD), flexible_day_range]
        if newMethod:
            flexibleDays = route[3]
            date_obj = datetime.strptime(route[2], "%Y-%m-%d")
            for counter in range(flexibleDays, 0, -1):
                date_range.append((date_obj - timedelta(days=counter)))
            date_range.append(date_obj)
            for counter in range(flexibleDays):
                date_range.append((date_obj + timedelta(days=counter+1)))
        
        # [origin, destination, to (YYYY - MM -DD), returndate (YYYY - MM - DD), flexible_day_range]
        # TODO turn into def(s)
        elif newNewMethod:
            flexibleDays = route[4]
            to_date_obj = datetime.strptime(route[2], "%Y-%m-%d")
            return_date_obj = datetime.strptime(route[3], "%Y-%m-%d")
            for counter in range(flexibleDays, 0, -1):
                toDate = to_date_obj - timedelta(days=counter)
                for counter2 in range(flexibleDays, 0, -1):
                    date_range.append([toDate, (return_date_obj - timedelta(days=counter2))])
                date_range.append([toDate, return_date_obj])
                for counter3 in range(flexibleDays):
                    date_range.append([toDate, (return_date_obj + timedelta(days=counter3+1))])
            
            toDate = to_date_obj
            for counter2 in range(flexibleDays, 0, -1):
                date_range.append([toDate, (return_date_obj - timedelta(days=counter2))])
            date_range.append([toDate, return_date_obj])
            for counter3 in range(flexibleDays):
                date_range.append([toDate, (return_date_obj + timedelta(days=counter3+1))])

            for counter in range(flexibleDays):
                toDate = to_date_obj + timedelta(days=counter)
                for counter2 in range(flexibleDays, 0, -1):
                    date_range.append([toDate, (return_date_obj - timedelta(days=counter2))])
                date_range.append([toDate, return_date_obj])
                for counter3 in range(flexibleDays):
                    date_range.append([toDate, (return_date_obj + timedelta(days=counter3+1))])

            # remove anywhere returnDate is ON or BEFORE toDate
            # TODO add parameter (min travel time) so we can also cut out trips that aren't X in length. Would need to go in config.ini

            removal_dates = []
            for index, dates in enumerate(date_range):
                if dates[0] >= dates[1]:
                    removal_dates.append(dates)
            
            for removal in removal_dates:
                date_range.remove(removal)


        # [origin, destination, range_of_days_from_today]
        else:
            date_range = [(datetime.today() + timedelta(days=i+1)) for i in range(route[2])]
        
        # [origin, destination, to (YYYY - MM -DD), returndate (YYYY - MM - DD), flexible_day_range]
        if newNewMethod:
            date_range_str = []
            for date in date_range:
                to_date_str = date[0].strftime("%Y-%m-%d")
                from_date_str = date[1].strftime("%Y-%m-%d")
                date_range_str.append([to_date_str, from_date_str])
            
            date_range = date_range_str
        else:
            date_range = [date.strftime("%Y-%m-%d") for date in date_range]

        # iterate over dates
        for i, date in enumerate(date_range):
            if newNewMethod:
                scrape = Scrape(origin, destination, date[0], ourCountry, ourCurrency, date[1])
            else:
                scrape = Scrape(origin, destination, date, ourCountry, ourCurrency)

            try:
                time_start = datetime.now()
                
                # run scrape
                scrape.run_scrape()
                
                time_end = datetime.now()

                time_iteration = (time_end - time_start).seconds + round(((time_end - time_start).microseconds * 1e-6), 2)
                all_iter_times.append(time_iteration)
                avg_iter_time = round(np.array(all_iter_times).mean(), 2)

                if newNewMethod:
                    logger.info(f"[{n_iter}/{n_total_scrapes}] [{time_iteration} sec - avg: {avg_iter_time}] Scraped: {origin} {destination} {date[0]} - {date[1]} - {scrape.data.shape[0]} results")
                else:
                    logger.info(f"[{n_iter}/{n_total_scrapes}] [{time_iteration} sec - avg: {avg_iter_time}] Scraped: {origin} {destination} {date} - {scrape.data.shape[0]} results")
                all_results.append(scrape.data)
            except Exception as e:
                logger.error(f"ERROR: {origin} {destination} {date}")
                logger.error(e)
                
            n_iter += 1

    all_results_df = pd.concat(all_results)

    # save to csv so we don't keep re-running
    # if newNewMethod:
    #     all_results_df.to_csv('flight-analysis/flight-analysis/assets/dataframe_roundtrip.csv', index=False)
    # else:
    #     all_results_df.to_csv('flight-analysis/flight-analysis/assets/dataframe_oneway.csv', index=False)


    # grab our csv so we don't keep polling
    # TODO need to choose to load oneway or round trip someway.
    # all_results_df = pd.read_csv('flight-analysis/flight-analysis/assets/dataframe_oneway.csv')

    # 2. add results to sql database
    # connect to database
    db = Database(db_host=private.DB_HOST, db_name=private.DB_NAME, db_user=private.DB_USER, db_pw=private.DB_PW, db_table=private.DB_TABLE, db_sql=private.DB_SQL)

    # prepare database and tables
    db.prepare_db_and_tables(overwrite_table=False)

    # add results to database
    db.add_pandas_df_to_db(all_results_df)
