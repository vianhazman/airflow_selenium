import datetime

from progress.bar import Bar
import time
import pandas as pd
from pymongo import MongoClient


def get_movement_range(driver, fb_url, fb_email, fb_pass):
    # Get latest date in MongoDB
    client = MongoClient('mongodb://root:rootpassword@10.119.105.232:27017')
    db = client["testing"]
    col_district = db["movement_range_district"]
    date = col_district.find().sort([("date", -1)]).limit(1)
    last_data_date = (list(date)[0]["date"])
    start_date = (datetime.strptime(last_data_date, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')

    date_list = [d.strftime('%Y-%m-%d') for d in pd.date_range(start_date, end_date)]

    driver.get(fb_url.format(DATE_LIST[0]))

    driver.find_element_by_xpath('//*[@id="email"]').send_keys(fb_email)

    driver.find_element_by_xpath('//*[@id="pass"]').send_keys(fb_pass)

    driver.find_element_by_xpath('//*[@id="loginbutton"]').click()
    time.sleep(1)

    bar = Bar('Downloading', max=len(date_list[1:]))
    for date in date_list[1:]:
        driver.get(fb_url.format(date))
        bar.next()

    bar.finish()
