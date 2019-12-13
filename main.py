# -*- coding: utf-8 -*-
import sqlite3
import time
import csv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime



GLOBAL_COUNTER = 0


def data2csv(list_2d, csvname):
    with open(csvname, 'a', encoding='utf8') as csvfile:
        writer = csv.writer(csvfile)
        for item in list_2d:
            writer.writerow(item)


def wait_load_finish(driver):
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )
    except:
        print('>>> >>> first 60s for next page, wait falled, try again')
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )


def next_page(driver):
    driver.find_element_by_class_name('xl-nextPage').click()
    wait_load_finish(driver)


def parse_to_db(driver, num, db_cursor):
    global GLOBAL_COUNTER
    gzlist = driver.find_elements_by_xpath("//ul[@id='gzlist']/li")

    name = gzlist[0].find_element_by_tag_name('dl').text
    print('GLOBAL_COUNTER = ' + str(GLOBAL_COUNTER))
    print(name)
    GLOBAL_COUNTER += 1

    # current_page = list()
    for li_tag in gzlist:
        list_row = list()
        list_row.append(li_tag.find_element_by_tag_name('dl').text)
        list_row.append(li_tag.find_element_by_tag_name('ol').text)
        list_row.append(li_tag.find_element_by_tag_name('p').text)
        list_row.append(li_tag.find_element_by_tag_name('i').text)
        list_row.append(li_tag.find_element_by_tag_name('a').get_attribute("href"))
        # current_page.append(list_row)
        db_cursor.execute("INSERT OR IGNORE INTO ftban(product_name, "
                          "cert_id, "
                          "company_name, "
                          "month_date, "
                          "detail_url) "
                          "VALUES "
                          "(?, ?, ?, ?, ?)",
                          (list_row[0], list_row[1], list_row[2], list_row[3], list_row[4]))


def main():
    # TODO: 反向爬取
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)

    options = webdriver.FirefoxOptions()
    options.headless = True

    # with webdriver.Firefox(firefox_profile=profile, executable_path='./geckodriver') as driver:
    with webdriver.Firefox(firefox_profile=profile, options=options) as driver:
        driver.get("http://125.35.6.80:8181/ftban/fw.jsp")

        wait_load_finish(driver)

        print(">>> >>> >>>")
        print(datetime.now())
        print(driver.title)

        with sqlite3.connect('./data.db') as conn:
            db_cursor = conn.cursor()
            for K in range(1000):
                for i in range(1000):
                    parse_to_db(driver, K, db_cursor)
                    conn.commit()
                    time.sleep(2)
                    next_page(driver)
                    time.sleep(2)


if __name__ == '__main__':
    main()
