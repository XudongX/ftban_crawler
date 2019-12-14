# -*- coding: utf-8 -*-
import argparse
import gc
import os
import sqlite3
import time
import csv
import traceback

from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

GLOBAL_COUNTER = 1


def data2csv(list_2d, csv_name):
    with open(csv_name, 'a', encoding='utf8') as csv_file:
        writer = csv.writer(csv_file)
        for item in list_2d:
            writer.writerow(item)


def wait_load_finish(driver):
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )
    except:
        print(">>>> PAGE SOURCE:")
        print(driver.page_source)
        print('!! >>>> >>>> first 15s wait failed, try another 60s')
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )


def next_page(driver):
    driver.find_element_by_class_name('xl-nextPage').click()
    wait_load_finish(driver)


def prev_page(driver):
    driver.find_element_by_class_name('xl-prevPage').click()
    wait_load_finish(driver)


def parse_to_db(driver, db_cursor):
    global GLOBAL_COUNTER
    gzlist = driver.find_elements_by_xpath("//ul[@id='gzlist']/li")

    name = gzlist[0].find_element_by_tag_name('dl').text
    print('GLOBAL_COUNTER = ' + str(GLOBAL_COUNTER))
    print(name)
    GLOBAL_COUNTER += 1

    for li_tag in gzlist:
        list_row = list()
        list_row.append(li_tag.find_element_by_tag_name('dl').text)
        list_row.append(li_tag.find_element_by_tag_name('ol').text)
        list_row.append(li_tag.find_element_by_tag_name('p').text)
        list_row.append(li_tag.find_element_by_tag_name('i').text)
        list_row.append(li_tag.find_element_by_tag_name('a').get_attribute("href"))

        db_cursor.execute("INSERT OR IGNORE INTO ftban(product_name, "
                          "cert_id, "
                          "company_name, "
                          "month_date, "
                          "detail_url) "
                          "VALUES "
                          "(?, ?, ?, ?, ?)",
                          (list_row[0], list_row[1], list_row[2], list_row[3], list_row[4]))


def main(reverse=False, start_at_pagenum=None):
    # use firefox
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)

    firefox_options = webdriver.FirefoxOptions()
    firefox_options.headless = True

    # use chrome
    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")
    # chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument("--disable-application-cache")
    # chrome_options.add_argument('blink-settings=imagesEnabled=false')
    # chrome_options.add_argument("--incognito")
    # chrome_options.add_argument('--ignore-certificate-errors')
    # chrome_options.add_argument('--allow-running-insecure-content')
    # capabilities = DesiredCapabilities.CHROME.copy()
    # capabilities["acceptSslCerts"] = True
    # capabilities['acceptInsecureCerts'] = True
    # capabilities["ignore-certificate-errors"] = True
    # with webdriver.Chrome(options=chrome_options,
    #                       executable_path='./chromedriver') as driver:

    # with webdriver.Firefox(firefox_profile=profile, executable_path='./geckodriver') as driver:
    with webdriver.Firefox(firefox_profile=profile, options=firefox_options) as driver:
        driver.get("http://125.35.6.80:8181/ftban/fw.jsp")

        wait_load_finish(driver)

        print(">>>> >>>> >>>>")
        print(datetime.now())
        print(driver.title)

        # parse begin at last page and reverse parsing
        if reverse:
            print(">>>> REVERSE! parse order reversed")
            driver.find_element_by_xpath("//div[@id='page']/ul/li[7]").click()
            wait_load_finish(driver)
            input("not finished! C-c!")

        # jump to certain page number
        if start_at_pagenum is not None:
            page_num = str(start_at_pagenum)
            global GLOBAL_COUNTER
            GLOBAL_COUNTER = int(page_num)
            print(">>>> Try to jump page" + page_num)
            driver.find_element_by_id("xlJumpNum").send_keys(page_num)
            driver.find_element_by_class_name("xl-jumpButton").click()
            wait_load_finish(driver)
            xl_active = driver.find_element_by_class_name("xl-active")
            if xl_active.text == str(page_num):
                print(">>>> Jump succeeded")

        # start parsing
        with sqlite3.connect('./data.db') as conn:
            db_cursor = conn.cursor()
            print(">>>> db_cursor created, begin LOOP")
            page_count = 1
            if start_at_pagenum is not None:
                page_count = int(start_at_pagenum)
            while True:
                parse_to_db(driver, db_cursor)
                conn.commit()
                print(">>>> Page-" + str(page_count) + " parsed and commited")
                page_count += 1
                # time.sleep(1)

                if not reverse:
                    next_page(driver)
                else:
                    prev_page(driver)
                time.sleep(1)
                raise ValueError()



if __name__ == '__main__':
    # parse command line arguments
    parser = argparse.ArgumentParser(description='ftban-crawler')
    parser.add_argument('-db', dest='database_path',
                        metavar='database path',
                        action='store',
                        default='./data.db')
    parser.add_argument('-rp', dest='reverse_parse',
                        action='store_true')
    parser.add_argument('-pn', dest='page_num',
                        metavar='page number',
                        action='store',
                        default=None)
    args = parser.parse_args()

    print(">>>> Got command line arguments:")
    print("database_path: " + str(args.database_path))
    print("reverse_parse: " + str(args.reverse_parse))
    print("page_num: " + str(args.page_num))

    # try to loop the main() in case of unknown exception
    try:
        main(reverse=args.reverse_parse, start_at_pagenum=args.page_num)
    except Exception as e:
        print(">>>> !!!! Exception!!!! EXCEPTION")
        traceback.print_exc()
        print("<<<< !!!! Exception!!!! EXCEPTION")
        gc_num = gc.collect()
        print(">>>> GC Number: "+str(gc_num))
        print(">>>> try to begin at GLOBAL_COUNTER: " + str(GLOBAL_COUNTER - 1))
        os.system("PYTHONIOENCODING=utf-8 python3 ./main.py -pn " + str(GLOBAL_COUNTER - 1))
        print(">>>> after os.system, GLOBAL_COUNTER: " + str(GLOBAL_COUNTER))
