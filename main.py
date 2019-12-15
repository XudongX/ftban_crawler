# -*- coding: utf-8 -*-
import argparse
import gc
import logging
import os
import sqlite3
import time
import csv
import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

logger = logging.getLogger()  # 不加名称设置root logger
logger.setLevel(logging.DEBUG)  # 设置logger整体记录的level
formatter = logging.Formatter(
    '%(asctime)s %(name)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# 使用FileHandler输出到文件
fh = logging.FileHandler(str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')) + '_ftban.log',
                         mode='a',
                         encoding='utf-8')
fh.setLevel(logging.INFO)  # 输出到handler的level
fh.setFormatter(formatter)

# 使用StreamHandler输出到标准输出
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)

# 添加两个Handler
logger.addHandler(fh)
logger.addHandler(sh)


def data2csv(list_2d, csv_path):
    """
    convert a 2 dimension list to a csv file
    :param list_2d: 2-d list
    :param csv_path: csv file path
    :return:
    """
    with open(csv_path, 'a', encoding='utf8') as csv_file:
        writer = csv.writer(csv_file)
        for item in list_2d:
            writer.writerow(item)


def wait_load_finish(driver):
    """
    wait for loading finished, it will try twice for 15s
    :param driver:
    :return:
    """
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )
    except:
        logging.warning(">>>> First 15s wait failed, PAGE SOURCE:")
        logging.warning(driver.page_source)
        logging.warning('!! >>>> >>>> first 15s wait failed, try another 60s')
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "xl-nextPage"))
        )


def next_page(driver):
    """
    jump to next page
    :param driver:
    :return:
    """
    driver.find_element_by_class_name('xl-nextPage').click()
    wait_load_finish(driver)


def prev_page(driver):
    """
    jump to previous page
    :param driver:
    :return:
    """
    driver.find_element_by_class_name('xl-prevPage').click()
    wait_load_finish(driver)


def get_current_page(driver) -> int:
    """
    return current page number
    :param driver:
    :return: current page number
    """
    return int(driver.find_element_by_class_name("xl-active").text)


def jump2pagenum(driver, to_pagenum) -> int:
    """
    :param driver:
    :param to_pagenum: target page number
    :return: jumped page number
    """

    # change GLOBAL_PAGE_NUMBER
    page_num_str = str(to_pagenum)

    # find btn and click
    logging.info(">>>> Try to jump page-" + page_num_str)
    driver.find_element_by_id("xlJumpNum").send_keys(page_num_str)
    driver.find_element_by_class_name("xl-jumpButton").click()
    wait_load_finish(driver)

    # verify jump to correct page
    current_page_number = get_current_page(driver)
    if current_page_number == page_num_str:
        logging.info(">>>> Jump succeeded, current page number: " + str(current_page_number))

    return current_page_number


def parse_to_db(driver, db_cursor):
    """
    parse current page, store table elements into database
    :param driver:
    :param db_cursor:
    :return:
    """
    gzlist = driver.find_elements_by_xpath("//ul[@id='gzlist']/li")

    name = gzlist[0].find_element_by_tag_name('dl').text  # find the first row in table
    logging.info('>>>> Current page number: ' + str(get_current_page(driver)))
    logging.info(name)  # log the first row's title.

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


def main(db_path='./data.db', reverse=False, start_at_pagenum=None, limitation=1000) -> int:
    """
    main function
    ONLY Parse 1000 pages(default), then quit webdriver and browser
    :param db_path:
    :param reverse:
    :param start_at_pagenum:
    :param limitation:
    :return: last parsed page number
    """

    # use firefox
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)

    firefox_options = webdriver.FirefoxOptions()
    firefox_options.add_argument("--private")  # try to disable browser cache
    firefox_options.headless = True

    # with webdriver.Firefox(firefox_profile=profile, options=firefox_options, executable_path='./geckodriver') as driver:
    with webdriver.Firefox(firefox_profile=profile, options=firefox_options) as driver:

        driver.get("http://125.35.6.80:8181/ftban/fw.jsp")
        wait_load_finish(driver)

        logging.info(">>>> >>>> >>>> WebDriver Initiated")
        logging.info(datetime.now())
        logging.info(driver.title)

        # driver.quit()
        # input("Wait")

        # parse begin at last page and reverse parsing
        if reverse:
            logging.info(">>>> REVERSE! parse order reversed")
            last_page_btn = driver.find_element_by_xpath("//div[@id='page']/ul/li[7]")
            last_page_btn.click()
            wait_load_finish(driver)
            logging.info(">>>> Jumped to last page: " + str(last_page_btn.text))

            # TODO reverse debug
            input("not finished! C-c!")

        # jump to certain page number
        if start_at_pagenum is not None:
            jump2pagenum(driver, start_at_pagenum)

        # start parsing
        with sqlite3.connect('./data.db') as conn:
            db_cursor = conn.cursor()
            logging.info(">>>> db_cursor created, begin LOOP")

            parsed_counter = 0  # record the number of parsed pages
            while True:
                parse_to_db(driver, db_cursor)
                conn.commit()
                logging.info(">>>> Page-" + str(get_current_page(driver)) + " parsed and committed")

                if parsed_counter >= limitation:  # in case memory leaks
                    logging.warning(">>>> !!!! Reached the parsed limitation %d, page number at %d",
                                    limitation,
                                    get_current_page(driver))
                    break
                parsed_counter += 1

                if not reverse:
                    next_page(driver)
                else:
                    prev_page(driver)
                time.sleep(0.25)

        # end parsing
        current_pagenum = get_current_page(driver)
        driver.quit()

    logging.info(">>>> Ready to finish main() function")
    time.sleep(1)
    gc_num = gc.collect()
    logging.warning(">>>> GC Number: " + str(gc_num))
    return current_pagenum


if __name__ == '__main__':
    # parse command line arguments
    parser = argparse.ArgumentParser(description='ftban-crawler')
    parser.add_argument('-db', dest='database_path',
                        metavar='database path',
                        action='store',
                        default='./data.db')
    parser.add_argument('-rv', dest='reverse_parse',
                        action='store_true')
    parser.add_argument('-pn', dest='page_num',
                        metavar='page number',
                        action='store',
                        default=None)
    args = parser.parse_args()

    logging.info(">>>> Got command line arguments:")
    logging.info("database_path: " + str(args.database_path))
    logging.info("reverse_parse: " + str(args.reverse_parse))
    logging.info("page_num: " + str(args.page_num))

    # loop the main()
    page_number = args.page_num
    while True:
        page_number = main(db_path=args.database_path,
                           reverse=args.reverse_parse,
                           start_at_pagenum=page_number,
                           limitation=1000)
