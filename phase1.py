import json
import logging
import signal
import sqlite3
import time
import traceback
from datetime import datetime
from functools import wraps
from json import JSONDecodeError
from queue import Queue, Empty, Full
from threading import Thread

import requests

from util import ThreadDecorator

logger = logging.getLogger()  # 不加名称设置root logger
logger.setLevel(logging.DEBUG)  # 设置logger整体记录的level
formatter = logging.Formatter(
    '%(asctime)s %(name)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# 使用FileHandler输出到文件
fh = logging.FileHandler('p1.log',
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


def parse_and_return(page_num):
    target_url = "http://125.35.6.80:8181/ftban/fw.jsp"
    URL_getBaNewInfoPage = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaNewInfoPage"
    headers = {
        'user-agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:71.0) Gecko/20100101 Firefox/71.0'
    }
    session = requests.Session()
    GET_result = session.get(target_url, headers=headers)

    time.sleep(0.2)

    response = session.post(URL_getBaNewInfoPage,
                            data={'on': 'true', 'conditionType': 1, 'num': page_num},
                            headers=headers)

    result_dict = json.loads(response.text)
    item_list = result_dict['list']
    detail_url_fm = 'http://125.35.6.80:8181/ftban/itownet/hzp_ba/fw/pz.jsp?processid={processid}&nid={nid}'
    info_list = list()
    for item in item_list:
        cert_id = item['applySn']
        process_id = item['newProcessid']
        product_name = item['productName']
        month_date = item['provinceConfirm']
        company_name = item['enterpriseName']
        company_type = item['apply_enter_address']
        if company_name != "":
            company_name = company_name + '（' + company_type + '）'
        detail_url = detail_url_fm.format(processid=process_id, nid=process_id)
        info = (product_name, cert_id, company_name, month_date, detail_url)
        info_list.append(info)

    logging.info(">>>> parse_and_return() finished at page_num:" + str(page_num))
    # time.sleep(1)
    return info_list


def save2db(info_list):
    with sqlite3.connect('./data.db') as conn:
        cur = conn.cursor()
        for info in info_list:
            cur.execute('''INSERT OR IGNORE INTO ftban(product_name, cert_id, company_name, month_date, detail_url)
                            VALUES (?, ?, ?, ?, ?)''',
                        (info[0], info[1], info[2], info[3], info[4]))
        conn.commit()
    logging.info(">>>> save2db() committed: " + info_list[0][1])


def page_num_generator(start_at_page_num, page_num_q, page_num_q_maxsize):
    for num in range(start_at_page_num, 1000000):
        if page_num_q.qsize() > page_num_q_maxsize:
            while True:
                logging.warning(">>>> Queue Full, too many page_num in page_num_q: %d", page_num_q.qsize())
                time.sleep(10)
                if page_num_q.qsize() < page_num_q_maxsize:
                    break
        page_num_q.put(num, block=False)
        logging.debug(">>>> page_num_q.put(): " + str(num))
        time.sleep(0.5)


def process_worker(page_num_q, output_q):
    wait_time = 1
    while True:
        if datetime.now().hour == 4 and datetime.now().minute == 50:
            logging.warning(">>>> SLEEP 1 HOUR for server shutdown time")
            time.sleep(60*60*1.5)
        try:
            page_num = page_num_q.get(block=True, timeout=30)
            # logging.info(">>>> in porcess_worker(), page_num_q.got(): %d", page_num)
        except Empty as e:
            logging.critical(">>>> >>>> page_num_q EMPTY!")
            break
        try:
            info_list = parse_and_return(page_num)
            wait_time = 1  # parse success, reset wait time
        except JSONDecodeError as e:
            logging.error(">>>> JSONDecodeError at page_num: " + str(page_num) + ". Put it back to page_num_q")
            page_num_q.put(page_num, block=True, timeout=60)
            logging.info(">>>> process_worker wait_time: %d", wait_time)
            time.sleep(int(wait_time))
            wait_time *= 1.1  # increase wait time
            continue
        except requests.exceptions.ConnectionError as e:
            logging.error(">>>> Connection error at page_num: " + str(page_num) + ". Put it back to page_num_q")
            logging.error(traceback.format_exc())
            page_num_q.put(page_num, block=True, timeout=60)
            logging.info(">>>> process_worker wait_sleep: %d", wait_time)
            time.sleep(int(wait_time))
            wait_time *= 1.1  # increase wait time
            continue
        except TypeError as e:
            logging.error(">>>> Typeerror at page_num: " + str(page_num) + ". Put it back to page_num_q")
            logging.error(traceback.format_exc())
            page_num_q.put(page_num, block=True, timeout=60)
            logging.info(">>>> process_worker wait_sleep: %d", wait_time)
            time.sleep(int(wait_time))
            wait_time *= 1.1  # increase wait time
            continue
        output_q.put(info_list, block=True)
        logging.info(">>>> output_q.put() OK, process_worker() finished at page_num: %d", page_num)


def save_worker(output_q):
    while True:
        info_list = output_q.get(block=True)
        logging.info(">>>> in save_worker(), output_q.get(): " + info_list[0][1])
        save2db(info_list)


def main(start_at_page_num):
    # use Queue and Thread
    page_num_q = Queue()  # 不设置maxsize，在page-num-generator里控制大小
    page_num_q_maxsize = 10
    output_q = Queue(maxsize=10)
    logging.info(">>>> Queues created")

    threads_q = Queue()

    threads_q.put(ThreadDecorator(page_num_generator,
                                  start_at_page_num,
                                  page_num_q,
                                  page_num_q_maxsize,
                                  threads_q=threads_q,
                                  sleep=5
                                  ))
    for _ in range(20):  # bigger than page_num_q_maxsize , at least there is threads can perform page_num_q.get()
        threads_q.put(ThreadDecorator(process_worker,
                                      page_num_q,
                                      output_q,
                                      threads_q=threads_q,
                                      sleep=5
                                      ))
    threads_q.put(ThreadDecorator(save_worker,
                                  output_q,
                                  threads_q=threads_q,
                                  sleep=5))
    threads_q.put(ThreadDecorator(save_worker,
                                  output_q,
                                  threads_q=threads_q,
                                  sleep=5))
    logging.info(">>>> Threads created, starting >>>>")

    # def sigint_handler(signum, frame):
    #     logging.critical(">>>> KeyboardInterrupt: %d", signum)
    #     logging.critical(">>>> page_num_q element:")
    #     while not page_num_q.full():
    #         logging.critical(">>>> >>>> %d", page_num_q.get())
    #     print(frame)
    #
    # signal.signal(signal.SIGINT, sigint_handler)

    while True:
        t = threads_q.get(block=True)
        t.start()
        logging.info(">>>> Thread started: %s", t)


if __name__ == '__main__':
    main(60020)
