import json
import logging
import sqlite3
import time
from json import JSONDecodeError
from queue import Queue, Empty, Full
from threading import Thread

import requests

logger = logging.getLogger()  # 不加名称设置root logger
logger.setLevel(logging.DEBUG)  # 设置logger整体记录的level
formatter = logging.Formatter(
    '%(asctime)s %(name)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# 使用FileHandler输出到文件
fh = logging.FileHandler('phase1_ftban.log',
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

    time.sleep(3)

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
    time.sleep(3)
    return info_list


def save2db(info_list):
    with sqlite3.connect('./data.db') as conn:
        cur = conn.cursor()
        for info in info_list:
            cur.execute('''INSERT OR IGNORE INTO ftban(product_name, cert_id, company_name, month_date, detail_url)
                            VALUES (?, ?, ?, ?, ?)''',
                        (info[0], info[1], info[2], info[3], info[4]))
        conn.commit()
        logging.info(">>>> save2db() committed: " + info_list[0][0])


def process_worker(page_num_q, output_q):
    while True:
        try:
            page_num = page_num_q.get(block=True, timeout=30)
        except Empty as e:
            logging.critical(">>>> >>>> page_num_q EMPTY!")
        try:
            info_list = parse_and_return(page_num)
        except JSONDecodeError as e:
            logging.error(">>>> JSONDecodeError at page_num: " + str(page_num)+". Put it back to page_num_q")
            page_num_q.put(page_num, block=True, timeout=60)
            continue
        output_q.put(info_list, block=True)


def save_workder(output_q):
    while True:
        info_list = output_q.get(block=True)
        save2db(info_list)


def main(start_at_page_num):
    page_num_q = Queue(maxsize=15)
    output_q = Queue(maxsize=10)
    logging.info(">>>> Queues created")

    thread_list = list()
    for _ in range(16):  # == page_num_q maxsize+1 , at least there is a thread can perform page_num_q.get()
        thread_list.append(Thread(target=process_worker, args=(page_num_q, output_q)))
    thread_list.append(Thread(target=save_workder, args=(output_q,)))
    logging.info(">>>> Threads created")

    for t in thread_list:
        t.start()

    logging.info(">>>> worked started")

    for num in range(start_at_page_num, 1000000):
        page_num_q.put(num, block=True)
        logging.info(">>>> page_num_q.put(): " + str(num))
        time.sleep(0.5)


if __name__ == '__main__':
    main(17500)
