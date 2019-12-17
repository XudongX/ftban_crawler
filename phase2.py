# -*- coding: utf-8 -*-
import json
import logging
import re
import time
import traceback

from collections import defaultdict
from json import JSONDecodeError
from queue import Queue, Empty, Full
from threading import Thread

import requests
import sqlite3

logger = logging.getLogger()  # 不加名称设置root logger
logger.setLevel(logging.DEBUG)  # 设置logger整体记录的level
formatter = logging.Formatter(
    '%(asctime)s %(name)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# 使用FileHandler输出到文件
fh = logging.FileHandler('phase2_ftban.log',
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


def url_parse(target_url) -> dict:
    """
    :param target_url:
    :return: result dictionary
    """
    reg = re.search('(processid=)(\w+)&nid=', target_url)
    process_id = reg.group(2)
    headers = {
        'user-agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:71.0) Gecko/20100101 Firefox/71.0'
    }
    session = requests.Session()
    GET_result = session.get(target_url, headers=headers)

    time.sleep(0.5)  # rest between requests

    # product detail
    url01 = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaInfo"
    response_1 = session.post(url01, data={'processid': process_id}, headers=headers)  # process{i}d
    # print(response_1.encoding)
    # print(response_1.status_code)
    result_dict = json.loads(response_1.text)
    logging.debug(response_1.text[:100])

    # detail info
    cert_id = result_dict['apply_sn']
    header1 = result_dict['productname']
    scqyUnitinfo = result_dict['scqyUnitinfo']
    producer_name = scqyUnitinfo['enterprise_name']
    producer_address = scqyUnitinfo['enterprise_address']
    producer_detail = "企业名称：" + scqyUnitinfo['enterprise_name'] \
                      + "\n企业地址：" + scqyUnitinfo['enterprise_address'] \
                      + "\n生产许可证号：" + scqyUnitinfo['enterprise_healthpermits']
    pfList = result_dict['pfList']

    # ingredient
    ingredient_dict = defaultdict(list)
    for item in pfList:
        ingredient_dict[item['pfname']].append(item['cname'])
    ingredient_str = ""
    for k, v in ingredient_dict.items():
        ingredient_str += (k + '\n(')
        ingredient_str += (', '.join(v) + ')\n')
    if ingredient_str == "":
        ingredient_str = "无（注：仅供出口）"
    # print(ingredient_str)

    # note1&2
    notes1 = result_dict['remark']
    notes2 = result_dict['remark1']

    # attachment
    url02 = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getAttachmentCpbz"
    response_attach = session.post(url02, data={'processId': process_id}, headers=headers)  # process{I}d
    attach_result_dict = json.loads(response_attach.text)
    ssid = attach_result_dict['ssid']
    pic_2d_id = attach_result_dict['result'].pop()['id']
    pic_3d_id = attach_result_dict['result'].pop()['id']

    pic_url_fm = "http://125.35.6.80:8181/ftban/itownet/download.do?method=downloadFile&fid={id}&ssid={ssid}"
    pic_2d_url = pic_url_fm.format(id=pic_2d_id, ssid=ssid)
    pic_3d_url = pic_url_fm.format(id=pic_3d_id, ssid=ssid)

    session.close()

    return {'product_name': header1,
            'cert_id': cert_id,
            'header1': header1,
            'producer_name': producer_name,
            'producer_address': producer_address,
            'producer_detail': producer_detail,
            'ingredient': ingredient_str,
            'notes1': notes1,
            'notes2': notes2,
            'picture_2d': pic_2d_url,
            'picture_3d': pic_3d_url,
            'json_1': response_1.text,
            'json_2': response_attach.text
            }


def read_worker(q, input_q_maxsize, offset=0):
    query_limit = 60
    while True:
        time.sleep(0.5)  # slow down the speed
        with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            cur.execute("SELECT product_name, cert_id, detail_url, header1 FROM ftban LIMIT ? OFFSET ?;",
                        (query_limit, offset))
            result = cur.fetchall()
        if result is None:
            # an event here
            break
        if len(result) == 0:
            logging.error(">>>>  in read_worker(), EMPTY TABLE: No New Rows!")
            break
        offset += len(result)
        logging.info(">>>> in read_worker(), Query %d rows, offset = %d", len(result), offset)
        for row in result:
            time.sleep(2)
            if row[3] is None:  # header1 是否已经存在
                if q.qsize() > input_q_maxsize:
                    while True:
                        logging.warning(">>>> Queue Full, too many rows in input_q: %d", q.qsize())
                        time.sleep(10)
                        if q.qsize() < input_q_maxsize:
                            break
                logging.debug(">>>> in read_worker(), put in input queue: " + str(row)[:50])
                q.put(row, block=True)
            else:
                logging.warning(">>>> in read_worker(), header1 existed, row skipped")
                continue
        logging.info(">>>> read_worker() finished at offset = %d", offset)


def process_worker(in_q, out_q):
    wait_time = 1
    while True:
        try:
            item_tuple = in_q.get(block=True, timeout=10)
        except Empty as e:
            # Event() could be used in here
            logging.error(">>>> input_q empty for 10s")
            break
        try:
            time.sleep(0.5)  # slow down
            result_dict = url_parse(item_tuple[2])
            wait_time = 1
        except JSONDecodeError as e:
            logging.error(">>>> JSONDecodeError at item_tuple: " + str(item_tuple))
            logging.warning(">>>> Put item_tuple back to in_q")
            in_q.put(item_tuple, block=True)
            time.sleep(wait_time)
            wait_time *= 1.2
            continue
        except requests.exceptions.ConnectionError as e:
            logging.error(
                ">>>> Connection error at url: %s", item_tuple[2][-10:])
            logging.error(traceback.format_exc())
            in_q.put(item_tuple, block=True, timeout=60)
            logging.error(">>>> Have put back to in_q, url: %s", item_tuple[2][-10:])
            time.sleep(wait_time)
            wait_time *= 1.2  # increase wait time
            continue
        logging.debug(">>>> in process_worker(), result_dict: " + str(result_dict)[:100])

        if result_dict['cert_id'] == item_tuple[1]:
            out_q.put(result_dict, block=True)
        else:
            logging.error(">>>> Conflict between result and record")
            logging.error("result_dict['cert_id']: %s", result_dict['cert_id'])
            logging.error("record: ", item_tuple)
            logging.error(">>>> ")


def save_worker(in_q, out_q):
    while True:
        result_dict = in_q.get(block=True)

        with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            cur.execute('''UPDATE ftban SET 
            header1=?,
            producer_name=?,
            producer_address=?,
            producer_detail=?,
            ingredient=?,
            notes1=?,
            notes2=?,
            picture_2d=?,
            picture_3d=?
            WHERE cert_id=? 
            ''', (
                result_dict['header1'],
                result_dict['producer_name'],
                result_dict['producer_address'],
                result_dict['producer_detail'],
                result_dict['ingredient'],
                result_dict['notes1'],
                result_dict['notes2'],
                result_dict['picture_2d'],
                result_dict['picture_3d'],
                result_dict['cert_id'],
            ))
            conn.commit()
            logging.info(">>>> in save_worker(), UPDATE TABLE conn.commit(): " + str(result_dict['product_name']))
        try:
            out_q.put(result_dict, block=True)
        except Full as e:
            # Event() here
            logging.critical(">>>> out_q is full, thread terminated")
            return


def save_raw_worker(q):
    while True:
        result_dict = q.get(block=True)
        with sqlite3.connect('./raw_data.db') as conn:
            cur = conn.cursor()
            cur.execute('''INSERT OR IGNORE INTO raw_json
                               (cert_id, product_name, json_1, json_2) VALUES (?, ?, ?, ?)''',
                        (result_dict['cert_id'],
                         result_dict['product_name'],
                         result_dict['json_1'],
                         result_dict['json_2'])
                        )
            conn.commit()
        logging.debug(">>>> in save_raw_worker(), conn.commit(): " + str(result_dict['product_name']))


def main(db_offset):
    # TODO db connection pool
    # create queue
    input_q = Queue()  # no limitation here, read_worker will limit queue size
    input_q_maxsize = 15
    output_q = Queue(maxsize=15)
    json_q = Queue(maxsize=15)
    logging.debug(">>>> Queues created")

    # create threads
    threads_list = list()
    threads_list.append(Thread(target=read_worker, args=(input_q, input_q_maxsize, db_offset)))
    for i in range(15):  # should be bigger than input_q_maxsize
        threads_list.append(Thread(target=process_worker, args=(input_q, output_q)))
    threads_list.append(Thread(target=save_worker, args=(output_q, json_q)))
    threads_list.append(Thread(target=save_raw_worker, args=(json_q,)))
    logging.debug(">>>> Threads Created")

    # start
    for t in threads_list:
        t.start()
    logging.info(">>>> >>>> All Threads Started!")


if __name__ == '__main__':
    main(db_offset=0)
