# -*- coding: utf-8 -*-
import json
import logging
import re
import time

from collections import defaultdict
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


def read_worker(q):
    offset = 0
    while True:
        with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            cur.execute("SELECT product_name, cert_id, detail_url, header1 FROM ftban LIMIT 150 OFFSET (?);", (offset,))
            result = cur.fetchall()
        if result is None:
            # an event here
            break
        if len(result) == 0:
            logging.warning(">>>>  in read_worker(), EMPTY TABLE: No New Rows!")
            break
        offset += len(result)
        logging.info(">>>> in read_worker(), Query %d rows, offset = %d", len(result), offset)
        for row in result:
            if row[3] is None:  # header1 是否已经存在
                logging.debug(">>>> in read_worker(), put in input queue: " + str(row)[:100])
                q.put(row, block=True)
            else:
                logging.warning(">>>> in read_worker(), header1 existed, row skipped")
                continue


def process_worker(in_q, out_q):
    while True:
        try:
            item_tuple = in_q.get(block=True, timeout=10)
        except Empty as e:
            # Event() could be used in here
            logging.error(">>>> input_q empty for 10s")
            break
        result_dict = url_parse(item_tuple[2])
        logging.debug(">>>> in process_worker(), result_dict: " + str(result_dict))
        if result_dict['cert_id'] == item_tuple[1]:
            out_q.put(result_dict, block=True)
        else:
            logging.error(">>>> Conflict between result and record")
            logging.error("result_dict['cert_id']: %s", result_dict['cert_id'])
            logging.error("record: ", item_tuple)
            continue


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
            continue


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


def main():
    # TODO db connection pool
    # create queue
    input_q = Queue(maxsize=5)
    output_q = Queue(maxsize=5)
    json_q = Queue(maxsize=3)
    logging.debug(">>>> Queues created")

    # create threads
    read_worker_01 = Thread(target=read_worker, args=(input_q,))
    process_worker_list = list()
    for i in range(10):
        process_worker_list.append(Thread(target=process_worker, args=(input_q, output_q)))
    save_worker_01 = Thread(target=save_worker, args=(output_q, json_q))
    save_raw_worker_01 = Thread(target=save_raw_worker, args=(json_q,))
    logging.debug(">>>> Threads Created")

    # start
    read_worker_01.start()
    for t in process_worker_list:
        t.start()
    save_worker_01.start()
    save_raw_worker_01.start()
    logging.info(">>>> >>>> All Threads Started!")


if __name__ == '__main__':
    # url = "http://125.35.6.80:8181/ftban/itownet/hzp_ba/fw/pz.jsp?processid=20191125125809djs0r&nid=20191125125809djs0r"
    # url_parse(url)

    main()
