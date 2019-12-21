import asyncio
import json
import logging
import sqlite3
import time
from asyncio import Queue
from collections import defaultdict
from json import JSONDecodeError

import aiohttp
import httpx
import requests

logger = logging.getLogger()  # 不加名称设置root logger
logger.setLevel(logging.DEBUG)  # 设置logger整体记录的level
formatter = logging.Formatter(
    '%(asctime)s %(name)s:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# 使用FileHandler输出到文件
fh = logging.FileHandler('p3.log',
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


def post_process():
    sql_01 = '''UPDATE ftban SET header1='X' WHERE cert_id LIKE '%（已注销）';'''


async def select_null_item(in_q):
    offset = 0
    while True:
        with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            cur.execute("SELECT cert_id, id FROM ftban WHERE header1 is null LIMIT 150 OFFSET ?", (offset,))
            results = cur.fetchall()
        offset += len(results)
        logging.info(">>>> in read_worker(), Query %d rows, offset = %d", len(results), offset)
        for db_row in results:
            await asyncio.sleep(0.5)
            # await in_q.put(db_row)
            await in_q.put(db_row)


async def find_product_info(db_row):
    target_url = "http://125.35.6.80:8181/ftban/fw.jsp"
    URL_getBaNewInfoPage = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaNewInfoPage"
    headers = {
        'user-agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:71.0) Gecko/20100101 Firefox/71.0'
    }

    # session = requests.Session()
    # GET_result = session.get(target_url, headers=headers)
    # print(GET_result.text)
    # response = session.post(URL_getBaNewInfoPage,
    #                         data={'on': 'true', 'conditionType': 1, 'num': 1},
    #                         headers=headers)
    # print(response.text)

    # get session
    # async with aiohttp.ClientSession() as session:
    async with httpx.Client(headers=headers) as session:
        await session.get(target_url, headers=headers)
        await asyncio.sleep(0.5)

        # get product list
        params_dict = {'on': 'true', 'productName': db_row[0], 'conditionType': 2}
        # response = await session.post(URL_getBaNewInfoPage,
        #                     data={'on': 'true', 'conditionType': 1, 'num': 1},
        #                     headers=headers)
        response = await session.post(URL_getBaNewInfoPage, headers=headers, data=params_dict, timeout=2)
        print(response.text)
        # print(response.request_info)
        response_dict = json.loads(response.text)
        product_info = response_dict['list'][0]  # precise query above, so need the first item in list

        # get product detail
        logging.debug("search result, post response: " + response.text[:100])
        process_id = product_info['newProcessid']
        URL_getBaInfo = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaInfo"
        response_detail = await session.post(URL_getBaInfo, data={'processid': process_id},
                                             headers=headers)  # process{i}d
        detail_dict = json.loads(response_detail.text)

        # parse detail info
        cert_id = detail_dict['apply_sn']
        header1 = detail_dict['productname']
        scqyUnitinfo = detail_dict['scqyUnitinfo']
        producer_name = scqyUnitinfo['enterprise_name']
        producer_address = scqyUnitinfo['enterprise_address']
        producer_detail = "企业名称：" + scqyUnitinfo['enterprise_name'] \
                          + "\n企业地址：" + scqyUnitinfo['enterprise_address'] \
                          + "\n生产许可证号：" + scqyUnitinfo['enterprise_healthpermits']
        pfList = detail_dict['pfList']

        # ingredient
        ingredient_dict = defaultdict(list)
        for pf in pfList:
            ingredient_dict[pf['pfname']].append(pf['cname'])
        ingredient_str = ""
        for k, v in ingredient_dict.items():
            ingredient_str += (k + '\n(')
            ingredient_str += (', '.join(v) + ')\n')
        if ingredient_str == "":
            ingredient_str = "无（注：仅供出口）"

        # note1&2
        notes1 = detail_dict['remark']
        notes2 = detail_dict['remark1']

        # attachment
        url02 = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getAttachmentCpbz"
        response_attach = await session.post(url02, data={'processId': process_id}, headers=headers)  # process{I}d
        attach_detail_dict = json.loads(response_attach.text)
        ssid = attach_detail_dict['ssid']
        pic_2d_id = attach_detail_dict['result'].pop()['id']
        pic_3d_id = attach_detail_dict['result'].pop()['id']

        pic_url_fm = "http://125.35.6.80:8181/ftban/itownet/download.do?method=downloadFile&fid={id}&ssid={ssid}"
        pic_2d_url = pic_url_fm.format(id=pic_2d_id, ssid=ssid)
        pic_3d_url = pic_url_fm.format(id=pic_3d_id, ssid=ssid)

        final_dict = {'product_name': header1,
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
                      'json_1': response.text,
                      'json_2': response_attach.text
                      }

        logging.info("in find_product_info(), put final_dict: %s", final_dict['cert_id'])
        return final_dict


async def process_worker(in_q, out_q):
    wait_time = 1
    while True:
        db_row = await in_q.get()
        try:
            result_dict = await find_product_info(db_row)
            wait_time = 1
        except JSONDecodeError as e:
            logging.error(">>>> JSONDecodeError at: %s, Put item_tuple back to in_q.", db_row)
            await in_q.put(db_row)
            await asyncio.sleep(wait_time)
            wait_time *= 1.2
            continue
        except httpx.exceptions.ReadTimeout as e:
            logging.error(">>>> httpx.exceptions.ReadTimeout at: %s, Put item_tuple back to in_q.", db_row)
            await in_q.put(db_row)
            await asyncio.sleep(wait_time)
            wait_time *= 1.2
            continue

        await out_q.put(result_dict)


async def save_worker(out_q):
    while True:
        result_dict = await out_q.get()
        logging.info("in save_worker(), result_dict get: %s", result_dict['cert_id'])

        # save product detail info to data.db
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
            logging.debug(">>>> in save_worker(), UPDATE TABLE conn.commit(): " + str(result_dict['product_name']))

        # save raw json to ./raw_data.db
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

        logging.debug(">>>> in save_raw_worker(), conn.commit(): " + str(result_dict['cert_id']))


async def main():
    # use async and asyncio
    in_q = Queue(maxsize=10)
    out_q = Queue(maxsize=10)

    corous = [select_null_item(in_q), save_worker(out_q)]
    for _ in range(20):
        corous.append(process_worker(in_q, out_q))

    await asyncio.gather(*corous)


if __name__ == '__main__':
    # main()
    asyncio.run(main())
