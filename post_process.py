import asyncio
import json
import logging
import sqlite3
import time
from collections import defaultdict
from queue import Full

import requests

from util import init_my_logging

init_my_logging('p3.log')


def post_process():
    sql_01 = '''UPDATE ftban SET header1='X' WHERE cert_id LIKE '%（已注销）';'''


async def select_null_item():
    offset = 0
    while True:
        async with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            await cur.execute("SELECT cert_id, id FROM ftban WHERE header1 is null LIMIT 150 OFFSET ?", (offset,))
            results = cur.fetchall()
        offset += len(results)

        async for db_row in results:
            yield db_row


async def find_product_info(db_row):
    # get session
    target_url = "http://125.35.6.80:8181/ftban/fw.jsp"
    URL_getBaNewInfoPage = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaNewInfoPage"
    headers = {
        'user-agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:71.0) Gecko/20100101 Firefox/71.0'
    }
    async with requests.Session() as session:
        pass

    session = requests.Session()
    GET_result = await session.get(target_url, headers=headers)
    await asyncio.sleep(0.5)

    # get product list
    # item = ('粤G妆网备字2019297935', 123)
    params_dict = {'on': 'true', 'productName': db_row[0], 'conditionType': 2}
    response = await session.post(URL_getBaNewInfoPage, headers=headers, data=params_dict)
    response_dict = await json.loads(response.text)
    product_info = response_dict['list'][0]  # precise query above, so need the first item in list
    # total_count = response_dict['totalCount']

    # get product detail
    logging.debug("search result, post response: " + response.text[:100])
    process_id = product_info['newProcessid']
    URL_getBaInfo = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaInfo"
    response_detail = await session.post(URL_getBaInfo, data={'processid': process_id}, headers=headers)  # process{i}d
    detail_dict = await json.loads(response_detail.text)
    # print(detail_dict)

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
    async for pf in pfList:
        ingredient_dict[pf['pfname']].append(pf['cname'])
    ingredient_str = ""
    async for k, v in ingredient_dict.items():
        ingredient_str += (k + '\n(')
        ingredient_str += (', '.join(v) + ')\n')
    if ingredient_str == "":
        ingredient_str = "无（注：仅供出口）"
    # print(ingredient_str)

    # note1&2
    notes1 = detail_dict['remark']
    notes2 = detail_dict['remark1']

    # attachment
    url02 = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getAttachmentCpbz"
    response_attach = await session.post(url02, data={'processId': process_id}, headers=headers)  # process{I}d
    attach_detail_dict = await json.loads(response_attach.text)
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
    return final_dict
    # out_q.put(final_dict)


async def parse_worker(db_row, limitation):
    async with limitation:
        product_info_dict = await find_product_info(db_row)
        return product_info_dict


async def save_worker(in_q, out_q):
    while True:
        result_dict = await in_q.get(block=True)

        async with sqlite3.connect('./data.db') as conn:
            cur = conn.cursor()
            await cur.execute('''UPDATE ftban SET 
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
            await conn.commit()
            logging.debug(">>>> in save_worker(), UPDATE TABLE conn.commit(): " + str(result_dict['product_name']))
        yield result_dict
        # try:
        #     out_q.put(result_dict, block=True)
        # except Full as e:
        #     # Event() here
        #     logging.critical(">>>> out_q is full, thread terminated")
        #     return


async def save_raw_worker(q):
    while True:
        result_dict = q.get(block=True)
        async with sqlite3.connect('./raw_data.db') as conn:
            cur = conn.cursor()
            await cur.execute('''INSERT OR IGNORE INTO raw_json
                               (cert_id, product_name, json_1, json_2) VALUES (?, ?, ?, ?)''',
                              (result_dict['cert_id'],
                               result_dict['product_name'],
                               result_dict['json_1'],
                               result_dict['json_2'])
                              )
            await conn.commit()
        logging.debug(">>>> in save_raw_worker(), conn.commit(): " + str(result_dict['product_name']))


async def main():
    # use async and asyncio
    # save_worker(in_q, out_q)
    # save_json_worker(out_q)

    limitation = asyncio.Semaphore(20)



    pass


if __name__ == '__main__':
    asyncio.run(main())
