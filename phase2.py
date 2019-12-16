# -*- coding: utf-8 -*-
import json
import re
import time
from collections import defaultdict

import requests
import sqlite3


def url_parse(target_url):
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
    print(response_1.encoding)
    print(response_1.status_code)
    result_dict = json.loads(response_1.text)

    # detail info
    cert_id = result_dict['apply_sn']
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
    print(ingredient_str)

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


if __name__ == '__main__':
    url = "http://125.35.6.80:8181/ftban/itownet/hzp_ba/fw/pz.jsp?processid=20191125125809djs0r&nid=20191125125809djs0r"
    url_parse(url)
