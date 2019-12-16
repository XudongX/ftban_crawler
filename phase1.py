import json
import sqlite3

import requests


def parse_and_return(page_num):
    target_url = "http://125.35.6.80:8181/ftban/fw.jsp"
    URL_getBaNewInfoPage = "http://125.35.6.80:8181/ftban/itownet/fwAction.do?method=getBaNewInfoPage"
    headers = {
        'user-agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:71.0) Gecko/20100101 Firefox/71.0'
    }
    session = requests.Session()
    GET_result = session.get(target_url, headers=headers)

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

    return info_list


def save2db(info_list):
    with sqlite3.connect('./data.db') as conn:
        cur = conn.cursor()
        for info in info_list:
            cur.execute('''INSERT OR IGNORE INTO ftban(product_name, cert_id, company_name, month_date, detail_url)
                            VALUES (?, ?, ?, ?, ?)''',
                        (info[0], info[1], info[2], info[3], info[4]))
        conn.commit()


def process_worker():
    pass


def save_workder():
    pass


def main():
    pass


if __name__ == '__main__':
    pass
