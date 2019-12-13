# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import sqlite3
import csv


def csv2db(csvpath, dbpath):
    conn = sqlite3.connect(dbpath)
    db_cursor = conn.cursor()
    with open(csvpath, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            db_cursor.execute("insert into ftban(product_name, "
                              "cert_id, "
                              "company_name, "
                              "month_date, "
                              "detail_url) "
                              "values "
                              "(?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4]))
    conn.commit()
    conn.close()


def db2csv(dbpath, csvpath):
    conn = sqlite3.connect(dbpath)
    db_cursor = conn.cursor()
    with open(csvpath, 'r') as csvfile:
        writer = csv.writer(csvfile)
        for row in db_cursor.execute("select (product_name, "
                              "cert_id, "
                              "company_name, "
                              "month_date, "
                              "detail_url) from ftban"):
            writer.writerow(row)
    conn.close()


# csv2db('./ftban.csv')