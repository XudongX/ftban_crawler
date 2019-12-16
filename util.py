import csv
import sqlite3


def csv2db(csv_path, db_path):
    conn = sqlite3.connect(db_path)
    db_cursor = conn.cursor()
    with open(csv_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
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


def db2csv(db_path, csv_path):
    conn = sqlite3.connect(db_path)
    db_cursor = conn.cursor()
    with open(csv_path, 'r') as csv_file:
        writer = csv.writer(csv_file)
        for row in db_cursor.execute("select (product_name, "
                                     "cert_id, "
                                     "company_name, "
                                     "month_date, "
                                     "detail_url) from ftban"):
            writer.writerow(row)
    conn.close()
