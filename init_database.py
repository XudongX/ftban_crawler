import sqlite3


def init_sqlite3(db_path):
    with sqlite3.connect(db_path) as conn:
        db_cursor = conn.cursor()
        db_cursor.execute("DROP TABLE IF EXISTS ftban;")
        db_cursor.execute('''
CREATE TABLE "ftban" (
  "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
  "product_name" string NOT NULL,
  "cert_id" string NOT NULL,
  "company_name" string NOT NULL,
  "month_date" date,
  "detail_url" string NOT NULL,
  "header1" string,
  "producer_name" string,
  "producer_address" string,
  "producer_detail" string,
  "ingredient" string,
  "notes1" string,
  "notes2" string,
  "picture_2d" string,
  "picture_3d" string,
  "history_record" string
);''')
        db_cursor.execute("create unique index ftban_cert_id_uindex on ftban (cert_id);")
        conn.commit()

if __name__ == '__main__':
    init_sqlite3('./test.db')

