import csv
import logging
import sqlite3
import time
import traceback
from threading import Thread

logger = logging.getLogger('util.py')  # 不加名称设置root logger


class ThreadDecorator(Thread):
    """
    ThreadDecorator extends threading.Thread
    run a function in a new thread, automatically catch Runtime Error in the thread
    logging the error and put thread back to threads queue if it exist.
    """

    def __init__(self, target_func, *args, threads_q=None, **kw):
        """
        :param target_func:
        :param args:
        :param threads_q: threads queue
        :param kw:
        """
        Thread.__init__(self)
        self._func = target_func
        self._q = threads_q
        self._args = args
        self._kw = kw

    def run(self) -> None:
        """
        override threading.Thread.run()
        :return:
        """
        try:
            self._func(*self._args, **self._kw)
        except RuntimeError as e:
            logging.critical(">>>> Unknown Runtime Error arose")
            logging.critical(traceback.format_exc())
            if self._q is not None:
                logging.error(">>>> Put thread func back to threads queue:")
                logging.error(self._func.__name__)
                logging.error(self._args)
                print(type(self._args))
                print(type(self._kw))
                logging.error(self._kw)
                time.sleep(5)
                self._q.put(ThreadDecorator(self._func, threads_q=self._q,
                                            *self._args, **self._kw),
                            block=True)
            else:
                logging.critical(">>>> Lost one thread: " + self._func.__name__ + "()")


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
