import csv
import logging
import sqlite3
import time
import traceback
from queue import Full
from threading import Thread

logger = logging.getLogger('util.py')  # 不加名称设置root logger


class ThreadDecorator(Thread):
    """
    ThreadDecorator extends threading.Thread

    run a function in a new thread, automatically catch Runtime Error in this thread.
    when runtime error was caught, logging the error,
    put thread back to threads queue if it exist or rerun the function again in a new thread.
    """

    # TODO: try decorator class
    # TODO:
    # def __init__(self, group=None, target=None, name=None,
                 # args=(), kwargs=None, *, daemon=None):
    def __init__(self, target, *args, threads_q=None, sleep=0, **kw):
        """
        :param target:
        :param args:
        :param threads_q: threads queue
        :param sleep: time.sleep() when error occurred
        :param kw:
        """
        Thread.__init__(self)
        self._func = target
        self._q = threads_q
        self._args = args
        self._kw = kw
        self._sleep = sleep

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
            logging.info(self._func.__name__)
            logging.info(self._args)
            logging.info(self._kw)
            time.sleep(self._sleep)
            if self._q is not None:
                logging.warning(">>>> Put thread above function back to threads queue")
                try:
                    self._q.put(ThreadDecorator(self._func, threads_q=self._q,
                                                *self._args, **self._kw),
                                block=False)
                    logging.debug(">>>> Successfully put function back into the thread queue")
                except Full as e:
                    logging.critical(">>>> thread queue is Full, lost one function above")
            else:
                logging.warning(">>>> Create rerun above function in new thread")
                ThreadDecorator(target=self._func,
                                *self._args,
                                threads_q=None,
                                **self._kw).start()
                logging.debug(">>>> Successfully rerun the function again")


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
