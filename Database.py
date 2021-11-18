import pyodbc
import numpy as np

class Database:

    def __init__(self):
        self.connect_to_db()

    def connect_to_db(self):
        self.cnxn = pyodbc.connect(
            r'Driver=SQL Server;Server=.\SQLEXPRESS;Database=Reddit;Trusted_Connection=yes;')
        self.cursor = self.cnxn.cursor()

    def get_tickers(self):
        self.cursor.execute("WITH ticker_count AS ( \
            SELECT \
            COUNT(ticker) AS cnt, \
            ticker \
            FROM submissions \
            GROUP BY ticker \
            ) \
            SELECT TOP 90 PERCENT cnt, ticker FROM ticker_count \
            WHERE cnt > (SELECT avg(cnt) FROM ticker_count) \
            ORDER BY cnt ASC")
        return np.array(self.cursor.fetchall())[:,1]
    
    def save_stock_to_database(self, stock, ticker):
        self.cursor.execute("INSERT INTO stock(date, [open], high, \
            low, [close], volume, ticker) \
            values (?, ?, ?, ?, ?, ?, ?)", stock["Date"], stock["Open"], stock["High"],
                            stock["Low"], stock["Close"], stock["Volume"], ticker)
        self.cnxn.commit()

    def save_submission_to_database(self, submission, ticker):
        self.cursor.execute("INSERT INTO submissions(ticker, created_utc, submission_id, \
            num_of_comments, score, title) \
            values (?, ?, ?, ?, ?, ?)", ticker.upper(), submission["created_utc"], submission["id"],
                            submission["num_comments"], submission["score"], submission["title"])
        self.cnxn.commit()

    def get_stock_data(self):
        self.cursor.execute("SELECT \
            date, open, high, low, close, volume, ticker \
            FROM stock")
        return np.array(self.cursor.fetchall())

    def get_submissions_data(self):
        self.cursor.execute("SELECT \
            ticker, created_utc, submission_id, \
            num_of_comments, score, upvote_ratio, title \
            FROM submissions")
        return np.array(self.cursor.fetchall())

    def get_last_created_utc_from_db(self):
        self.cursor.execute("SELECT MAX(created_utc) FROM submissions")
        last_created_utc = self.cursor.fetchone()[0]

        print(last_created_utc)

        if last_created_utc == None:
            return 1337968000
        else:
            return last_created_utc
    
    def get_last_created_stock_date(self):
        self.cursor.execute("SELECT MAX(date) FROM stock")
        last_date = self.cursor.fetchone()[0]

        print(last_date)

        if last_date == None:
            return '2012-01-01'
        else:
            return last_date
