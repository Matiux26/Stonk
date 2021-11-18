import yfinance as yf
import pyodbc
import pandas as pd
import sys
import getopt
import numpy as np
import Database
from datetime import date

class StockFetcher:

    def __init__(self):
        self.db_conn = Database()

    def get_stock_data_from_yfinance(self):
        concatenated_tickers = self.get_concatenated_tickers()
        tickers = self.db_conn.get_tickers()
        df = yf.download(concatenated_tickers, start=self.db_conn.get_last_created_stock_date(),
            end=str(date.today()), group_by="ticker")

        for ticker in tickers:
            for index, row in df[ticker].iterrows():
                self.db_conn.save_stock_to_database(row, ticker)

    def get_concatenated_tickers(self):
        tickers = self.get_avg_tickers()
        concatenated_tickers = ' '.join(tickers)
        return concatenated_tickers

def main(argv):
    try:
        data = StockFetcher()
        opts, args = getopt.getopt(argv, "hs")
    except getopt.GetoptError as e:
        print(e)
        print('stock_fetcher.py -s')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('stock_fetcher.py -s')
            sys.exit()
        elif opt == ("-s"):
            data.get_stock_data_from_yfinance()
            break

if __name__ == "__main__":
    main(sys.argv[1:])