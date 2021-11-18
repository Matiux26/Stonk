import pyodbc
import pandas as pd
import praw
import sys
import getopt
import requests
import time
from Database import Database

class DataFetcher:

    def __init__(self):
        self.nasdaq_symbols = self.get_nasdaq_symbols()
        self.db_conn = Database()

    def connect_to_reddit_api(self):
        self.reddit = praw.Reddit(user_agent='Comments analyzer',
                                  client_id='', client_secret="",
                                  username='', password='')

    def get_recent_submissions_from_reddit_api(self):
        self.connect_to_reddit_api()
        for submission in self.reddit.subreddit("wallstreetbets").hot():
            self.try_to_save_submission_to_db(self.map_submission_object_to_dict(submission))

    def map_submission_object_to_dict(self, submission):
        return {
            "title": submission.title,
            "created_utc": submission.created_utc,
            "id": submission.id,
            "num_comments": submission.num_comments,
            "score": submission.score,
            "title": submission.title
        }

    def get_all_submissions_from_pushshift_api(self):
        while True:
            params = {"subreddit": "wallstreetbets", "sort": "created_utc:asc",
                "size": 100, "after": self.db_conn.get_last_created_utc_from_db()}
            result = requests.get("https://api.pushshift.io/reddit/search/submission/", params)

            if result.status_code == 200:
                submissions = result.json()['data']
            
            if not submissions:
                return False
            
            self.handle_respone(submissions)
            time.sleep(5)

    def handle_respone(self, submissions):
        for submission in submissions:
            self.try_to_save_submission_to_db(submission)

    def check_for_error_in_response(self, response):
        print(response['data'][0]['metadata'])
        return not response['metadata']['timed_out'] \
            and (response['metadata']['shards']['total'] == response['metadata']['shards']['successful']) \
            and response['metadata']['shards']['failed'] == 0

    def try_to_save_submission_to_db(self, submission):
        individual_title_words = submission["title"].split()
        for individual_title_word in individual_title_words:
            if(self.normalize_string(individual_title_word) in self.nasdaq_symbols):
                self.db_conn.save_submission_to_database(submission, individual_title_word)

    def normalize_string(self, string):
        return string.strip().upper()

    # csv from nasdaq site 03.05.2021
    def get_nasdaq_symbols(self):
        nasdaq_symbols_dataframe = pd.read_csv('nasdaq.csv')
        return nasdaq_symbols_dataframe['Symbol'].to_numpy()

def main(argv):
    try:
        data = DataFetcher()
        opts, args = getopt.getopt(argv, "hpr")
    except getopt.GetoptError as e:
        print(e)
        print('data_fetcher.py -p | -r')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('data_fetcher.py -p | -r')
            sys.exit()
        elif opt == ("-p"):
            data.get_all_submissions_from_pushshift_api()
            break
        elif opt == ("-r"):
            data.get_recent_submissions_from_reddit_api()
            break


if __name__ == "__main__":
    main(sys.argv[1:])