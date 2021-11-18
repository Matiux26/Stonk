import pyodbc
import pandas as pd
import sys
import getopt
from Database import Database
import numpy as np
import DataFrameSelector
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelBinarizer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import StratifiedShuffleSplit

class Model:

    def __init__(self):
        self.db_conn = Database()
        submissions = self.db_conn.get_submissions_data()
        stocks = self.db_conn.get_stock_data()
        print(np.shape(submissions))
        exit()

    def create_pipeline(self, num_attribs, cat_attribs):
        num_pipeline = Pipeline([
            ('selector', DataFrameSelector(num_attribs)),
            ('imputer', SimpleImputer(strategy="median")),
            ('std_scaler', StandardScaler()),
        ])

        cat_pipeline = Pipeline([
            ('selector', DataFrameSelector(cat_attribs)),
            ('label_binarizer', LabelBinarizer()),
        ])

        full_pipeline = FeatureUnion(transformer_list=[
            ("num_pipeline", num_pipeline),
            ("cat_pipeline", cat_pipeline),
        ])
        
        self.full_pipeline = full_pipeline


def main(argv):
    model = Model()

if __name__ == "__main__":
    main(sys.argv[1:])