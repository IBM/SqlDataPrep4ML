#append system path to import cousin packages
import sys
sys.path.append("/Users/weisun/Coding Projects/Machine Learning/SQLDATAPREP4ML")

import pandas as pd
import numpy as np
import scipy
from sklearn_pandas import DataFrameMapper
import sklearn.preprocessing as sp
from sql_preprocessing import *


from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import LabelBinarizer
from sklearn.preprocessing import StandardScaler

# Example 1 - CSV + Label Binarizer
df = pd.read_csv('/ds1.csv')

lb = LabelBinarizer()
lb.fit(df['code_20'])
lb_results = lb.transform(df['code_20'])
 


# Example 1 - SQL + Label Binarizer
sdf = SqlDataFrame('postgresql://weisun:password@localhost:5432/db1', 'ds1', 'schema')

lb = SqlLabelBinarizer()
lb.fit(sdf, 'code_20')
lb_results = lb.transform_df(sdf, 'code_20')



#Example 2 - CSV + DataFrameMapper
df = pd.read_csv('/ds1.csv')

mapper = DataFrameMapper([
    ('pet', LabelBinarizer()),
    ('salary', StandardScaler())
])

df_transform = mapper.fit_transform(df)


# Example 2 - SQL + DataFrameMapper
sdf = SqlDataFrame('postgres://postgres:password@localhost:5432/db1', 'ds1', 'schema')

mapper = SqlDataFrameMapper([
    ('pet', SqlLabelBinarizer()),
    ('salary', SqlStandardScaler())
])

df_transform = mapper.fit_transform_df(sdf)


