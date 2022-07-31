#append system path to import cousin packages
import sys
sys.path.append("/Users/weisun/Coding Projects/Machine Learning/SQLDATAPREP4ML")

from sql_preprocessing import *
import pandas as pd
import sqlalchemy
from sklearn.linear_model import LogisticRegression

from sklearn.model_selection import train_test_split
from sklearn.compose import *
from sklearn.preprocessing import *
from sklearn.pipeline import *

from tempfile import mkdtemp
import os
savedir = mkdtemp()

import warnings
from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)



# fix for sklearn.LabelEncoder
class LabelEncoder2(LabelEncoder):
    def fit(self, X, y=None):
        super(LabelEncoder2, self).fit(X)
    def transform(self, X, y=None):
        res = super(LabelEncoder2, self).transform(X)
        df = pd.DataFrame(data=res)
        return df
    def fit_transform(self, X, y=None):
        return super(LabelEncoder2, self).fit(X).transform(X)





sdf_name = 'titanic'
dataset_schema = 'S1'
dataset_table = 'TITANIC'
key_column = 'passengerid'

connection_string = "postgresql://weisun:password@localhost:5432/db1"



dbconn = SqlConnection(connection_string, print_sql=True)
sdf = dbconn.get_sdf_for_table(sdf_name, dataset_schema, dataset_table, key_column, default_order_by="index")
df1 = sdf.get_table_head(1000)

engine = sqlalchemy.create_engine(connection_string)
df2 = pd.read_sql_query("select * from " + dataset_schema + "." + dataset_table + " order by index", engine)


print(np.array_equal(df2.to_numpy(), df1.to_numpy()))


x_train_sdf, x_test_sdf, y_train_df1, y_test_df1 = sdf.train_test_split(test_size=0.25, random_state=0, y_column='survived')
# x_train_sdf, x_test_sdf = sdf.train_test_split(test_size=0.25, random_state=0)




x_df2 = df2.drop('survived', axis=1)
y_df2 = df2['survived']

x_train_df2, x_test_df2, y_train_df2, y_test_df2 = train_test_split(x_df2, y_df2, test_size=0.25, random_state=0)

compare_arrays(y_train_df2.array._ndarray, y_train_df1, 0)
compare_arrays(y_test_df2.array._ndarray, y_test_df1, 0)


#print(y_train_df1.equals(y_train_df2))