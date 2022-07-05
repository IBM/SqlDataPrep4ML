# Transformation tests - validation of identical results with Sklearn
#----------------------------------------------------------------
#append system path to import cousin packages
import sys
sys.path.append("/Users/weisun/Coding Projects/Machine Learning/SQLDATAPREP4ML")

from sql_preprocessing import *
import pandas as pd
from sklearn.preprocessing import *



dbconn = SqlConnection("postgresql://weisun:password@localhost:5432/db1", print_sql=True)

csv_file = "simulated_dataset.csv"
db_schema = "s1"
db_table = "sd_1"
key_column = "key_column"


df = pd.read_csv(csv_file)


df['sort_column'] = df.index
#dbconn.drop_table(db_schema, db_table)


if (not dbconn.table_exists(db_schema, db_table)):
    dbconn.upload_df_to_db(df, db_schema, db_table) 

sdf = dbconn.get_sdf_for_table(db_table, db_schema, db_table, key_column)
sdf.add_unique_id_column()

#print(sdf.get_table_head(20, order_by="sort_column"))
#print(df.head(20))


# comparison of functions
results = ComparisonArray()

compare_functions_noargs (Binarizer(threshold=50), SqlBinarizer(50), 'Binarizer', csv_file, sdf.clone(), 'code_20', True, '100 ints (thr 50)', 0, 'index', results)
#check for binary column case

compare_functions_noargs (OneHotEncoder(categories='auto'), SqlOneHotEncoder(), 'OneHotEncoder', csv_file, sdf.clone(), 'code_20', True, '100 int labels', 0, 'index', results)

compare_functions_noargs (LabelEncoder(), SqlLabelEncoder(), 'LabelEncoder', csv_file, sdf.clone(), 'code_20', True, '100 int labels', 0, 'index', results)

compare_functions_noargs (StandardScaler(), SqlStandardScaler(), 'StandardScaler', csv_file, sdf.clone(), 'code_20', True, '1 column int/bigint', 1e-03, 'index', results)

compare_functions_noargs (OrdinalEncoder(), SqlOrdinalEncoder(), 'OrdinalEncoder', csv_file, sdf.clone(), 'code_20', True, '100 int labels', 0, 'index', results)

compare_functions_noargs (LabelBinarizer(), SqlLabelBinarizer(), 'LabelBinarizer', csv_file, sdf.clone(), 'code_20', True, '100 int labels', 0, 'index', results)

compare_functions_noargs (MinMaxScaler(), SqlMinMaxScaler(), 'MinMaxScaler', csv_file, sdf.clone(), 'code_20', True, '1 column int/sql-bigint', 1e-05, 'index', results)

compare_functions_noargs (MaxAbsScaler(), SqlMaxAbsScaler(), 'MaxAbsScaler', csv_file, sdf.clone(), 'code_20', True, '1 column int/sql-bigint', 0, 'index', results)


results.print_table()






# comparison of mappers
results = ComparisonArray()

skl_mapper = DataFrameMapper([
    (['code_20'], MinMaxScaler()),
    (['code_21'], MinMaxScaler()),
])

sql_mapper = SqlDataFrameMapper([
    ('code_20', SqlMinMaxScaler()),
    ('code_21', SqlMinMaxScaler()),
])

compare_mappers_noargs (skl_mapper, sql_mapper, csv_file, sdf.clone(), True, '2x minmax', 0, 'index', results)


skl_mapper = DataFrameMapper([
    (['code_20'], LabelEncoder()),
    (['code_21'], MinMaxScaler()),
    (['code_22'], MaxAbsScaler()),
    (['code_23'], Binarizer(5)),
    (['code_24'], Binarizer(0)),
    (['code_25'], StandardScaler())
])

sql_mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMaxAbsScaler()),
    ('code_23', SqlBinarizer(5)),
    ('code_24', SqlBinarizer(0)),
    ('code_25', SqlStandardScaler())
])

compare_mappers_noargs (skl_mapper, sql_mapper, csv_file, sdf.clone(), True, '6 columns', 1e-05, 'index', results)

results.print_table()
