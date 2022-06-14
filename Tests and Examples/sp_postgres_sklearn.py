#----------------------------------------------------------------
# SQL Preprocessing
# Postgres based examples - https://www.postgresql.org/
# Setup is described in README.md
#----------------------------------------------------------------

from sql_preprocessing import *
import pandas as pd
from sklearn.preprocessing import *


# Postgress connection
# User: postgress, password: password
dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=True)


# Database functions
#----------------------------------------------------------------

csv_file = "simulated_dataset.csv"
db_schema = "s1"
db_table = "sd_1"
key_column = "key_column"


#dbconn.drop_table(db_schema, db_table)

# load csv and store it to db (if does not exist yet)
df = pd.read_csv(csv_file)
if (not dbconn.table_exists(db_schema, db_table)):
    dbconn.upload_df_to_db(df, db_schema, db_table) 

# create SqlDataFrame pointing to table s1.sd1 (loaded above)
# the default_order_by is needed in order to guarantee same order of rows as is in the source dataframe - after storing records from csv/dataframe to db, rows may have different order
sdf = dbconn.get_sdf_for_table(db_table, db_schema, db_table, key_column, default_order_by='index')

# create unique key on the new table (with the name "key_column") - this is needed for some of the functions which require key column to match results of subqueries such as normalizer
sdf.add_unique_id_column()













# Test and comparison of functions 
# 1) train sklearn function
# 2) transform into sql function
# 3) compare results 

def test_sklearn_to_sql_conversion(sklearn_function, df, sdf, columns, compare_tolerance):

    print('\n--------------------------------------------------------')
    print('Test conversion of Sklearn to Sql function')
    print(sklearn_function)
    
    skl_df = sklearn_function.fit_transform(df[columns])

    sql_function = SklearnToSqlConverter.convert_function(sklearn_function, sdf, columns)
    print(sql_function)

    sdf = sdf.clone()
    sql_function.transform(sdf, columns)
    sql_df = sdf.execute_df()

    compare_arrays(skl_df, sql_df, compare_tolerance)


test_sklearn_to_sql_conversion(MinMaxScaler(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(MaxAbsScaler(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(Binarizer(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(StandardScaler(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(OneHotEncoder(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(LabelBinarizer(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(Normalizer(), df, sdf, ["code_20", "code_21"], 1e-03)
test_sklearn_to_sql_conversion(LabelEncoder(), df, sdf, ["code_20"], 1e-03)
test_sklearn_to_sql_conversion(OrdinalEncoder(), df, sdf, ["code_20"], 1e-03)


