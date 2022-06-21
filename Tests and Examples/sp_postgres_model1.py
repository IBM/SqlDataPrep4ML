#----------------------------------------------------------------
# SQL Preprocessing
# Postgres based examples - https://www.postgresql.org/
# Setup is described in README.md
#----------------------------------------------------------------

#append system path to import cousin packages
import sys
sys.path.append("/Users/weisun/Coding Projects/Machine Learning/SDPL - Public Version v2")

from sql_preprocessing import *
import pandas as pd


# Postgress connection
# User: postgress, password: password
dbconn = SqlConnection("postgresql://weisun:password@localhost:5432/db1", print_sql=True)


# Database functions
#----------------------------------------------------------------

csv_file = "simulated_dataset.csv"
db_schema = "s1"
db_table = "sd_1"
key_column = "key_column"


# load csv and store it to db (if does not exist yet)
df = pd.read_csv(csv_file)
if (not dbconn.table_exists(db_schema, db_table)):
    dbconn.upload_df_to_db(df, db_schema, db_table) 

# create SqlDataFrame pointing to table s1.sd_1 (loaded above)
sdf = dbconn.get_sdf_for_table(db_table, db_schema, db_table, key_column)

# create unique key on the new table (with the name "key_column")
#sdf.add_unique_id_column()


#split the dataset - to training and test
#x_train, x_test, y_target_train, y_target_test = cross_validation.train_test_split(tmp_x, tmp_y, test_size=0.25, random_state=0)
train_sdf, test_sdf = sdf.train_test_split(test_size=0.25, random_state=0)


# transform both sets
mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlLabelEncoder()),
    ('code_22', SqlLabelEncoder()),
    ('code_23', SqlLabelEncoder()),
    ('code_24', SqlLabelEncoder()),
    ('code_25', SqlLabelEncoder())
])

mapper.fit_transform(test_sdf)
x_test = test_sdf.execute_df(return_df = True, order_by='index') # column index is not automatically generated
y_target_test = test_sdf.get_y_df('prediction', order_by='index')

mapper.fit_transform(train_sdf)
x_train = train_sdf.execute_df(return_df = True, order_by='index')
y_target_train = train_sdf.get_y_df('prediction', order_by='index')


#invoke linear regression 

from sklearn.linear_model import LinearRegression
lr = LinearRegression()
model = lr.fit(x_train, y_target_train)

print(model)

