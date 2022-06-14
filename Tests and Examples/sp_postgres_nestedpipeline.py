#----------------------------------------------------------------
# SQL Preprocessing
# Postgres based examples - https://www.postgresql.org/
# Setup is described in README.md
#----------------------------------------------------------------

from sql_preprocessing import *
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.compose import *
from sklearn.preprocessing import *



# Postgress connection
# User: postgress, password: password
dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=True)


# Database functions
#----------------------------------------------------------------

csv_file = "simulated_dataset.csv"
sdf_name = 'sd_1'
dataset_schema = 's1'
dataset_table = 'sd_1'
key_column = 'key_column'
catalog_schema = dataset_schema
fit_schema = dataset_schema
default_order_by = None
db_args = {}


# load csv and store it to db (if does not exist yet)
df = pd.read_csv(csv_file)
if (not dbconn.table_exists(dataset_schema, dataset_table)):
    dbconn.upload_df_to_db(df, dataset_schema, dataset_table) 

# create SqlDataFrame pointing to table s1.sd_1 (loaded above)
sdf = dbconn.get_sdf_for_table(sdf_name, dataset_schema, dataset_table, key_column, catalog_schema, fit_schema, default_order_by, **db_args)

# create unique key on the new table (with the name "key_column")
#sdf.add_unique_id_column()


#split the dataset - to training and test
#x_train, x_test, y_target_train, y_target_test = cross_validation.train_test_split(tmp_x, tmp_y, test_size=0.25, random_state=0)
x_train_sdf, x_test_sdf = sdf.train_test_split(test_size=0.25, random_state=0)
y_train_df = x_train_sdf.get_y_df('prediction')
y_test_df = x_test_sdf.get_y_df('prediction')









# Nested pipeline






preprocessor_l1 = SqlColumnTransformer(
    transformers=[
        ('l1_code_21', SqlPassthroughColumn('l1_code_21'), 'code_21'),
        ('l1_code_22', SqlPassthroughColumn('l1_code_22'), 'code_22'),
        ('l1_code_23', SqlLabelEncoder('l1_code_23'), 'code_23'),
])

print(preprocessor_l1)




preprocessor_l2 = SqlColumnTransformer(
    transformers=[
        ('l2_code_21', SqlPassthroughColumn('l2_code_21'), 'l1_code_21'),
        ('l2_code_23', SqlPassthroughColumn('l2_code_23'), 'l1_code_23'),
        ('l2_b_code_23', SqlBinarizer(50, 'l2_b_code_23'), 'l1_code_23')
])

print(preprocessor_l2)





pipeline = SqlNestedPipeline(steps=[('preprocessor_l1', preprocessor_l1),
                                ('preprocessor_l2', preprocessor_l2),
                                ('classifier', LogisticRegression())])

print(pipeline)

pipeline.fit(x_train_sdf, y_train_df)

print("model score: %.3f" % pipeline.score(x_test_sdf, y_test_df))