#----------------------------------------------------------------
# SQL Preprocessing
# Postgres based examples - https://www.postgresql.org/
# Setup is described in README.md
#----------------------------------------------------------------

from sql_preprocessing import *
import pandas as pd


# Postgress connection
# User: postgress, password: password
dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=True)


# Database functions
#----------------------------------------------------------------

csv_file = "simulated_dataset.csv"
sdf_name = 'sd_1'
dataset_schema = 's1'
dataset_table = 'sd_1_catalog_test'
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
print(sdf.dbcatalog)
print(sdf.dbcatalog.get_list_of_catalog_tables())
print(sdf)

# split dataset and create two temporary tables added to shared catalog under the same sdf_name
x_train_sdf, x_test_sdf = sdf.train_test_split(test_size=0.25, random_state=0)
print(x_train_sdf.dbcatalog)
print(x_test_sdf.dbcatalog)
print(x_train_sdf.dbcatalog.get_list_of_catalog_tables())


# create mapper with a set of label encoders, each needs an temporary table
mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlLabelEncoder()),
    ('code_22', SqlLabelEncoder()),
    ('code_23', SqlLabelEncoder()),
    ('code_24', SqlLabelEncoder()),
    ('code_25', SqlLabelEncoder())
])


# train the label encoders, create 6 temporary fit tables registered in catalog
mapper.fit(x_train_sdf)
print(x_train_sdf.dbcatalog.get_list_of_catalog_tables())


# use the tables
mapper.transform(x_test_sdf)
x_test_sdf.execute_df()



# create another sdf with different unique name
sdf2 = sdf.clone(sdf_name='sdf2')
print(sdf2)

# create set of functions for sdf2
mapper2 = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlLabelEncoder()),
    ('code_22', SqlLabelEncoder()),
    ('code_23', SqlLabelEncoder()),
    ('code_24', SqlLabelEncoder()),
    ('code_25', SqlLabelEncoder())
])

# train the label encoders, create 6 temporary fit tables registered in catalog
mapper2.fit(sdf2)

# now the catalog has sdf2 records
print(sdf2.dbcatalog.get_list_of_catalog_tables())

# view of both sdfs catalog records
print(sdf2.dbcatalog.get_list_of_catalog_tables(include_all_sdfs=True))


# drop all temporary tables of the first sdf
# any sdf pointing to same catalog can drop all catalog tables (sdf, x_train_sdf, x_test_sdf)
sdf.dbcatalog.drop_temporary_tables()
print(sdf.dbcatalog.get_list_of_catalog_tables(include_all_sdfs=True))



