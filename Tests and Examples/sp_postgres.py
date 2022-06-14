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
sdf.add_unique_id_column()


# show schema of the underlying table
print(sdf.get_table_schema())

# show first 5 rows of the underlying table
print(sdf.get_table_head())










# Transformation examples
#----------------------------------------------------------------

# Example 1 - single function/column transformation
#lable encoding on column code_20
le = SqlLabelEncoder()
le.fit(sdf, "code_20")
le.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
bd = SqlKBinsDiscretizer()
bd.fit(sdf, "code_20")
bd.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))

sdf = sdf.clone()
si = SqlSimpleImputer(strategy="mean", cast_as='int')
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
si = SqlSimpleImputer(strategy="constant", fill_value=50)
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
si = SqlSimpleImputer(strategy="most_frequent")
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


#Example 2 - combine two functions/columns into a transformation

#clone sdf definition (and leave behind any preceeding transformations)
sdf = sdf.clone()

#lable encoding on column code_20
le = SqlLabelEncoder()
le.fit(sdf, "code_20")
le.transform(sdf, "code_20")

#onehotencoder on column code_21
ohe = SqlOneHotEncoder()
ohe.fit(sdf, "code_21")
ohe.transform(sdf, "code_21")

print(sdf.execute_df(return_df = True))



# Example 3 - use of mapper to combine multiple transformations
sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMaxAbsScaler()),
    ('code_23', SqlBinarizer(5)),
    ('code_24', SqlBinarizer(0)),
    ('code_25', SqlStandardScaler())
])



mapper.fit(sdf)
mapper.transform(sdf)
#show dataframe head and include the table source columns to allow inspection of impact of transformation functions
print(sdf.head(include_source_columns = False))

print(sdf)
print(mapper)





# Example 4 - UDF, passthrough column, and cutom sql transformation

sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_21', SqlPassthroughColumn()),
    ('code_21', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 10])),
    ('code_21', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 1000])),
    ('code_22', SqlPassthroughColumn("code22_renamed_in_output")),
    ('code_22', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 10])),
    ('code_22', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 1000])),
    ('code_23', SqlPassthroughColumn("code23_renamed_in_output")),
    ('code_23', SqlCustomSqlTransformer("{column} * 1000000"))
])


mapper.fit(sdf)
mapper.transform(sdf)

print(sdf.head())

print(mapper)








# Example 5 - Additional functions - generic sql transformation, create table from transformation, etc

sdf = sdf.clone()
mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlLabelEncoder()),
    ('code_22', SqlLabelEncoder()),
    ('code_23', SqlLabelEncoder()),
    ('code_24', SqlLabelEncoder()),
    ('code_25', SqlLabelEncoder())
])


mapper.fit(sdf)
mapper.transform(sdf)


# generate sql with a replaced source i.e. for storing and executing it later
print(sdf.generate_sql(replace_data_source='{data_source}', replace_fit_schema='{fit_schema}'))

# create a new table from execution of the transformation query
sdf.execute_transform_to_table(target_schema = dataset_schema, target_table = dataset_table + '_generated')






# Example 6 - Case and Map encoding 


sdf = sdf.clone()
ce = SqlCaseEncoder(cases={('{column} < 10', 0), ('{column} >= 10 and {column} < 20', 2)}, else_value=3)
print(ce)
ce.fit(sdf, "code_20")
ce.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True, include_source_columns = True))



sdf = sdf.clone()
me = SqlMapEncoder(pairs={(6, "'600'"), (14, "'1400'")}, else_value=0)
print(me)
me.fit(sdf, "code_20")
me.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True, include_source_columns = True))





# Example 7 - sampling 


sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_20', SqlMinMaxScaler()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMinMaxScaler()),
])

mapper.fit_transform(sdf)

print(sdf.get_table_size())

# retrieve sample size 10
print(sdf.execute_sample_df(n=10))


# retrieve sample of fraction 0.01
print(sdf.execute_sample_df(frac=0.01, random_state=0.5))


sdf.execute_sample_transform_to_table(fit_schema, 'sample_test', n=100)


sdf.dbcatalog.drop_temporary_tables()

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
sdf.add_unique_id_column()


# show schema of the underlying table
print(sdf.get_table_schema())

# show first 5 rows of the underlying table
print(sdf.get_table_head())










# Transformation examples
#----------------------------------------------------------------

# Example 1 - single function/column transformation
#lable encoding on column code_20
le = SqlLabelEncoder()
le.fit(sdf, "code_20")
le.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
bd = SqlKBinsDiscretizer()
bd.fit(sdf, "code_20")
bd.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))

sdf = sdf.clone()
si = SqlSimpleImputer(strategy="mean", cast_as='int')
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
si = SqlSimpleImputer(strategy="constant", fill_value=50)
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


sdf = sdf.clone()
si = SqlSimpleImputer(strategy="most_frequent")
si.fit(sdf, "code_20")
si.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True))


#Example 2 - combine two functions/columns into a transformation

#clone sdf definition (and leave behind any preceeding transformations)
sdf = sdf.clone()

#lable encoding on column code_20
le = SqlLabelEncoder()
le.fit(sdf, "code_20")
le.transform(sdf, "code_20")

#onehotencoder on column code_21
ohe = SqlOneHotEncoder()
ohe.fit(sdf, "code_21")
ohe.transform(sdf, "code_21")

print(sdf.execute_df(return_df = True))



# Example 3 - use of mapper to combine multiple transformations
sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMaxAbsScaler()),
    ('code_23', SqlBinarizer(5)),
    ('code_24', SqlBinarizer(0)),
    ('code_25', SqlStandardScaler())
])



mapper.fit(sdf)
mapper.transform(sdf)
#show dataframe head and include the table source columns to allow inspection of impact of transformation functions
print(sdf.head(include_source_columns = False))

print(sdf)
print(mapper)





# Example 4 - UDF, passthrough column, and cutom sql transformation

sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_21', SqlPassthroughColumn()),
    ('code_21', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 10])),
    ('code_21', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 1000])),
    ('code_22', SqlPassthroughColumn("code22_renamed_in_output")),
    ('code_22', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 10])),
    ('code_22', SqlUDFTransformer("public.increment2", ["cast({column} as integer)", 1000])),
    ('code_23', SqlPassthroughColumn("code23_renamed_in_output")),
    ('code_23', SqlCustomSqlTransformer("{column} * 1000000"))
])


mapper.fit(sdf)
mapper.transform(sdf)

print(sdf.head())

print(mapper)








# Example 5 - Additional functions - generic sql transformation, create table from transformation, etc

sdf = sdf.clone()
mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlLabelEncoder()),
    ('code_22', SqlLabelEncoder()),
    ('code_23', SqlLabelEncoder()),
    ('code_24', SqlLabelEncoder()),
    ('code_25', SqlLabelEncoder())
])


mapper.fit(sdf)
mapper.transform(sdf)


# generate sql with a replaced source i.e. for storing and executing it later
print(sdf.generate_sql(replace_data_source='{data_source}', replace_fit_schema='{fit_schema}'))

# create a new table from execution of the transformation query
sdf.execute_transform_to_table(target_schema = dataset_schema, target_table = dataset_table + '_generated')






# Example 6 - Case and Map encoding 


sdf = sdf.clone()
ce = SqlCaseEncoder(cases={('{column} < 10', 0), ('{column} >= 10 and {column} < 20', 2)}, else_value=3)
print(ce)
ce.fit(sdf, "code_20")
ce.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True, include_source_columns = True))



sdf = sdf.clone()
me = SqlMapEncoder(pairs={(6, "'600'"), (14, "'1400'")}, else_value=0)
print(me)
me.fit(sdf, "code_20")
me.transform(sdf, "code_20")
print(sdf.execute_df(return_df = True, include_source_columns = True))





# Example 7 - sampling 


sdf = sdf.clone()

mapper = SqlDataFrameMapper([
    ('code_20', SqlMinMaxScaler()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMinMaxScaler()),
])

mapper.fit_transform(sdf)

print(sdf.get_table_size())

# retrieve sample size 10
print(sdf.execute_sample_df(n=10))


# retrieve sample of fraction 0.01
print(sdf.execute_sample_df(frac=0.01, random_state=0.5))


sdf.execute_sample_transform_to_table(fit_schema, 'sample_test', n=100)


sdf.dbcatalog.drop_temporary_tables()

