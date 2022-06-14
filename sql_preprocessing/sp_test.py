from sql_preprocessing import *
import pandas as pd
import unittest
import pathlib
import os 



csv_file = "titanic_dataset.csv"
print_sql = False

sdf_name = 's1_td'
dataset_schema = 's1'
dataset_table = 'td'
key_column = 'PassengerId'
uniue_key_column = "ukc"
fit_schema = dataset_schema
default_order_by = None
catalog_schema = dataset_schema
catalog_kwargs = {}



test_df = None

def get_test_df():

    global test_df

    if (test_df is None):
        file_path = os.path.join(pathlib.Path(__file__).parent.absolute(), csv_file)
        test_df = pd.read_csv(file_path)

    return test_df.copy()


def compare_dfs (df1, df2):
    return df1.equals(df2)


def compare_dfs_diff(df1, df2):

    result = True

    df = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
    if (df.shape[0] > 0):
        print (df)
        result = False

    df = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='right_only']
    if (df.shape[0] > 0):
        print (df)
        result = False

    return result


def compare_dfs_diff2(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    
    print(diff_df)

    return not diff_df.shape[0] > 0


def compare_dfrow_to_dbrow (test, dfrow, dbrow):
    for i in range(len(dbrow)):
        dfvalue = dfrow[i]
        dbvalue = dbrow[i]

        #special case NaN in df
        dfvalue = dfvalue if not pd.isna(dfvalue) else None
        
        test.assertEqual(dfvalue, dbvalue)


def compare_df_to_cursor(test, df, cursor):
    
    counter = 0

    for row in cursor:
        compare_dfrow_to_dbrow(test, df.iloc[counter], row)
        counter = counter + 1


def create_dummy_table(dbconn, schema, table_name):
    dbconn.drop_table(schema, table_name)
    sql = "CREATE TABLE " + schema + "." + table_name + " (dummy INT)"
    dbconn.execute_command(sql)




def get_dbconn():
    return SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=print_sql)




## check all arguments !!!
## add all comments 







class Test_Connection_Postgres(unittest.TestCase):

    def setUp(self):
        self.dbconn = get_dbconn()
        self.test_df = get_test_df()

    def tearDown(self):
        self.dbconn.close()

    def test__repr__(self):
        repr = self.dbconn.__repr__()
        self.assertGreater(len(repr), 0)

    def test_dbtype(self):
        self.assertEqual(self.dbconn.dbtype, SqlConnection.DbType.STANDARD_SQL)
        
    def test_upload_df_to_db_and_table_exists(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.assertTrue(self.dbconn.table_exists(dataset_schema, dataset_table))
        
    def test_column_exists(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.assertTrue(self.dbconn.column_exists(dataset_schema, dataset_table, "PassengerId"))

    def test_upload_df_to_db_and_drop_table(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.dbconn.drop_table(dataset_schema, dataset_table)
        self.assertFalse(self.dbconn.table_exists(dataset_schema, dataset_table))

        # upload includes drop table
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.assertTrue(self.dbconn.table_exists(dataset_schema, dataset_table))

        self.dbconn.drop_table(dataset_schema, dataset_table)
        self.assertFalse(self.dbconn.table_exists(dataset_schema, dataset_table))

    def test_get_table_as_df(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        df2 = self.dbconn.get_table_as_df(dataset_schema, dataset_table)
        self.assertTrue(compare_dfs(self.test_df, df2))

    def test_get_table_as_df_order_by(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        df2 = self.dbconn.get_table_as_df(dataset_schema, dataset_table, order_by='"PassengerId"')
        self.test_df = self.test_df.sort_values(by=["PassengerId"])
        self.assertTrue(compare_dfs(self.test_df, df2))

    '''
    def test_get_table_as_df_order_by2(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        df2 = self.dbconn.get_table_as_df(dataset_schema, dataset_table, order_by='"Age" desc, "PassengerId"')
        self.test_df = self.test_df.sort_values(by=["Age", "PassengerId"])
        self.assertTrue(compare_dfs_diff2(self.test_df, df2))
        #self.assertTrue(compare_dfs(self.test_df, df2))
        '''

    def test_get_table_schema(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        df_schema = self.dbconn.get_table_schema(dataset_schema, dataset_table)
        self.assertEqual(self.test_df.shape[1], df_schema.shape[0])

    def test_create_unique_key(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.assertFalse(self.dbconn.column_exists(dataset_schema, dataset_table, uniue_key_column))
        self.dbconn.create_unique_key(dataset_schema, dataset_table, uniue_key_column)
        df_schema = self.dbconn.get_table_schema(dataset_schema, dataset_table)
        self.assertTrue(self.dbconn.column_exists(dataset_schema, dataset_table, uniue_key_column))

    def test_execute_sql_to_df(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        sql = "select * from " + dataset_schema + "." + dataset_table
        df2 = self.dbconn.execute_sql_to_df(sql)
        self.assertTrue(compare_dfs(self.test_df, df2))

    def test_execute_query_onerow(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        sql = "select * from " + dataset_schema + "." + dataset_table
        row = self.dbconn.execute_query_onerow(sql)
        compare_dfrow_to_dbrow(self, self.test_df.iloc[0], row)  

    #def test_execute_command(self, sql):
    # execute_command is used in almost all tests as the underlying function to interact with the db  

    def test_execute_query_cursor(self):
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        sql = "select * from " + dataset_schema + "." + dataset_table
        result = self.dbconn.execute_query_cursor(sql)
        compare_df_to_cursor(self, self.test_df, result)
    
    def test_get_sdf(self):
        catalog = None
        sdf_query_data_source = "sql"
        sdf = self.dbconn.get_sdf(catalog, sdf_name, sdf_query_data_source, dataset_schema, dataset_table, key_column, fit_schema, default_order_by)
        self.assertEqual(sdf.catalog, catalog)
        self.assertEqual(sdf.sdf_name, sdf_name)
        self.assertEqual(sdf.sdf_query_data_source, sdf_query_data_source)
        self.assertEqual(sdf.dataset_schema, dataset_schema)
        self.assertEqual(sdf.dataset_table, dataset_table)
        self.assertEqual(sdf.key_column, key_column)
        self.assertEqual(sdf.fit_schema, fit_schema)
        self.assertEqual(sdf.default_order_by, default_order_by)

    def test_get_sdf_for_table(self):
        catalog = None
        sdf = self.dbconn.get_sdf_for_table(sdf_name, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, catalog)
        self.assertTrue(isinstance(sdf.catalog, InMemoryTableCatalog))
        self.assertEqual(sdf.sdf_name, sdf_name)
        self.assertEqual(sdf.dataset_schema, dataset_schema)
        self.assertEqual(sdf.dataset_table, dataset_table)
        self.assertEqual(sdf.key_column, key_column)
        self.assertEqual(sdf.fit_schema, fit_schema)
        self.assertEqual(sdf.default_order_by, default_order_by)

    def test_get_sdf_for_query(self):
        catalog = None
        sql_query = "sql"
        sdf = self.dbconn.get_sdf_for_query(sdf_name, sql_query, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, catalog)
        self.assertTrue(isinstance(sdf.catalog, InMemoryTableCatalog))
        self.assertEqual(sdf.sdf_name, sdf_name)
        self.assertTrue(sql_query in sdf.sdf_query_data_source)
        self.assertEqual(sdf.dataset_schema, dataset_schema)
        self.assertEqual(sdf.dataset_table, dataset_table)
        self.assertEqual(sdf.key_column, key_column)
        self.assertEqual(sdf.fit_schema, fit_schema)
        self.assertEqual(sdf.default_order_by, default_order_by)








class Test_TableCatalog(unittest.TestCase):

    def setUp(self):
        self.dbconn = get_dbconn()
        self.catalog = TableCatalog(self.dbconn, sdf_name, dataset_schema, dataset_table, fit_schema)
        
    def tearDown(self):
        self.dbconn.close()

    def test_constructor(self):
        self.assertEqual(self.catalog.dbconn, self.dbconn)
        self.assertEqual(self.catalog.sdf_name, sdf_name)
        self.assertEqual(self.catalog.dataset_schema, dataset_schema)
        self.assertEqual(self.catalog.dataset_table, dataset_table)
        self.assertEqual(self.catalog.fit_schema, fit_schema)

    def test_get_fit_table_name(self):
        fit_table = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column")
        self.assertEqual(fit_table, "fit_" + self.catalog.sdf_name + "_column_le")








class Test_InMemoryTableCatalog(unittest.TestCase):

    def setUp(self):
        self.dbconn = get_dbconn()
        self.catalog = InMemoryTableCatalog(self.dbconn, sdf_name, dataset_schema, dataset_table, fit_schema)
        
    def tearDown(self):
        self.dbconn.close()

    def test__repr__(self):
        repr = self.catalog.__repr__()
        self.assertGreater(len(repr), 0)

    def test_clone(self):
        catalog2 = self.catalog.clone()
        self.assertEqual(self.catalog.dbconn, catalog2.dbconn)
        self.assertEqual(self.catalog.sdf_name, catalog2.sdf_name)
        self.assertEqual(self.catalog.dataset_schema, catalog2.dataset_schema)
        self.assertEqual(self.catalog.dataset_table, catalog2.dataset_table)
        self.assertEqual(self.catalog.fit_schema, catalog2.fit_schema)
        
    def test_register_table(self):
        self.assertFalse(self.catalog.is_table_registered(dataset_schema, dataset_table))
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertTrue(self.catalog.is_table_registered(dataset_schema, dataset_table))
        
    def test_un_register_table(self):
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertTrue(self.catalog.is_table_registered(dataset_schema, dataset_table))
        self.catalog.un_register_table(dataset_schema, dataset_table)
        self.assertFalse(self.catalog.is_table_registered(dataset_schema, dataset_table))

    def test_get_list_of_tables_1(self):
        self.assertEqual(len(self.catalog.get_list_of_tables()), 0)
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertEqual(len(self.catalog.get_list_of_tables()), 1)
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertEqual(len(self.catalog.get_list_of_tables()), 1)
        
    def test_get_list_of_tables_2(self):
        self.catalog.register_table(dataset_schema, dataset_table)
        self.catalog.register_table(dataset_schema, dataset_table + "2")
        self.assertEqual(len(self.catalog.get_list_of_tables()), 2)
        
    def test_register_fit_table(self):
        fit_name = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column")
        self.catalog.register_fit_table(SqlLabelEncoder(), "column")
        self.assertTrue(self.catalog.is_table_registered(fit_schema, fit_name))

    def test_drop_fit_table(self):
        fit_name = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column")
        create_dummy_table(self.dbconn, fit_schema, fit_name)
        self.catalog.register_fit_table(SqlLabelEncoder(), "column")
        self.catalog.drop_fit_table(SqlLabelEncoder(), "column")
        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name))
        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name))
        
    def test_drop_temporary_tables(self):

        fit_name1 = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column1")
        fit_name2 = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column1")

        create_dummy_table(self.dbconn, fit_schema, fit_name1)
        create_dummy_table(self.dbconn, fit_schema, fit_name2)

        self.catalog.register_fit_table(SqlLabelEncoder(), "column1")
        self.catalog.register_fit_table(SqlLabelEncoder(), "column2")

        self.catalog.drop_temporary_tables()

        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name1))
        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name2))

        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name1))
        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name2))








class Test_InDbTableCatalog(unittest.TestCase):

    def setUp(self):
        self.dbconn = get_dbconn()
        self.catalog = InDbTableCatalog(self.dbconn, sdf_name, dataset_schema, dataset_table, fit_schema, catalog_schema, **catalog_kwargs)
        self.catalog.drop_catalog_table()
        
    def tearDown(self):
        self.catalog.drop_catalog_table()
        self.dbconn.close()
        
    def test__repr__(self):
        repr = self.catalog.__repr__()
        self.assertGreater(len(repr), 0)

    def test_clone(self):
        catalog2 = self.catalog.clone()
        self.assertEqual(self.catalog.dbconn, catalog2.dbconn)
        self.assertEqual(self.catalog.sdf_name, catalog2.sdf_name)
        self.assertEqual(self.catalog.dataset_schema, catalog2.dataset_schema)
        self.assertEqual(self.catalog.dataset_table, catalog2.dataset_table)
        self.assertEqual(self.catalog.fit_schema, catalog2.fit_schema)
        self.assertEqual(self.catalog.catalog_schema, catalog2.catalog_schema)
        self.assertEqual(self.catalog.kwargs, catalog2.kwargs)
        
    def test_register_table(self):
        self.assertFalse(self.catalog.is_table_registered(dataset_schema, dataset_table))
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertTrue(self.catalog.is_table_registered(dataset_schema, dataset_table))
        
    def test_un_register_table(self):
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertTrue(self.catalog.is_table_registered(dataset_schema, dataset_table))
        self.catalog.un_register_table(dataset_schema, dataset_table)
        self.assertFalse(self.catalog.is_table_registered(dataset_schema, dataset_table))

    def test_get_list_of_tables_1(self):
        self.assertEqual(len(self.catalog.get_list_of_tables()), 0)
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertEqual(len(self.catalog.get_list_of_tables()), 1)
        self.catalog.register_table(dataset_schema, dataset_table)
        self.assertEqual(len(self.catalog.get_list_of_tables()), 1)
        
    def test_get_list_of_tables_2(self):
        self.catalog.register_table(dataset_schema, dataset_table)
        self.catalog.register_table(dataset_schema, dataset_table + "2")
        self.assertEqual(len(self.catalog.get_list_of_tables()), 2)
        
    def test_register_fit_table(self):
        fit_name = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column")
        self.catalog.register_fit_table(SqlLabelEncoder(), "column")
        self.assertTrue(self.catalog.is_table_registered(fit_schema, fit_name))

    def test_drop_fit_table(self):
        fit_name = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column")
        create_dummy_table(self.dbconn, fit_schema, fit_name)
        self.catalog.register_fit_table(SqlLabelEncoder(), "column")
        self.catalog.drop_fit_table(SqlLabelEncoder(), "column")
        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name))
        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name))
        
    def test_drop_temporary_tables(self):

        fit_name1 = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column1")
        fit_name2 = self.catalog.get_fit_table_name(SqlLabelEncoder(), "column1")

        create_dummy_table(self.dbconn, fit_schema, fit_name1)
        create_dummy_table(self.dbconn, fit_schema, fit_name2)

        self.catalog.register_fit_table(SqlLabelEncoder(), "column1")
        self.catalog.register_fit_table(SqlLabelEncoder(), "column2")

        self.catalog.drop_temporary_tables()

        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name1))
        self.assertFalse(self.catalog.is_table_registered(fit_schema, fit_name2))

        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name1))
        self.assertFalse(self.dbconn.table_exists(fit_schema, fit_name2))





# ordering?? everywhere 

    ## use this for class with sdf_query_data_source
    #clone_as_sql_source

# sdf_query_data_source

class Test_SqlDataFrame_table(unittest.TestCase):

    def setUp(self):
        self.test_df = get_test_df()
        self.dbconn = get_dbconn()
        self.dbconn.upload_df_to_db(self.test_df, dataset_schema, dataset_table)
        self.sdf = self.dbconn.get_sdf_for_table(sdf_name, dataset_schema, dataset_table, key_column, fit_schema, default_order_by)
        
    def tearDown(self):
        self.dbconn.close()
        
    def test__repr__(self):
        repr = self.sdf.__repr__()
        self.assertGreater(len(repr), 0)

    def test_clone(self):
        sdf2 = self.sdf.clone()
        self.assertEqual(self.catalog.dbconn, sdf2.dbconn)
        self.assertEqual(self.catalog.catalog, sdf2.catalog)
        self.assertEqual(self.catalog.sdf_name, sdf2.sdf_name)
        self.assertEqual(self.catalog.sdf_query_data_source, sdf2.sdf_query_data_source)
        self.assertEqual(self.catalog.dataset_schema, sdf2.dataset_schema)
        self.assertEqual(self.catalog.dataset_table, sdf2.dataset_table)
        self.assertEqual(self.catalog.key_column, sdf2.key_column)
        self.assertEqual(self.catalog.fit_schema, sdf2.fit_schema)
        self.assertEqual(self.catalog.default_order_by, sdf2.default_order_by)

    def test_get_table_head(self):
        df2 = self.sdf.get_table_head()
        self.assertEqual(self.test_df.shape[1], df2.shape[1])
        self.assertEqual(df2.shape[0], 5)

        df2 = self.sdf.get_table_head(limit=10)
        self.assertEqual(self.test_df.shape[1], df2.shape[1])
        self.assertEqual(df2.shape[0], 10)

    def test_info(self):
        info_schema = self.sdf.info()
        # row in schema for each column
        self.assertEqual(self.test_df.shape[1], info_schema.shape[0])

    def test_get_table_size(self):
        size = self.sdf.get_table_size()
        self.assertEqual(self.test_df.shape[0], size)

    def test_shape(self):
        shape = self.sdf.shape()
        self.assertEqual(self.test_df.shape, shape)

    def test_add_unique_id_column(self):
        self.assertFalse(self.dbconn.column_exists(dataset_schema, dataset_table, uniue_key_column))
        self.assertNotEqual(self.sdf.key_column, uniue_key_column)
        self.sdf.add_unique_id_column(uniue_key_column)
        df_schema = self.dbconn.get_table_schema(dataset_schema, dataset_table)
        self.assertTrue(self.dbconn.column_exists(dataset_schema, dataset_table, uniue_key_column))
        self.assertEqual(self.sdf.key_column, uniue_key_column)

    def test_get_table_column_df_all(self):
        df1 = self.sdf.get_table_column_df(key_column, return_df=True)
        self.assertEqual(self.test_df.shape[0], df1.shape[0])

    introduce function which will put double quotues around all column names - but only if needed i.e. for postgres but not for db2
    which effectively means its goinig to be case sensistive 
    #https://lerner.co.il/2013/11/30/quoting-postgresql/

    def test_get_table_column_df_limit(self):
        df1 = self.sdf.get_table_column_df(key_column, limit=100, return_df=True)
        self.assertEqual(10, df1.shape[0])

#if __name__ == '__main__':
#    unittest.main()
