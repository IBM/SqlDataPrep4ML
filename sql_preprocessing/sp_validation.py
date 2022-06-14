
#####################################################################################
# Testign and Validation of SQL functions
# Comparison of results between SKLearn and SQL Data Preprocessing
#####################################################################################




import scipy
import time
import pandas as pd
import numpy as np
from decimal import *


'''

# Step 3 - pre processing on DataFrame
from sklearn.preprocessing import LabelBinarizer
lb = LabelBinarizer()
lb.fit(df["code_4_pass1"].astype(str))
lb_output = lb.transform(df['code_4_pass1'].astype(str))
#print(lb_output.toarray())
#lb_output = lb_output[0]
print(lb_output)


# Step 4 - pre processing on Sql DataFrame

lb = SqlLabelBinarizer()
lb.fit(sdf, 'code_4_pass1')
lb.transform(sdf, 'code_4_pass1')

sql = sdf.generate_sql(True)
#dbconn.execute_query_print_table(sql)
db_df = pd.read_sql_query(sql, engine)
db_df['xxx'] = lb_output
#print(db_df['code_4_pass1_m'].values)
#print(db_df.as_matrix(columns=["code_4_pass1_m"]))

db_df['xxxx'] = np.where(db_df['code_4_pass1_f'] == db_df['xxx'], True, False)


print(db_df)

print(db_df.loc[db_df['xxxx'] == False])
## ordering !!!! sort both in the same way!!!
a = 1
'''





class ExecutionTimer:

    def start(self):
        self.start_time = time.time()

    def end(self):
        self.duration = time.time() - self.start_time
        
    def get_duration(self):
        return self.duration if self.duration != None else 0


class ComparisonTimer:

    def __init__ (self):
        self.fit_f1 = ExecutionTimer()
        self.transform_f1 = ExecutionTimer()
        self.load_f1 = ExecutionTimer()
        self.fit_f2 = ExecutionTimer()
        self.transform_f2 = ExecutionTimer()
        self.load_f2 = ExecutionTimer()


def compare_functions_noargs (sklearn_function, sql_function, function_name, csv_file, sdf, column, compare_values, test_name, compare_tolerance, compare_order_by, results):

    print('\n--------------------------------------------------------')
    print('Compare Function: ' + function_name + ' - column: ' + column + ' - compare_values: ' + str(compare_values) + ' - test: ' + test_name)
    print(sklearn_function)
    print(sql_function)

    timer = ComparisonTimer()

    # sklearn.preprocessing function
    timer.load_f1.start()
    df = pd.read_csv(csv_file)
    timer.load_f1.end()

    timer.fit_f1.start()
    sklearn_function.fit(df[[column]])
    timer.fit_f1.end()

    timer.transform_f1.start()
    skl_output = sklearn_function.transform(df[[column]])
    timer.transform_f1.end()
    
    # sql function    
    timer.fit_f2.start()
    sql_function.fit(sdf, column)
    timer.fit_f2.end()
    
    timer.transform_f2.start()
    sql_function.transform(sdf, column)
    
    if (compare_values):
        timer.load_f2.start()
        sql_output = sdf.execute_df()
        timer.load_f2.end()
    else:
        sdf.excute_transform_only()
        
    timer.transform_f2.end()

    # compare results (if data are retrieved from db)
    if (compare_values):
        sql_output = sdf.execute_df(order_by=compare_order_by)
        compare_arrays(skl_output, sql_output, compare_tolerance)

    results.add_record(function_name, test_name, timer)



def compare_arrays(sklearn_array, sql_array, compare_tolerance):

    print('Sklearn array  - size: ' + str(sklearn_array.size) + ' shape: ' + str(sklearn_array.shape))
    print('SQL array      - size: ' + str(sql_array.size) + ' shape: ' + str(sql_array.shape))

    # some sklearn functions return vector and not 1d array as most of others, fix that here 
    if (len(sklearn_array.shape) == 1):
        sklearn_array = sklearn_array.reshape((-1,1))

    # some sklearn functions return sparse matrix - need to convert to full array
    if (isinstance(sklearn_array, scipy.sparse.csr.csr_matrix)):
        sklearn_array = sklearn_array.toarray()
        print('Sklearn array  - size: ' + str(sklearn_array.size) + ' shape: ' + str(sklearn_array.shape))

    '''
    # to compare results - floats need to be rounded since on some decimal positions there might be insignifnicant difference which would result into no match
    if (compare_round_decimals is not None):
        sklearn_array = numpy.around(sklearn_array, compare_round_decimals)
        sql_array = numpy.around(sql_array, compare_round_decimals)
    
    # find if arrays are equal
    arrays_equal = np.array_equal(sklearn_array, sql_array)
    print('Arrays are equal     : ' + str(arrays_equal))
    '''

    # check if shapes are the same
    if (sklearn_array.shape != sql_array.shape):
        print('Shapes do not match')
        return False


    # comparison of two arrays - this appraoch does not require much of rounding
    arrays_equal = np.allclose(sklearn_array, sql_array, atol=compare_tolerance)
    print('Arrays are allclose     : ' + str(arrays_equal))

    if (not arrays_equal):
        print('Difference sklearn/sql (first 5)')

        n = 1

        print(sklearn_array)
        print(sql_array)

        for y in range(sklearn_array.shape[0]):
            for x in range(sklearn_array.shape[1]):

                value_diff = (abs(sklearn_array[y,x] - sql_array[y,x]) > compare_tolerance)

                if (value_diff):
                    print('[' + str(y) + ',' + str(x) + '] ' +  str(sklearn_array[y,x]) + ' != ' + str(sql_array[y,x]) + ' diff ' + str(abs(sklearn_array[y,x] - sql_array[y,x])))
                    n = n + 1
                    if (n > 5): break
            if (n > 5): break

    return arrays_equal


class ComparisonArray:

    def __init__(self):
        self.records = []

    def add_record(self, function, test, timer):
        record = [
            function, 
            test, 
            timer.fit_f1.get_duration(), 
            timer.transform_f1.get_duration(), 
            timer.load_f1.get_duration(), 
            timer.fit_f2.get_duration(), 
            timer.transform_f2.get_duration(), 
            timer.load_f2.get_duration()]

        self.records.append(record)

    def print_table(self):
        print('--------------------------------------------------------')
        print(' Comparison results')
        print('--------------------------------------------------------')
        print('Function             | Test                 | skl-fit | db-fit  | diff    | skl-tra | db-tra  | diff    | skl_loa | db_load | diff    | skl-tot | db-tot  | diff    |')

        format_val = lambda v : ' | ' + (str(round(v, 5)) + ' ' * 7)[:7]
        format_two_vals = lambda v1, v2 : format_val(v1) + format_val(v2) + format_val(v1 - v2)

        for record in self.records:
            function = (record[0] + ' ' * 20)[:20]
            test = (record[1] + ' ' * 20)[:20]
            m1 = format_two_vals(record[2], record[5])
            m2 = format_two_vals(record[3], record[6])
            m3 = format_two_vals(record[4], record[7])
            m4 = format_two_vals(record[2]+record[3]+record[4], record[5]+record[6]+record[7])
            print(function + ' | ' + test + m1 + m2 + m3 + m4 + ' |')

    


# Step 1 - load data from CSV into DataFrame

#df = pd.read_csv('simulated_dataset.csv')
#print(df.head())
'''
dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1")
sdf = dbconn.get_sdf_for_query("(select * from s1.simulated_dataset order by index)", key_column="index", fit_df_name="simulated_dataset", fit_db_schema="s1")

csv_file = 'simulated_dataset.csv'
'''

# Step 2 - reshape DF as needed
'''
import numpy as np
from sklearn.impute import SimpleImputer

imp = SimpleImputer(missing_values=np.nan, strategy='constant', fill_value="F")
imp.fit(df["code_4_pass1"].values.reshape(1, -1))
arr = imp.transform(df["code_4_pass1"].values.reshape(1, -1))
df["code_4_pass1_i"] = arr[0]
'''

# Step 3 - store DF into DB
#dbconn.upload_df_to_db(df, 's1', 'simulated_dataset')

# Step 4 - compare preprocessing functions


'''
mapper = SqlDataFrameMapper([
    ('code_20', SqlLabelEncoder()),
    ('code_21', SqlMinMaxScaler()),
    ('code_22', SqlMaxAbsScaler()),
    ('code_23', SqlBinarizer(5)),
    ('code_24', SqlBinarizer(0)),
    ('code_25', SqlStandardScaler())
])

mapper.fit(sdf)
sql = mapper.transform(sdf)
dbconn.execute_query_print_table(sql)
'''


def compare_mappers_noargs (sklearn_mapper, sql_mapper, csv_file, sdf, compare_values, test_name, compare_tolerance, compare_order_by, results):

    print('--------------------------------------------------------')
    print('Mapper comparison - compare_values: ' + str(compare_values) + ' - test: ' + test_name)
    print(sklearn_mapper)
    print(sql_mapper)
    

    timer = ComparisonTimer()

    # sklearn.preprocessing. function
    timer.load_f1.start()
    df = pd.read_csv(csv_file)
    timer.load_f1.end()

    timer.fit_f1.start()
    sklearn_mapper.fit(df)
    timer.fit_f1.end()

    timer.transform_f1.start()
    skl_output = sklearn_mapper.transform(df)
    timer.transform_f1.end()
    
    # SQL function    
    timer.fit_f2.start()
    sql_mapper.fit(sdf)
    timer.fit_f2.end()
    
    timer.transform_f2.start()
    sql_mapper.transform(sdf)
    
    if (compare_values):
        timer.load_f2.start()
        sql_output = sdf.execute_df()
        timer.load_f2.end()
    else:
        sdf.excute_transform_only()
        
    timer.transform_f2.end()

    # compare results (if data are retrieved from db)
    if (compare_values):
        sql_output = sdf.execute_df(order_by=compare_order_by)
        compare_arrays(skl_output, sql_output, compare_tolerance)

    results.add_record('Mapper', test_name, timer)



