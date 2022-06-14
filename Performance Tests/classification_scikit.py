#!/usr/bin/env python
# coding: utf-8

# # Classification with sklearn

# ## Imports

# Database connectivity
import ibm_db
import ibm_db_dbi

# warning suppresion
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

# sklearn imports
from sklearn import tree
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn import preprocessing
from sklearn import metrics

## calculate run time
from time import time

def perform_test(conn_str, in_schema = "DATA", working_schema = "TEMP", in_table_name = "TITANIC"):

    # start mesuring time delta
    total_t0 = time()

    # ___________________________________________________________________________________________________________________
    # ## Extract data from db2

    export_t0 = time()
    # Import all data from database into memory
    ibm_db_conn = ibm_db.connect(conn_str,"","")
    conn = ibm_db_dbi.Connection(ibm_db_conn)
    sql = "SELECT * FROM " + in_schema + "." + in_table_name
    df = pd.read_sql(sql,conn)

    rc = ibm_db.close(ibm_db_conn)

    export_t1 = time()

    # ___________________________________________________________________________________________________________________
    # ## Train-Test Split

    transform_t0 = time()

    # drop columns 
    df.drop(['NAME', 'CABIN', "TICKET"], axis=1, inplace=True)

    split_time_t0 = time()
    X = df.loc[:, df.columns != 'SURVIVED']
    Y = df["SURVIVED"]

    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.20, random_state=1)
    split_time_t1 = time()

    # ___________________________________________________________________________________________________________________
    # ## Data Transformation

    # Missing values imputation
    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')

    X_train["AGE"] = X_train["AGE"].fillna(X_train["AGE"].mean())
    X_test["AGE"] = X_test["AGE"].fillna(X_train["AGE"].mean())

    X_train["EMBARKED"] = X_train["EMBARKED"].fillna('S')
    X_test["EMBARKED"] = X_test["EMBARKED"].fillna('S')


    # Standardization
    std_scaler = preprocessing.StandardScaler()

    X_train["AGE"] = std_scaler.fit_transform(X_train["AGE"].values.reshape(-1, 1))
    X_test["AGE"] = std_scaler.transform(X_test["AGE"].values.reshape(-1, 1))

    X_train["FARE"] = std_scaler.fit_transform(X_train["FARE"].values.reshape(-1, 1))
    X_test["FARE"] = std_scaler.transform(X_test["FARE"].values.reshape(-1, 1))

    # discretization FARE 
    X_train.loc[ X_train['FARE'] <= 7.85, 'FARE'] = 0
    X_train.loc[(X_train['FARE'] <= 10.50) & (X_train['FARE'] <= 14.454), 'FARE'] = 1
    X_train.loc[(X_train['FARE'] <= 21.07) & (X_train['FARE'] <= 31), 'FARE']   = 2
    X_train.loc[ X_train['FARE'] <= 39.69, 'FARE'] = 3
    X_train.loc[ X_train['FARE'] > 39.69, 'FARE'] = 4
    X_train['FARE'] = X_train['FARE'].astype(int)

    X_test.loc[ X_test['FARE'] <= 7.85, 'FARE'] = 0
    X_test.loc[(X_test['FARE'] <= 10.50) & (X_test['FARE'] <= 14.454), 'FARE'] = 1
    X_test.loc[(X_test['FARE'] <= 21.07) & (X_test['FARE'] <= 31), 'FARE']   = 2
    X_test.loc[ X_test['FARE'] <= 39.69, 'FARE'] = 3
    X_test.loc[ X_test['FARE'] > 39.69, 'FARE'] = 4
    X_test['FARE'] = X_test['FARE'].astype(int)


    # this is not required for IDAX.DECTREE since it automatically handles non-numeric values
    # but sklearn.DecisionTreeClassifier() requiers all values to be numeric, so the converstion is done here
    le = preprocessing.LabelEncoder()

    X_train["SEX"] = le.fit_transform(X_train["SEX"])
    X_test["SEX"] = le.transform(X_test["SEX"])

    X_train["EMBARKED"] = le.fit_transform(X_train["EMBARKED"])
    X_test["EMBARKED"] = le.transform(X_test["EMBARKED"])

    transform_t1 = time()


    # ___________________________________________________________________________________________________________________
    # ## Model Training

    # Train a Decision Tree Classifier

    train_t0 = time()

    clf = tree.DecisionTreeClassifier(max_depth=10, min_samples_split = 50, 
                                      min_impurity_decrease = 0.02, criterion = "entropy") 
    clf = clf.fit(X_train, y_train)

    train_t1 = time()

    #___________________________________________________________________________________________________________________
    # ## Model Prediction
    predict_t0 = time()
    result = clf.predict(X_test)
    predict_t1 = time()

    # stop mesuring time delta
    total_t1 = time()

    time_to_export_data = (export_t1 - export_t0) 
    time_split = (split_time_t1 - split_time_t0)
    time_train_model = (train_t1 - train_t0)
    time_transform = (transform_t1 - transform_t0)
    time_predict = (predict_t1 - predict_t0)
    total_time = (total_t1-total_t0)

    test_results = {"test_name":"scikit" , "dataset_name":in_table_name, "data_retrive": time_to_export_data, "split": time_split, "transform": time_transform, "train": time_train_model, "predict": time_predict, "cleanup":0, "total":total_time}

    return test_results

    # ___________________________________________________________________________________________________________________
    # # ## Model Evaluation

    # metrics.accuracy_score(y_test, result)


    # # Create confusion matrix
    # c_mtx = metrics.confusion_matrix(y_test, result)
    # sns.heatmap(c_mtx, annot = True, fmt = "0.2f");


    # #clf.cost_complexity_pruning_path(X_train, y_train)


    # print(metrics.classification_report(y_test, result))


if __name__ == "__main__":
    conn_str = "DATABASE=in_db;"
    conn_str += "HOSTNAME=x;"          
    conn_str += "PROTOCOL=TCPIP;"  
    conn_str += "PORT=50000;"
    conn_str += "UID=MLP;"
    conn_str += "PWD=x;"

    result_df = pd.DataFrame(columns = ["dataset_size", "data_retrive", "split", "transform", "train", "predict", "total"])

    result_df.append(perform_test(conn_str), ignore_index=True)

    print(result_df)

    # print("Time breakdown:")
    # print("\t Retrieve data from db2 = " + str(round(time_to_export_data, 3)) + " seconds")
    # print("\t Spliting data = " + str(round(time_split, 3)) + " seconds")
    # print("\t Preprocessing data = " + str(round(time_transform, 3)) + " seconds")
    # print("\t Training model = " + str(round(time_train_model, 3)) + " seconds")
    # print("\t Predicting on trained model = " + str(round(time_predict, 3)) + " seconds")
    # print("total time = " + str(round(total_time, 3)) + " seconds")

# ## SciKit Learn df size limit

# Traceback (most recent call last):
#   File "joint_performance_test.py", line 73, in <module>
#     result_df = main(ibm_db_conn_str, sql_alchemy_conn_str, data_schema, working_schema, itterations, file_name)
#   File "joint_performance_test.py", line 33, in main
#     result_df = result_df.append(scikit_test.perform_test(ibm_db_conn_str, data_schema, working_schema, table), ignore_index=True)
#   File "/home/db2inst1/testing_scripts/classification_scikit.py", line 42, in perform_test
#     df = pd.read_sql(sql,conn)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/io/sql.py", line 412, in read_sql
#     chunksize=chunksize,
#   File "/usr/local/lib64/python3.6/site-packages/pandas/io/sql.py", line 1654, in read_query
#     parse_dates=parse_dates,
#   File "/usr/local/lib64/python3.6/site-packages/pandas/io/sql.py", line 124, in _wrap_result
#     frame = DataFrame.from_records(data, columns=columns, coerce_float=coerce_float)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/frame.py", line 1676, in from_records
#     mgr = arrays_to_mgr(arrays, arr_columns, result_index, columns)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/internals/construction.py", line 74, in arrays_to_mgr
#     return create_block_manager_from_arrays(arrays, arr_names, axes)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/internals/managers.py", line 1670, in create_block_manager_from_arrays
#     blocks = form_blocks(arrays, names, axes)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/internals/managers.py", line 1757, in form_blocks
#     object_blocks = _simple_blockify(items_dict["ObjectBlock"], np.object_)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/internals/managers.py", line 1801, in _simple_blockify
#     values, placement = _stack_arrays(tuples, dtype)
#   File "/usr/local/lib64/python3.6/site-packages/pandas/core/internals/managers.py", line 1848, in _stack_arrays
#     stacked = np.empty(shape, dtype=dtype)
# MemoryError: Unable to allocate 763. MiB for an array with shape (5, 20000000) and data type object



# echo 1 > /proc/sys/vm/overcommit_memory
# [db2inst1@soloed1 testing_scripts]$ python3 joint_performance_test.py
# Number of iterations to run: 1
# Name of file: scikit_test_20M
# Itteration: 0/1
#         starting test for TITANIC_20M
#                 scikit_test
# Killed





