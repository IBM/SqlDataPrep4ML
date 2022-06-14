#!/usr/bin/env python
# coding: utf-8

import ibm_db
import ibm_db_dbi

import pandas as pd

# tests

import classification_sp as sp_test
import classification_scikit as scikit_test
import classification_sql_pushdown as sql_pushdown_test


def main(ibm_db_conn_str, sql_alchemy_conn_str, data_schema = "DATA", working_schema = "TEMP", test_iterations = 3, file_name = "scikit_vs_sp_test_results.csv"):
    
    result_df = pd.DataFrame(columns = ["test_name", "dataset_name", "data_retrive", "split", "transform", "train", "predict", "cleanup", "total"])

    #test_tables =  ["TITANIC"] # , 'TITANIC_10K', "TITANIC_100K", "TITANIC_500K", "TITANIC_1M", "TITANIC_5M", "TITANIC_10M", "TITANIC_20M", "TITANIC_30M"] 
    #test_tables =  ["TITANIC_500K", "TITANIC_1M", "TITANIC_5M"] #, "TITANIC_10M", "TITANIC_20M", "TITANIC_30M"] 
    test_tables =  ["TITANIC_10M"]

    for i in range(0, test_iterations):
        print("Itteration: " + str(i) + "/" + str(test_iterations))

        for table in test_tables:
            print("\tstarting test for " + table)

            # in_db test
            print("\t\tsp_test")
            result_df = result_df.append(sp_test.perform_test(ibm_db_conn_str, data_schema, working_schema, table), ignore_index=True)

            # sci_kit test
            #print("\t\tscikit_test")
            #result_df = result_df.append(scikit_test.perform_test(ibm_db_conn_str, data_schema, working_schema, table), ignore_index=True)

            # sql_pushdown test
            print("\t\tsql_pushdown_test")
            result_df = result_df.append(sql_pushdown_test.perform_test(sql_alchemy_conn_str, data_schema, working_schema, table, ibm_db_conn_str), ignore_index=True)

            # print results to table after each table
            result_df.to_csv(file_name, index=False)
            print("\tResult written to " + file_name)
            print(result_df)

    print("Test Complete")

    result_df.to_csv(file_name, index=False)
    print("Result written to " + file_name)

    return result_df

if __name__ == "__main__":
    DATABASE= "in_db"
    HOSTNAME = "x"
    PROTOCOL= "TCPIP"  
    PORT="50000"
    UID="MLP"
    PWD="x"

    ibm_db_conn_str = "DATABASE=" + DATABASE + ";"
    ibm_db_conn_str += "HOSTNAME=" + HOSTNAME + ";"
    ibm_db_conn_str += "PROTOCOL=" + PROTOCOL + ";"
    ibm_db_conn_str += "PORT=" + PORT + ";"
    ibm_db_conn_str += "UID=" + UID + ";"
    ibm_db_conn_str += "PWD=" + PWD + ";"

    sql_alchemy_conn_str = "db2+ibm_db://" + UID + ":" + PWD + "@" + HOSTNAME + ":" + PORT + "/" + DATABASE

    data_schema = "DATA"
    working_schema = "TEMP"

    itterations = 3
    file_name = "test.csv"

    #itterations = int(input("Number of iterations to run: "))
    #file_name = input("Name of file: ") + ".csv"

    result_df = main(ibm_db_conn_str, sql_alchemy_conn_str, data_schema, working_schema, itterations, file_name)
    
    print(result_df)

