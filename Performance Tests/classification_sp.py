#!/usr/bin/env python
# coding: utf-8

# # Classification with Db2 Warehouse Stored Procedures

# ## Table of Contents:
# * [1. Introduction](#Introduction)
# * [2. Imports](#Imports)
# * [3. Connect to DB](#Connect-to-DB)
# * [4. Data Exploration](#EDA)
# * [5. Splitting Data into Training, Validation, and Test Sets](#Split-Data)
# * [6. Data Transformation](#Data-Transformation)
# * [7. Model Training](#Model-Training)
# * [8. Evaluate Unpruned on Training Set](#Eval-UP-Train)
# * [9. Evaluate Unpruned Model on Test Set](#Eval-UP-Test)
# * [10. Hyperparameter Tuning](#Hyperparam-Tuning)
# * [11. Evaluate Pruned Model on Training Set](#Eval-P-Train)
# * [12. Evaluate Pruned Model on Test Set](#Eval-P-Test)
# * [13. Conclusion](#Conclusion)

# ## 1. Introduction <a class="anchor" id="Introduction"></a>
# 
# NOTE: NO HYPERPARAM TUNING BEING DONE --> NO T_VAL

# ## 2. Imports <a class="anchor" id="Imports"></a>

# Database connectivity
import ibm_db
import ibm_db_dbi

import pandas as pd

from time import time

def drop_trained_models(conn_str, table_list, schema = "MODEL", print_error = False):
    # Delete all outstanding Tables
    ibm_db_conn = ibm_db.connect(conn_str,"","")
    conn = ibm_db_dbi.Connection(ibm_db_conn) 

    for table in table_list:
        sql= "CALL IDAX.DROP_MODEL('model="+ schema + "." + table + "')"
        try:
            stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
        except:
            if print_error: print(table +" removal error, skiping removal")
    
    rc = ibm_db.close(ibm_db_conn)

def remove_outstanding_tables(conn_str, schema, in_table, print_error=False):
    # Delete all outstanding Tables
    ibm_db_conn = ibm_db.connect(conn_str,"","")
    conn = ibm_db_dbi.Connection(ibm_db_conn) 

    #______________________________________________________________________________________________
    # drop trained models
    try:
            sql= "CALL IDAX.DROP_MODEL('model="+ schema +".titanic_dt')"
            stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
    except:
            if print_error: print("DROP_MODEL error, skiping removal")  
                
    try:
            sql= "DROP VIEW "+ schema +"."+ in_table +"_VIEW"
            stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    except:
            if print_error: print("DROP VIEW error, skiping removal")   
                
              
    #______________________________________________________________________________________________
    # drop any other table created in the schema
    get_tables_sql = "select NAME from sysibm.systables where CREATOR = '" + schema +"'"
    created_tables = pd.read_sql(get_tables_sql,conn).values.tolist()
    for table_list in created_tables:
        for table in table_list:
            sql= "DROP TABLE " + schema + "." + table
            try:
                stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
            except:
                if print_error: print(table +" removal error, skiping removal")
    rc = ibm_db.close(ibm_db_conn)


def perform_test(conn_str, in_data_schema = "DATA", in_working_schema = "TEMP", in_table_name = "TITANIC"):

    #test variables
    data_schema = in_data_schema
    working_schema = in_working_schema
    model_schema = in_working_schema #"MODEL"
    table_name = in_table_name


    full_table_name = data_schema + "." + table_name

    # initial table cleanup (just in case)
    remove_outstanding_tables(conn_str, working_schema, table_name)
    drop_trained_models(conn_str, [table_name], model_schema)


    ibm_db_conn = ibm_db.connect(conn_str,"","")
    conn = ibm_db_dbi.Connection(ibm_db_conn)

    # Splitting Data into Training, Validation, and Test Set<a class="anchor" id="Split-Data"></a>
    t0=time()

    # ___________________________________________________________________________________________________________________
    # ## Train-Test Split

    split_t0=time()

    # Create view
    sql= "CREATE VIEW " + working_schema + "." + table_name + "_VIEW "
    sql += "AS SELECT PASSENGERID, SURVIVED, PCLASS, SEX, AGE, SIBSP, PARCH, FARE, EMBARKED FROM "+ full_table_name
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # Train-test split
    sql= "CALL IDAX.SPLIT_DATA('intable=" + working_schema + "." + table_name + "_view ," 
    sql += "id=PASSENGERID, traintable=" + working_schema + "." + "T_TRAIN, testtable=" + working_schema + "." + "T_TEST, fraction=0.8, seed=1')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    split_t1=time()


    # ___________________________________________________________________________________________________________________
    # Data Transformation

    transform_t0=time()
    
    # Create T_STATS table that contains feature stats (mean, stdev, freq, etc)
    sql = "CALL IDAX.SUMMARY1000('intable=" + working_schema + "." + "T_TRAIN, outtable=" + working_schema + "." + "T_TRAIN_STATS')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # Missing values imputation for T_TRAIN
    sql = "CALL IDAX.IMPUTE_DATA('intable=" + working_schema + "." + "T_TRAIN,method=mean,inColumn=AGE')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    sql = "CALL IDAX.IMPUTE_DATA('intable=" + working_schema + "." + "T_TRAIN,method=replace,nominalValue=S,inColumn=EMBARKED')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)


    # **Missing values imputation**

    sql = "UPDATE " + working_schema + "." + "T_TEST"
    sql += " SET AGE = (SELECT AVERAGE FROM " + working_schema + "." + "T_TRAIN_STATS_NUM WHERE COLUMNNAME='AGE')"
    sql += " WHERE AGE IS NULL"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    sql = "UPDATE " + working_schema + "." + "T_TEST"
    sql += " SET EMBARKED = (SELECT MOSTFREQUENTVALUE FROM " + working_schema + "." + "T_TRAIN_STATS_CHAR WHERE COLNAME='EMBARKED')"
    sql += " WHERE EMBARKED IS NULL"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)


    # **Feature Standardization**

    # Standardization for T_TRAIN
    sql = "CALL IDAX.STD_NORM('intable=" + working_schema + "." + "T_TRAIN, id=PASSENGERID,"
    sql += "inColumn=SURVIVED:L;PCLASS:L;SEX:L;SIBSP:L;PARCH:L;EMBARKED:L;AGE:S;FARE:L,"
    sql += "outtable=" + working_schema + "." + "T_TRAIN_NORMED')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # Drop T_TRAIN
    sql= "DROP TABLE " + working_schema + "." + "T_TRAIN"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)


    sql = "UPDATE " + working_schema + "." + "T_TEST "
    sql += "SET AGE = ((CAST(AGE AS FLOAT) - (SELECT AVERAGE FROM " + working_schema + "." + "T_TRAIN_STATS_NUM WHERE COLUMNNAME='AGE'))/(SELECT STDDEV FROM " + working_schema + "." + "T_TRAIN_STATS_NUM WHERE COLUMNNAME='AGE'))"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # Rename AGE to STD_AGE for later comparison with T_TRAIN

    sql= "ALTER TABLE " + working_schema + "." + "T_TEST RENAME COLUMN AGE TO STD_AGE"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # **Discretization**

    # make timer
    # Create bins
    sql = "CALL IDAX.EFDISC('intable=" + working_schema + "." + "T_TRAIN_NORMED, inColumn=FARE:5,"
    sql += "outtable=" + working_schema + "." + "T_BTABLE')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # make timer
    # Apply discretization
    sql = "CALL IDAX.APPLY_DISC('intable=" + working_schema + "." + "T_TRAIN_NORMED, btable=" + working_schema + "." + "T_BTABLE, outtable=" + working_schema + "." + "T_TRAIN_CLEANED')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    sql = "CALL IDAX.APPLY_DISC('intable=" + working_schema + "." + "T_TEST, btable=" + working_schema + "." + "T_BTABLE, outtable=" + working_schema + "." + "T_TEST_CLEANED')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    # Drop T_TRAIN_NORMED, T_VAL_NORMED, T_TEST_NORMED

    sql= "DROP TABLE " + working_schema + "." + "T_TRAIN_NORMED"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)

    sql= "DROP TABLE " + working_schema + "." + "T_TEST"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)


    transform_t1=time()

    # ___________________________________________________________________________________________________________________
    # Model Training

    # Train a Decision Tree Classifier: https://www.ibm.com/support/knowledgecenter/SSCJDQ/com.ibm.swg.im.dashdb.analytics.doc/doc/r_decision_trees_build_model_procedure.html

    #wro(ibm_db_conn, "select count(*) from " + working_schema + "." + "T_TRAIN_CLEANED")
    #wro(ibm_db_conn, "select count(*) from " + working_schema + "." + "T_TEST_CLEANED")

    train_t0=time()
    # Train model --> unpruned DTC

    sql = "CALL IDAX.GROW_DECTREE('model=" + model_schema + "." + table_name + ", intable=" + working_schema + "." + "T_TRAIN_CLEANED, "
    sql += "id=PASSENGERID, target=SURVIVED, incolumn=SURVIVED:nom;PCLASS:nom')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
    train_t1=time()

    #___________________________________________________________________________________________________________________
    # Predict on Test Set

    # Predict again on Test Set
    predict_t0=time()
    sql = "CALL IDAX.PREDICT_DECTREE('model=" + model_schema + "." + table_name + ", intable=" + working_schema + "." + "T_TEST_CLEANED,outtable=" + working_schema + "." + "T_PRUNED_PREDICTIONS')"
    stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
    predict_t1=time()

    cleanup0=time()
    # clean up tables
    remove_outstanding_tables(conn_str, working_schema, table_name)
    drop_trained_models(conn_str, [table_name])
    cleanup1=time()

    # Stop Clock
    t1=time()


    # get Disk Space used
    # Disk_Space_df =  pd.DataFrame(columns = ["dataset_name", "COLUMNS", "DISCRETE_STATISTICS", "MODEL", "NODES", "PREDICATES", "total"])

    # ___________________________________________________________________________________________________________________
    # Close DB Connection
    rc = ibm_db.close(ibm_db_conn)

    # compute time
    tot_time=t1-t0
    predict_time=predict_t1-predict_t0
    train_time=train_t1-train_t0
    transform_time=transform_t1-transform_t0
    split_time=split_t1-split_t0
    cleanup_time=cleanup1-cleanup0

    test_results = {"test_name":"in_db" , "dataset_name":table_name, "data_retrive": 0.0, "split": split_time, "transform": transform_time, "train": train_time, "predict": predict_time, "cleanup":cleanup_time, "total":tot_time}

    #print("clean up time = " + str(clean_up1 - clean_up0))

    return test_results


def wro(conn, sql):
    stmt = ibm_db.exec_immediate(conn, sql)
    result = ibm_db.fetch_both(stmt)
    if( result ):
        print (result[0])
        

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

    # print("It took ",tot_time," s to run this notebook")

    # print("Time breakdown:")
    # print("\t Splitting data = " + str(round(split_time, 3)) + " seconds")
    # print("\t Data Transformation = " + str(round(transform_time, 3)) + " seconds")
    # print("\t Training model = " + str(round(train_time, 3)) + " seconds")
    # # print("\t Tuning model = " + str(round(tune_time, 3)) + " seconds")
    # print("\t Predicting = " + str(round(predict_time, 3)) + " seconds")
    # print("Total time = " + str(round(tot_time, 3)) + " seconds")



