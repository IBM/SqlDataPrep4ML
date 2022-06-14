from sql_preprocessing import *
import pandas as pd
from time import time

# # Classification with SQL Pushdown

def perform_test(conn_str, in_schema = "DATA", working_schema = "TEMP", in_table_name = "TITANIC", ibm_db_conn_str=''):

    #test variables
    sdf_name = 'titanic_pt'
    dataset_schema = in_schema
    dataset_table = in_table_name
    key_column = 'PASSENGERID'
    catalog_schema = in_schema
    fit_schema = working_schema
    default_order_by = None
    db_args = {}


    dbconn = SqlConnection(conn_str, print_sql=False)

    sdf = dbconn.get_sdf_for_table(sdf_name, dataset_schema, dataset_table, key_column, catalog_schema, fit_schema, default_order_by, **db_args)

    # If the model stays in database even after calling IDAX.DROP_MODEL
    # it is dropped here in case it still exists
    try:
        dbconn.execute_command("CALL IDAX.DROP_MODEL('model=TEMP.titanic_pt')")
    except:
        print("model drop err")

    """
    sql = "select * from TEMP.T_TRAIN_CLEANED \n"
    sql += "except\n"
    sql += "select * from DATA.titanic_pt_fitted_train"
    
    dbconn.execute_query_print_table(sql)
    """

    #return

    # Splitting Data into Training, Validation, and Test Set<a class="anchor" id="Split-Data"></a>
    t0=time()

    # ___________________________________________________________________________________________________________________
    # ## Train-Test Split
    split_t0=time()

    train_sdf, test_sdf = sdf.train_test_split(test_size=0.2, random_state=1)
    #sdf.dbcatalog.drop_temporary_tables()

    split_t1=time()

    # ___________________________________________________________________________________________________________________
    # Data Transformation
    transform_t0=time()
    
    preprocessor_l1 = SqlColumnTransformer(
    transformers=[
        ('l1_passengerid', SqlPassthroughColumn(), 'passengerid'),
        ('l1_survived', SqlPassthroughColumn(), 'survived'),
        ('l1_pclass', SqlPassthroughColumn(), 'pclass'),
        ('l1_sex', SqlPassthroughColumn(), 'sex'),
        ('l1_age', SqlSimpleImputer(strategy="mean", cast_as='int', target_column='age'), 'age'),
        ('l1_fare', SqlPassthroughColumn(), 'fare'),
        ('l1_embarked', SqlSimpleImputer(strategy="constant", fill_value="S", target_column='embarked'), 'embarked'),
        ('l1_sibsp', SqlPassthroughColumn(), 'sibsp'),
        ('l1_parch', SqlPassthroughColumn(), 'parch'),
    ])

    preprocessor_l2 = SqlColumnTransformer(
        transformers=[
            ('l2_passengerid', SqlPassthroughColumn(), 'passengerid'),
            ('l2_survived', SqlPassthroughColumn(), 'survived'),
            ('l2_pclass', SqlPassthroughColumn(), 'pclass'),
            ('l2_sex', SqlPassthroughColumn(), 'sex'),
            ('l2_age', SqlStandardScaler(target_column='age'), 'age'),
            ('l2_fare', SqlKBinsDiscretizer(n_bins=5, target_column='fare'), 'fare'),
            ('l2_embarked', SqlPassthroughColumn(), 'embarked'),
            ('l2_sibsp', SqlPassthroughColumn(), 'sibsp'),
            ('l2_parch', SqlPassthroughColumn(), 'parch'),
    ])

    model=IDAXDecTree(sdf=sdf, column_target='SURVIVED', column_in='SURVIVED:nom;PCLASS:nom')

    #model.drop_model(sdf)

    pipeline = IDAXNestedPipeline(steps=[('preprocessor_l1', preprocessor_l1),
                                    ('preprocessor_l2', preprocessor_l2)],
                                    model=model)

 
    # training set 
    train_intable_table = train_sdf.sdf_name + "_fitted_train"

    #fit transformers
    train_sdf = pipeline.nested_sql_fit_transform(train_sdf)

    #create input table
    train_sdf.execute_transform_to_table(train_sdf.fit_schema, train_intable_table)

    transform_t1=time()

    # ___________________________________________________________________________________________________________________
    # Model Training

    train_t0=time()
    pipeline.model.fit(train_sdf, train_sdf.fit_schema, train_intable_table)
    train_t1=time()

    # ___________________________________________________________________________________________________________________
    # Testing Set Transform
    transform_t3=time()
    # testing set 
    test_intable_table = test_sdf.sdf_name + "_fitted_test"
    test_outtable_name = test_sdf.sdf_name + "_predictions_test"
   
    #generate transformation sql
    test_sdf = pipeline.transform(test_sdf)
    #create intput table
    test_sdf.execute_transform_to_table(test_sdf.fit_schema, test_intable_table)

    transform_t4=time()
    
    #___________________________________________________________________________________________________________________
    # Predict on Test Set

    # Predict again on Test Set
    predict_t0=time()
    pipeline.model.predict(test_sdf, test_sdf.fit_schema, test_intable_table, test_sdf.fit_schema, test_outtable_name)
    predict_t1=time()

    # ___________________________________________________________________________________________________________________
    # Close DB Connection
    


    # clean up tables
    cleanup0=time()
    sdf.dbcatalog.drop_temporary_tables()
    model.drop_model(sdf)
    cleanup1=time()
    
    # Stop Clock
    t1=time()

    # compute time
    tot_time=t1-t0
    predict_time=predict_t1-predict_t0
    train_time=train_t1-train_t0
    transform_time=(transform_t1-transform_t0) + (transform_t4-transform_t3)
    split_time=split_t1-split_t0
    cleanup_time=cleanup1-cleanup0

    test_results = {"test_name":"sql_pushdown" , "dataset_name":dataset_table, "data_retrive": 0.0, "split": split_time, "transform": transform_time, "train": train_time, "predict": predict_time, "cleanup":cleanup_time, "total":tot_time}

    return test_results

if __name__ == "__main__":
    result = perform_test("db2+ibm_db://MLP:x@x:50000/in_db")
    print(result)


