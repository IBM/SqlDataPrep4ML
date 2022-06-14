






# Class: IDAXModel
# Base for all DB2/IDAX models
class IDAXModel():

    def __init__(self, sdf, model_schema, model_table, column_id, column_target, column_in):

        model_schema = model_schema if (model_schema != None) else sdf.fit_schema
        model_table = model_table if (model_table != None) else sdf.sdf_name
        column_id = column_id if (column_id != None) else sdf.key_column

        self.model_schema = model_schema
        self.model_table = model_table
        self.model_name = model_schema + "." + model_table
        self.column_id = column_id
        self.column_target = column_target
        self.column_in = column_in


    def __repr__(self):
        return "IDAXModel(model_name=%s, column_id=%s, column_target=%s, column_in=%s)" % (self.model_name, self.column_id, self.column_target, self.column_in)


    def fit(self):
        print("not to be used")


    def predict(self):
        print("not to be used")


    def score(self):
        print("not to be used")


    def drop_model(self, sdf):
        '''
                sql = "CALL IDAX.MODEL_EXISTS('model=%s')" % self.model_name
                row = sdf.dbconn.execute_query_onerow(sql)

                if row is not None and row[0] == 1:
                    sql= "CALL IDAX.DROP_MODEL('model=%s')" % self.model_name
                    sdf.dbconn.execute_command(sql)
        '''

        sql= "CALL IDAX.DROP_MODEL('model=%s')" % self.model_name
        sdf.dbconn.execute_command(sql)
        

    def build_confusion_matrix(self, sdf, intable_schema = None, intable_table = None, resulttable_schema = None, resulttable_table = None, matrixtable_schema = None, matrixtable_table = None):

        intable_schema = sdf.fit_schema if (intable_schema == None) else intable_schema
        intable_table = sdf.sdf_name + "_fitted" if (intable_table == None) else intable_table
        resulttable_schema = sdf.fit_schema if (resulttable_schema == None) else resulttable_schema
        resulttable_table = sdf.sdf_name + "_predictions" if (resulttable_table == None) else resulttable_table
        matrixtable_schema = sdf.fit_schema if (matrixtable_schema == None) else matrixtable_schema
        matrixtable_table = sdf.sdf_name + "_cm" if (matrixtable_table == None) else matrixtable_table

        intable = intable_schema + "." + intable_table
        resulttable = resulttable_schema + "." + resulttable_table
        matrixtable = matrixtable_schema + "." + matrixtable_table

        sdf.dbconn.drop_table(matrixtable_schema, matrixtable_table)        

        sql = "CALL IDAX.CONFUSION_MATRIX('intable=%s, id=%s, target=%s, resulttable=%s, matrixtable=%s')" % (intable, self.column_id, self.column_target, resulttable, matrixtable)
        sdf.dbconn.execute_command(sql)

        
    def get_confusion_matrix(self, sdf):

        matrixtable_schema = sdf.fit_schema
        matrixtable_table = sdf.sdf_name + "_cm"
        matrixtable = matrixtable_schema + "." + matrixtable_table

        sql = "SELECT CNT FROM " + matrixtable
        return sdf.dbconn.execute_sql_to_df(sql)


    def get_confusion_matrix_stats(self, sdf):
        
        matrixtable_schema = sdf.fit_schema
        matrixtable_table = sdf.sdf_name + "_cm"
        matrixtable = matrixtable_schema + "." + matrixtable_table

        sql = "CALL IDAX.CMATRIX_STATS('matrixtable=%s')" % (matrixtable)
        #return sdf.dbconn.execute_query_result(sql)
        
        #conn = sdf.dbconn.engine.connect()
        conn = sdf.dbconn.engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        results_one = cursor.fetchall()
        cursor.nextset()
        results_two = cursor.fetchall()
        cursor.close()

        #return sdf.dbconn.execute_sql_to_df(sql)

        return cursor



    def get_model_table(self, sdf):
        sql = "SELECT * FROM %s_MODEL" % self.model_name
        return sdf.dbconn.execute_sql_to_df(sql)


    def get_model_columns(self, sdf):
        sql = "SELECT * FROM %s_COLUMNS" % self.model_name
        return sdf.dbconn.execute_sql_to_df(sql)


    def get_model_predicates(self, sdf):
        sql = "SELECT * FROM %s_PREDICATES" % self.model_name
        return sdf.dbconn.execute_sql_to_df(sql)


    def get_model_nodes(self, sdf):
        sql = "SELECT * FROM %s_NODES" % self.model_name
        return sdf.dbconn.execute_sql_to_df(sql)


# end of class IDAXModel






# Class: IDAXDecTree
# https://www.ibm.com/support/producthub/db2w/docs/content/SSCJDQ/com.ibm.swg.im.dashdb.analytics.doc/doc/r_decision_trees_functions.html
class IDAXDecTree(IDAXModel):

    def __init__(self, sdf = None, model_schema = None, model_table = None, column_id = None, column_target = None, column_in = None):
        super(IDAXDecTree, self).__init__(sdf, model_schema, model_table, column_id, column_target, column_in)
        

    def __repr__(self):
        return "IDAXDecTree(model_name=%s, column_id=%s, column_target=%s, column_in=%s)" % (self.model_name, self.column_id, self.column_target, self.column_in)

 
    # https://www.ibm.com/support/producthub/db2w/docs/content/SSCJDQ/com.ibm.swg.im.dashdb.analytics.doc/doc/r_decision_trees_grow_procedure.html
    def fit(self, sdf, intable_schema, intable_table):

        intable = intable_schema + "." + intable_table

        # skip for now - need the exists function first
        #self.drop_model(sdf)

        sql = "CALL IDAX.GROW_DECTREE('model=%s, intable=%s, id=%s, target=%s, incolumn=%s')" % (self.model_name, intable, self.column_id, self.column_target, self.column_in)
        sdf.dbconn.execute_command(sql)
    

    # https://www.ibm.com/support/producthub/db2w/docs/content/SSCJDQ/com.ibm.swg.im.dashdb.analytics.doc/doc/r_decision_trees_predict_procedure.html
    def predict(self, sdf, intable_schema, intable_table, outtable_schema, outtable_table):
        
        intable = intable_schema + "." + intable_table
        outtable = outtable_schema + "." + outtable_table

        sdf.dbconn.drop_table(outtable_schema, outtable_table)        

        sql = "CALL IDAX.PREDICT_DECTREE('model=%s, intable=%s, outtable=%s')" % (self.model_name, intable, outtable)
        sdf.dbconn.execute_command(sql)

        sdf.dbcatalog.register_table(outtable_schema, outtable_table)



'''
https://www.ibm.com/support/producthub/db2w/docs/content/SSCJDQ/com.ibm.swg.im.dashdb.analytics.doc/doc/r_decision_trees_prune_procedure.html
sql = """CALL IDAX.PRUNE_DECTREE('model=TEMP.titanic_dt, valtable=TEMP.T_VAL_CLEANED')"""
stmt = ibm_db.exec_immediate(ibm_db_conn, sql)
'''

# end of class IDAXDecTree






# Class: IDAXNestedPipeline
# Allows for nested transformations where each ColumnTransnformer is nested into following ColumnTransformer
# sdf.sdf_name is used to generate table names (i.e. intable and outtable)
# All tables are created in the sdf.fit_schema
class IDAXNestedPipeline():


    # self.steps [name, transformer]

    def __init__(self, steps, model):
        self.steps = steps
        self.model = model


    def __repr__(self):

        step_list = ''
        
        for step in self.steps: 
            step_list += '\n\t(' + str(step[0]) + ', ' + str(step[1]) + ')'
        
        return "IDAXNestedPipeline(steps=[%s],\nmodel=[%s])" % (step_list, self.model)


    def fit(self, sdf):

        intable_table = sdf.sdf_name + "_fitted"


        #fit transformers
        sdf = self.nested_sql_fit_transform(sdf)

        #create input table
        sdf.execute_transform_to_table(sdf.fit_schema, intable_table)
          
        #fit model 
        self.model.fit(sdf, sdf.fit_schema, intable_table)
        

    def nested_sql_fit_transform(self, sdf):

        copy_sdf = None

        #fit sql transformers - every step output is input into next step
        # to generate sql, transform must follow fit before moving onto next step
        for step in self.steps:
            
            if (copy_sdf is None):
                copy_sdf = sdf.clone()
            else: 
                copy_sdf = copy_sdf.clone_as_sql_source()

            function = step[1]
            function.fit(copy_sdf)
            function.transform(copy_sdf)

        return copy_sdf


    # populates sdf but does not execute model
    def transform(self, sdf):

        copy_sdf = None

        for step in self.steps:
            if (copy_sdf is None):
                copy_sdf = sdf.clone()
            else: 
                copy_sdf = copy_sdf.clone_as_sql_source()

            function = step[1]
            function.transform(copy_sdf)

        return copy_sdf


    def predict(self, sdf, execute_transform = True, outtable_name = None):

        intable_table = sdf.sdf_name + "_fitted"
        outtable_name = sdf.sdf_name + "_predictions" if (outtable_name == None) else outtable_name

        
        if (execute_transform):            
            #generate transformation sql
            sdf = self.transform(sdf)
            #create intput table
            sdf.execute_transform_to_table(sdf.fit_schema, intable_table)
            
        self.model.predict(sdf, sdf.fit_schema, intable_table, sdf.fit_schema, outtable_name)


    def fit_predict(self, sdf):
        self.fit(sdf)
        return self.predict(sdf, execute_transform = False)



# end of class IDAXNestedPipeline


