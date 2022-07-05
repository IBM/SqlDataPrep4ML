"""SQL Data Preprocessing Core Module
This module contains the core classes implementing the Machine Learning data pre-processing 
functions as a client generated SQL executed in SQL based DBMS 
as opposed to the traditional in-memory execution in traditional libraries such as SKLearn.

SQAlchemy
https://docs.sqlalchemy.org/en/13/core/connections.html#sqlalchemy.engine.Connection.execute

Notes
-----

"""


import sqlalchemy
import pandas as pd
import numpy as np
import scipy
from sklearn_pandas import DataFrameMapper
import sklearn.preprocessing as sp
import sklearn.compose
from enum import Enum
import joblib





class SqlConnection:
    """Connection to DBMS engine with functions for invocation of SQL statements.
    Encapsulates SQL Alchemy `Engine and Connection`_.
    
    Parameters
    ----------
    connection_string : string
        SQL Alchemy `connection string`_

    print_sql : bool
        If True all SQL statements submitted to database are printed into output

    Attributes
    ----------
    engine : sqlalchemy.engine.Engine
        Instance of sqlalchemy.engine.Engine used to submit SQL statements to the DB.

    dbtype : DbType
        The type of underlying Database engine. 
        It is used whitin the library to selected the dialect of SQL statements to generate. 
        This attribute is inferred from the conneciton string.
        

    Examples
    --------
    Postgres connection

    >>> dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=True)

    DB2 connection
    
    >>> dbconn = SqlConnection("db2+ibm_db://user:password@test.ibm.com:5100/A2B", print_sql=True)
    
    .. _Engine and Connection:
        https://docs.sqlalchemy.org/en/13/core/engines_connections.html

    .. _connection string:
        https://docs.sqlalchemy.org/en/13/core/connections.html

    """

    class DbType(Enum):
        """List of supported Database engines. This option determins the dialect of the generated SQL statements. 
        """

        STANDARD_SQL = 1        #PostgreSQL
        DB2 = 2                 #DB2 (z and LUW)


    def __init__(self, connection_string, print_sql = False):
        self.engine = sqlalchemy.create_engine(connection_string)
        self.print_sql = print_sql
        self.dbtype = SqlConnection.DbType.DB2 if (connection_string[:3].lower() == "db2") else SqlConnection.DbType.STANDARD_SQL
        self.conn = self.engine.connect()
        

    def __repr__(self):
        return "SqlConnection(engine=%s, print_sql=%s, dbtype=%s, conn=%s)" % (self.engine, self.print_sql, self.dbtype, self.conn)


    def get_sdf(self, catalog, sdf_name, sdf_query_data_source, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, **kwargs):
        """Creates a new instance of :class:`SqlDataFrame`.
        For description of parameters see documentation of :class:`SqlDataFrame`.

        Returns
        -------
        sdf
            A new instance of :class:`SqlDataFrame`.
        """

        return SqlDataFrame(self, catalog, sdf_name, sdf_query_data_source, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, **kwargs)


    def get_sdf_for_table(self, sdf_name, dataset_schema, dataset_table, key_column, fit_schema = None, default_order_by = None, catalog = None, **kwargs):
        """Creates a new instance of :class:`SqlDataFrame` for an existing table or a view.
        For description of parameters see documentation of :class:`SqlDataFrame`.

        Returns
        -------
        sdf
            A new instance of :class:`SqlDataFrame`.
        """

        sdf_query_data_source = dataset_schema + "." + dataset_table
        catalog = catalog if catalog is not None else InMemoryTableCatalog(self, sdf_name, dataset_schema, dataset_table, fit_schema)

        return self.get_sdf(catalog, sdf_name, sdf_query_data_source, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, **kwargs)


    def get_sdf_for_query(self, sdf_name, sql_query, dataset_schema, dataset_table, key_column, fit_schema = None, default_order_by = None, catalog = None, **kwargs):
        """Creates a new instance of :class:`SqlDataFrame` for a SQL query.
        Can be used only in limited cases (such as for nesting of sdfs) because many functions require table as source and not sql.
        For description of parameters see documentation of :class:`SqlDataFrame`.

        Returns
        -------
        sdf
            A new instance of :class:`SqlDataFrame`.
        """

        sql_query = "(" + sql_query + ")"
        catalog = catalog if catalog is not None else InMemoryTableCatalog(self, sdf_name, dataset_schema, dataset_table, fit_schema)
        
        return self.get_sdf(catalog, sdf_name, sql_query, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, **kwargs)


    def print_command(self, sql):
        """Prints SQL statement to output.

            Parameters
            ----------
            sql : string
                The sql statement to print.
        """
        if (self.print_sql):
            print("\n" + sql)


    def execute_command(self, sql):
        """Executes SQL statement with no output.

            Parameters
            ----------
            sql : string
                The sql statement to execute.

            Raises
            ------
                Exception
                Exception received from SQAlchemy connection
        """

        self.print_command(sql)

        try:
            self.conn.execute(sql)

        except (Exception) as error:
            print("SQL command failed:")
            print(error)
            raise (error)


    def execute_query_onerow(self, sql):
        """Executes SQL statement and retrieves the first row.

            Parameters
            ----------
            sql : string
                The sql statement to execute.

            Returns
            -------
            row
                A single result row of type sqlalchemy.engine.RowProxy.
                https://docs.sqlalchemy.org/en/13/core/connections.html?highlight=fetchone#sqlalchemy.engine.RowProxy
        """

        self.print_command(sql)

        try:
            result = self.conn.execute(sql)

            #db2 driver does not return number of rows
            #if (self.print_sql):
            #    print("Number of rows: ", result.rowcount)
            
            row = result.fetchone()

            if row is not None:
                return row
            else:
                return None
    
        except (Exception) as error:
            print("SQL query failed:")
            print(error)
            return None


    def execute_query_cursor(self, sql):
        """Executes SQL statement and returns a cursor for fetching rows.

            Parameters
            ----------
            sql : string
                The sql statement to execute.

            Returns
            -------
            cursor
                A cursor of type sqlalchemy.engine.ResultProxy.
                https://docs.sqlalchemy.org/en/13/core/connections.html?highlight=fetchone#sqlalchemy.engine.ResultProxy
        """

        self.print_command(sql)

        try:
            result = self.conn.execute(sql)
            return result
    
        except (Exception) as error:
            print("SQL query failed:")
            print(error)
            return None

    
    def drop_table (self, schema, table):
        """Drops table in the database.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.
        """

        if (self.table_exists(schema, table)):
            sql = "DROP TABLE " + schema + '.' + table 
            self.execute_command(sql)


    def upload_df_to_db(self, df, schema, table):
        """Stores <pandas.DataFrame> into a database table.

            Parameters
            ----------
            df : pandas.DataFrame
                The DataFrame to store.

            schema : string
                The schema of the table.

            table : string
                The name of the table.
        """

        self.drop_table(schema, table)
        df.to_sql(table, self.engine, schema, index=False) 


    def execute_sql_to_df(self, sql):
        """Executes SQL statement and returns <pandas.DataFrame>.

            Parameters
            ----------
            sql : string
                The sql to execute.
        """

        self.print_command(sql)
        return pd.read_sql_query(sql, self.conn)


    def get_table_as_df(self, schema, table, order_by=None):
        """Retrieves a table stored in database as returns it as <pandas.DataFrame>.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.

            order_by : string
                The content of ORDER BY clause of the SQL statement, defining ordering of the rows.
        """

        sql = "SELECT * FROM " + schema + "." + table
        
        if order_by != None:
            sql += " ORDER BY " + order_by
        
        return self.execute_sql_to_df(sql)


    def table_exists (self, schema, table):
        """Returns true if table exists.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.
        """

        if (self.dbtype == SqlConnection.DbType.DB2):
            # DB2 INFORMATION_SCHEMA
            # https://www.ibm.com/support/knowledgecenter/en/SSAE4W_9.5.1/db2/rbafzcatalog.htm
            sql = "SELECT * FROM SYSIBM.SYSTABLES WHERE UPPER(NAME)=UPPER('" + table + "') AND UPPER(CREATOR)=UPPER('" + schema + "')"
        else: 
            sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = UPPER('" + table + "') AND UPPER(TABLE_SCHEMA) = UPPER('" + schema + "')"
        
        result = self.execute_query_cursor(sql)
        
        # this is not clean - but db2 driver does not return number of rows
        #return (result.rowcount > 0)
        table_exists = (result.fetchone() is not None)

        return table_exists


    def column_exists (self, schema, table, column):
        """Returns true if column exists.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.

            column : string
                The name of the column.

        """

        if (self.dbtype == SqlConnection.DbType.DB2):
            # DB2 INFORMATION_SCHEMA
            # https://www.ibm.com/support/knowledgecenter/en/SSAE4W_9.5.1/db2/rbafzcatalog.htm
            sql = "?SELECT * FROM SYSIBM.SYSTABLES WHERE UPPER(NAME)=UPPER('" + table + "') AND UPPER(CREATOR)=UPPER('" + schema + "')"
        else: 
            sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = UPPER('" + table + "') AND UPPER(TABLE_SCHEMA) = UPPER('" + schema + "') AND UPPER(COLUMN_NAME) = UPPER('" + column + "')"
        
        result = self.execute_query_cursor(sql)
        
        # this is not clean - but db2 driver does not return number of rows
        #return (result.rowcount > 0)
        column_exists = (result.fetchone() is not None)

        return column_exists


    def get_table_schema (self, schema, table):
        """Returns <pandas.DataFrame> with a schema (the list of columns with names, datatypes, etc) of the table or view.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.
        """

        if (self.dbtype == SqlConnection.DbType.DB2):
            sql = "SELECT * FROM SYSIBM.SYSCOLUMNS WHERE TBNAME='" + table + "' AND TBCREATOR='" + schema + "'"
        else: 
            sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = UPPER('" + table + "') AND UPPER(TABLE_SCHEMA) = UPPER('" + schema + "') ORDER BY ORDINAL_POSITION"

        return self.execute_sql_to_df(sql)


    def create_unique_key(self, schema, table, column):
        """Adds identity column to the table and creates unique index on the column.

            Parameters
            ----------
            schema : string
                The schema of the table.

            table : string
                The name of the table.

            column : string
                The name of the column to add.
        """

        # drop the column if it exists
        if (self.column_exists(schema, table, column)):
            sql = "ALTER TABLE " + schema + "." + table + " DROP COLUMN " + column
            self.execute_command(sql)

        # add the column
        if (self.dbtype == SqlConnection.DbType.DB2):
            sql = "ALTER TABLE " + schema + "." + table + " ADD COLUMN " + column + " INT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"
        else:
            sql = "ALTER TABLE " + schema + "." + table + " ADD COLUMN " + column + " INT GENERATED ALWAYS AS IDENTITY"

        self.execute_command(sql)

        # add unique index on the column
        sql = "CREATE UNIQUE INDEX " + schema + "_" + table + "_" + column + " ON " + schema + "." + table + "(" + column + ")"
        self.execute_command(sql)


    def close(self):
        self.conn.close()


# end of class SqlConnection








# Class: 
class TableCatalog:


    def __init__(self, dbconn, sdf_name, dataset_schema, dataset_table, fit_schema):
        self.dbconn = dbconn
        self.sdf_name = sdf_name
        self.dataset_schema = dataset_schema
        self.dataset_table = dataset_table
        self.fit_schema = fit_schema if fit_schema is not None else dataset_schema
        

    #def __repr__(self):


    def clone(self, sdf_name = None):
        print("not to be used")


    def is_table_registered(self, schema, table):
        print("not to be used")


    def register_table(self, schema, table):
        print("not to be used")


    def un_register_table(self, schema, table):
        print("not to be used")


    def get_fit_table_name(self, function, column):
        return "fit_" + self.sdf_name + "_" + column + "_" + function.get_fit_table_suffix()
        

    def register_fit_table(self, function, column):
        fit_table = self.get_fit_table_name(function, column)
        self.register_table(self.fit_schema, fit_table)


    def drop_fit_table(self, function, column):
        fit_table = self.get_fit_table_name(function, column)
        self.dbconn.drop_table(self.fit_schema, fit_table)
        self.un_register_table(self.fit_schema, fit_table)


    def get_list_of_tables(self, include_all_sdfs = False):
        print("not to be used")


    def drop_temporary_tables(self, drop_dataset_table = False):
        print("not to be used")


# end of class TableCatalog




# Class: InMemoryTableCatalog
class InMemoryTableCatalog (TableCatalog):


    def __init__(self, dbconn, sdf_name, dataset_schema, dataset_table, fit_schema):
        super(InMemoryTableCatalog, self).__init__(dbconn, sdf_name, dataset_schema, dataset_table, fit_schema)
        self.tables = set()


    def __repr__(self):
        return "InMemoryTableCatalog(\ndbconn=%s,\nsdf_name=%s,\ndataset_schema=%s,\ndataset_table=%s,\nfit_schema=%s)" % (self.dbconn, \
            self.sdf_name, self.dataset_schema, self.dataset_table, self.fit_schema)


    def clone(self, sdf_name = None):
        
        sdf_name = sdf_name if sdf_name is not None else self.sdf_name
        clonedCatalog = InMemoryTableCatalog(self.dbconn, sdf_name, self.dataset_schema, self.dataset_table, self.fit_schema)
        
        # The list of tables is shared across all in memory catalogs
        clonedCatalog.tables = self.tables
        
        return clonedCatalog


    def is_table_registered(self, schema, table):
        return (schema, table) in self.tables


    def register_table(self, schema, table):
        self.tables.add((schema, table))


    def un_register_table(self, schema, table):
        if (self.is_table_registered(schema, table)):
            self.tables.remove((schema, table))


    def get_list_of_tables(self, include_all_sdfs = False):
        return self.tables


    def drop_temporary_tables(self):
        
        for record in self.tables:
            self.dbconn.drop_table(record[0], record[1])
            
        self.tables = set()


# end of class InMemoryTableCatalog





class InDbTableCatalog (TableCatalog):
    """Catalog keeps track of and manages all tables created during a lifetime of a :class:`SqlDataFrame`.

    Several functions of SqlDataFrame create new temporary tables in the DB such as split or fit functions of some transformers. 
    To keep track of these, every created table is recorded into the catalog table stored in the DB. 

    To associate tables with a specific SqlDataFrame, each SqlDataFrame has a unique name (SqlDataFrame.sdf_name), 
    which is recorded into the catalog table along with the new table details.

    In order to keep track of the tables created by a SqlDataFrame derived from an underlying SqlDataFrame,
    such in case when a new SqlDataFrame represents train table produced by split function from the original SqlDataFrame; 
    the derived SqlDataFrame uses the same name as the source SqlDataFrame.

    
    Parameters
    ----------
    dbconn : SqlConnection 
        Connection to DBMS.
    
    sdf_name : string
        The name of the SqlDataFrame under which all tables will be registered.

    dataset_schema : string
        The schema of the SqlDataFrame under which all tables will be registered.

    dataset_table : string 
        The table of the SqlDataFrame under which all tables will be registered.

    catalog_schema : string
        The schema in which the catalog table is stored. 
        If the table does not exist it will be created when first temporary table is registered.

    **kwargs : array of key value pairs
        Addtional arguments used when catalog table is created.
        For more information see :class:`SqlDataFrame`.
    
    """

    # Default name of the catalog table
    SDF_TABLE_CATALOG_TABLE = "SQLDP_TABLE_CATALOG"


    def __init__(self, dbconn, sdf_name, dataset_schema, dataset_table, fit_schema, catalog_schema, **kwargs):
        super(InDbTableCatalog, self).__init__(dbconn, sdf_name, dataset_schema, dataset_table, fit_schema)
        self.catalog_table = self.SDF_TABLE_CATALOG_TABLE
        self.catalog_schema = catalog_schema if catalog_schema is not None else dataset_schema
        self.kwargs = kwargs
    

    def __repr__(self):
        return "InDbTableCatalog(\ndbconn=%s,\nsdf_name=%s,\ndataset_schema=%s,\ndataset_table=%s,\nfit_schema=%s,\ncatalog_schema=%s,\ncatalog_name=%s,\nkwargs=%s)" % (self.dbconn, \
            self.sdf_name, self.dataset_schema, self.dataset_table, self.fit_schema, self.catalog_schema, self.catalog_table, self.kwargs)

    
    def clone(self, sdf_name = None):
        sdf_name = sdf_name if sdf_name is not None else self.sdf_name
        return InDbTableCatalog(self.dbconn, sdf_name, self.dataset_schema, self.dataset_table, self.fit_schema, self.catalog_schema, **self.kwargs)


    def create_catalog_table(self):

        if (not hasattr(self, "catalog_table_exists")):
            self.catalog_table_exists = self.dbconn.table_exists(self.catalog_schema, self.catalog_table)

        if (not self.catalog_table_exists):

            sql = "CREATE TABLE " + self.catalog_schema + "." + self.catalog_table
            #sql += " (sdf_name VARCHAR(255) NOT NULL, dataset_schema VARCHAR(255) NOT NULL, dataset_table VARCHAR(255) NOT NULL, table_schema VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, created TIMESTAMP, PRIMARY KEY (sdf_name, dataset_schema, dataset_table, table_schema, table_name))"
            sql += " (sdf_name VARCHAR(100) NOT NULL, dataset_schema VARCHAR(100) NOT NULL, dataset_table VARCHAR(100) NOT NULL, table_schema VARCHAR(100) NOT NULL, table_name VARCHAR(100) NOT NULL, created TIMESTAMP, PRIMARY KEY (sdf_name, dataset_schema, dataset_table, table_schema, table_name))"

            # create catalog table in specific database or tablespace
            if ((self.dbconn.dbtype == SqlConnection.DbType.DB2) and ('db2_create_catalog_table_in' in self.kwargs)):
                sql += " " + self.kwargs.get("db2_create_catalog_table_in")

            self.dbconn.execute_command(sql)
            self.catalog_table_exists = True


    def drop_catalog_table(self):
        self.dbconn.drop_table(self.catalog_schema, self.catalog_table)


    def is_table_registered(self, schema, table):

        self.create_catalog_table()
        
        sql = "SELECT table_schema, table_name FROM " + self.catalog_schema + "." + self.catalog_table 
        sql += " WHERE sdf_name = '" + self.sdf_name + "' and dataset_schema = '" + self.dataset_schema + "' and sdf_name = '" + self.sdf_name + "' and table_schema = '" + schema + "' and table_name = '" + table + "'"

        row = self.dbconn.execute_query_onerow(sql)
        return row is not None


    def register_table(self, schema, table):
        
        self.create_catalog_table()

        self.un_register_table(schema, table)

        sql = "INSERT INTO " + self.catalog_schema + "." + self.catalog_table
        sql += " VALUES ('" + self.sdf_name + "', '" + self.dataset_schema + "', '" + self.dataset_table + "', '" + schema + "', '" + table + "', current_timestamp)"    
        self.dbconn.execute_command(sql)


    def un_register_table(self, schema, table):

        self.create_catalog_table()

        sql = "DELETE FROM " + self.catalog_schema + "." + self.catalog_table
        sql += " WHERE sdf_name = '" + self.sdf_name + "' and dataset_schema = '" + self.dataset_schema + "' and dataset_table = '" + self.dataset_table + "' and table_schema = '" + schema + "' and table_name = '" + table + "'"
        self.dbconn.execute_command(sql)
        

    def get_list_of_tables(self, include_all_sdfs = False):

        self.create_catalog_table()
        
        sql = "SELECT * FROM " + self.catalog_schema + "." + self.catalog_table 
        
        if (not include_all_sdfs):
            sql += " WHERE sdf_name = '" + self.sdf_name + "' and dataset_schema = '" + self.dataset_schema + "' and sdf_name = '" + self.sdf_name + "'"
        
        return self.dbconn.execute_sql_to_df(sql)


    def drop_temporary_tables(self):

        self.create_catalog_table()
        
        sql = "SELECT table_schema, table_name FROM " + self.catalog_schema + "." + self.catalog_table 
        sql += " WHERE sdf_name = '" + self.sdf_name + "' and dataset_schema = '" + self.dataset_schema + "' and sdf_name = '" + self.sdf_name + "'"
        result = self.dbconn.execute_query_cursor(sql)

        for row in result:
            self.dbconn.drop_table(row[0], row[1])
            self.un_register_table(row[0], row[1])


# end of class InDbTableCatalog







class SqlDataFrame:
    """Representation of the underlying dataset (table, view, or query) and of the transformations applied to it.
    It is similar to :class:`pandas.DataFrame`, however, the data are held in the DB. 

    SqlDataFrame(SDF) has always a connection to DBMS and points to a specific dataset.     
    
    SDF provides two sets of functions:
    - Functions which allow to work with the uderlying dataset, such as retreiving data or modifying the dataset.
    - Functions which allow to define and execute transformations applied to the dataset. The result of transformation is either retrieved to memory or stored into a DB table. 

    Parameters
    ----------
    dbconn : SqlConnection 
        Connection to DBMS.
    
    sdf_name : string
        A unique name of the SqlDataFrame. The name is used to register and manage all related temporary tables.
        For more information see :class:`SqlDbTableCatalog`.

    sdf_query_data_source : string
        The FROM clause of SQL statement. This can be either a fully qualified name of table or view, or it can be a SQL statement returning a table.
        This mechanism allows to nest queries as sources of data for transformations.
        If the value is not an existing table of view, some dataset related functions will not work correctly.

    dataset_schema : string
        The schema of of the underlying table.

    dataset_table : string 
        The name of the underlying table or view.

    key_column : string
        The name of a column in the dataset holding a unique row key.
        Some transformation functions require a unique column to function correctly. For more details see descrition of invidual functions.
        
    catalog_schema : string
        The schema in which the catalog table is stored. 
        For more details see :class:`SqlDbTableCatalog`.

    fit_schema : string
        The schema in which temporary tables created by split and fit functions will be created.

    default_order_by : string
        Defines the order of rows.
        It is expressed as the ORDER BY clause of the generated transformation sql statements. 
        This default value is used if not other order_by argument is specified while invoking SQL generating functions.
        
        Note:
            The ORDER BY argument is important when comparing results with outside of db data (i.e. with DataFrame). 
            This is because if not ordered, the retrieved data may have different order.

    **kwargs : array of key value pairs
        Addtional arguments used when tables are created.

        Currently supported arguments are for DB2 on z:
            db2_create_catalog_table_in : The part of SQL statement defining in which database to create a catalog table.
            db2_create_fit_table_in : The part of SQL statement defining in which database to create new tables with split and fit functions.

            Example::

                {
                    'db2_create_catalog_table_in' : 'IN DATABASE DBLG', 
                    'db2_create_fit_table_in' : 'IN DATABASE DBLG'
                }


    Attributes
    ----------
    transformations : list of :class:`Transformation`
        Contains list of :class:`Transformation` representing transformation functions applied to the SDF.
        Each transformation function applied to the SDF is translated into one or more of these items.
                

    """



    class Transformation:
        """
        Represents the attributes of a transformation of a single column.
                    
        Parameters
        ----------               
            source_column : string
                The name of the column in the underlying dataset to be transformed.

            target_column : string
                The name of the column in the output table.

            column_function : string 
                The column transformation SQL i.e. representation of the transformation logic.

            fit_tables : string
                The name of a fit table, if the transformation referres to such table. This table will be joined in the transformation SQL.

            sub_tables : string
                The name of a table referred to in the column_function. The table will be joined by LEFT OUTER JOIN.

                Note: if provided, the table must have a unique key column named same as the source table key column (key_column). 
        """

        def __init__(self, source_column, target_column, column_function, fit_table = None, sub_table = None):
            self.source_column = source_column
            self.target_column = target_column
            self.column_function = column_function
            self.fit_table = fit_table
            self.sub_table = sub_table

    # end of class Transformation




    def __init__(self, dbconn, catalog, sdf_name, sdf_query_data_source, dataset_schema, dataset_table, key_column, fit_schema, default_order_by, **kwargs):
        self.dbconn = dbconn
        self.catalog = catalog
        self.sdf_name = sdf_name
        self.sdf_query_data_source = sdf_query_data_source
        self.dataset_schema = dataset_schema
        self.dataset_table = dataset_table
        self.key_column = key_column
        self.fit_schema = fit_schema if fit_schema is not None else dataset_schema
        self.default_order_by = default_order_by
        self.kwargs = kwargs
        
        self.transformations = []


    def __repr__(self):
        return "SqlDataFrame(\ndbconn=%s,\ncatalog=%s,\nsdf_name=%s,\nsdf_query_data_source=%s,\ndataset_schema=%s,\ndataset_table=%s,\nkey_column=%s,\nfit_schema=%s,\ndefault_order_by=%s,\nkwargs=%s,\ntransformations=%s)" % (self.dbconn, \
            self.catalog, \
            self.sdf_name, \
            self.sdf_query_data_source, \
            self.dataset_schema, \
            self.dataset_table, \
            self.key_column, \
            self.fit_schema, \
            self.default_order_by,
            self.kwargs,
            self.transformations)


    def clone(self, sdf_name = None):
        """Creates copy of the SDF. Copies connection and data source attributes but not transformations.

            Parameters
            ----------
            sdf_name : string
                Name of the new SDF. If not supplied, the name is copied from the orignal SDF.
        """

        sdf_name = sdf_name if (sdf_name is not None) else self.sdf_name
        catalog = self.catalog.clone(sdf_name)
        
        return SqlDataFrame(self.dbconn, catalog, sdf_name, self.sdf_query_data_source, self.dataset_schema, self.dataset_table, self.key_column, self.fit_schema, self.default_order_by, **self.kwargs)


    # creates copy of the sdf, with the transform sql of this sdf as a data source in the new sdf
    def clone_as_sql_source(self, sdf_name = None, include_source_columns = False, limit = None, include_all_source_columns = False, order_by = None):
        """Creates copy of the SDF. Copies connection and populates the new SDF with generated SQL as a data source.
            Allows to create nested SDFs.

            Parameters
            ----------
            sdf_name : string
                Name of the new SDF. If not supplied, the name is copied from the orignal SDF.

            include_source_columns : bool
                For each transformed column add the source column into output.

            limit : int
                The number of output rows. If None, all rows are included.

            include_all_source_columns : bool
                Add all source dataset columns into output.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.

        """
        
        sdf_name = sdf_name if (sdf_name is not None) else self.sdf_name
        catalog = self.catalog.clone(sdf_name)
        sdf_query_data_source =  '(' + self.generate_sql(include_source_columns, limit, include_all_source_columns, order_by) + ')'

        return SqlDataFrame(self.dbconn, catalog, sdf_name, sdf_query_data_source, self.dataset_schema, self.dataset_table, self.key_column, self.fit_schema, self.default_order_by, **self.kwargs)


    def add_column_to_output(self, source_column, target_column):
        """Adds column to the output.        

            Parameters
            ----------
            source_column : string
                The name of the column in the underlying dataset to be transformed.

            target_column : string
                The name of the column in the output table.
        """

        self.transformations.append(self.Transformation(source_column, target_column, source_column))


    def add_single_column_transformation(self, source_column, target_column, column_function, fit_table):
        """Adds transformation of a single column to the output.        

            Parameters
            ----------
            source_column : string
                The name of the column in the underlying dataset to be transformed.

            target_column : string
                The name of the column in the output table.

            column_function : string 
                The column transformation SQL i.e. representation of the transformation logic.

            fit_tables : string
                The name of a fit table, if the transformation referres to such table. This table will be joined in the transformation SQL.

        """

        self.transformations.append(self.Transformation(source_column, target_column, column_function, fit_table))
        

    def add_multiple_column_transformation(self, source_columns, target_columns, column_functions, sub_table):
        """Adds transformation of a multiple columns to the output.        

            Parameters
            ----------
            source_columns : list of string
                The name of the column in the underlying dataset to be transformed.

            target_columns : list of string
                The name of the column in the output table.

            column_functions : list of string 
                The column transformation SQL i.e. representation of the transformation logic.

            fit_tables : list of string
                The name of a fit table, if the transformation referres to such table. This table will be joined in the transformation SQL.

            sub_tables : list of string
                The name of a table referred to in the column_function. The table will be joined by LEFT OUTER JOIN.

                Note: if provided, the table must have a unique key column named same as the source table key column (key_column). 
        """

        for i in range(len(source_columns)):
            self.transformations.append(self.Transformation(source_columns[i], target_columns[i], column_functions[i], None, sub_table if i == 0 else None))


    def generate_sql (self, \
        include_source_columns = False, \
        limit = None, \
        include_all_source_columns = False, \
        order_by = None, \
        replace_data_source = None, \
        replace_fit_schema = None):
        """Generates transformation SQL statement from the list of transformation functions applied on the SDF.

            Parameters
            ----------
            include_source_columns : bool
                If True, for each transformed column add the source column into output.

            limit : int
                The number of output rows. If None, all rows are included.

            include_all_source_columns : bool
                Add all source dataset columns into output.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.

            replace_data_source : string
                If provided, overides the sdf_replace_data_source. 
                Most likely with a predefined string constant which can be later found and replaced in the statement.
                This is useful when the generated SQL statement is inteded to be used later with a different data source.

            replace_fit_schema : string
                Same as replace_data_source, if provided, it allows to overide the fit_schema with a different string.
                
        """

        column_list_sql = ""
        join_table = ""
        join_sql = ""
        result_sql = ""


        # replace fit schema
        fit_schema = replace_fit_schema if (replace_fit_schema is not None) else self.fit_schema

        # for every column in transformation list
        i = 0
        while i < len(self.transformations):

            transformation = self.transformations[i]

            if i > 0: column_list_sql += ",\n"

            # append the encoded column
            column_list_sql += transformation.column_function
            if (transformation.target_column is not None):
                column_list_sql += " AS " + transformation.target_column

            # append the source column (if requested)
            if include_source_columns and transformation.source_column is not None:
                column_list_sql += ",\n " + transformation.source_column

            # generate joins for fit tables
            if transformation.fit_table is not None:
                join_sql += "\nLEFT OUTER JOIN " + fit_schema + "." + transformation.fit_table + " AS " + transformation.fit_table + " ON data_table." + transformation.source_column + " = " + transformation.fit_table + ".label_key"

            # generate joins for complex sub tables
            if transformation.sub_table is not None:
                join_table = "sub_table" + str(i)
                join_sql += "\nLEFT OUTER JOIN \n(\n" + transformation.sub_table + "\n)\nAS " + join_table + " ON data_table." + self.key_column + " = " + join_table + "." + self.key_column 

            # if the column contains reference to join_table replace it with the actual name of the latest added join_table 
            column_list_sql = column_list_sql.replace("{join_table}", join_table)

            i += 1


        # if there are no functions added yet
        if (len(self.transformations) == 0):
            column_list_sql = "data_table.*"

        if (len(self.transformations) > 0) and (include_all_source_columns):
            column_list_sql += ", data_table.*"            

        if (order_by == None): order_by = self.default_order_by
        order_by_sql = "\nORDER BY " + order_by if (order_by != None) else ""

        # limit number of retrieved rows
        if (self.dbconn.dbtype == SqlConnection.DbType.DB2):
            limit_sql = "\nFETCH FIRST " + str(limit) + " ROWS ONLY" if (limit != None) else ""
        else: 
            limit_sql = "\nLIMIT " + str(limit) if (limit != None) else ""

        # replace data source
        data_source = replace_data_source if (replace_data_source is not None) else self.sdf_query_data_source

        result_sql = "SELECT\n" + column_list_sql + "\nFROM " + data_source + " AS data_table" + join_sql + order_by_sql + limit_sql

        #print (result_sql)
        return result_sql


    def __execute_sql_to_df(self, sql, return_df):

        df = self.dbconn.execute_sql_to_df(sql)

        if (return_df):
            return df
        else:
            return df.to_numpy()


    def head(self, limit = 5, return_df = True, include_source_columns = False, order_by = None):
        """Generates transformation SQL and retrieves the first n rows.
            Replicates :meth:`pandas.DataFrame.head`.

            Parameters
            ----------
            limit : int Default 5
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            include_source_columns : bool
                If True, for each transformed column add the source column into output.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
                
        """

        sql = self.generate_sql(include_source_columns, limit, order_by = order_by)    
        return self.__execute_sql_to_df(sql, return_df)


    def get_table_head(self, limit = 5, return_df = True, order_by = None):
        """Retrieves the first n rows from the underlying dataset.
            Replicates :meth:`pandas.DataFrame.head`.

            Parameters
            ----------
            limit : int
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
                
        """

        sql = self.generate_sql(include_source_columns = False, limit=limit, include_all_source_columns = True, order_by = order_by)    
        return self.__execute_sql_to_df(sql, return_df)


    def info (self):
        """Returns the information schema of the underlying dataset (table or view). 
            Note: the schema struture and content is platform dependent.
        """

        return self.dbconn.get_table_schema (self.dataset_schema, self.dataset_table)


    def get_table_size(self):
        """Returns the number of rows in the underlying dataset.
        """

        sql = 'SELECT COUNT(*) FROM ' + self.sdf_query_data_source
        row = self.dbconn.execute_query_onerow(sql)
        return int(row[0]) if row is not None else 0


    def shape(self):
        return (self.get_table_size(), self.info().shape[0])


    def execute_df(self, include_source_columns = False, limit = None, return_df = False, order_by = None):
        """Executes the transformation SQL and retrieves the output table into memory.

            Parameters
            ----------
            include_source_columns : bool
                If True, fFor each transformed column add the source column into output.

            limit : int
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
                
        """
        
        sql = self.generate_sql(include_source_columns=include_source_columns, limit=limit, order_by=order_by)        
        return self.__execute_sql_to_df(sql, return_df)


    def execute_sample_df(self, return_df = True, n = 0, frac = 0, random_state = 0):
        """Generates transformation SQL and retrieves a random sample of the rows.
            Replicates  :meth:`pandas.DataFrame.sample`.
        
            Parameters
            ----------
            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            n : int, optional
                Number of items to return. Cannot be used with frac.

            frac : float, optional
                Fraction of axis items to return. Cannot be used with n.

            random_state : int, optional
                Seed for the random number generator.
        """

        sql = self.__generate_sample_sql(n, frac, random_state)
        return self.__execute_sql_to_df(sql, return_df)


    def __generate_sample_sql(self, n = 0, frac = 0, random_state = 0):
        assert(not(n == 0 and frac == 0))
        assert(n == 0 or frac == 0)
        assert(frac >= 0 and frac <= 1)

        if (self.dbconn.dbtype == SqlConnection.DbType.DB2):
                
            # get n if fraction is used
            if (frac != 0):
                n = int(self.get_table_size() * frac)
                
            sql = self.generate_sql(limit=n, order_by = 'RAND(' + str(random_state) + ')')    

        else:            

            if (frac != 0):
                limit = '(SELECT count(*) * ' + str(frac) + ' FROM ' + self.sdf_query_data_source + ')'
            else:
                limit = n

            sql = self.generate_sql(limit=limit, order_by = 'random()')    
            sql = 'SELECT setseed(' + str(random_state) + ');\n' + sql

        return sql


    def __execute_sql_to_table(self, target_schema, target_table, register_in_catalog, sql):
        assert(len(target_schema) > 0)
        assert(len(target_table) > 0)
        assert(register_in_catalog is not None)
        assert(len(sql) > 0)

        if (self.dbconn.dbtype == SqlConnection.DbType.DB2):
            sql = 'CREATE TABLE ' + target_schema + '.' + target_table + ' AS\n(' + sql + ') WITH DATA'
        else: 
            sql = 'CREATE TABLE ' + target_schema + '.' + target_table + ' AS\n' + sql
        
        self.dbconn.drop_table(target_schema, target_table)
        self.dbconn.execute_command(sql)

        if (register_in_catalog):
            self.catalog.register_table(target_schema, target_table)


    def execute_transform_to_table(self, target_schema, target_table, register_in_catalog = True):
        """Executes the transformation SQL and stores the output into a new table.

            Parameters
            ----------
            target_schema : string
                The schema of the new table.

            target_table : string
                The name of the new table.

            register_in_catalog : bool
                If True, registers the new table in the SDF catalog.
                
        """
        
        sql = self.generate_sql()
        self.__execute_sql_to_table(target_schema, target_table, register_in_catalog, sql)


    def execute_sample_transform_to_table(self, target_schema, target_table, register_in_catalog = True, n = 0, frac = 0, random_state = 0):
        """Generates transformation SQL and rstores the output into a new table.
            Replicates  :meth:`pandas.DataFrame.sample`.
        
            Parameters
            ----------
            target_schema : string
                The schema of the new table.

            target_table : string
                The name of the new table.

            register_in_catalog : bool
                If True, registers the new table in the SDF catalog.
                
            n : int, optional
                Number of items to return. Cannot be used with frac.

            frac : float, optional
                Fraction of axis items to return. Cannot be used with n.

            random_state : int, optional
                Seed for the random number generator.
        """
        
        sql = self.__generate_sample_sql(n, frac, random_state)
        self.__execute_sql_to_table(target_schema, target_table, register_in_catalog, sql)
        

    def add_unique_id_column (self, column = None):
        """Add an new unique key column to the dataset.

            Parameters
            ----------
            column : string
                If provided, the colum is created and key_column is set the new value.
                If not provided, the SDF.key_column is used.
        """

        if (self.dataset_table == None):
            raise ValueError("Unique id column cannot be added - the underlying data source is not a table")

        column = column if (column != None) else self.key_column

        self.dbconn.create_unique_key(self.dataset_schema, self.dataset_table, column)

        self.key_column = column
        

    def get_table_column_df(self, column, limit = None, return_df = False, order_by = None):
        """Retrives a single column from the underlying dataset.
        
            Parameters
            ----------
            column : string
                The column to retrieve.

            limit : int
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
                
        """

        column_sdf = self.clone()
        SqlPassthroughColumn().transform(column_sdf, column)
        
        return column_sdf.execute_df(include_source_columns = False, limit = limit, return_df = return_df, order_by = order_by)


    def get_table_columns_df(self, columns, limit = None, return_df = False, order_by = None):
        """Retrives mulitple columns from the underlying dataset.
        
            Parameters
            ----------
            columns : string
                The list of columns to retrieve.

            limit : int
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
                
        """

        column_sdf = self.clone()

        for column in columns:
            SqlPassthroughColumn().transform(column_sdf, column)
        
        return column_sdf.execute_df(include_source_columns = False, limit = limit, return_df = return_df, order_by = order_by)


    def get_y_df(self, column, limit = None, return_df = False, order_by = None):
        """Returns y from the underlying dataset e.g.: for model scoring
        
            Parameters
            ----------
            column : string
                The column to retrieve.

            limit : int
                The number of output rows.

            return_df : bool
                If True, returns pandas.DataFrame, otherwise numpy.array.

            order_by : string
                Defines the order of rows.
                It is expressed as the ORDER BY clause of the generated transformation sql statements. 
                If provided, overides the default_order_by.

                Note: ordering should be used only when needed for testing purposes. It carries performance penalty.
        """

        return self.get_table_column_df(column, limit, return_df, order_by)


    def train_test_split(self, test_size=0.25, random_state=0, y_column = None, train_sdf_name = None, test_sdf_name = None):
        """Splits the underlying dataset into random train and test subsets.
            The function creates two new tables and registers them in catalog.

            The names of the new tables are based on the SDF.dataset_table with a suffix of _test and _train.

            Replicates  :meth:`sklearn.model_selection.train_test_split`.
        
            Parameters
            ----------
            test_size : float
                Should be between 0.0 and 1.0 and represent the proportion of the dataset to include in the test split.
                The size of the train is a complement.

            random_state : int, optional
                Seed for the random number generator.
            
            train_sdf_name : string
                The name of the new SDF for train table. If not provided, the name is same as the source SDF.

            test_sdf_name : string
                The name of the new SDF for test table. If not provided, the name is same as the source SDF.

        """



        """
            Fast method of selecting random subset from a large dataset with SQL
            To be fast the SQL must avoid order, join, group by, etc

            1 – assign random number to each row
            -	There is an initial penalty for the random number assignment but this happens only once and is not repeated for subsequent queries

            2 – to retrieve a subset, select range within the random variable domain e.g.: for 25% of variable 0 to 1 range can be 0 to 0.25

            3 – add to “select” clause “where” with conditions of the range

            4 – since there can be somewhat more or less rows within the range than the needed fraction - the where condition needs to somewhat expand the range 

            5 – add LIMIT to the select to constrain the exact number of rows

            6 - If x and y split is needed for ML and the dataset is retrieved to client, this should be performed at the client
            
            https://www.sisense.com/blog/how-to-sample-rows-in-sql-273x-faster/


            method 1 above - its approximate split
            -- but if each row has uniue id starting from 1
            method 2 with use of rowid and rand
            method 3 use of map/join
            method 4 as today with assigning rand and order 


            method 5 - approximate solution
            SELECT setseed(0);
            select count(*)
            from S1.TITANIC
            where random() <= 0.05


            """




        test_table = self.dataset_table + '_test'
        train_table = self.dataset_table + '_train'
        
        if (self.dbconn.dbtype == SqlConnection.DbType.DB2):
            self.train_test_split_db2(test_size, random_state, test_table, train_table)
        else:
            self.train_test_split_standard_sql(test_size, random_state, test_table, train_table)

        # create new sdf
        train_sdf_name = train_sdf_name if (train_sdf_name is not None) else self.sdf_name
        train_catalog = self.catalog.clone(train_sdf_name)
        test_sdf_name = test_sdf_name if (test_sdf_name is not None) else self.sdf_name
        test_catalog = self.catalog.clone(test_sdf_name)

        train_sdf = self.dbconn.get_sdf_for_table(train_sdf_name, self.fit_schema, train_table, self.key_column, self.fit_schema, self.default_order_by, train_catalog)
        test_sdf = self.dbconn.get_sdf_for_table(test_sdf_name, self.fit_schema, test_table, self.key_column, self.fit_schema, self.default_order_by, test_catalog)

        if (y_column is not None):
            # returns X_train, X_test, y_train, y_test 

            y_train_df = train_sdf.get_y_df(y_column)
            y_test_df = test_sdf.get_y_df(y_column)

            return train_sdf, test_sdf, y_train_df, y_test_df
        else:
            # returns X_train, X_test
            return train_sdf, test_sdf


    def train_test_split_standard_sql(self, test_size, random_state, test_table, train_table):

        # create table for test subset
        sql = 'SELECT setseed(' + str(random_state) + ');\n'
        sql += 'SELECT * INTO ' + self.fit_schema + '.' + test_table + '\nFROM ' + self.sdf_query_data_source + '\nORDER BY random() LIMIT '
        sql += '(SELECT count(*) * ' + str(test_size) + ' FROM ' + self.sdf_query_data_source + ')'

        self.dbconn.drop_table(self.fit_schema, test_table)
        self.catalog.un_register_table(self.fit_schema, test_table)
        self.dbconn.execute_command(sql)
        self.catalog.register_table(self.fit_schema, test_table)
        

        # create table for train subset
        sql = 'SELECT setseed(' + str(random_state) + ');\n'
        sql += 'SELECT * INTO ' + self.fit_schema + '.' + train_table + '\nFROM ' + self.sdf_query_data_source + '\nORDER BY random() LIMIT '
        sql += 'ALL OFFSET (SELECT count(*) * ' + str(test_size) + ' FROM ' + self.sdf_query_data_source + ')'

        self.dbconn.drop_table(self.fit_schema, train_table)
        self.catalog.un_register_table(self.fit_schema, train_table)
        self.dbconn.execute_command(sql)
        self.catalog.register_table(self.fit_schema, train_table) 


    def train_test_split_db2(self, test_size, random_state, test_table, train_table):

        row_count = self.get_table_size()
        test_count = int(row_count * test_size)
        train_count = row_count - test_count
            
        # create table for test subset
        sql = 'SELECT * FROM ' + self.sdf_query_data_source + '\nORDER BY RAND(' + str(random_state) + ') LIMIT ' + str(test_count)
        sql = 'CREATE TABLE ' + self.fit_schema + '.' + test_table + ' AS\n(' + sql + ') WITH DATA'
        self.dbconn.drop_table(self.fit_schema, test_table)
        self.catalog.un_register_table(self.fit_schema, test_table)
        self.dbconn.execute_command(sql)
        self.catalog.register_table(self.fit_schema, test_table)

        # create table for train subset                    
        sql = 'SELECT * FROM ' + self.sdf_query_data_source + '\nORDER BY RAND(' + str(random_state) + ') LIMIT ' + str(train_count) + ' OFFSET ' + str(test_count)
        sql = 'CREATE TABLE ' + self.fit_schema + '.' + train_table + ' AS\n(' + sql + ') WITH DATA'

        self.dbconn.drop_table(self.fit_schema, train_table)
        self.catalog.un_register_table(self.fit_schema, train_table)
        self.dbconn.execute_command(sql)
        self.catalog.register_table(self.fit_schema, train_table) 


# end of class SqlDataFrame









# Class: SklearnToSqlConverter
# Converts sklearn transformers, pipelines and mappers into to corresponding Sql library objects
class SklearnToSqlConverter:

    @classmethod
    def convert_function(cls, sklearn_function, sdf = None, columns = None):
        
        sklearn_type = type(sklearn_function) 
        sql_function = None


        if (sklearn_type is sp.MinMaxScaler): sql_function = SqlMinMaxScaler()
        if (sklearn_type is sp.MaxAbsScaler): sql_function = SqlMaxAbsScaler()
        if (sklearn_type is sp.Binarizer): sql_function = SqlBinarizer()
        if (sklearn_type is sp.StandardScaler): sql_function = SqlStandardScaler()
        if (sklearn_type is sp.LabelEncoder): sql_function = SqlLabelEncoder()
        if (sklearn_type is sp.OrdinalEncoder): sql_function = SqlOrdinalEncoder()
        if (sklearn_type is sp.OneHotEncoder): sql_function = SqlOneHotEncoder()
        if (sklearn_type is sp.LabelBinarizer): sql_function = SqlLabelBinarizer()
        if (sklearn_type is sp.Normalizer): sql_function = SqlNormalizer()
        if (sklearn_type is sp.KernelCenterer): sql_function = SqlKernelCenterer()

        return sql_function.load_from_sklearn(sklearn_function, sdf, columns)


    @classmethod
    def convert_dataframemapper(cls, sdf, dataframemapper):

        sql_features = []


        features = dataframemapper.features

        if not isinstance(features, list): raise ValueError("DataFrameMapper is not in correct state")

        for feature in features:
            columns = feature[0]
            sklearn_function = feature[1]

            sql_function = cls.transform_function(sdf, sklearn_function, columns)
            sql_features.append(sql_function)

        return SqlDataFrameMapper(sql_features)

# end of class SklearnToSqlConverter








# Class: SQL Function - Base class for all data preparation functions
class SqlFunction:

    #def __init__(self):

    #def __repr__(self):

    # sdf - instance of SqlDataFrame 
    # column - the column holding the data to fit
    def fit(self, sdf, column):
        print("not to be used")


    # returns a string to attach at the end of a fit table name to distinguish the type of function applied at the column
    def get_fit_table_suffix(self):
        print("not to be used")


    # sdf - instance of SqlDataFrame 
    # column - the column to be transformed
    def transform(self, sdf, columns):
        print("not to be used")


    def fit_transform(self, sdf, columns):
        self.fit(sdf, columns)
        return self.transform(sdf, columns)


    # Loads its Fit state from an instance of the corresponding sklearn function
    # sklearn_function - instance of sklearn_function to load fit data from
    # sdf - the sdf with db connection and attributes (fit schema, etc) which will be used to store fit data into db
    # columns - columns for which the fit data should be stored
    def load_from_sklearn(self, sklearn_function, sdf, columns):
        print("not to be used")


# end of class SqlFunction




# Class SqlPassthroughColumn
# Includes in output table a column from the source table
class SqlPassthroughColumn (SqlFunction):

    def __init__(self, target_column = None):
        self.target_column = target_column

    def __repr__(self):
        return "SqlPassthroughColumn(target_column=%s)" % self.target_column

    def fit(self, sdf, column):
        return None

    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_column_to_output(column, target_column)


# End of class SqlPassthroughColumn




# Class SqlUDFTransformer
# Invokes internal UDF function of the underlying database platform
class SqlUDFTransformer (SqlFunction):


    # arguments if init
    # udf - the name of udf in database
    # arguments - a list of arguments used to invoke the udf, the list must contain a string "{column}"" which will be replaced with the name of the actual column
    #   if arguments is None, the column will be the first and only argument

    def __init__(self, udf, arguments = None, target_column = None):
        self.udf = udf
        self.arguments = arguments
        self.target_column = target_column


    def __repr__(self):
        return "SqlUDFTransformer(udf=%s, arguments=[%s], target_column=%s)" % (self.udf, ', '.join(map(str, self.arguments)), self.target_column)


    def fit(self, sdf, column):
        return None


    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        
        argument_column = "data_table." + column

        if (self.arguments == None):
            arguments = argument_column
        else:
            arguments = ', '.join(map(str, self.arguments))
            arguments = arguments.replace("{column}", argument_column)

        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, self.udf + "(" + arguments + ")", None)


# End of class SqlUDFTransformer





# Class SqlCustomSqlTransformer
# Allows to define and execute custom sql transformation on a column
class SqlCustomSqlTransformer (SqlFunction):


    # arguments if init
    # udf - the name of udf in database
    # custom_transformation - sql string used to transform the source table column, the string must contain a string "{column}" which will be replaced with the name of the actual column

    def __init__(self, custom_transformation, target_column = None):
        self.custom_transformation = custom_transformation
        self.target_column = target_column


    def __repr__(self):
        return "SqlCustomSqlTransformer(custom_transformation=%s, target_column=%s)" % (self.custom_transformation, self.target_column)


    def fit(self, sdf, column):
        return None


    def transform(self, sdf, columns):
        assert(sdf is not None)
        assert(columns is not None)

        column = columns if (not isinstance(columns, list)) else columns[0]

        custom_transformation = self.custom_transformation.replace("{column}", column)

        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, custom_transformation, None)


# End of class SqlCustomSqlTransformer






# Class SqlCaseEncoder
# Allows to define specific cases - uses a pairs of conditions and values to determine new value of the column
class SqlCaseEncoder (SqlFunction):


    # self.cases [condition, value]

    # cases - an array of CASE conditions and values 
    # else_value - value used when no other options fit
    def __init__(self, cases, else_value = None, target_column = None):
        assert(cases is not None and len(cases) > 0)

        self.cases = cases
        self.else_value = else_value
        self.target_column = target_column
        

    def __repr__(self):
        return "SqlCaseEncoder(cases=%s, else_value=%s, target_column=%s)" % (self.cases, self.else_value, self.target_column)


    def fit(self, sdf, column):
        return None


    def transform(self, sdf, column):
        assert(sdf is not None)
        assert(column is not None)

        column_function = ''

        for case in self.cases:
            condition = case[0]
            value = case[1]
            condition = condition.replace("{column}", column)    
            column_function += 'WHEN ' + condition + ' THEN ' + str(value) + ' ' 

        if (self.else_value is not None):
            column_function += 'ELSE ' + str(self.else_value) + ' '

        column_function = 'CASE ' + column_function + 'END'

        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, column_function, None)


# End of class SqlCaseEncoder






# Class SqlMapEncoder
# Maps keys to values 
class SqlMapEncoder (SqlFunction):


    # self.pairs [key, value]

    # pairs - an array of key value pairs
    # else_value - value used when no other options fit
    def __init__(self, pairs, else_value = None, target_column = None):
        assert(pairs is not None and len(pairs) > 0)

        self.pairs = pairs
        self.else_value = else_value
        self.target_column = target_column
        

    def __repr__(self):
        return "SqlMapEncoder(pairs=%s, else_value=%s, target_column=%s)" % (self.pairs, self.else_value, self.target_column)


    def fit(self, sdf, column):
        return None


    def transform(self, sdf, column):
        assert(sdf is not None)
        assert(column is not None)

        column_function = ''

        for pair in self.pairs:
            key = pair[0]
            value = pair[1]
            column_function += 'WHEN ' + str(key) + ' THEN ' + str(value) + ' ' 

        if (self.else_value is not None):
            column_function += 'ELSE ' + str(self.else_value) + ' '

        column_function = 'CASE ' + column + ' ' + column_function + 'END'

        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, column_function, None)


# End of class SqlMapEncoder








# Class: MinMaxScaler
# MinMax scaling - range 0 to 1
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html#sklearn.preprocessing.MinMaxScaler
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_data.py
# X_std = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
# fit - Compute the minimum and maximum to be used for later scaling.
# transform - Scaling features of X according to feature_range.
class SqlMinMaxScaler (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        return "SqlMinMaxScaler(target_column=%s)" % self.target_column


    def fit(self, sdf, column):

        # compute the min and max values
        sql = "SELECT MIN(" + column + ") AS min_value, MAX(" + column + ") AS max_value FROM " + sdf.sdf_query_data_source + " AS data_table"
        
        row = sdf.dbconn.execute_query_onerow(sql)

        if row is not None:
            self.min_value = row[0]
            self.max_value = row[1]
            

    #(cast(c2 as float) - x_min) / (x_max - x_min) 
    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, "(CAST(data_table." + column + " AS FLOAT) - " + str(self.min_value) + ") / " + str(self.max_value - self.min_value), None)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.MinMaxScaler): raise ValueError("argument is not of type sklearn.preprocessing.MinMaxScaler")
        self.min_value = sklearn_function.data_min_[0]
        self.max_value = sklearn_function.data_max_[0]
        return self


# end of class SqlMinMaxScaler








# Class: MaxAbsScaler
# MaxAbs scaling - range 0 to 1
# Scale each feature by its maximum absolute value.
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MaxAbsScaler.html#sklearn.preprocessing.MaxAbsScaler
# Unlike min max this estimator does not consider min value but only max
# fit - Compute the minimum and maximum to be used for later scaling.
# transform - Scaling features of X according to feature_range.
class SqlMaxAbsScaler (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self, target_column = None):
        return "SqlMaxAbsScaler(target_column=%s)" % self.target_column


    def fit(self, sdf, column):

        # compute the min and max values
        sql = "SELECT MIN(" + column + ") AS min_value, MAX(ABS(" + column + ")) AS max_value FROM " + sdf.sdf_query_data_source + " AS data_table"
        
        row = sdf.dbconn.execute_query_onerow(sql)

        if row is not None:
            #self.min_value = row[0]
            self.max_value = row[1]
            

    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, "(CAST(data_table." + column + " AS FLOAT)) / " + str(self.max_value), None)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.MaxAbsScaler): raise ValueError("argument is not of type sklearn.preprocessing.MaxAbsScaler")
        self.max_value = sklearn_function.max_abs_[0]
        return self


# end of class SqlMaxAbsScaler





# Class: Binarizer
# Binarize data (set feature values to 0 or 1) according to a threshold
# Values greater than the threshold map to 1, while values less than or equal to the threshold map to 0.
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Binarizer.html
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_data.py
# fit - Do nothing
# transform - Binarize each element of X
class SqlBinarizer (SqlFunction):


    def __init__(self, threshold=0.0, target_column = None):
        self.threshold = threshold
        self.target_column = target_column


    def __repr__(self):
        return "SqlBinarizer(threshold=%s, target_column=%s)" % (self.threshold, self.target_column)


    def set_params (self, threshold):
        self.threshold = threshold


    def fit(self, sdf, column):
        return None

            
    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, "CASE WHEN " + column + " > " + str(self.threshold) + " THEN 1 ELSE 0 end", None)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.Binarizer): raise ValueError("argument is not of type sklearn.preprocessing.Binarizer")
        self.threshold = sklearn_function.threshold
        return self

# end of class SqlBinarizer






# Class: StandardScaler
# Standardize features by removing the mean and scaling to unit variance
"""
-- The standard score of a sample x is calculated as:
-- z = (x - u) / s
-- where u is the mean of the training samples
-- and s is the standard deviation of the training samples
-- The results of this function differ depending on implementation of stddev which is tech/platform/lib dependent
-- - therefore comparizon of results between different libraries must do some degree of rounding to match outcomes
"""
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html#sklearn.preprocessing.StandardScaler
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_data.py
# fit - Compute the mean and std to be used for later scaling.
# transform - Perform standardization by centering and scaling
class SqlStandardScaler (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self, target_column = None):
        return "SqlStandardScaler(target_column=%s)" % self.target_column


    def fit(self, sdf, column):

        # compute the mean and stddev values
        sql = "SELECT AVG(" + column + ") AS mean_value, STDDEV(" + column + ") AS stddev_value"
        sql += "\nFROM " + sdf.sdf_query_data_source + " AS data_table"
        
        row = sdf.dbconn.execute_query_onerow(sql)

        if row is not None:
            self.mean_value = row[0]
            self.stddev_value = row[1]
            

    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        #sdf.add_single_column_transformation(column, column + "_encoded", "round((CAST(" + column + " AS FLOAT) - " + str(self.mean_value) + ") / " + str(self.stddev_value) + ")", None)
        sdf.add_single_column_transformation(column, target_column, "(CAST(" + column + " AS FLOAT) - " + str(self.mean_value) + ") / " + str(self.stddev_value), None)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.StandardScaler): raise ValueError("argument is not of type sklearn.preprocessing.StandardScaler")
        self.mean_value = sklearn_function.mean_[0]
        self.stddev_value = sklearn_function.scale_[0]
        return self

# end of class SqlStandardScaler






# Class: LabelEncoder
# Encode labels with value between 0 and n_classes-1.
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelEncoder.html#sklearn.preprocessing.LabelEncoder
# Fit - Fit label encoder - generates codes/indexes of labels
# transform - Transform labels to normalized encoding.

# The result of Fit function is stored in table in the same DB as the original table is located at
# This can be implemented as storing to any other destination

class SqlLabelEncoder (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        fit_table = self.fit_table if (hasattr(self, "fit_table")) else ''
        return "SqlLabelEncoder(fit_table=%s, target_column=%s)" % (fit_table, self.target_column)


    def fit(self, sdf, column):
            if (sdf.dbconn.dbtype == SqlConnection.DbType.DB2):
                self.fit_db2(sdf, column)
            else: 
                self.fit_standard_sql(sdf, column)


    def get_fit_table_suffix(self):
        return "le"


    def fit_standard_sql(self, sdf, column):

        # drop fit table if already exists
        sdf.catalog.drop_fit_table(self, column)


        self.fit_table = sdf.catalog.get_fit_table_name(self, column)
        sdf.catalog.register_fit_table(self, column)        

        # generate table with labels
        sql = "SELECT label_key, (ROW_NUMBER () OVER (ORDER BY label_key)) - 1 AS label_encoded"
        sql += "\nINTO " + sdf.fit_schema + "." + self.fit_table + "\nFROM (SELECT DISTINCT " + column + " AS label_key FROM " + sdf.sdf_query_data_source + " AS data_table ) AS table_input"
        sdf.dbconn.execute_command(sql)
    
        # add primary key
        sql = "ALTER TABLE " + sdf.fit_schema + "." + self.fit_table + " ADD CONSTRAINT " + self.fit_table.replace(".", "_") + "_key PRIMARY KEY (label_key)"
        sdf.dbconn.execute_command(sql)


    # for now - converts all values to string for simplicity
    def fit_db2(self, sdf, column):
        
        # drop fit table if already exists
        sdf.catalog.drop_fit_table(self, column)


        self.fit_table = sdf.catalog.get_fit_table_name(self, column)
        sdf.catalog.register_fit_table(self, column)   

        # create new table
        sql = "CREATE TABLE " + sdf.fit_schema + "." + self.fit_table + " (label_key VARCHAR(255) NOT NULL, label_encoded INT, PRIMARY KEY (label_key)) "

        # create fit table in specific database or tablestapce
        if ('db2_create_fit_table_in' in sdf.kwargs):
            sql += " " + sdf.kwargs.get("db2_create_fit_table_in")

        sdf.dbconn.execute_command(sql)

        # generate labels
        sql = "INSERT INTO " + sdf.fit_schema + "." + self.fit_table + "(label_key, label_encoded)"
        sql += "\nSELECT label_key, (ROW_NUMBER () OVER (ORDER BY label_key)) - 1 AS label_encoded"
        sql += "\nFROM (SELECT DISTINCT " + column + " AS label_key FROM " + sdf.sdf_query_data_source + " AS data_table ) AS table_input"
        sdf.dbconn.execute_command(sql)


    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        sdf.add_single_column_transformation(column, target_column, self.fit_table + ".label_encoded", self.fit_table)


    def load_from_sklearn(self, sklearn_function, sdf, columns):
        assert(sdf is not None)
        assert(columns is not None)

        column = columns if (not isinstance(columns, list)) else columns[0]
        
        if (type(sklearn_function) is not sp.LabelEncoder): raise ValueError("argument is not of type sklearn.preprocessing.LabelEncoder")
        if len(sklearn_function.classes_) < 1: return


        sdf.catalog.drop_fit_table(self, column)
        self.fit_table = sdf.catalog.get_fit_table_name(self, column)
        sdf.catalog.register_fit_table(self, column)  

        # store dictionary into db
        df = pd.DataFrame()
        df['label_key'] = sklearn_function.classes_
        df.index.rename('label_encoded', inplace=True)

        sdf.dbconn.upload_df_to_db(df, sdf.fit_schema, self.fit_table)

        # add primary key
        sql = "ALTER TABLE " + sdf.fit_schema + "." + self.fit_table + " ADD CONSTRAINT " + self.fit_table.replace(".", "_") + "_key PRIMARY KEY (label_key)"
        sdf.dbconn.execute_command(sql)

        return self

# end of class SqlLabelEncoder







# Class: OrdinalEncoder
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OrdinalEncoder.html#sklearn.preprocessing.OrdinalEncoder
# Encode categorical features as an integer array.
# Reuses almost all functionality of SqlLabelEncoder
class SqlOrdinalEncoder (SqlLabelEncoder):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        fit_table = self.fit_table if (hasattr(self, "fit_table")) else ''
        return "SqlOrdinalEncoder(fit_table=%s, target_column=%s)" % (fit_table, self.target_column)


    def get_fit_table_suffix(self):
        return "oe"


    def load_from_sklearn(self, sklearn_function, sdf, columns):
        assert(sdf is not None)
        assert(columns is not None)

        column = columns if (not isinstance(columns, list)) else columns[0]
        
        if (type(sklearn_function) is not sp.OrdinalEncoder): raise ValueError("argument is not of type sklearn.preprocessing.OrdinalEncoder")
        if len(sklearn_function.categories_) < 1: return
        if len(sklearn_function.categories_[0]) < 1: return

        sdf.catalog.drop_fit_table(self, column)
        self.fit_table = sdf.catalog.get_fit_table_name(self, column)
        sdf.catalog.register_fit_table(self, column)  

        # store dictionary into db
        df = pd.DataFrame()
        df['label_key'] = sklearn_function.categories_[0]
        df.index.rename('label_encoded', inplace=True)

        sdf.dbconn.upload_df_to_db(df, sdf.fit_schema, self.fit_table)

        # add primary key
        sql = "ALTER TABLE " + sdf.fit_schema + "." + self.fit_table + " ADD CONSTRAINT " + self.fit_table.replace(".", "_") + "_key PRIMARY KEY (label_key)"
        sdf.dbconn.execute_command(sql)

        return self

# end of class SqlOrdinalEncoder







# Class: OneHotEncoder
# Encode categorical integer features as a one-hot numeric array.
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.OneHotEncoder.html#sklearn.preprocessing.OneHotEncoder
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_encoders.py

class SqlOneHotEncoder (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        return "SqlOneHotEncoder(target_column=%s)" % self.target_column


    def fit(self, sdf, column):

        # generate list of columns sql
        self.categories = []

        sql = "SELECT DISTINCT TRIM(CAST(" + column + " AS VARCHAR(255))) AS label_code FROM " + sdf.sdf_query_data_source + " AS data_table WHERE " + column + " IS NOT NULL ORDER BY TRIM(CAST(" + column + " AS VARCHAR(255)))"

        result = sdf.dbconn.execute_query_cursor(sql)

        for row in result:
            self.categories.append(str(row[0]))

        result.close()


    def are_all_categories_number(self):
        try:
            for category in self.categories:
                complex(category)
        except ValueError:
            return False

        return True


    def generate_columns_sql(self, column):
        
        columns = ""
        are_all_categories_number = self.are_all_categories_number()

        for category in self.categories:
            label_name = category.strip().replace(" ", "_").replace(".", "_")
            category_value = category if are_all_categories_number else "'" + category + "'"
            columns += "CASE WHEN " + column + " = " + category_value + " THEN 1 ELSE 0 END AS " + column + "_" + label_name + ",\n"

        return columns[:-2]


    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        columns = self.generate_columns_sql(column)
        sdf.add_single_column_transformation(column, None, columns, None)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.OneHotEncoder): raise ValueError("argument is not of type sklearn.preprocessing.OneHotEncoder")
        
        self.categories = []
        
        if (len(sklearn_function.categories_) < 1): return
        if (len(sklearn_function.categories_[0]) < 1): return

        for category in sklearn_function.categories_[0]:
            self.categories.append(str(category))

        return self


# end of class SqlOneHotEncoder







# Class: LabelBinarizer
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelBinarizer.html#sklearn.preprocessing.LabelBinarizer
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_label.py
class SqlLabelBinarizer (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        return "SqlLabelBinarizer(target_column=%s)" % self.target_column


    def fit(self, sdf, column):

        # generate list of columns sql
        self.classes = []

        sql = "SELECT DISTINCT " + column + " AS label_code FROM " + sdf.sdf_query_data_source + " AS data_table ORDER BY " + column 
        #WHERE """ + column + " IS NOT NULL" #sklearn - null is important - sklearn considers null as a value on its own, thus it has to have own column

        result = sdf.dbconn.execute_query_cursor(sql)

        for row in result:
            self.classes.append(str(row[0]))

        result.close()


    def generate_columns_sql(self, column):
        
        # sklearn - special case - if number of columns is 2, the result is a single column i.e. binary values
        # in this respect the label binarizer differs from 1hot encoder
        # - the single column is the second one based on string order of labels
        if (len(self.classes) == 2): 
            target_columns = self.get_sql_for_label(column, self.classes[1])
        else:
            target_columns = ''
            for class_name in self.classes:
                target_columns += self.get_sql_for_label(column, class_name)            
                
        return target_columns[:-2]


    def get_sql_for_label(self, column, class_name):
        if class_name is not None: 
            class_name = str(class_name).replace(" ", "_")
            return "CASE WHEN " + column + " = '" + str(class_name) + "' THEN 1 ELSE 0 END AS " + column + "_" + class_name + ",\n"
        else:
            return "CASE WHEN " + column + " IS NULL THEN 1 ELSE 0 END AS " + column + "_NULL,\n"


    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_columns = self.generate_columns_sql(column)
        sdf.add_single_column_transformation(column, None, target_columns, None)
    

    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.LabelBinarizer): raise ValueError("argument is not of type sklearn.preprocessing.LabelBinarizer")
        if (len(sklearn_function.classes_) < 1): return

        self.classes = sklearn_function.classes_.copy()
                
        return self


# end of class SqlLabelBinarizer    







# Class: Normalizer 
# Normalize samples individually to unit norm.
# Sql implimentationsupports all three norms (l1, l2, max)
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.Normalizer.html#sklearn.preprocessing.Normalizer
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_data.py
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/utils/extmath.py
class SqlNormalizer (SqlFunction):

    def __init__(self, norm='l2', copy=True, target_column = None):
        self.norm = norm
        self.target_column = target_column
                
        if norm not in ('l1', 'l2', 'max'):
            raise ValueError("'%s' is not a supported norm" % norm)


    def __repr__(self):
        return "SqlNormalizer(norm=%s, target_column=%s)" % (self.norm, self.target_column)


    def fit(self, X, y=None):
        return self


    def get_sql_from_for_source_matrix(self, sdf_query_data_source, columns, key):
        
        sql = ""
        
        for i in range(len(columns)):
            sql += "CAST(" + columns[i] + " AS FLOAT) AS " + columns[i]
            if (i < len(columns) - 1): 
                sql += ", "
            
        sql = "SELECT " + key + ", " + sql + "\nFROM " + sdf_query_data_source
        return sql


    def get_sql_from_for_target_matrix(self, columns, key):
        
        sql = "source_matrix." + key + ","
        
        for i in range(len(columns)):
            sql += " " + columns[i] + " / row_norm AS " + columns[i]
            if (i < len(columns) - 1): 
                sql += ","

        return sql
                

    def get_sql_from_for_norms_max(self, sdf_query_data_source, columns, key):

        sql = ""
        
        for i in range(len(columns)):
            sql += "\nSELECT " + key + ", " + columns[i] + " AS row_val FROM " + sdf_query_data_source
            if (i < len(columns) - 1):
                sql += "\nUNION ALL"

        sql = "SELECT " + key + ", MAX(row_val) as row_norm\nFROM \n(" + sql + "\n)\nAS row_norms GROUP BY " + key

        return sql


    def get_sql_from_for_norms_l1(self, sdf_query_data_source, columns, key):

        sql = ""
        
        for i in range(len(columns)):
            sql += "ABS(" + columns[i] + ")"
            if (i < len(columns) - 1): 
                sql += " + "

        sql = "SELECT " + key + ", " + sql + " AS row_norm\nFROM " + sdf_query_data_source

        return sql


    def get_sql_from_for_norms_l2(self, sdf_query_data_source, columns, key):

        sql = ""
        
        for i in range(len(columns)):
            sql += columns[i] + " * " + columns[i]
            if (i < len(columns) - 1): 
                sql += " + "

        sql = "SELECT " + key + ", SQRT(" + sql + ") AS row_norm\nFROM " + sdf_query_data_source

        return sql


    def transform(self, sdf, columns):

        key = sdf.key_column
        sdf_query_data_source = sdf.sdf_query_data_source
        sql_source_matrix = self.get_sql_from_for_source_matrix(sdf_query_data_source, columns, key)
        sql_target_matrix = self.get_sql_from_for_target_matrix(columns, key)
        sql_norms = ''
        sql_normalized_table = ''
                
        if self.norm == 'l1':
            sql_norms = self.get_sql_from_for_norms_l1(sdf_query_data_source, columns, key)
        
        elif self.norm == 'l2':
            sql_norms = self.get_sql_from_for_norms_l2(sdf_query_data_source, columns, key)

        elif self.norm == 'max':
            sql_norms = self.get_sql_from_for_norms_max(sdf_query_data_source, columns, key)


        sql = "SELECT " + sql_target_matrix + "\nFROM \n(\n" + sql_source_matrix + "\n)\nAS source_matrix,\n(\n" + sql_norms + "\n)\nAS norms\nWHERE source_matrix." + key + " = norms." + key

        #source_columns, target_columns, column_functions, sub_table
        target_columns = [column + "_encoded" for column in columns]
        column_functions = ["{join_table}." + column for column in columns]
        sdf.add_multiple_column_transformation(columns, target_columns, column_functions, sql)
    

    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.Normalizer): raise ValueError("argument is not of type sklearn.preprocessing.Normalizer")
        self.norm = sklearn_function.norm
        return self

# end of class SqlNormalizer 




'''
l2 form / normalize row
select 
c1 / row_norm as c1, 
c2 / row_norm as c2, 
c3 / row_norm as c3 
from
(
	select *, ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index 
	from s1.kc1
) as source_matrix,
(
	select sqrt(c1 * c1 + c2 * c2 + c3 * c3) as row_norm,
		ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index 
	from s1.kc1
) as norms
where source_matrix.row_index = norms.row_index

l1
select 
c1 / row_norm as c1, 
c2 / row_norm as c2, 
c3 / row_norm as c3 
from
(
	select cast(c1 as float) as c1,
		cast(c2 as float) as c2,
		cast(c3 as float) as c3,
		ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index 
	from s1.kc1
) as source_matrix,
(
	select abs(c1) + abs(c2) + abs(c3) as row_norm,
		ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index 
	from s1.kc1
) as norms
where source_matrix.row_index = norms.row_index;

max
select 
source_matrix.pk,
c1 / row_norm as c1, 
c2 / row_norm as c2, 
c3 / row_norm as c3 
from
(
	select cast(c1 as float) as c1,
		cast(c2 as float) as c2,
		cast(c3 as float) as c3,
		pk
	from s1.kc1
) as source_matrix,
(
	select pk, max(mc) as row_norm
	from 
	(
	SELECT pk, c1 as mc
		FROM s1.kc1
		UNION ALL
		SELECT pk, c2 as mc
		FROM s1.kc1
		UNION ALL
		SELECT pk, c3 as mc
		FROM s1.kc1
	) as m
	group by pk
) as norms
where source_matrix.pk = norms.pk;
'''









# Class: KernelCenterer
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KernelCenterer.html#sklearn.preprocessing.KernelCenterer
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_data.py
# Center a kernel matrix
class SqlKernelCenterer (SqlFunction):


    def __init__(self, target_column = None):
        self.target_column = target_column


    def __repr__(self):
        return "SqlKernelCenterer(target_column=%s)" % self.target_column


    def fit(self, sdf, columns):

        n_samples = len(columns)


        sql_sum_row = " + ".join(columns)

        # get k_fit_all 
        sql = "SELECT SUM(k_fit_row) / " + str(n_samples) + " AS k_fit_all\nFROM\n(\nSELECT CAST((" + sql_sum_row + ") AS FLOAT) / " + str(n_samples) + " AS k_fit_row\nFROM s1.kc1\n)\n AS data_table"
        row = sdf.dbconn.execute_query_onerow(sql)

        if row is not None:
            self.k_fit_all = row[0]
                        
        # get k_fit_row
        sql = ""
        
        for i in range(len(columns)):
            sql += "\nSUM(CASE WHEN row_index = " + str(i + 1) + " THEN k_fit_row END) " + columns[i]
            if (i < len(columns) - 1):
                sql += ","

        sql = "SELECT " + sql + "\nFROM\n(\n"
        sql += "SELECT CAST((" + sql_sum_row + ") AS FLOAT) / " + str(n_samples) + " AS k_fit_row, ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index\nFROM\n" + sdf.sdf_query_data_source
        sql += "\n)\nAS data_table"
        row = sdf.dbconn.execute_query_onerow(sql)

        if row is not None:
            self.k_fit_row = row


    def get_sql_for_target_columns(self, columns):

        sql = ""
        
        for i in range(len(columns)):
            sql += "k_fit_rows." + columns[i] + " - k_pred_cols.k_pred_col + " + str(self.k_fit_all) + " AS " + columns[i]
            if (i < len(columns) - 1): 
                sql += ", "

        return sql


    def get_sql_from_for_k_fit_rows(self, sdf_query_data_source, columns, key):

        sql = ""
        
        for i in range(len(columns)):
            sql += "\n" + columns[i] + " - " + str(self.k_fit_row[i]) + " AS " + columns[i]
            if (i < len(columns) - 1):
                sql += ", "

        sql = "SELECT\n" + key + ", " + sql + ",\nROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index\nFROM " + sdf_query_data_source

        return sql


    def get_sql_from_for_columns(self, sdf_query_data_source, columns):

        n_samples = len(columns)
        sql = ""
        
        for i in range(len(columns)):
            sql += "\nSELECT SUM(" + columns[i] + ") / " + str(n_samples) + " AS k_pred_col, " + str(i + 1) + " AS row_index FROM " + sdf_query_data_source
            if (i < len(columns) - 1):
                sql += "\nUNION ALL"

        return sql


    def transform(self, sdf, columns):

        key = sdf.key_column
        sdf_query_data_source = sdf.sdf_query_data_source
        sql_target_columns = self.get_sql_for_target_columns(columns)
        sql_k_fit_rows = self.get_sql_from_for_k_fit_rows(sdf_query_data_source, columns, key)
        sql_k_fit_columns = self.get_sql_from_for_columns(sdf_query_data_source, columns)
        

        sql = "SELECT k_fit_rows." + key + ",\n" + sql_target_columns + "\nFROM \n(\n" 
        sql += sql_k_fit_rows + "\n)\nAS k_fit_rows\nINNER JOIN\n(" + sql_k_fit_columns + "\n)\nAS k_pred_cols ON k_pred_cols.row_index = k_fit_rows.row_index"

        #source_columns, target_columns, column_functions, sub_table
        target_columns = [column + "_encoded" for column in columns]
        column_functions = ["{join_table}." + column for column in columns]
        sdf.add_multiple_column_transformation(columns, target_columns, column_functions, sql)


    def load_from_sklearn(self, sklearn_function, sdf, column):
        if (type(sklearn_function) is not sp.KernelCenterer): raise ValueError("argument is not of type sklearn.preprocessing.KernelCenterer")
        
        self.k_fit_all = sklearn_function.K_fit_all_
        self.k_fit_row = sklearn_function.K_fit_rows_
        
        return self


# end of class SqlKernelCenterer



'''
        fit

        1 - get number of columns (or rows??)
        n_samples = K.shape[0]

        2 - get sum of values on earch row divided by number of samples, each value goes into a separate column
        self.K_fit_rows_ = np.sum(K, axis=0) / n_samples

        3 - get sum or previous result devided by number of samples 
        self.K_fit_all_ = self.K_fit_rows_.sum() / n_samples
        return self


        transform

        1 - get sum of values in each column, each devided by number of rows, and organized as single column/array

                K_pred_cols = (np.sum(K, axis=1) /
                       self.K_fit_rows_.shape[0])[:, np.newaxis]

        2 - from each element of a row minus value from K_fit_rows_ in corresponding column
        K -= self.K_fit_rows_

        3 - each elemnt minus value from K_pred_cols of its row
        K -= K_pred_cols

        4 - to each element add K_fit_all_
        K += self.K_fit_all_





--fit
SELECT 
     SUM(CASE WHEN row_index =  1 THEN k_fit_row END) c1,
     SUM(CASE WHEN row_index =  2 THEN k_fit_row END) c2,
	 SUM(CASE WHEN row_index =  3 THEN k_fit_row END) c3
from 
(
select cast((c1 + c2 + c3) as float) / 3 as k_fit_row, ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index from s1.kc1
) as a;


select sum(k_fit_row) / 3 as k_fit_all
from (
	select cast((c1 + c2 + c3) as float) / 3 as k_fit_row from s1.kc1
) as a;


-- transform 

select 
kc.c1 - k_pred_cols.k_pred_col + 2 as c1,
kc.c2 - k_pred_cols.k_pred_col + 2as c2,
kc.c3 - k_pred_cols.k_pred_col + 2 as c3
from 
(
	select kc1.c1 - k_fit_rows.c1 as c1,
	kc1.c2 - k_fit_rows.c2 as c2,
	kc1.c3 - k_fit_rows.c3 as c3,
	ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index
	from s1.kc1,
	(
		SELECT 
			 SUM(CASE WHEN row_index =  1 THEN k_fit_row END) c1,
			 SUM(CASE WHEN row_index =  2 THEN k_fit_row END) c2,
			 SUM(CASE WHEN row_index =  3 THEN k_fit_row END) c3
		from 
		(
		select cast((c1 + c2 + c3) as float) / 3 as k_fit_row, ROW_NUMBER() OVER (ORDER BY (SELECT 100)) row_index from s1.kc1
		) as a
	) as k_fit_rows
) as kc
inner join 
(
	select sum(c1) / 3 as k_pred_col, 1 as row_index
	from s1.kc1
	union
	select sum(c2) / 3, 2
	from s1.kc1
	union
	select sum(c3) / 3, 3
	from s1.kc1
) as k_pred_cols on kc.row_index = k_pred_cols.row_index

'''








# Class: KBinsDiscretizer
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.KBinsDiscretizer.html#sklearn.preprocessing.KBinsDiscretizer
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/preprocessing/_discretization.py
# At this point, functionality is limited to ordinal encoding with quantile strategy
class SqlKBinsDiscretizer (SqlFunction):


    def __init__(self, n_bins = 5, target_column = None):
        self.n_bins = n_bins
        self.target_column = target_column


    def __repr__(self):
        return "SqlKBinsDiscretizer(n_bins=%s, target_column=%s)" % (self.n_bins, self.target_column)


    def fit(self, sdf, column):

        # get list of edges - min and max values for each bin
        self.bin_edges = []

        sql = "SELECT MIN(" + column + "), MAX(" + column + ") FROM ("
        sql += "SELECT " + column + ", NTILE(" + str(self.n_bins) + ") OVER(ORDER BY " + column + ") AS nbin "
        sql += "FROM " + sdf.sdf_query_data_source + ") as data_source GROUP BY nbin ORDER BY nbin"

        result = sdf.dbconn.execute_query_cursor(sql)

        for row in result:
            self.bin_edges.append(row)

        result.close()


    def generate_bins_sql(self, column):

        sql = ""

        for i in range(len(self.bin_edges) - 1):
            sql +=  "WHEN " + column + " <= " + str(self.bin_edges[i][1]) + " THEN " + str(i + 1) + " "

        sql = "CASE " + sql + "ELSE " + str(len(self.bin_edges)) + " END"

        return sql

            
    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        column_function = self.generate_bins_sql(column)
        sdf.add_single_column_transformation(column, target_column, column_function, None)
    

    def load_from_sklearn(self, sklearn_function, sdf, column):            
        return self


# end of class SqlLabelBinarizer    







# Class: SimpleImputer
# https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
# Supported strategies mean, most_frequent, constant
class SqlSimpleImputer (SqlFunction):


    def __init__(self, strategy='mean', fill_value=None, cast_as=None, target_column = None):
        self.strategy = strategy
        self.fill_value = fill_value
        self.cast_as = cast_as
        self.target_column = target_column


    def __repr__(self):
        return "SqlSimpleImputer(strategy=%s, fill_value=%s, cast_as=%s, target_column=%s)" % (self.strategy, self.fill_value, self.cast_as, self.target_column)


    def fit(self, sdf, column):

        sql = None

        if (self.strategy == "mean"):
            sql = "AVG(" + column + ")"
            if (self.cast_as != None):
                sql = "CAST(" + sql + " AS " + self.cast_as + ")"
            sql = "SELECT " + sql + " FROM " + sdf.sdf_query_data_source

        elif (self.strategy == "most_frequent"):
            sql = "SELECT " + column + ", COUNT(" + column + ") AS value_frequency FROM " + sdf.sdf_query_data_source
            sql += " GROUP BY " + column + " ORDER BY value_frequency DESC LIMIT 1"

        else:
            return None

        result = sdf.dbconn.execute_query_cursor(sql)

        for row in result:
            self.fill_value = row[0]

        result.close()


    def generate_function_sql(self, column):
        
        if (isinstance(self.fill_value, str)):
            sql = "'" + self.fill_value + "'"
        else:
            sql = str(self.fill_value)

        sql = "COALESCE(" + column + ", " + sql + ")"

        return sql

            
    def transform(self, sdf, columns):
        column = columns if (not isinstance(columns, list)) else columns[0]
        target_column = self.target_column if (self.target_column is not None) else column
        column_function = self.generate_function_sql(column)
        sdf.add_single_column_transformation(column, target_column, column_function, None)
    

    def load_from_sklearn(self, sklearn_function, sdf, column):            
        return self


# end of class SqlSimpleImputer  
















# Class: SqlDataFrameMapper
# Same as DataFrameMapper from sklearn-pandas
# Maps SQL data source column subsets to transformations.
# https://github.com/scikit-learn-contrib/sklearn-pandas
class SqlDataFrameMapper:


    # self.features [columns, feature]

    def __init__(self, features):
        self.features = features

    def __repr__(self):

        string_list = ''

        for feature in self.features: 
            string_list += '\n\t(' + str(feature[0]) + ', ' + str(feature[1]) + ')'
            
        return "SqlDataFrameMapper(\nfeatures=[%s])" % string_list

    def fit(self, sdf):

        for feature in self.features: 
            columns = feature[0]
            function = feature[1]
            function.fit(sdf, columns)

    def transform(self, sdf):

        for feature in self.features: 
            column = feature[0]
            function = feature[1]
            function.transform(sdf, column)

    def fit_transform(self, sdf):
        self.fit(sdf)
        self.transform(sdf)


# end of class SqlDataFrameMapper





# Class: SqlColumnTransformer
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/compose/_column_transformer.py
class SqlColumnTransformer():


    # self.transformers [name, transformer, columns]

    def __init__(self, transformers):
        self.transformers = transformers


    def __repr__(self):

        string_list = ''

        for transformer in self.transformers: 
            string_list += '\n\t(' + str(transformer[0]) + ', ' + str(transformer[1]) + ', ' + str(transformer[2]) + ')'
            
        return "SqlColumnTransformer(transformers=[%s])" % string_list


    def fit(self, sdf):

        for transformer in self.transformers:
            function = transformer[1]
            columns = transformer[2]
            function.fit(sdf, columns)


    def transform(self, sdf):

        for transformer in self.transformers: 
            function = transformer[1]
            columns = transformer[2]
            function.transform(sdf, columns)


    def fit_transform(self, sdf):
        self.fit(sdf)
        self.transform(sdf)

# end of class SqlColumnTransformer







# Class: SqlPipeline
# Partial image of SqlPipeline from sklearn
# https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/pipeline.py
# --- https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/utils/metaestimators.py
class SqlPipeline():


    # self.steps [name, transformer]

    def __init__(self, steps, sklearn_steps = []):
        self.steps = steps
        self.sklearn_steps = sklearn_steps


    def __repr__(self):

        step_list = ''
        sklearn_step_list = ''

        for step in self.steps: 
            step_list += '\n\t(' + str(step[0]) + ', ' + str(step[1]) + ')'
            
        for step in self.sklearn_steps: 
            sklearn_step_list += '\n\t(' + str(step[0]) + ', ' + str(step[1]) + ')'

        return "SqlPipeline(steps=[%s],\nsklearn_steps=[%s])" % (step_list, sklearn_step_list)


    def fit(self, x_sdf, y_df=None, **fit_params):

        #fit sql transformers
        for step in self.steps[:len(self.steps) - 1]: 
            function = step[1]
            function.fit(x_sdf)

        #transform x_sdf to x_df to fit model
        x_sdf = x_sdf.clone()
        self.transform(x_sdf, skip_final_estimator = True)
        x_df = x_sdf.execute_df(return_df = True)

        # for sklearn steps (after retrieving df)
        for step in self.sklearn_steps[:len(self.steps) - 1]: 
            function = step[1]
            x_df = function.fit_transform(x_df)

        #fit final estimator
        final_estimator = self.steps[len(self.steps) - 1]
        model = final_estimator[1]
        model.fit(x_df, y_df, **fit_params)

        return self


    # populates sdf but does not execute sklearn transformers
    def transform(self, x_sdf, skip_final_estimator = False):

        len_to_skip = 1 if (skip_final_estimator) else 0

        for step in self.steps[:len(self.steps) - len_to_skip]: 
            function = step[1]
            function.transform(x_sdf)

        return self


    # retrives data from sdf and applies sklearn transformers
    def execute_df(self, x_sdf, return_df = True):

        x_df = x_sdf.execute_df(return_df = True)

        # apply sklearn transformers if defined
        for step in self.sklearn_steps[:len(self.steps) - 1]: 
            function = step[1]
            x_df = function.transform(x_df)

        return x_df


    def fit_transform(self, x_sdf, y_df=None, **fit_params):
        self.fit(x_sdf, y_df, **fit_params)
        return self.transform(x_sdf)


    def predict(self, x_sdf, **predict_params):

        self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        return self.steps[-1][-1].predict(x_df, **predict_params)


    def fit_predict(self, x_sdf, y_df=None, **fit_params):
        self.fit(x_sdf, y_df, **fit_params)
        return self.predict(x_sdf, **fit_params)

    
    def score_samples(self, x_sdf):

        self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        return self.steps[-1][-1].score_samples(x_df)


    def score(self, x_sdf, y_df=None, sample_weight=None):

        x_sdf = x_sdf.clone()
        self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        score_params = {}
        if sample_weight is not None:
            score_params['sample_weight'] = sample_weight
        return self.steps[-1][-1].score(x_df, y_df, **score_params)


# end of class SqlPipeline










# Class: SqlNestedPipeline
# Allows for nested transformations where each ColumnTransnformer is nested into following ColumnTransformer
class SqlNestedPipeline():


    # self.steps [name, transformer]

    def __init__(self, steps, sklearn_steps = []):
        self.steps = steps
        self.sklearn_steps = sklearn_steps


    def __repr__(self):

        step_list = ''
        sklearn_step_list = ''

        for step in self.steps: 
            step_list += '\n\t(' + str(step[0]) + ', ' + str(step[1]) + ')'
            
        for step in self.sklearn_steps: 
            sklearn_step_list += '\n\t(' + str(step[0]) + ', ' + str(step[1]) + ')'

        return "SqlNestedPipeline(steps=[%s],\nsklearn_steps=[%s])" % (step_list, sklearn_step_list)


    def fit(self, x_sdf, y_df=None, **fit_params):

        x_sdf = self.nested_sql_fit_transform(x_sdf)
          
        #get x_sdf to x_df to fit model
        x_df = x_sdf.execute_df(return_df = True)

        # for sklearn steps (after retrieving df)
        for step in self.sklearn_steps[:len(self.steps) - 1]: 
            function = step[1]
            x_df = function.fit_transform(x_df)

        #fit final estimator
        final_estimator = self.steps[len(self.steps) - 1]
        model = final_estimator[1]
        model.fit(x_df, y_df, **fit_params)

        return self


    def nested_sql_fit_transform(self, x_sdf):

        copy_x_sdf = None

        #fit sql transformers - every step output is input into next step
        # to generate sql, transform must follow fit before moving onto next step
        for step in self.steps[:len(self.steps) - 1]: 
            
            if (copy_x_sdf is None):
                copy_x_sdf = x_sdf.clone()
            else: 
                copy_x_sdf = copy_x_sdf.clone_as_sql_source()

            function = step[1]
            function.fit(copy_x_sdf)
            function.transform(copy_x_sdf)

        return copy_x_sdf


    # populates sdf but does not execute sklearn transformers
    def transform(self, x_sdf, skip_final_estimator = False):

        len_to_skip = 1 if (skip_final_estimator) else 0
        copy_x_sdf = None

        for step in self.steps[:len(self.steps) - len_to_skip]: 
            if (copy_x_sdf is None):
                copy_x_sdf = x_sdf.clone()
            else: 
                copy_x_sdf = copy_x_sdf.clone_as_sql_source()

            function = step[1]
            function.transform(copy_x_sdf)

        return copy_x_sdf


    # retrives data from sdf and applies sklearn transformers
    def execute_df(self, x_sdf, return_df = True):

        x_df = x_sdf.execute_df(return_df = True)

        # apply sklearn transformers if defined
        for step in self.sklearn_steps[:len(self.steps) - 1]: 
            function = step[1]
            x_df = function.transform(x_df)

        return x_df


    def fit_transform(self, x_sdf, y_df=None, **fit_params):
        self.fit(x_sdf, y_df, **fit_params)
        return self.transform(x_sdf)


    def predict(self, x_sdf, **predict_params):

        x_sdf = self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        return self.steps[-1][-1].predict(x_df, **predict_params)


    def fit_predict(self, x_sdf, y_df=None, **fit_params):
        self.fit(x_sdf, y_df, **fit_params)
        return self.predict(x_sdf, **fit_params)

    
    def score_samples(self, x_sdf):

        x_sdf = self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        return self.steps[-1][-1].score_samples(x_df)


    def score(self, x_sdf, y_df=None, sample_weight=None):

        x_sdf = self.transform(x_sdf, True)
        x_df = self.execute_df(x_sdf, return_df = True)

        score_params = {}
        if sample_weight is not None:
            score_params['sample_weight'] = sample_weight
        return self.steps[-1][-1].score(x_df, y_df, **score_params)


# end of class SqlNestedPipeline





# Class: SqlPipelineSerializer
# File serialilzation is based on joblib (superseeds pickle)
# https://joblib.readthedocs.io/en/latest/persistence.html
class SqlPipelineSerializer:

    @classmethod
    def dump_pipeline_to_file(cls, pipeline, filename):
        joblib.dump(pipeline, filename)
        
    @classmethod
    def load_pipeline_from_file(cls, filename):
        return joblib.load(filename)

    @classmethod
    def store_pipeline_to_db(cls, pipeline):
        return None

    @classmethod
    def load_pipeline_from_db(cls):
        return None









# Class: SqlPipelineTestModel
# Dummy model class which allows to build pipelines and test results of transformations
# This model can be added as a classifier at the end of pipeline to display pipline transformation results
class SqlPipelineTestModel():

    def __init__(self, print_df = False):
        self.print_df = print_df
        

    def __repr__(self):
        return "SqlPipelineTestModel(print_df=[%s])" % self.print_df


    def fit(self, x_df, y_df=None, **fit_params):
        if (self.print_df):
            print('SqlPipelineTestModel - fit:')
            print(x_df)


    def transform(self, x_df, y_df=None, **fit_params):
        if (self.print_df):
            print('SqlPipelineTestModel - transform:')
            print(x_df)


    def predict(self, x_sdf, **predict_params):
        if (self.print_df):
            print('SqlPipelineTestModel - predict:')
            print(x_sdf)


    def score(self, x_sdf, y_df=None, **score_params):
        if (self.print_df):
            print('SqlPipelineTestModel - score:')
            print(x_sdf)


# end of class SqlPipelineTestModel

