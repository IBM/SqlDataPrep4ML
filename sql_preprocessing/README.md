<h1>Table Of Content</h1>

- [**sql_preprocessing**](#sql_preprocessing)
- [**sp.py**](#sppy)
- [**Structure of this module**](#structure-of-this-module)
- [**Classes**](#classes)
  - [**Class : SqlConnection**](#class--sqlconnection)
    - [Parameters](#parameters)
    - [Attributes](#attributes)
    - [Examples](#examples)
    - [Methods](#methods)
  - [**Class : TableCatalog**](#class--tablecatalog)
    - [Parameters](#parameters-1)
    - [Methods](#methods-1)
  - [**Class : InMemoryTableCatalog**](#class--inmemorytablecatalog)
    - [Parameters](#parameters-2)
    - [Methods](#methods-2)
  - [**Class : InDbTableCatalog**](#class--indbtablecatalog)
    - [Parameters](#parameters-3)
    - [Methods](#methods-3)
  - [**Class : SqlDataFrame**](#class--sqldataframe)
    - [Parameters](#parameters-4)
    - [Methods](#methods-4)
  - [**Class : SklearnToSqlConverter**](#class--sklearntosqlconverter)
    - [Methods](#methods-5)
  - [**Class : SqlFunction**](#class--sqlfunction)
    - [Methods](#methods-6)
  - [**Class : SqlPassthroughColumn**](#class--sqlpassthroughcolumn)
    - [Parameters](#parameters-5)
    - [Methods](#methods-7)
  - [**Class : SqlUDFTransformer**](#class--sqludftransformer)
    - [Parameters](#parameters-6)
    - [Methods](#methods-8)
  - [**Class : SqlCustomSqlTransformer**](#class--sqlcustomsqltransformer)
    - [Parameters](#parameters-7)
    - [Methods](#methods-9)
  - [**Class : SqlCaseEncoder**](#class--sqlcaseencoder)
    - [Parameters](#parameters-8)
    - [Methods](#methods-10)
  - [**Class : SqlMapEncoder**](#class--sqlmapencoder)
    - [Parameters](#parameters-9)
    - [Methods](#methods-11)
  - [**Class : SqlMinMaxScaler**](#class--sqlminmaxscaler)
    - [Parameters](#parameters-10)
    - [Methods](#methods-12)
  - [**Class : SqlMaxAbsScaler**](#class--sqlmaxabsscaler)
    - [Parameters](#parameters-11)
    - [Methods](#methods-13)
  - [**Class : SqlStandardScaler**](#class--sqlstandardscaler)
    - [Parameters](#parameters-12)
    - [Methods](#methods-14)
  - [**Class : SqlLabelEncoder**](#class--sqllabelencoder)
    - [Parameters](#parameters-13)
    - [Methods](#methods-15)
  - [**Class : SqlOrdinalEncoder**](#class--sqlordinalencoder)
    - [Parameters](#parameters-14)
    - [Methods](#methods-16)
  - [**Class : SqlOneHotEncoder**](#class--sqlonehotencoder)
    - [Parameters](#parameters-15)
    - [Methods](#methods-17)
  - [**Class : SqlLabelBinarizer**](#class--sqllabelbinarizer)
    - [Parameters](#parameters-16)
    - [Methods](#methods-18)
  - [**Class : SqlNormalizer**](#class--sqlnormalizer)
    - [Parameters](#parameters-17)
    - [Methods](#methods-19)
  - [**Class : SqlKernelCenterer**](#class--sqlkernelcenterer)
    - [Parameters](#parameters-18)
    - [Methods](#methods-20)
  - [**Class : SqlKBinsDiscretizer**](#class--sqlkbinsdiscretizer)
    - [Parameters](#parameters-19)
    - [Methods](#methods-21)
  - [**Class : SqlSimpleImputer**](#class--sqlsimpleimputer)
    - [Parameters](#parameters-20)
    - [Methods](#methods-22)
  - [**Class : SqlDataFrameMapper**](#class--sqldataframemapper)
    - [Parameters](#parameters-21)
    - [Methods](#methods-23)
  - [**Class : SqlColumnTransformer**](#class--sqlcolumntransformer)
    - [Parameters](#parameters-22)
    - [Methods](#methods-24)
  - [**Class : SqlPipeline**](#class--sqlpipeline)
    - [Parameters](#parameters-23)
    - [Methods](#methods-25)
  - [**Class : NestedPipeline**](#class--nestedpipeline)
    - [Parameters](#parameters-24)
    - [Methods](#methods-26)
  - [**Class : SqlPipelineSerializer**](#class--sqlpipelineserializer)
    - [Parameters](#parameters-25)
    - [Methods](#methods-27)
  - [**Class : SqlPipelineTestModel**](#class--sqlpipelinetestmodel)
    - [Parameters](#parameters-26)
    - [Methods](#methods-28)
# **sql_preprocessing**
The library python module


# **sp.py**
+ SQL Data Preprocessing Core Module </br>
This module contains the core classes implementing the Machine Learning data pre-processing 
functions as a client generated SQL executed in SQL based DBMS 
as opposed to the traditional in-memory execution in traditional libraries such as SKLearn.

# **Structure of this module**

![ClassDiagram](classDiagram/UML%20Class%20Diagram.png 'ClassDiagram1')

# **Classes**
## **Class : SqlConnection**
Connection to DBMS engine with functions for invocation of SQL statements. Encapsulates SQL Alchemy Engine and Connection.
Encapsulates SQL Alchemy `Engine and Connection`_.
    
### Parameters

+ connection_string : string
    SQL Alchemy `connection string`_
+ print_sql : bool
    If True all SQL statements submitted to database are printed into output
### Attributes

+ engine : sqlalchemy.engine.Engine
    Instance of sqlalchemy.engine.Engine used to submit SQL statements to the DB.
+ dbtype : DbType
    The type of underlying Database engine. 
    It is used whitin the library to selected the dialect of SQL statements to generate. 
    This attribute is inferred from the conneciton string.
    
### Examples

Postgres connection
\>>> dbconn = SqlConnection("postgres://postgres:password@localhost:5432/db1", print_sql=True)
DB2 connection

\>>> dbconn = SqlConnection("db2+ibm_db://user:password@test.ibm.com:5100/A2B", print_sql=True)

.. _Engine and Connection:
    https://docs.sqlalchemy.org/en/13/core/engines_connections.html

.. _connection string:
    https://docs.sqlalchemy.org/en/13/core/connections.html

### Methods
<hr>

**get_sdf**<br>        

Creates a new instance of :class:`SqlDataFrame`<br>
For description of parameters see documentation of :class:`SqlDataFrame`<br>

Returns: 
+ sdf A new instance of :class:`SqlDataFrame`<br>
<hr>

**get_sdf_for_table**<br>        

Creates a new instance of :class:`SqlDataFrame` for an existing table or a view.<br>
For description of parameters see documentation of :class:`SqlDataFrame`<br>

Returns: 
+ sdf A new instance of :class:`SqlDataFrame`<br>
<hr>

**get_sdf_for_query**<br>        

Creates a new instance of :class:`SqlDataFrame` for a SQL query<br>
Can be used only in limited cases (such as for nesting of sdfs) because many functions require table as source and not sql<br>
For description of parameters see documentation of :class:`SqlDataFrame`<br>

Returns: 
+ sdf A new instance of :class:`SqlDataFrame`<br>
<hr>

**print_command**<br>        

Prints SQL statement to output<br>

Parameters:<br> 
+ sql : The sql statement to print<br>
<hr>

**execute_command**<br>        

Executes SQL statement with no output<br>

Parameters:<br> 
+ sql : The sql statement to execute<br>

Raises:<br> 
+ Exception : Exception received from SqlAlchemy connection<br>
<hr>

**execute_query_onerow**<br>        

Executes SQL statement and retrieves the first row<br>

Parameters:<br> 
+ sql : The sql to execute<br>

Returns:<br> 
+ Row : A single result row of type sqlalchemy.engine.RowProxy (https://docs.sqlalchemy.org/en/13/core/connections.html?highlight=fetchone#sqlalchemy.engine.RowProxy)<br>
<hr>

**execute_query_cursor**<br>        

Executes SQL statement and returns a cursor for fetching rows<br>

Parameters:<br> 
+ sql : The sql statement to execute<br>

Returns<br>
+ Cursor : A cursor of type sqlalchemy.engine.ResultProxy (https://docs.sqlalchemy.org/en/13/core/connections.html?highlight=fetchone#sqlalchemy.engine.ResultProxy)<br>
<hr>

**drop_table**<br>        

Drops table in the database<br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the table<br>
<hr>

**upload_df_to_db**<br>        

Stores \<pandas.DataFrame> into a database table<br>

Parameters:<br> 
+ df : The DataFrame to store<br>
+ schema : The schema of the table<br>
+ table : The name of the table<br>
<hr>

**execute_sql_to_df**<br>        

Executes SQL statement and returns \<pandas.DataFrame><br>

Parameters:<br> 
+ sql : The sql to execute<br>
<hr>

**get_table_as_df**<br>        

Retrieves a table stored in database as returns it as \<pandas.DataFrame><br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the table<br>
+ order_by : The content of ORDER BY clause of the SQL statement, defining ordering of the rows<br>
<hr>

**table_exists**<br>        

Returns true if table exists<br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the table<br>
<hr>

**column_exists**<br>        

Returns true if column exists<br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the table<br>
+ column  : The name of the column<br>
<hr>

**get_table_schema**<br>        

Returns \<pandas.DataFrame> with a schema (the list of columns with names, datatypes, etc) of the table or view<br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the tablee<br>
<hr>

**create_unique_key**<br>        

Adds identity column to the table and creates unique index on the column<br>

Parameters:<br> 
+ schema : The schema of the table<br>
+ table : The name of the tablee<br>
+ column : The name of the column to add<br>
<hr>

**close**<br>        

Close the sql connection<br>
<hr>

## **Class : TableCatalog**
Catalog of the tables
    
### Parameters
For description of parameters see documentation of :class:`SqlDataFrame`<br>

+ dbonn : SQL Alchemy connection to DBMS
+ sdf_name : The name of the SqlDataFrame under which all tables will be registered
+ dataset_schema : The schema of the SqlDataFrame under which all tables will be registered
+ dataset_table : The table of the SqlDataFrame under which all tables will be registered
+ fit_schema : The schema in which temporary tables created by split and fit functions will be created

### Methods
<hr>

**get_fit_table_name**<br>        
Retrieves the name of the name of the table<br>

Returns: 
+ str The name of the fit table<br>
<hr>

**register_fit_table**<br>        
Register the table on the DB<br>
<hr>

**drop_fit_table**<br>        
Drop the table on the DB<br>
<hr>

## **Class : InMemoryTableCatalog**
Subclass of `TableCatalog`, Catalog of the in-memory tables
    
### Parameters
For description of parameters see documentation of :class:`SqlDataFrame`<br>

+ dbonn : SQL Alchemy connection to DBMS
+ sdf_name : The name of the SqlDataFrame under which all tables will be registered
+ dataset_schema : The schema of the SqlDataFrame under which all tables will be registered
+ dataset_table : The table of the SqlDataFrame under which all tables will be registered
+ fit_schema : The schema in which temporary tables created by split and fit functions will be created
+ tables : The set of the in-memery tables

### Methods
<hr>

**clone**<br>        
Clone the InMemoryTableCatalog of the table in the DB<br>

Returns: 
+ InMemoryTableCatalog : A Instance of InMemoryTableCatalog for the table<br>
<hr>

**is_table_registered**<br>        
Returns True if the table is registered<br>
<hr>

**register_table**<br>        
Register the table on the in-memory table catalog<br>
<hr>

**unregister_table**<br>        
Unregister the table on the in-memory table catalog<br>
<hr>

**get_list_of_tables**<br>        
Returns registered tables on the in-memory table catalog<br>
<hr>

**drop_temporary_table**<br>        
Drop all the tables on the in-memory table catalog<br>
<hr>

## **Class : InDbTableCatalog**
Subclass of `TableCatalog`, Catalog of the tables created temporily in the DB <br>
Several functions of SqlDataFrame create new temporary tables in the DB such as split or fit functions of some transformers. 
To keep track of these, every created table is recorded into the catalog table stored in the DB.<br> 
To associate tables with a specific SqlDataFrame, each SqlDataFrame has a unique name (SqlDataFrame.sdf_name), 
which is recorded into the catalog table along with the new table details.<br>
In order to keep track of the tables created by a SqlDataFrame derived from an underlying SqlDataFrame,
such in case when a new SqlDataFrame represents train table produced by split function from the original SqlDataFrame; 
the derived SqlDataFrame uses the same name as the source SqlDataFrame.<br>
    
### Parameters
For description of parameters see documentation of :class:`SqlDataFrame`<br>

+ dbonn : SQL Alchemy connection to DBMS
+ sdf_name : The name of the SqlDataFrame under which all tables will be registered
+ dataset_schema : The schema of the SqlDataFrame under which all tables will be registered
+ dataset_table : The table of the SqlDataFrame under which all tables will be registered
+ fit_schema : The schema in which temporary tables created by split and fit functions will be created

### Methods
<hr>

**clone**<br>        
Clone the InDBTableCatalog of the table in the DB<br>

Returns: 
+ InMemoryTableCatalog : A Instance of InMemoryTableCatalog for the table<br>
<hr>

**create_catalog_table**<br>        
Create the catalog table in the DB<br>
<hr>

**drop_catalog_table**<br>        
Drop the catalog table in the DB<br>
<hr>

**is_table_registered**<br>        
Returns True if the table is registered<br>
<hr>

**register_table**<br>        
Register the table on the in-memory table catalog<br>
<hr>

**unregister_table**<br>        
Unregister the table on the in-memory table catalog<br>
<hr>

**get_list_of_tables**<br>        
Returns registered tables on the in-memory table catalog<br>
<hr>

**drop_temporary_table**<br>        
Drop all the tables on the in-memory table catalog<br>
<hr>

## **Class : SqlDataFrame**
Representation of the underlying dataset (table, view, or query) and of the transformations applied to it.<br>
It is similar to :class:`pandas.DataFrame`, however, the data are held in the DB. <br>
SqlDataFrame(SDF) has always a connection to DBMS and points to a specific dataset.<br>   
    
SDF provides two sets of functions:
1. Functions which allow to work with the uderlying dataset, such as retreiving data or modifying the dataset.
2. Functions which allow to define and execute transformations applied to the dataset. The result of transformation is either retrieved to memory or stored into a DB table. 
    
### Parameters

+ dbconn : SqlConnection Connection to DBMS.    
+ sdf_name : A unique name of the SqlDataFrame. The name is used to register and manage all related temporary tables. <br>
For more information see :class: `SqlDbTableCatalog`.
+ sdf_query_data_source : 
The FROM clause of SQL statement. This can be either a fully qualified name of table or view, or it can be a SQL statement returning a table. <br>
This mechanism allows to nest queries as sources of data for transformations. <br>
If the value is not an existing table of view, some dataset related functions will not work correctly. <br>
+ dataset_schema : The schema of of the underlying table.
+ dataset_table : The name of the underlying table or view.
+ key_column : The name of a column in the dataset holding a unique row key. <br>
Some transformation functions require a unique column to function correctly. For more details see descrition of invidual functions.
+ catalog_schema : The schema in which the catalog table is stored.<br>
+ For more details see :class: `SqlDbTableCatalog`.
+ fit_schema : The schema in which temporary tables created by split and fit functions will be created.
+ default_order_by : Defines the order of rows. <br>
It is expressed as the ORDER BY clause of the generated transformation sql statements. <br>
This default value is used if not other order_by argument is specified while invoking SQL generating functions.<br>     
  + Note:<br>
    The ORDER BY argument is important when comparing results with outside of db data (i.e. with DataFrame). 
    This is because if not ordered, the retrieved data may have different order.
+ **kwargs : Addtional arguments used when tables are created.
Currently supported arguments are for DB2 on z:<br>
    1. db2_create_catalog_table_in : The part of SQL statement defining in which database to create a catalog table.
    2. db2_create_fit_table_in : The part of SQL statement defining in which database to create new tables with split and fit functions.

            Example::

                {
                    'db2_create_catalog_table_in' : 'IN DATABASE DBLG', 
                    'db2_create_fit_table_in' : 'IN DATABASE DBLG'
                }

### Methods
<hr>

**clone**<br>        

Creates copy of the SDF. Copies connection and data source attributes but not transformations<br>
For description of parameters see documentation of :class:`SqlDataFrame`<br>

Parameters: 
+ sdf_name : Name of the new SDF. If not supplied, the name is copied from the orignal SDF<br>
Returns: 
+ sdf : A new instance of :class:`SqlDataFrame`<br>
<hr>

**clone_as_sql_source**<br>        

Creates copy of the SDF. Copies connection and populates the new SDF with generated SQL as a data source.<br>
Allows to create nested SDFs<br>

Parameters: 
+ sdf_name : Name of the new SDF. If not supplied, the name is copied from the orignal SDF<br>
+ include_source_columns : For each transformed column add the source column into output<br>
+ limit : The number of output rows. If None, all rows are included<br>
+ include_all_source_columns : Add all source dataset columns into output<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>

  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty.


Returns: 
+ sdf A new instance of :class:`SqlDataFrame`<br>
<hr>

**add_column_to_output**<br>        

Adds column to the output<br>

Parameters: 
+ source_column : The name of the column in the underlying dataset to be transformed<br>
+ target_column : The name of the column in the output table<br>
<hr>

**add_single_column_transformation**<br>        

Adds transformation of a single column to the output<br>

Parameters: 
+ source_column : The name of the column in the underlying dataset to be transformed<br>
+ target_column : The name of the column in the output table<br>
+ column_function : The column transformation SQL i.e. representation of the transformation logic<br>
+ fit_tables : The name of a fit table, if the transformation referres to such table. This table will be joined in the transformation SQL<br>
<hr>

**add_multiple_column_transformation**<br>        

Adds transformation of a multiple columns to the output<br>

Parameters: 
+ source_columns : The name of the columns in the underlying dataset to be transformed<br>
+ target_columns : The name of the columns in the output table<br>
+ column_functions : The column transformation SQL i.e. representation of the transformation logic<br>
+ fit_tables : The name of fit tables, if the transformation referres to such table. This table will be joined in the transformation SQL<br>
+ sub_tables : The name of a table referred to in the column_function. The table will be joined by LEFT OUTER JOIN.<br>
  + Note: if provided, the table must have a unique key column named same as the source table key column (key_column)
<hr>

**generate_sql**<br>        

Generates transformation SQL statement from the list of transformation functions applied on the SDF<br>

Parameters: 
+ include_source_columns : If True, for each transformed column add the source column into output<br>
+ limit : The number of output rows. If None, all rows are included<br>
+ include_all_source_columns : Add all source dataset columns into output<br>
+ order_by : Defines the order of rows.<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note : ordering should be used only when needed for testing purposes. It carries performance penalty
+ replace_data_source : If provided, overides the sdf_replace_data_source.<br>
Most likely with a predefined string constant which can be later found and replaced in the statement<br>
This is useful when the generated SQL statement is inteded to be used later with a different data source<br>
+ replace_fit_schema : Same as replace_data_source, if provided, it allows to overide the fit_schema with a different string<br>
Returns:
+ str : The Sql statements generated<br>
<hr>

**head**<br>        

Generates transformation SQL and retrieves the first n rows<br>
Replicates :meth:`pandas.DataFrame.head` <br>

Parameters: 
+ limit : The number of output rows (Default 5)<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ include_source_columns : If True, for each transformed column add the source column into output<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**get_table_head**<br>        

Retrieves the first n rows from the underlying dataset<br>
Replicates :meth:`pandas.DataFrame.head` <br>

Parameters: 
+ limit : The number of output rows (Default 5)<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ include_source_columns : If True, for each transformed column add the source column into output<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**info**<br>        

Returns the information schema of the underlying dataset (table or view)<br>
Note: the schema struture and content is platform dependent <br>
<hr>

**get_table_size**<br>        

Returns the number of rows in the underlying dataset<br>
<hr>

**shape**<br>        

Returns the dimensions of the underlying dataset<br>
<hr>

**execute_df**<br>        

Executes the transformation SQL and retrieves the output table into memory<br>

Parameters: 
+ include_source_columns : If True, for each transformed column add the source column into output<br>
+ limit : The number of output rows<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**execute_sample_df**<br>        

Generates transformation SQL and retrieves a random sample of the rows<br>
Replicates :meth:`pandas.DataFrame.sample` <br>

Parameters: 
+ include_source_columns : If True, for each transformed column add the source column into output<br>
+ n : The number of output rows<br>
+ frac : Fraction of axis items to return. Cannot be used with n<br>
+ random_state : Seed for the random number generator<br>
<hr>

**execute_transform_to_table**<br>        

Executes the transformation SQL and stores the output into a new table<br>

Parameters: 
+ target_schema : The schema of the new table<br>
+ target_table : The name of the new table<br>
+ register_in_catalog : If True, registers the new table in the SDF catalog<br>
<hr>

**execute_sample_transform_to_table**<br>        

Generates transformation SQL and rstores the output into a new table<br>
Replicates :meth:`pandas.DataFrame.sample` <br>

Parameters: 
+ target_schema : The schema of the new table<br>
+ target_table : The name of the new table<br>
+ register_in_catalog : If True, registers the new table in the SDF catalog<br>
+ n : The number of output rows<br>
+ frac : Fraction of axis items to return. Cannot be used with n<br>
+ random_state : Seed for the random number generator<br>
<hr>

**add_unique_id_column**<br>        

Add an new unique key column to the dataset<br>

Parameters: 
+ column : If provided, the colum is created and key_column is set the new balue<br>
If not provided, the SDF.key_column is used<br>
<hr>

**get_table_column_df**<br>        

Retrives a single column from the underlying dataset<br>

Parameters: 
+ column : The column to retrieve<br>
+ limit : The number of output rows (Default 5)<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**get_table_columns_df**<br>        

Retrives a single column from the underlying dataset<br>

Parameters: 
+ columns : The list of columns to retrieve<br>
+ limit : The number of output rows<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**get_y_df**<br>        

Retrives a single column from the underlying dataset<br>

Parameters: 
+ column : The column to retrieve<br>
+ limit : The number of output rows<br>
+ return_df : If True, returns pandas.DataFrame, otherwise numpy.array<br>
+ order_by : Defines the order of rows<br>
It is expressed as the ORDER BY clause of the generated transformation sql statements<br>
If provided, overides the default_order_by<br>
  + Note: ordering should be used only when needed for testing purposes. It carries performance penalty
<hr>

**train_test_split**<br>        

Splits the underlying dataset into random train and test subsets<br>
The function creates two new tables and registers them in catalog<br>
The names of the new tables are based on the SDF.dataset_table with a suffix of _test and _train<br>
Replicates  :meth:`sklearn.model_selection.train_test_split` <br>

Parameters: 
+ test_size : The size of the train is a complement<br>
Should be between 0.0 and 1.0 and represent the proportion of the dataset to include in the test split<br>
+ random_state : Seed for the random number generator<br>
+ train_sdf_name : The name of the new SDF for train table. If not provided, the name is same as the source SDF<br>
+ test_sdf_name : The name of the new SDF for test table. If not provided, the name is same as the source SDF<br>
<hr>

+ Note <br>
    Fast method of selecting random subset from a large dataset with SQL
    To be fast the SQL must avoid order, join, group by, et
    1. assign random number to each row<br>
    There is an initial penalty for the random number assignment but this happens only once and is not repeated for subsequent querie
    2. to retrieve a subset, select range within the random variable domain e.g.: for 25% of variable 0 to 1 range can be 0 to 0.2
    3. add to “select” clause “where” with conditions of the rang
    4. since there can be somewhat more or less rows within the range than the needed fraction - the where condition needs to somewhat expand the range
    5. add LIMIT to the select to constrain the exact number of row
    6. If x and y split is needed for ML and the dataset is retrieved to client, this should be performed at the client
    Reference: https://www.sisense.com/blog/how-to-sample-rows-in-sql-273x-faster/

Examples:

method 1 above - its approximate split <br>
but if each row has uniue id starting from 1<br>
method 2 with use of rowid and rand<br>
method 3 use of map/join<br>
method 4 as today with assigning rand and order<br>
method 5 - approximate solution<br>

            SELECT setseed(0);
            select count(*)
            from S1.TITANIC
            where random() <= 0.05

**train_test_split_standard_sql**<br>        

Internal functions to complete ```train_test_split``` for postgres sql<br>
<hr>

**train_test_split_db2**<br>        

Internal functions to complete ```train_test_split``` for DB2<br>
<hr>

## **Class : SklearnToSqlConverter**
Function Converter from sklearn to SqlFunction<br>

### Methods
<hr>

**convert_function**<br>        

Convert sklearn function to SqlFunction<br>

Parameters: 
+ sklearn_function : ski-learn function<br>
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**convert_dataframemapper**<br>        

convert sklearn_pandas.dataframemaper to SqlFrameMapper<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ dataframemapper : sklearn_pandas.DataFrameMapper<br>
<hr>

## **Class : SqlFunction**
SqlFunction to apply sklearn functions
    
### Methods
<hr>

**fit_transform**<br>        

fit_transform function for SqlDataFrame<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>


## **Class : SqlPassthroughColumn**
Includes in output table a column from the source table
    
### Parameters

+ target_column : target columns to be included in the output table

### Methods
<hr>

**fit**<br>        

add the columns into the output table<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns to be added to the output table<br>
<hr>

## **Class : SqlUDFTransformer**
Subclass of SqlFunction, UDFTransformer
    
### Parameters

+ udf : the name of udf in database<br>
+ arguments : a list of arguments used to invoke the udf, the list must contain a string "{column}"" which will be replaced with the name of the actual column<br>
if arguments is None, the column will be the first and only argument

### Methods
<hr>

**transform**<br>        

apply UDFTransformer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the UDFTransformer<br>
<hr>

## **Class : SqlCustomSqlTransformer**
Subclass of SqlFunction, CustomSqlTransformer
    
### Parameters

+ udf : the name of udf in database<br>
+ custom_transformation : sql string used to transform the source table column, the string must contain a string "{column}" which will be replaced with the name of the actual column

### Methods
<hr>

**transform**<br>        

apply CustomSqlTransformer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply CustomSqlTransformer<br>
<hr>

## **Class : SqlCaseEncoder**
Subclass of SqlFunction, CaseEncoder
    
### Parameters

+ cases : an array of CASE conditions and values 
+ else_value : value used when no other options fit

### Methods
<hr>

**transform**<br>        

apply CaseEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlMapEncoder**
Subclass of SqlFunction, CaseEncoder
    
### Parameters

+ pairs : an array of key value pairs
+ else_value : value used when no other options fit

### Methods
<hr>

**transform**<br>        

apply MapEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlMinMaxScaler**
Subclass of SqlFunction, MinMaxScaler
    
### Parameters

+ target_column : target column to apply MinMaxScaler

### Methods
<hr>

**fit**<br>        

Calculate the min and max value in the column<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

apply MinMaxScaler<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply MapEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlMaxAbsScaler**
Subclass of SqlFunction, MaxAbsScaler
    
### Parameters

+ target_column : target column to apply MinMaxScaler

### Methods
<hr>

**fit**<br>        

Calculate the max value in the column<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

apply MaxAbsScaler<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply MaxAbsScaler<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlStandardScaler**
Subclass of SqlFunction, StandardScaler
    
### Parameters

+ target_column : target column to apply StandardScaler

### Methods
<hr>

**fit**<br>        

Compute the mean and std to be used for later scaling<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Perform standardization by centering and scaling<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply MapEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlLabelEncoder**
Subclass of SqlFunction, LabelEncoder
    
### Parameters

+ target_column : target column to apply LabelEncoder

### Methods
<hr>

**fit**<br>        

Fit label encoder - generates codes/indexes of labels<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ column : The column in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Transform labels to normalized encoding<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply LableEncoder<br>

Parameters: 
+ sklearn_function : sklearn function to apply<br>
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlOrdinalEncoder**
Subclass of LabelEncoder, OrdinalEncoder
    
### Parameters

+ target_column : target column to apply OrdinalEncoder

### Methods
<hr>

**fit**<br>        

Compute the mean and std to be used for later scaling<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ column : The column in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Encode categorical features as an integer array<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply OrdinalLabelEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlOneHotEncoder**
Subclass of SqlFunction, OneHotEncoder
    
### Parameters

+ target_column : target column to apply OneHotEncoder

### Methods
<hr>

**fit**<br>        

generate and execute list of columns sql<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ column : The column in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Encode categorical integer features as a one-hot numeric array<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply OneHotEncoder<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlLabelBinarizer**
Subclass of SqlFunction, LabelBinarizer
    
### Parameters

+ target_column : target column to apply LabelBinarizer

### Methods
<hr>

**fit**<br>        

generate and execute list of columns sql<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ column : The column in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Binarize labels in a one-vs-all fashion<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply LabelBinarizer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlNormalizer**
Subclass of SqlFunction, Normalizer
    
### Parameters

+ norm : l1, l2, or max norm
+ target_column : target column to apply Normalizer

### Methods
<hr>

**transform**<br>        

Normalize samples individually to unit norm<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply Normalizer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlKernelCenterer**
Subclass of SqlFunction,KernelCenterer
    
### Parameters

+ target_column : target column to apply KernelCenterer

### Methods
<hr>

**fit**<br>        

get average of each column and the matrix<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Center an arbitrary kernel matrix<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply KernelCenterer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlKBinsDiscretizer**
Subclass of SqlFunction,KBinsDiscretizer<br>
At this point, functionality is limited to ordinal encoding with quantile strategy
    
### Parameters

+ n_bins : number of bins
+ target_column : target column to apply KBinsDiscretizer

### Methods
<hr>

**fit**<br>        

get list of edges - min and max values for each bin<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

Bin continuous data into intervals<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**load_from_sklearn**<br>        

apply KBinsDiscretizer<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlSimpleImputer**
Subclass of SqlFunction,SimpleImputer<br>
    
### Parameters

+ strategy : mean (by default), most_frequent, constant
+ fill_values : assigned values to be fill on NA cells
+ cast_as : datatype to be casted
+ target_column : target column to apply KBinsDiscretizer

### Methods
<hr>

**fit**<br>        

get the values to be filled based on the strategy<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

**transform**<br>        

impute the NA values<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
+ columns : The columns in the SqlDataFrame to apply the function on<br>
<hr>

## **Class : SqlDataFrameMapper**
Same as DataFrameMapper from sklearn-pandas<br>
    
### Parameters

+ features : list of tuples (column, function)

### Methods
<hr>

**fit**<br>        

apply function.fit on the dataframe<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

**transform**<br>        

apply function.transform on the datafram<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

**fit_transform**<br>        

apply fit and transform operations on the dataframe<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

## **Class : SqlColumnTransformer**
Same as ColumnTransformer from sklearn-pandas<br>
    
### Parameters

+ transformers : list of tuples

### Methods
<hr>

**fit**<br>        

apply function.fit on the dataframe<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

**transform**<br>        

apply function.transform on the datafram<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

**fit_transform**<br>        

apply fit and transform operations on the dataframe<br>

Parameters: 
+ sdf : The name of the SqlDataFrame<br>
<hr>

## **Class : SqlPipeline**
Partial image of SqlPipeline from sklearn<br>
    
### Parameters

+ steps : The steps to apply SqlFunctions
+ sklearn_steps : The steps to apply sklearn functions

### Methods
<hr>

**fit**<br>        

fit sql transformers and transform x_sdf to x_df to fit model, then fit final estimators<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**transform**<br>        

populates sdf but does not execute sklearn transformers<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ skip_final_estimator : If True, skip the final estimator
<hr>

**fit_transform**<br>        

apply fit and transform operations on the dataframe<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**predict**<br>        

get the predicted values for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
<hr>

**fit_predict**<br>        

get the predicted values for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**score_samples**<br>        

get the fit score for the sample using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
<hr>

**score**<br>        

get the fit score for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
+ sample_weight : The weight to apply on the samples
<hr>

## **Class : NestedPipeline**
Allows for nested transformations where each ColumnTransnformer is nested into following ColumnTransformer<br>
    
### Parameters

+ steps : The steps to apply SqlFunctions
+ sklearn_steps : The steps to apply sklearn functions

### Methods
<hr>

**fit**<br>        

fit sql transformers and transform x_sdf to x_df to fit model, then fit final estimators<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**transform**<br>        

populates sdf but does not execute sklearn transformers<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ skip_final_estimator : If True, skip the final estimator
<hr>

**fit_transform**<br>        

apply fit and transform operations on the dataframe<br>

Parameters: 
+ x_ sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**predict**<br>        

get the predicted values for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
<hr>

**fit_predict**<br>        

get the predicted values for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe
<hr>

**score_samples**<br>        

get the fit score for the sample using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
<hr>

**score**<br>        

get the fit score for the dataframe using the fitted model<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
+ sample_weight : The weight to apply on the samples
<hr>

## **Class : SqlPipelineSerializer**
Pipeline file serialilzation, which is based on joblib (https://joblib.readthedocs.io/en/latest/persistence.html)<br>
    
### Parameters

+ steps : The steps to apply SqlFunctions
+ sklearn_steps : The steps to apply sklearn functions

### Methods
<hr>

**dump_pipeline_to_file**<br>        

dump pipeline into file<br>

Parameters: 
+ pipeline : The pipeline to be dumped<br>
+ filename : The file to save the pipeline
<hr>

**load_pipeline_to_file**<br>        

load pipeline from file<br>

Parameters: 
+ filename : The file to load the pipeline
<hr>

## **Class : SqlPipelineTestModel**
Dummy model class which allows to build pipelines and test results of transformations<br>
This model can be added as a classifier at the end of pipeline to display pipline transformation results<br>
    
### Parameters

+ print_df : If True, print the procedures in the test model

### Methods
<hr>

**fit**<br>        

print the dataframe after fitting<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
<hr>

**transform**<br>        

print the dataframe after transformation<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
<hr>

**predict**<br>        

print the dataframe after prediction<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
<hr>

**score**<br>        

print the dataframe after score calculation<br>

Parameters: 
+ x_sdf : The columns of the features in SqlDataFrame<br>
+ y_df : The column of the target in Dataframe<br>
<hr>