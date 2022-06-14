# SQLDataPrep4ML - SQL Data Preprocessing Library

Library providing implementation of Machine Learning data pre-processing functions which allows to process data directly in SQL database. 
As opposed to the traditional in-memory execution approach of traditional libraries such as SKLearn, the mechanism does not retrieve the data from the source but instead dynamically generates SQL queries and executes them in RDBMS where the data are stored.

The current version supports following RDBMS backends: IBM DB2 (LUW and Z) and PostgreSQL

### Structure of the library 
+ Implementation of namespaces of the following standard libraries:
	+ sklearn.preprocessing: Preprocessing and Normalization
https://scikit-learn.org/stable/modules/classes.html#module-sklearn.preprocessing
	+ sklearn.impute: Impute (to be included)
https://scikit-learn.org/stable/modules/classes.html#module-sklearn.impute
	+ sklearn.pipeline: Pipeline
https://scikit-learn.org/stable/modules/classes.html#module-sklearn.pipeline
	+ sklearn_pandas.DataFrameMapper
https://github.com/scikit-learn-contrib/sklearn-pandas/tree/master/sklearn_pandas

+ Additional features:
	+ Code transformation functions: 
Transformation of existing python code and pickle files to code based on SQL (in progress)
	+ Tests: 
A test suite to validate and compare preprocessing functions implemented in SQL vs Sklearn on a given dataset
	+ Performance comparison functions: 
A test suite to execute preprocessing fucntions implemented in SQL vs Sklearn on a given dataset to compare performance



### Setup 

#### Dependencies
+ SQL Alchemy - https://www.sqlalchemy.org/
+ Sklearn
+ Sklearn_pandas
+ Pandas
+ Numpy
+ Scipy


#### DB2 driver
##### Driver to DB2 requires following libraries:
+ Python IBM DB2: https://github.com/ibmdb/python-ibmdb
+ IBM DB2 for SLQ Alchemy: https://pypi.org/project/ibm-db-sa/
+ Additional documentation: https://www.ibm.com/support/knowledgecenter/cs/SSEPGG_9.7.0/com.ibm.swg.im.dbclient.python.doc/doc/c0054366.html


###### DB2 Driver - setup steps
1. pip - https://github.com/ibmdb/python-ibmdb
2. install from source https://pypi.org/project/ibm-db-sa/



#### DB2 driver

##### Specifying DB2 Connection
A TCP/IP connection can be specified as follows:
e = create_engine("db2+ibm_db://user:pass@host[:port]/database")

For a local socket connection, exclude the "host" and "port" portions:
e = create_engine("db2+ibm_db://user:pass@/database")



### Directories
+ sp_preprocessing - the library python module
+ Tests and Examples - series of files showcasing various aspects of the the library
+ Jupyter Notebook Examples
+ Performance Tests