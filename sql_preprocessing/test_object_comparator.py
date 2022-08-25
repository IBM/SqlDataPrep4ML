#append system path to import cousin packages
import sys

from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LinearRegression
sys.path.append("/Users/weisun/Coding Projects/Machine Learning/SQLDATAPREP4ML")

from sql_preprocessing import *
from sql_preprocessing.sp_object_comparator import SpComparator
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.compose import *
from sklearn.preprocessing import *
from sklearn.pipeline import *
from sklearn.model_selection import train_test_split


# Example 1: LabelEncoder vs SqlLabelEncoder

le1 = LabelEncoder()
sql_le1 = SqlLabelEncoder()

print(f"Example 1: \n \t LabelEncoder is the same to SqlLabelEncoder, the test results is : {sql_le1.equal_to_sklean(le1)}")

# Example 2: SimpleImputer vs SqlSimpleImputer

si2 = SimpleImputer(strategy='mean')
sql_si2 = SqlSimpleImputer(strategy='most_frequent')

print(f"Example 2: \n \t SimpleImputer is different in 'strategy' to SqlLabelEncoder, the test results is : {sql_si2.equal_to_sklean(si2)}")

# Example 3: Pipeline vs converted SqlPipeline

preprocessor_df3 = ColumnTransformer(
    transformers=[
        ('Age', SimpleImputer(strategy='mean'), ['Age']),
        ('Pclass', OneHotEncoder(), ['Pclass']),
        ('Sex', OrdinalEncoder(), ['Sex'])
    ]
)

pipeline_df3 = Pipeline(steps=[
    ('preprocessor', preprocessor_df3),
    ('classifier', DecisionTreeClassifier())
    ])

pipeline_converter3 = SqlPipelineConverter(pipeline_df3)
converted_pipeline_df3 = pipeline_converter3.get_sql_pipeline()

print(f"Example 3: \n \t Pipeline is the same with converted SqlPipeline, the test results is : {converted_pipeline_df3.equal_to_sklean(pipeline_df3)}")

# Example 4: Pipeline (nested) vs converted SqlPipeline

categorical_attributes4 = ['month','day']
numerical_attributes4 = ['x','y','ffmc','dmc','dc','isi','temp','rh','wind','rain']

num_pipeline4 = Pipeline([
    ('imputer', SimpleImputer(strategy='mean')),
    ('scaler', StandardScaler()),
])

cat_pipeline4 = Pipeline([
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('encoder', OneHotEncoder()),
])

preprocessing_pipeline_df4 = ColumnTransformer([
    ('num', num_pipeline4, numerical_attributes4),
    ('cat', cat_pipeline4, categorical_attributes4)
])

pipeline_df4 = Pipeline(steps=[
    ('preprocessor', preprocessing_pipeline_df4),
    ('regressor', LinearRegression())
    ])

pipeline_converter4 = SqlPipelineConverter(pipeline_df4)
converted_pipeline_df4 = pipeline_converter4.get_sql_pipeline()

print(f"Example 4: \n \t Pipeline is the same with converted SqlPipeline, the test results is : {converted_pipeline_df4.equal_to_sklean(pipeline_df4)}")

# Example 5: Pipeline vs SqlPipeline

preprocessor_sdf5 = SqlColumnTransformer(
    transformers=[
        ('month', SqlOrdinalEncoder(), 'month'),
        ('day', SqlOrdinalEncoder(), 'day'),
        ('ffmc', SqlStandardScaler(), 'ffmc'),
        ('dmc', SqlMinMaxScaler(), 'dmc'),
        ('isi', SqlMaxAbsScaler(), 'isi'),
        ('temp', SqlStandardScaler(), 'temp'),
        ('rain', SqlBinarizer(threshold=0), 'rain'),
        ('rh', SqlStandardScaler(), 'rh'),
    ]
)

pipeline_sdf5 = SqlPipeline(steps=[
    ('preprocessor', preprocessor_sdf5),
    ('regressor', DecisionTreeRegressor())
    ])

preprocessor_df5 = ColumnTransformer(
    transformers=[
        ('month', OrdinalEncoder(), ['month']),
        ('day', OrdinalEncoder(), ['day']),
        ('ffmc', StandardScaler(), ['ffmc']),
        ('dmc', StandardScaler(), ['dmc']), # difference
        ('isi', MaxAbsScaler(), ['isi']),
        ('temp', StandardScaler(), ['temp']),
        ('rain', Binarizer(threshold=0), ['rain']),
        ('rh', StandardScaler(), ['rh'])
    ]
)

pipeline_df5 = Pipeline(steps=[
    ('preprocessor', preprocessor_df5),
    ('regressor', LinearRegression())
    ])

print(f"Example 5: \n \t Pipeline is different with SqlPipeline, the test results is : {pipeline_sdf5.equal_to_sklean(pipeline_df5)}")