from sp import *
from sklearn.pipeline import Pipeline

SklearnToSqlConverter()
SqlNestedPipeline()

class SqlPipelineConverter:
    """
    Convert sklearn.Pipeline into SqlNestedPipeline
    """

    def __init__(self, sklearn_pipeline=None, sql_pipeline=None):
        self.sklearn_pipeline=sklearn_pipeline
        self.sql_pipeline=None

    def print_sklearn_pipeline(self):
        print(self.sklearn_pipeline)

    def print_sql_pipeline(self):
        print(self.sql_pipeline)

    def convert_function(sklearn_function):
        
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

        return sql_function

    def convert(self):
        if not isinstance(self, Pipeline):
            raise TypeError('The pipeline is not a valid sklearn.pipeline.Pipeline')

        sql_steps = []

        sklearn_steps = self.sklearn_pipeline.named_steps
        
        for sklearn_step in sklearn_steps:
            if self.sklearn_pipeline.named_steps.get(sklearn_step).__class__.__name__=='ColumnTransformer':
                # step is preprocessing transformer for target columns
                sklearn_transformers = self.sklearn_pipeline.named_steps.get(sklearn_step).transformers
                # (name, transformer, columns)
                for sklearn_transformer in sklearn_transformers:
                    name, sklearn_functions, columns = sklearn_transformer
                    if isinstance(sklearn_functions, sklearn.pipeline.Pipeline):
                        #nested pipeline
                        nested_steps = sklearn_functions.named_steps
                        sql_nested_transformers = []
                        for nested_step in nested_steps:
                            nested_name, nested_function = nested_step
                            sql_function = self.convert_function(nested_function)
                            suffix = 1
                            if len(columns) > 1:
                                for column in columns:
                                    # sqlfunctions only support one column currently
                                    new_name = name + "_" + nested_name + "_" + str(suffix)
                                    suffix += 1
                                    sql_nested_transformers.append((nested_name, sql_function, column))
                                sql_steps.append((name, SqlColumnTransformer(sql_nested_transformers)))
                            else:
                                sql_steps.append((name, SqlColumnTransformer([(nested_name, sql_function, column)])))
                        
                    else:
                        sql_function = self.convert_function(sklearn_functions)
                        sql_transformer = []
                        # print(sql_function)
                        if len(columns) > 1:
                            suffix = 1
                            for column in columns:
                                # sqlfunctions only support one column currently
                                new_name = name + "_" + str(suffix)
                                suffix += 1
                                sql_transformer.append((new_name, sql_function, column))
                        else:
                            sql_transformer.append((name, sql_function, columns[0]))
                    sql_steps.append((name, SqlColumnTransformer(sql_transformer)))

            else:
                # step should be regressor or classifier
                sql_steps.append((sklearn_step, self.sklearn_pipeline.named_steps.get(sklearn_step)))

        converted_sql_pipeline = SqlPipeline(sql_steps)

        return converted_sql_pipeline

    def get_sql_pipeline(self):
        if self.sql_pipeline == None:
            return self.convert(self.sklearn_pipeline)
        else:
            return self.sql_pipeline

        
        

