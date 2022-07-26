from .sp import *
from collections import defaultdict


class SpComparator():
    """ Compare if two object (from sklearn or SqlPreprocessing) is the same

        Parameters:
        --------------
        object1: instance of sklearn.preprocessing/sklearn.Pipeline or SqlPreprocessing
        object2: instance of sklearn.preprocessing/sklearn.Pipeline or SqlPreprocessing
    """


    comparable_type_dict = {
            "SqlMinMaxScaler": "MinMaxScaler",
            "SqlMaxAbsScaler": "MaxAbsScaler",
            "SqlStandardScaler": "StandardScaler",
            "SqlBinarizer": "Binarizer",
            "SqlLabelEncoder": "LabelEncoder",
            "SqlOrdinalEncoder": "OrdinalEncoder",
            "SqlOneHotEncoder": "OneHotEncoder",
            "SqlLabelBinarizer": "LabelBinarizer",
            "SqlNormalizerr": "Normalizer",
            "SqlKernelCenterer": "KernelCenterer",        
            "SqlSimpleImputer": "SimpleImputer",
            "SqlPipeline": "Pipeline"
    }


    def __init__(self, sql_instance, sklearn_instance):
        self.sql_object = sql_instance
        self.sklearn_object = sklearn_instance


    def is_equal(self):
        """ Compare the sql_object and sklearn_object is the same
            return True if they are the same, else return False
        """
        sql_type = self.sql_object.__class__.__name__
        sklearn_type = self.sklearn_object.__class__.__name__

        if not self.comparable_type(sql_type, sklearn_type):
            return False
        
        if not self.same_arguments(self.sql_object, self.sklearn_object):
            return False
        
        return True
    

    def comparable_type(self, sql_type, sklearn_type):
        """ Compare if the type of the two objects is comparable
        """
        if self.comparable_type_dict[sql_type] == sklearn_type:
            return True
        else:
            return False


    def same_arguments(self, sql_object, sklearn_object):
        """ Compare if the arguments are the same for the two objects
        """

        if sql_object.__class__.__name__ == "SqlPipeline":
            return self.compare_pipeline(sql_object, sklearn_object)
        else:
            sql_dict = sql_object.__dict__
            sklearn_dict = sklearn_object.__dict__

            return self.compare_dict(sql_dict, sklearn_dict)

    
    def compare_pipeline(self, sql_object, sklearn_object):
        sqlPipeline_steps = self.get_sqlPipeline_steps(sql_object)
        pipeline_steps = self.get_pipeline_steps(sklearn_object)

        if self.is_same_steps(pipeline_steps, sqlPipeline_steps):
            return True
        else:
            return False


    def is_same_steps(self, pipeline_steps, sqlPipeline_steps):

        for column, steps in pipeline_steps.items():
            sql_steps = sqlPipeline_steps[column]
            for i in range(len(steps)):
                sql_transformer, _ = sql_steps[i]
                sklearn_transformer, _ = steps[i]
                if sql_transformer != sklearn_transformer:
                    return False
        
        return True



    def get_pipeline_steps(self, pipeline):
        column_steps = defaultdict(list)

        steps = pipeline.named_steps
        for step in steps:
            step_type = pipeline.named_steps.get(step).__class__.__name__
            if step_type == 'ColumnTransformer':
                transformers = steps.get(step).transformers
                for transformer in transformers:
                    name, functions, columns = transformer
                    if isinstance(functions, sklearn.pipeline.Pipeline):
                        nested_steps = functions.named_steps
                        for nested_step in nested_steps:
                            nested_function = nested_steps.get(nested_step)
                            if len(columns) > 1:
                                for column in columns:
                                    column_steps[column].append((nested_function.__class__.__name__, name))
                            else:
                                column_steps[columns].append(nested_function.__class__.__name__, name)
                    else:
                        if len(columns) > 1:
                            for column in columns:
                                column_steps[column].append((functions.__class__.__name__, name))
                        else:
                            column_steps[columns[0]].append((functions.__class__.__name__, name))
            else:
                # print(f"step is {step}")
                column_steps['regressor'].append((step_type, step))

        # print(f"sklearn pipeline steps dict : \n {column_steps}")
        
        return column_steps


    def get_sqlPipeline_steps(self, SqlPipeline):
        column_steps = defaultdict(list)
        steps = SqlPipeline.steps
        for step in steps:
            name, function = step
            function_name = function.__class__.__name__
            if isinstance(function, SqlColumnTransformer):
                transformers = function.transformers
                for transformer in transformers:
                    _, sql_function, column = transformer
                    sql_function_name = sql_function.__class__.__name__
                    sklearn_function_name = self.comparable_type_dict[sql_function_name]
                    column_steps[column].append((sklearn_function_name, name))
            else:
                # print(f"not SqlColumnTransformer {function_name} {name}")
                column_steps['regressor'].append((function_name, name))

        # print(f"Sql pipeline steps dict : \n {column_steps}")

        return column_steps

    
    def compare_dict(self, sql_dict, sklearn_dict):
        for sql_key in sql_dict.keys():
            if sql_key in sklearn_dict.keys() and sklearn_dict[sql_key] != sql_dict[sql_key]:
                return False

        return True
