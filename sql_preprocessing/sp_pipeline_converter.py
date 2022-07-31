from numpy import nested_iters
from .sp import *
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn import set_config

class SqlPipelineConverter():
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

    def _convert_function(self, sklearn_function):
        
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
        if (sklearn_type is SimpleImputer):
            sklearn_strat = sklearn_function.get_params().get('strategy')
            sql_function = SqlSimpleImputer(strategy=sklearn_strat)

        return sql_function

    def convert(self):
        if not isinstance(self.sklearn_pipeline, Pipeline):
            raise TypeError('The pipeline is not a valid sklearn.pipeline.Pipeline')

        sql_steps = []

        sklearn_steps = self.sklearn_pipeline.named_steps
        
        for sklearn_step in sklearn_steps:
            if self.sklearn_pipeline.named_steps.get(sklearn_step).__class__.__name__=='ColumnTransformer':
                # step is preprocessing transformer for target columns
                sklearn_transformers = self.sklearn_pipeline.named_steps.get(sklearn_step).transformers
                # (name, transformer, columns)
                unnested_sql_transformers = []
                step_count = 1
                for sklearn_transformer in sklearn_transformers:
                    name, sklearn_functions, columns = sklearn_transformer
                    step_suffix = 1
                    if isinstance(sklearn_functions, sklearn.pipeline.Pipeline):
                        #nested pipeline
                        # print(sklearn_functions)
                        # print(sklearn_functions.named_steps)
                        nested_steps = sklearn_functions.named_steps
                        for nested_name in nested_steps:
                            sql_transformers = []
                            sql_nested_transformers = []
                            nested_function = nested_steps.get(nested_name)
                            # print((nested_name, nested_function))
                            sql_function = self._convert_function(nested_function)
                            suffix = 1
                            if len(columns) > 1:
                                for column in columns:
                                    # sqlfunctions only support one column currently
                                    new_name = name + "_" + nested_name + "_" + str(suffix)
                                    suffix += 1
                                    sql_nested_transformers.append((new_name, sql_function, column))
                                # print(sql_nested_transformers)
                                step_name = name + "_" + str(step_suffix)
                                sql_steps.append((step_name, SqlColumnTransformer(sql_nested_transformers)))
                                step_count += 1
                            else:
                                step_name = name + "_" + str(step_suffix)
                                sql_transformers.append((step_name, SqlColumnTransformer([(nested_name, sql_function, column)])))
                                sql_steps.append(sql_transformers)
                                step_count += 1
                            step_suffix += 1
                        
                    else:
                        # This column transformer is not nested, every transformer should be aggregated into one transformer
                        sql_function = self._convert_function(sklearn_functions)
                        step_suffix = 1
                        # print(sql_function)
                        if len(columns) > 1:
                            suffix = 1
                            for column in columns:
                                # sqlfunctions only support one column currently
                                new_name = name + "_" + str(suffix)
                                suffix += 1
                                unnested_sql_transformers.append((new_name, sql_function, column))
                        else:
                            unnested_sql_transformers.append((name, sql_function, columns[0]))
                if len(unnested_sql_transformers) > 1:
                    step_name = 'step_' + str(step_count)
                    sql_steps.append((step_name, SqlColumnTransformer(unnested_sql_transformers)))
                    step_count += 1

            else:
                # step should be regressor or classifier
                sql_steps.append((sklearn_step, self.sklearn_pipeline.named_steps.get(sklearn_step)))

        converted_sql_pipeline = SqlPipeline(sql_steps)

        self.sql_pipeline = converted_sql_pipeline

        return converted_sql_pipeline

    def get_sql_pipeline(self):
        if self.sql_pipeline == None:
            return self.convert()
        else:
            return self.sql_pipeline

    def display_sql_pipeline(self):
        if self.sql_pipeline is None:
            raise ValueError("Sql Pipeline is not converted")
        
        steps = self.sql_pipeline.steps
        dummpyPipeline = Pipeline(steps)
        set_config(display='diagram')
        return dummpyPipeline

    def display_sklearn_pipeline(self):
        set_config(display='diagram')
        return self.sklearn_pipeline

    def output_sql_pipeline(self, nested_indent = 4):
        space = ' '
        print(f'SqlPipeline(steps=[')
        current_indent = nested_indent
        for step in self.sql_pipeline.steps:
            print(f'{space*nested_indent}{step[0]},')
            if isinstance(step[1], SqlColumnTransformer):
                # print(f'{space*indent}{step[1]}')
                print(f'{space*current_indent}SqlColumnTransformer(transformers=[')
                for transformer in step[1].transformers:
                    current_indent += nested_indent
                    print(f'{space*current_indent}({transformer[0]},{transformer[1]},{transformer[2]}),')
                    # print(f'{space*current_indent}]')
                    current_indent -= nested_indent
                    # print(f',\n')
                print(f'{space*current_indent}]')
            else:
                print(f'{space*current_indent}{step[1]}')
        print(']')


        
        

