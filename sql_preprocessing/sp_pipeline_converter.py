from numpy import nested_iters
from .sp import *
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn import set_config

class SqlPipelineConverter():
    """Convert sklearn.pipeline.Pipeline into SqlNestedPipeline

    Parameters
    ----------
        sklearn_pipeline: sklearn.pipeline.Pipeline
            The sklearn pipeline object to be converted
        sql_pipeline: SqlPipeline
            The converted SqlPipeline object
    """

    def __init__(self, sklearn_pipeline=None, sql_pipeline=None):
        self.sklearn_pipeline=sklearn_pipeline
        self.sql_pipeline=None


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
        """ Convert self.sklearn_pipeline into SqlPipeline object
            The steps for each column is transformed automatically

        Returns
        ---------
            SqlPipeline:
                An instance of :class: `SqlPipeline` with the same steps in the original sklearn pipeline
        """
        if not self.eligible_to_transform_sqlPipeline():
            print("The pipeline contains unsupported functions {}")
            return None

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


    def eligible_to_transform_sqlPipeline(self):
        """ Test if the sklearn pipeline can be converted into SqlPipeline
           
           Returns: bool
           should contains more information

            Raises
            ---------
            TypeError:
                when self.sklearn_pipeline is not a valid instance of sklearn.pipeline.Pipeline
            ValueError:
                when the transformer functions in sklearn pipeline is not supported in SqlPipeline
        """

        if not isinstance(self.sklearn_pipeline, Pipeline):
            raise TypeError('The pipeline is not a valid sklearn.pipeline.Pipeline')

        original_steps = self._get_pipeline_steps(self.sklearn_pipeline)

        supported_functions = [
            "MinMaxScaler",
            "MaxAbsScaler",
            "StandardScaler",
            "RobustScaler",
            "Binarizer",
            "LabelEncoder",
            "OrdinalEncoder",
            "OneHotEncoder",
            "LabelBinarizer",
            "Normalizer",
            "KernelCenterer",        
            "SimpleImputer",
            "Pipeline"
        ]

        unsupported_functions = []

        for step_name in original_steps.keys():
            if step_name != "regressor":
                for function in original_steps[step_name]:
                    if function not in supported_functions:
                        unsupported_functions.append(function)
        
        return unsupported_functions


    def _get_pipeline_steps(self, sklearn_object):
        column_steps = defaultdict(list)

        steps = sklearn_object.named_steps
        for step in steps:
            step_type = sklearn_object.named_steps.get(step).__class__.__name__
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


    def output_sklearn_pipeline(self):
        print(self.sklearn_pipeline)


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

# end of class SqlPipelineConverter

