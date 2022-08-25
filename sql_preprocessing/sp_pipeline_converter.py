from numpy import nested_iters
from sklearn.compose import ColumnTransformer
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


    def _convert_function(self, sklearn_function, sdf=None, column=None):
        
        sklearn_type = type(sklearn_function)
        sql_function = None


        if (sklearn_type is sp.MinMaxScaler):
            if hasattr(sklearn_function, 'data_max_'):
                sql_function = SqlMinMaxScaler().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlMinMaxScaler()
        if (sklearn_type is sp.MaxAbsScaler): 
            if hasattr(sklearn_function, 'max_abs_'):
                sql_function = SqlMaxAbsScaler().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlMaxAbsScaler()
        if (sklearn_type is sp.Binarizer): 
            if hasattr(sklearn_function, 'threshold'):
                sql_function = SqlBinarizer().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlBinarizer()
        if (sklearn_type is sp.StandardScaler): 
            if hasattr(sklearn_function, 'mean_'):
                sql_function = SqlStandardScaler().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlStandardScaler()
        if (sklearn_type is sp.RobustScaler): 
            if hasattr(sklearn_function, 'center_'):
                sql_function = SqlRobustScaler().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlRobustScaler()
        if (sklearn_type is sp.LabelEncoder): sql_function = SqlLabelEncoder() # TODO, need to know 'sdf' and 'column' to use load_from_sklearn
        if (sklearn_type is sp.OrdinalEncoder): sql_function = SqlOrdinalEncoder() # TODO, need to know 'sdf' and 'column' to use load_from_sklearn
        if (sklearn_type is sp.OneHotEncoder):
            if hasattr(sklearn_function, 'categories_'):
                sql_function = SqlOneHotEncoder().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlOneHotEncoder()
        if (sklearn_type is sp.LabelBinarizer): 
            if hasattr(sklearn_function, 'classes_'):
                sql_function = SqlLabelBinarizer().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlLabelBinarizer()
        if (sklearn_type is sp.Normalizer): 
            if hasattr(sklearn_function, 'norm'):
                sql_function = SqlNormalizer().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlNormalizer()
        if (sklearn_type is sp.KernelCenterer): 
            if hasattr(sklearn_function, 'K_fit_all_'):
                sql_function = SqlKernelCenterer().load_from_sklearn(sklearn_function, sdf, column)
            else:
                sql_function = SqlKernelCenterer()
        if (sklearn_type is sp.KBinsDiscretizer): sql_function = SqlKBinsDiscretizer()
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

        unsupported_functions = self.eligible_to_transform_sqlPipeline(supported_functions)
        hybrid_pipeline = False
        if len(unsupported_functions) > 0:
            print(f"The pipeline contains currently unsupported functions {unsupported_functions}, it will transform into hybrid pipeline.")
            hybrid_pipeline = True

        sql_steps, sklearn_steps = self._get_pipeline_steps(supported_functions)

        if hybrid_pipeline:
            converted_sql_pipeline = SqlPipeline(sql_steps, sklearn_steps=sklearn_steps)
        else:
            converted_sql_pipeline = SqlPipeline(sql_steps)

        self.sql_pipeline = converted_sql_pipeline

        return converted_sql_pipeline


    def eligible_to_transform_sqlPipeline(self, supported_functions):
        """ Test if the sklearn pipeline can be converted into SqlPipeline
           
           Returns: bool
           should contains more information

            Raises
            ---------
            TypeError:
                when self.sklearn_pipeline is not a valid instance of sklearn.pipeline.Pipeline
            
            Returns
            ---------
            unsupported_functions: list
                A list of the names of the unsupported functions in the pipeline
        """

        if not isinstance(self.sklearn_pipeline, Pipeline):
            raise TypeError('The pipeline is not a valid sklearn.pipeline.Pipeline')

        original_steps_dict = self._pipeline_steps_dict(self.sklearn_pipeline)
        unsupported_functions = []

        for step_name in original_steps_dict.keys():
            if step_name != "regressor":
                for function in original_steps_dict[step_name]:
                    if function not in supported_functions:
                        unsupported_functions.append(function)

        return unsupported_functions


    def _get_pipeline_steps(self, supported_functions):

        original_steps = self.sklearn_pipeline.named_steps
        sql_steps = []
        sklearn_steps = []

        for original_step in original_steps:
            if self.sklearn_pipeline.named_steps.get(original_step).__class__.__name__=='ColumnTransformer':
                # step is preprocessing transformer for target columns
                sklearn_transformers = self.sklearn_pipeline.named_steps[original_step].named_transformers_
                ori_sklearn_transformers = self.sklearn_pipeline.named_steps.get(original_step).transformers
                unnested_sql_transformers = []
                unnested_sklearn_transformers = []
                step_count = 1
                for ori_sklearn_transformer in ori_sklearn_transformers:
                    # print(f"~~~~~~~~~~~~~~{ori_sklearn_transformer}")
                    name, _, columns = ori_sklearn_transformer
                    original_functions = sklearn_transformers[name]
                    step_suffix = 1
                    if isinstance(original_functions, sklearn.pipeline.Pipeline):
                        # TODO: nested pipeline not work to get fitting parameters for now
                        # Nested fitted functions will have an array of parameters for each column, so unable to use load_from_sklearn
                        ori_nested_steps = ori_sklearn_transformer[1].steps
                        nested_steps = original_functions.named_steps
                        self._handle_nested_steps(nested_steps, name, columns, step_count, sql_steps, sklearn_steps, step_suffix, supported_functions, ori_sklearn_transformer, sklearn_transformers)
                        
                    else:
                        # This column transformer is not nested, every transformer should be aggregated into one transformer
                        self._handle_unnested_steps(unnested_sql_transformers, unnested_sklearn_transformers, supported_functions, ori_sklearn_transformer, original_functions)

                add_step = False
                if len(unnested_sql_transformers) > 0:
                    step_name = 'step_' + str(step_count)
                    sql_steps.append((step_name, SqlColumnTransformer(unnested_sql_transformers)))
                    add_step = True
                if len(unnested_sklearn_transformers) > 0:
                    step_name = 'step_' + str(step_count)
                    sklearn_steps.append((step_name, ColumnTransformer(unnested_sklearn_transformers)))
                    add_step = True
                if add_step:
                    step_count += 1
            else:
                # step should be regressor or classifier
                if len(sklearn_steps) > 0:
                    sklearn_steps.append((original_step, self.sklearn_pipeline.named_steps.get(original_step)))
                else:
                    sql_steps.append((original_step, self.sklearn_pipeline.named_steps.get(original_step)))

        return sql_steps, sklearn_steps


    def _handle_nested_steps(self, nested_steps, name, columns, step_count, sql_steps, sklearn_steps, step_suffix, supported_functions, ori_sklearn_transformer, sklearn_transformers):

        for nested_name in nested_steps:
            sql_transformers = []
            sql_nested_transformers = []
            sklearn_transformers = []
            sklearn_nested_transformers = []
            nested_function = nested_steps[nested_name]

            if nested_function.__class__.__name__ in supported_functions:
                sql_function = self._convert_function(nested_function)
            else:
                sklearn_function = nested_function
            suffix = 1
            if len(columns) > 1:
                for column in columns:
                    # sqlfunctions only support one column currently
                    new_name = name + "_" + nested_name + "_" + str(suffix)
                    suffix += 1
                    if nested_function.__class__.__name__ in supported_functions:
                        sql_nested_transformers.append((new_name, sql_function, column))
                    else:
                        sql_nested_transformers.append((new_name, SqlPassthroughColumn(), column))
                        sklearn_nested_transformers.append((new_name, sklearn_function, column))
                # print(sql_nested_transformers)
                step_name = name + "_" + str(step_suffix)
                if len(sql_nested_transformers) > 0:
                    sql_steps.append((step_name, SqlColumnTransformer(sql_nested_transformers)))
                if len(sklearn_nested_transformers) > 0:
                    sklearn_steps.append((step_name, ColumnTransformer(sklearn_nested_transformers)))
                step_count += 1
            else:
                step_name = name + "_" + str(step_suffix)
                if nested_function.__class__.__name__ in supported_functions:
                    sql_transformers.append((step_name, SqlColumnTransformer([(nested_name, sql_function, column)])))
                    sql_steps.append(sql_transformers)
                else:
                    sklearn_transformers.append((step_name, ColumnTransformer([(nested_name, sklearn_function, column)])))
                    sklearn_steps.append(sklearn_transformers)
                step_count += 1
            step_suffix += 1


    def _handle_unnested_steps(self, unnested_sql_transformers, unnested_sklearn_transformers, supported_functions, sklearn_transformer, original_functions):
        name, _, columns = sklearn_transformer

        if original_functions.__class__.__name__ in supported_functions:
            sql_function = self._convert_function(original_functions)
        else:
            sklearn_function = original_functions
        step_suffix = 1
        # print(sql_function)
        if len(columns) > 1:
            suffix = 1
            for column in columns:
                # sqlfunctions only support one column currently
                new_name = name + "_" + str(suffix)
                suffix += 1
                if original_functions.__class__.__name__ in supported_functions:
                    unnested_sql_transformers.append((new_name, sql_function, column))
                else:
                    unnested_sql_transformers.aplpend((new_name, SqlPassthroughColumn(), column))
                    unnested_sklearn_transformers.append((new_name, sklearn_function, column))
        else:
            if original_functions.__class__.__name__ in supported_functions:
                unnested_sql_transformers.append((name, sql_function, columns[0]))
            else:
                unnested_sql_transformers.append((name, SqlPassthroughColumn(), columns[0]))
                unnested_sklearn_transformers.append((name, sklearn_function, columns[0]))


    def _pipeline_steps_dict(self, sklearn_object):
        """Covert the pipeline steps into dictionary, used to check if there are unsupported preprocessing functions

            Returns:
            ----------
            column_steps_dict: dict
                a dictionary with (column_name, [preprocessing_functions]) as the the (key, value) pairs
        """
        column_steps_dict = defaultdict(list)

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
                                    column_steps_dict[column].append(nested_function.__class__.__name__)
                            else:
                                column_steps_dict[columns].append(nested_function.__class__.__name__)
                    else:
                        if len(columns) > 1:
                            for column in columns:
                                column_steps_dict[column].append(functions.__class__.__name__)
                        else:
                            column_steps_dict[columns[0]].append(functions.__class__.__name__)
            else:
                # print(f"step is {step}")
                column_steps_dict['regressor'].append(step_type)

        # print(f"sklearn pipeline steps dict : \n {column_steps_dict}")
        
        return column_steps_dict


    def get_sql_pipeline(self):
        """Pipeline Conversion"""
        if self.sql_pipeline == None:
            self.sql_pipeline = self.convert()
        
        return self.sql_pipeline


    def display_sql_pipeline(self):
        if self.sql_pipeline is None:
            raise ValueError("Sql Pipeline is not converted")
        
        steps, sklearn_steps = self.sql_pipeline.steps, self.sql_pipeline.sklearn_steps
        set_config(display='diagram')
        if len(sklearn_steps) > 0:
            dummpyPipeline1 = Pipeline(steps=steps)
            dummpyPipeline2 = Pipeline(steps=sklearn_steps)
            print(f"hybrid pipeline ----- SqlPipeline (1 of 2): \n")
            display(dummpyPipeline1)
            print(f"hybrid pipeline ----- sklearn pipeline (2 of 2): \n")
            display(dummpyPipeline2)
        else:
            dummpyPipeline1 = Pipeline(steps)
            display(dummpyPipeline1)


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
                print(f'{space*current_indent}])')
            else:
                print(f'{space*current_indent}{step[1]}')
            current_indent = nested_indent
        print('],')
        print('    sklearn_steps=[')
        current_indent = nested_indent
        for step in self.sql_pipeline.sklearn_steps:
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
        print('])')

# end of class SqlPipelineConverter

