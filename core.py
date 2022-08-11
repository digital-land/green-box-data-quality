import spatialite
import pandas as pd
import yaml
from dataclasses import dataclass, field
from datetime import datetime

def transform_df_first_column_into_set(dataframe:pd.DataFrame) -> set:
    "Given a pd dataframe returns the first column as a python set"
    return set(dataframe.iloc[:,0].unique())

def config_parser(filepath:str):
    "Will parse a config file"
    with open(filepath) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        config = dict(config)
    return config

class QueryRunner():
    "Class to run queries usings spatialite"

    def __init__(self, tested_dataset_path: str):
        "Receives a path/name of sqlite dataset against which it will run the queries"
        self.tested_dataset_path = tested_dataset_path
    
    def inform_dataset_path(self):
        return self.tested_dataset_path

    def run_query(self, sql_query: str, return_only_first_col_as_set: bool = False):
        """
        Receives a sql query and returns the results either in a pandas 
        dataframe or just the first column as a set (this is useful to
        test presence or absence of items like tables, columns, etc).

        Note: connection is openned and closed at each query, but for use 
        cases like the present one that would not offer big benefits and
        would mean having to dev thread-lcoal connection pools. For more 
        info see: https://stackoverflow.com/a/14520670
        """
        with spatialite.connect(self.tested_dataset_path) as con:
            cursor = con.execute(sql_query)
            cols = [column[0] for column in cursor.description]
            results = pd.DataFrame.from_records(data = cursor.fetchall(), columns = cols)

        if return_only_first_col_as_set:
            return transform_df_first_column_into_set(results)
        else:
            return results            
 
@dataclass
class ExpectationResponse():
    """Class to keep inputs and results of expectations"""
    expectation_input: dict
    result: bool = None
    msg: str = None    
    details: dict = None
    sqlite_dataset: str = None
    data_quality_execution_time: str = field(init=False)
    collection_name: str = field(init=False)
    

    def __post_init__(self):
        
        self.expectation_input.pop('query_runner')

        data_quality_execution_time = getattr(globals(), 'data_quality_execution_time', None)        
        if data_quality_execution_time:
            self.data_quality_execution_time = data_quality_execution_time
        else:
            now = datetime.now()
            self.data_quality_execution_time = now.strftime("%d/%m/%Y %H:%M:%S")
        
        data_quality_suite_config = getattr(globals(), 'data_quality_suite_config', None)
        
        if data_quality_suite_config:
            self.collection_name = data_quality_suite_config.get('collection_name', "Collection name not found")
        else:
            self.collection_name = "Collection name not found"
