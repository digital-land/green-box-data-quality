import spatialite
import pandas as pd
import yaml
from dataclasses import dataclass

def transform_df_first_column_into_set(dataframe:pd.DataFrame) -> set:
    "Given a pd dataframe returns the first column as a python set"
    return set(dataframe.iloc[:,0].unique())

def config_parser(filepath:str):
    "Will parse a config file"
    with open(filepath) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config

class QueryRunner():
    "Class to run queries usings spatialite"

    def __init__(self, tested_dataset_path: str):
        "Receives a path/name of sqlite dataset against which it will run the queries"
        self.tested_dataset_path = tested_dataset_path
    
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
 
class Expectation():
    """Parent class for expectations, all expectations receive a query runner 
    to be able to run queries against the db"""    
    def __init__(self, query_runner: QueryRunner):
        self.query_runner = query_runner 

@dataclass
class ExpectationResponse():
    """Class to keep inputs and results of expectations"""
    expectation_input: dict
    result: bool = None
    msg: str = None    
    details: dict = None