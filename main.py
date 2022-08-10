from select import epoll
from numpy import dtype
from core import QueryRunner, config_parser
from expectations import *
from math import inf
import click

@click.command()
@click.option("--collection-name", help="The collection name", required=True)
@click.option("--sqlite-dataset-path", help="The path to the sqlite3 dataset", required=True)
@click.option("--data-quality-suite-yaml", help="The path to the sqlite3 dataset", required=True)
def run_dq_suite(collection_name, sqlite_dataset_path, data_quality_suite_yaml):
    print(f"collection_name: {collection_name}")
    print(f"sqlite_path:{sqlite_dataset_path}")
    print(f"dq_suite:{data_quality_suite_yaml}")

    global query_runner
    query_runner = QueryRunner(sqlite_dataset_path)  

    data_quality_suite_config = config_parser(data_quality_suite_yaml)

    # table level checks    
    table_checks = data_quality_suite_config.get('table_checks', None)

    if table_checks:
        expected_tables_set = {table_checks[i]['tb_name'] for i in range(0,len(data_quality_suite_config["table_checks"]))}

        # check if expected tables are present
        response = expect_database_to_have_set_of_tables(query_runner=query_runner,
                                                expected_tables_set=expected_tables_set)
        # TODO save response if found issues

        # check if row counts for tables are within expected
        tables_dict = {i:table_checks[i]['tb_name'] for i in range(0,len(data_quality_suite_config["table_checks"]))}
        table_row_count_inputs = list()
        for i in tables_dict:
            table_rows_expectation = {
                "tb_name": tables_dict[i],
                "min_row_count":table_checks[i].get('tb_expected_min_row_count',0),
                "max_row_count":table_checks[i].get('tb_expected_max_row_count', inf)}
            table_row_count_inputs.append(table_rows_expectation)
        
        for tb in table_row_count_inputs:   
            response = expect_table_row_count_to_be_in_range(query_runner=query_runner, table_name=tb["tb_name"],
                            min_expected_row_count=tb["min_row_count"], max_expected_row_count=tb["max_row_count"])
            # TODO save response if found issues

    # field existence checks:
    field_existence_checks = data_quality_suite_config.get('field_existence_checks', None)  

    if field_existence_checks:
        for item in field_existence_checks:
            expected_columns_set = set(item["tb_expected_columns"])
            response = expect_table_to_have_set_of_columns(query_runner=query_runner, table_name=item["tb_name"], 
                                                           expected_columns_set=expected_columns_set)
            
            # TODO save response if found issues
     
    # field level checks
    field_level_checks = data_quality_suite_config.get("field_level_checks", None)   
    
    for item in field_level_checks:
        
        for expectation in item.get('expectations', None):
            
            arguments = {                    
                    "table_name": item["table_name"],
                    "field_name": item['field_name'],
                    **expectation}
            
            response = run_expectation(query_runner=query_runner,**arguments)
                
            # TODO save response if found issues
    
    # TODO custom queries expectations


def run_expectation(query_runner: QueryRunner, name:str , **kwargs):
    print(kwargs)    
    return globals()[name](query_runner=query_runner,**kwargs)
   
if __name__ == '__main__':
    run_dq_suite()
