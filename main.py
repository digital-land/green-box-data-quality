from core import QueryRunner, config_parser
from expectations import *

config_path = "/src/sharing_area/green-box-data-quality/unit_tests/testing_config_dq_suite_v2.yaml"

# Reads-in config yaml
data_quality_suite_config = config_parser(config_path)

# Initialise QueryRunner with dataset path
dataset_to_check = data_quality_suite_config["dataset_path_name"]
query_runner = QueryRunner(dataset_to_check)  


# Run expectations according to configuration yaml 
# Dataset level expectation
expected_tables = {data_quality_suite_config["tables"][i]['tb_name'] for i in range(0,len(data_quality_suite_config["tables"]))}
check_for_expected_tables= CheckDatabaseHasExpectedTables(query_runner = query_runner) 
response = check_for_expected_tables.check(expected_tables,fail_if_found_more_than_expected = False)

# FOR DEMO
print(f"response.result = {response.result}")
print(f"response.msg = {response.msg}")
print(f"response.details = {response.details}")
print(f"response.expectation_input = {response.expectation_input}")



#   For each expectation checked: 
#       - treat responses and severity according to use-case 
#       - output in a compatible way with pipelines issue logs

# TEST entity numbers are within RANGE

# check all entity table columns + json element should be == to datset in spec (many won't have all of dataset, 
# should not find fields in the json that are not in the dataset spec)

# check inside a json stored as a field if any fields in the json field of the entity table are not listed in the dataset fields

# check value inside nested field is in one of the sets

# CLI