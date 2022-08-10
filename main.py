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

response = expect_database_to_have_set_of_tables(query_runner, expected_tables,fail_if_found_more_than_expected = False)

# FOR DEMO
print(f"response.result = {response.result}")
print(f"response.msg = {response.msg}")
print(f"response.details = {response.details}")
print(f"response.expectation_input = {response.expectation_input}")
