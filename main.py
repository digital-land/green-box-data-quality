from core import QueryRunner, config_parser
from expectations import *

# Reads-in config yaml
    data_quality_suite_config = config_parser("path/to/config.yaml")

# Initialise QueryRunner with dataset path
    dataset_to_check = "data/set/path"
    query_runner = QueryRunner(dataset_to_check)  

# Run expectations according to configuration yaml 

#   For each expectation chekced: 
#       - treat responses and severity according to use-case 
#       - output in a compatible way with pipelines issue logs
