from core import QueryRunner, config_parser, DataQualityException
from expectations import *
from math import inf
from datetime import datetime
import click


@click.command()
@click.option("--results-path", help="path to save json results", required=True)
@click.option("--sqlite-dataset-path", help="path to sqlite3 dataset", required=True)
@click.option("--data-quality-yaml", help="path to expectations yaml", required=True)
def run_dq_suite(results_path, sqlite_dataset_path, data_quality_yaml):

    now = datetime.now()
    data_quality_execution_time = now.strftime("%Y%m%d_%H%M%S")

    data_quality_suite_config = config_parser(data_quality_yaml)

    query_runner = QueryRunner(sqlite_dataset_path)

    expectations = data_quality_suite_config.get("expectations", None)

    failed_expectation_with_error_severity = 0

    for expectation in expectations:

        arguments = {**expectation}

        response = run_expectation(
            query_runner=query_runner,
            data_quality_execution_time=data_quality_execution_time,
            **arguments
        )

        response.save_to_file(results_path)
        failed_expectation_with_error_severity += response.act_on_failure()

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation_name: str, **kwargs):
    return globals()[expectation_name](query_runner=query_runner, **kwargs)


if __name__ == "__main__":
    run_dq_suite()
