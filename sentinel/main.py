import sys

from typer import Option, Typer

from sentinel.config.logging_config import logger
from sentinel.data_quality.validator import executor
from sentinel.exception.ValidationError import ValidationError
from sentinel.utils.utils import add_params_to_table

app = Typer()


@app.command()
def main(
    jsonpath: str = Option(..., help='Path to the configuration JSON file'),
    source_table_name: str = Option(
        ..., help='The name of the table that will have the validated data'
    ),
    target_table_name: str = Option(
        ...,
        help='The name of the table where the validation results will be saved.',
    ),
):
    try:
        add_params_to_table(
            main,
            jsonpath=jsonpath,
            table_name=source_table_name,
            target_table_name=target_table_name,
        )
        executor(jsonpath, source_table_name, target_table_name)
        sys.exit(0)
    except ValidationError as e:
        logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.error(
            f'An error occurred during execution! Please consult table {target_table_name} for more information.'
        )
        logger.error(e)
        sys.exit(1)
