import sys
from typing import Optional

from typer import Option, Typer

from sentinel.config.logging_config import logger
from sentinel.data_quality.validator import evaluate_all
from sentinel.exception.ValidationError import ValidationError
from sentinel.utils.utils import add_params_to_table

app = Typer()


@app.command()
def main(
    json_path: str = Option(..., help='Path to the configuration JSON file'),
    source_table_name: str = Option(
        ..., help='The name of the table that will have the validated data'
    ),
    target_table_name: str = Option(
        ...,
        help='The name of the table where the validation results will be saved.',
    ),
    process_type: str = Option(
        'batch', help='Type of the process, typically "batch" or "stream"'
    ),
    source_config_name: str = Option(
        'default_config', help='The name of the source configuration to use'
    ),
    checkpoint: str = Option(
        ..., help='Path to the checkpoint directory for Spark'
    ),
    metric_set_name: Optional[str] = Option(None, help='Metrics set name')
):
    try:
        logger.info(
            f'Adding params to table: json_path={json_path}, source_table_name={source_table_name}, '
            f'target_table_name={target_table_name}, process_type={process_type}, '
            f'source_config_name={source_config_name}, checkpoint={checkpoint}'
        )

        if process_type not in ['batch', 'streaming']:
            raise ValidationError(
                "Invalid process type! Expected 'batch' or 'streaming'."
            )

        add_params_to_table(
            main,
            json_path=json_path,
            table_name=source_table_name,
            target_table_name=target_table_name,
            process_type=process_type,
            source_config_name=source_config_name,
            checkpoint=checkpoint,
            metric_set_name=metric_set_name,
        )
        evaluate_all(
            json_path,
            source_table_name,
            target_table_name,
            process_type,
            source_config_name,
            checkpoint=checkpoint,
            metric_set_name=metric_set_name,
        )
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
