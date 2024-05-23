from rich.console import Console
from rich.table import Table
from typer import Argument, Context, Exit, Option, Typer

from sentinel.config.spark_context import get_spark_context
from sentinel.data_quality.validator import (
    create_expectation_suite,
    validate_data,
)
from sentinel.utils.utils import load_files

console = Console()
app = Typer()


@app.command()
def main(
    jsonpath: str = Argument(
        'E:\\sentinel\\tests\\resources\\expectations.json',
        help='Path do json de configuracao',
    ),
    param2: str = Argument('maior', help='Tonalidade da escala'),
):
    table = Table()

    table.add_column('jsonpath')
    table.add_column('param2')
    table.add_row(*[jsonpath, param2])

    console.print(table)

    spark = get_spark_context()
    expectations = load_files(jsonpath, spark)['expectations']
    expectation_suite = create_expectation_suite(expectations)

    data = [
        ('Alice', 30, 'single'),
        ('Bob', 45, 'married'),
        ('Charlie', None, 'divorced'),
    ]

    columns = ['name', 'age', 'status']
    table_name = 'mock_table'
    spark_df = spark.createDataFrame(data, columns)

    result_df, success = validate_data(
        spark, spark_df, expectation_suite, table_name
    )

    result_df.show(truncate=False)
