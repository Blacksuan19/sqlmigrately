from dataclasses import dataclass, field
from enum import Enum
from string import Template
from typing import Any, Dict, List

import pandas as pd
from loguru import logger
from sqlalchemy import Engine, inspect, text

from sqlmigrately.exceptions import TableDoesNotExistError, UnknownOperationError
from sqlmigrately.types import map_type


class TableOps(str, Enum):
    ADD = "ADD"
    REMOVE = "REMOVE"


@dataclass
class ColumnDiff:
    """class to hold the difference between two sets of columns"""

    added: List[Dict[str, Any]] = field(default_factory=list)
    removed: List[Dict[str, Any]] = field(default_factory=list)


def check_table(table_name: str, db_eng: Engine) -> bool:
    """check if a table with a given name exists"""
    return inspect(db_eng).has_table(table_name)


def get_table_schema(table_name: str, db_eng: Engine) -> Dict[str, str]:
    """get mapping of table columns and their types"""

    if not check_table(table_name, db_eng):
        raise TableDoesNotExistError(table_name)

    cols = inspect(db_eng).get_columns(table_name)

    return {col.get("name"): col.get("type") for col in cols}


def get_schema_diff(table_name: str, df: pd.DataFrame, db_eng: Engine) -> ColumnDiff:
    """get the difference between the dataframe and the table schema"""
    table_cols = get_table_schema(table_name, db_eng)
    df_cols = set(df.columns)

    table_col_names = set(table_cols.keys())

    return ColumnDiff(
        added=[
            {"name": col, "type": df[col].dtype} for col in df_cols - table_col_names
        ],
        removed=[
            {"name": col, "type": table_cols[col]} for col in table_col_names - df_cols
        ],
    )


def alter_table(
    table_name: str,
    columns: List[Dict[str, str]],
    db_eng: Engine,
    *,
    operation: TableOps,
    column_type_map: Dict[str, str] = None,
):
    """
    alter table schema by adding or removing columns based on the operation,
    this works on a single column at a time for compatibility with sqlite.

    Args:
        table_name (str): name of the table
        columns (List[Dict[str, str]]): list of columns to add
        db_eng (Engine): sqlalchemy engine
        operation (TableOps): operation to perform
        column_type_map (Dict[str, str], optional): mapping of column names to their types. Defaults to None.

    Raises:
        UnknownOperationError: raised when an unknown operation is provided
    """

    sql_template = Template(f"ALTER TABLE {table_name} $operation $column")

    if operation == TableOps.ADD:
        sql_template = sql_template.safe_substitute(
            operation="ADD COLUMN", column="$column $column_type"
        )
    elif operation == TableOps.REMOVE:
        sql_template = sql_template.safe_substitute(operation="DROP COLUMN")
    else:
        raise UnknownOperationError(operation)

    for col in columns:
        col_type = map_type(col.get("type"))
        if column_type_map and col.get("name") in column_type_map:
            col_type = column_type_map[col.get("name")]

        sql = Template(sql_template).safe_substitute(
            column=col.get("name"), column_type=str(col_type)
        )

        logger.info(f"Executing: {sql}")

        with db_eng.connect().begin() as conn:
            conn.execute(text(sql))
