from functools import partial

import pandas as pd
from loguru import logger
from sqlalchemy import Engine

from sqlmigrately.utils import TableOps, alter_table, get_schema_diff


def migrate_table(
    table_name: str,
    df: pd.DataFrame,
    db_eng: Engine,
    *,
    push_data: bool = True,
    add_cols: bool = True,
    remove_cols: bool = False,
    column_type_map: dict = None,
):
    """
    Update given `table_name` schema in the database to match the schema of the given `df`.
    Assumes minimal changes to the table schema.

    Args:
        table_name (str): name of the table
        df (pd.DataFrame): dataframe to migrate
        db_eng (Engine): sqlalchemy engine
        push_data (bool, optional): whether to push dataframe data to the table. Defaults to True.
        add_cols (bool, optional): whether to add new columns in dataframe to the table. Defaults to True.
        remove_cols (bool, optional): whether to remove removed columns from the table. Defaults to False.
        column_type_map (dict, optional): mapping of column names to their types. Defaults to None, which means that the types are inferred from the dataframe.

    Raises:
        TableDoesNotExistError: raised when the given table does not exist in the database
    """
    diff = get_schema_diff(table_name, df, db_eng)
    partial_alter_table = partial(
        alter_table,
        table_name=table_name,
        db_eng=db_eng,
        column_type_map=column_type_map,
    )

    if diff.added:
        logger.info(f"Detected new columns: {diff.added}")
        if add_cols:
            partial_alter_table(columns=diff.added, operation=TableOps.ADD)

    if diff.removed:
        logger.info(f"Detected removed columns: {diff.removed}")
        if remove_cols:
            partial_alter_table(columns=diff.removed, operation=TableOps.REMOVE)

    if push_data:
        logger.info(f"Appending data to table: {table_name}")
        df.to_sql(
            table_name,
            db_eng,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
